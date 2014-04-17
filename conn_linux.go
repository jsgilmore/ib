//   Copyright 2014 Vastech SA (PTY) LTD
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

// +build linux

package ib

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"time"
)

// RCConn can only be used in one direction (read or write) by a
// single goroutine. Simplicity rules.

const (
	immRendezvousStart = iota + 1
	immRendezvousReply
	immRendezvousFinish
	immClose
)

type RCConn struct {
	iface             *Interface
	laddr             net.Addr
	raddr             net.Addr
	qp                QueuePair
	timeout           int64
	writeTimeoutFatal bool
	messages          []*MemoryRegion
	meta              *MemoryRegion
	keepalive         bool
}

func NewRCConn(iface *Interface, qp QueuePair, timeout int64, keepalive bool) *RCConn {
	messages, meta := CreateBuffers()
	return &RCConn{iface, nil, nil, qp, timeout, true, messages, meta, keepalive}
}

func ioDeadline() time.Time {
	return time.Now().Add(5 * time.Second)
}

func CreateBuffers() (messages []*MemoryRegion, meta *MemoryRegion) {
	// allocate receive buffers
	for i := 0; i < 2; i++ {
		mr, err := AllocateMemory(4096)
		if err != nil {
			panic(fmt.Errorf("AllocateMemory: %v", err))
		}
		messages = append(messages, mr)
	}

	// allocate send buffer
	meta, err := AllocateMemory(64)
	if err != nil {
		panic(fmt.Errorf("AllocateMemory: %v", err))
	}
	return messages, meta
}

func newRCConn(c *net.TCPConn, iface *Interface) (*RCConn, error) {
	// Leave enough room in the completion queue for any operation,
	// including inline sends, to return an error. CQ overruns
	// sometimes cause internal errors in the HCA, which can make the
	// kernel very unhappy.
	qp, err := iface.NewQueuePair(10)
	if err != nil {
		return nil, err
	}

	if err := c.SetDeadline(ioDeadline()); err != nil {
		checkClose(qp)
		return nil, err
	}
	destLid, destQpn, destPsn, err := writeReadQPParams(c, iface.Lid(), qp.Qpn(), qp.Psn())
	if err != nil {
		checkClose(qp)
		return nil, err
	}

	messages, meta := CreateBuffers()

	if err := qp.Setup(destLid, destQpn, destPsn, messages); err != nil {
		checkClose(qp)
		return nil, err
	}

	laddr, raddr := c.LocalAddr(), c.RemoteAddr()

	rcc := &RCConn{iface, laddr, raddr, qp, math.MaxInt64, true, messages, meta, false}
	return rcc, nil
}

func (c *RCConn) SetTimeout(nsec int64) {
	if nsec < 0 {
		panic("RCConn: SetTimeout < 0")
	}
	if nsec > 0 {
		c.timeout = nsec
	} else {
		c.timeout = math.MaxInt64
	}
}

func (c *RCConn) WriteTimeoutFatal(v bool) {
	c.writeTimeoutFatal = v
}

func (c *RCConn) LocalAddr() net.Addr {
	return c.laddr
}

func (c *RCConn) RemoteAddr() net.Addr {
	return c.raddr
}

func (c *RCConn) Setup(destLid uint16, destQpn, destPsn uint32) error {
	return c.qp.Setup(destLid, destQpn, destPsn, c.messages)
}

type rendezvousReply struct {
	remoteAddr uint64
	rkey       uint32
}

type timeout interface {
	Timeout() bool
}

func (this *RCConn) postKeepalive() error {
	// Don't send a keepalive if one is outstanding already. This
	// avoids CQ overruns if the timeout is very small.
	if this.keepalive {
		return nil
	}
	this.keepalive = true
	if err := this.qp.PostKeepalive(); err != nil {
		return fmt.Errorf("keepalive: %v", err)
	}
	return nil
}

func (this *RCConn) poll() (*WorkCompletion, error) {
	wc, err := this.qp.Poll(this.timeout)
	if err != nil {
		if t, ok := err.(timeout); ok && t.Timeout() {
			if err := this.postKeepalive(); err != nil {
				this.closeAfterError(err)
				return nil, err
			}
			// Return without shutting down the connection, because
			// some timeouts aren't fatal errors.
			return nil, err
		}
		this.closeAfterError(err)
		return nil, err
	}

	// Check if work completion is a keepalive. If so, clear the flag
	// so that another keepalive may be sent.
	if wc != nil && wc.Keepalive() {
		this.keepalive = false
		return nil, nil
	}

	return wc, nil
}

var errStartOnClosedConn = errors.New("start on closed connection")

func (this *RCConn) start() error {
	if this.qp == nil {
		return errStartOnClosedConn
	}

	for {
		wc, err := this.poll()
		if err != nil {
			// a timeout here is not fatal
			return err
		}
		if wc == nil {
			continue
		}
		if !wc.Success() {
			err := fmt.Errorf("receive start: error wc: %s", wc)
			this.closeAfterError(nil)
			return err
		}
		immData := wc.ImmData()
		if !wc.Receive() || immData != immRendezvousStart {
			this.closeAfterError(nil)
			if immData == immClose {
				return io.EOF
			}
			err := fmt.Errorf("receive start: unexpected wc: %s", wc)
			return err
		}
		// repost receive buffer
		recvMr := wc.MemoryRegion()
		if err := this.qp.PostReceive(recvMr); err != nil {
			this.closeAfterError(err)
			return err
		}
		break
	}
	return nil
}

func (this *RCConn) replyFinish(mr *MemoryRegion, meta []byte) error {
	// Post send for rendezvous reply.
	reply := (*rendezvousReply)(this.meta.Ptr())
	reply.remoteAddr = uint64(uintptr(mr.Ptr()))
	reply.rkey = mr.RemoteKey(this.iface.pd)

	if err := this.qp.PostSendImm(immRendezvousReply, this.meta); err != nil {
		this.closeAfterError(err)
		return fmt.Errorf("send reply: %v", err)
	}

	// Wait for receive of rendezvous finish
	for {
		wc, err := this.poll()
		if err != nil {
			this.closeAfterError(err)
			return makeTimeoutFatal(err)
		}
		// poll can return a nil wc in case of keepalive
		if wc == nil {
			continue
		}

		if !wc.Success() {
			err := fmt.Errorf("receive finish: error wc: %s", wc)
			this.closeAfterError(nil)
			return err
		}

		if wc.Send() {
			// reply has been sent
			continue
		}

		if !wc.Receive() || wc.ImmData() != immRendezvousFinish {
			err := fmt.Errorf("receive finish: unexpected wc: %s", wc)
			this.closeAfterError(nil)
			return err
		}
		// repost receive buffer
		recvMr := wc.MemoryRegion()
		if meta != nil {
			// recvMr is always 4k
			copy(meta, recvMr.Bytes()[:len(meta)])
		}
		if err := this.qp.PostReceive(recvMr); err != nil {
			this.closeAfterError(err)
			return err
		}
		break
	}
	return nil
}

func rcConnError(name string, err error) error {
	if err == nil {
		return nil
	}
	// don't break timeout errors
	if t, ok := err.(timeout); ok && t.Timeout() {
		return err
	}
	if err == io.EOF {
		return err
	}
	return fmt.Errorf("ib: RCConn.%s: %v", name, err)
}

// Reading on both ends of the connection will not return an error.
func (this *RCConn) Read(mr *MemoryRegion) error {
	return this.ReadMeta(mr, nil)
}

func (this *RCConn) ReadMeta(mr *MemoryRegion, meta []byte) error {
	if err := this.start(); err != nil {
		return rcConnError("ReadMeta", err)
	}
	return rcConnError("ReadMeta", this.replyFinish(mr, meta))
}

var errReadPooledIntr = errors.New("ib: RCConn: ReadPooled interrupted")

func (this *RCConn) ReadPooled(mrChan <-chan *MemoryRegion) (*MemoryRegion, error) {
	if err := this.start(); err != nil {
		return nil, rcConnError("ReadPooled", err)
	}
	mr, ok := <-mrChan
	if !ok || mr == nil {
		return nil, errReadPooledIntr
	}
	return mr, rcConnError("ReadPooled", this.replyFinish(mr, nil))
}

var errFatalTimeout = errors.New("fatal timeout")

func makeTimeoutFatal(err error) error {
	if t, ok := err.(timeout); ok && t.Timeout() {
		return errFatalTimeout
	}
	return err
}

var errWriteOnClosedConn = errors.New("write on closed connection")

// Writing on both ends of the connection will return an error.
func (this *RCConn) Write(mr *MemoryRegion) error {
	err := this.WriteMetaStart()
	if err != nil {
		return rcConnError("Write", err)
	}
	return rcConnError("Write", this.write(mr, nil))
}

func (this *RCConn) WriteMeta(mr *MemoryRegion, meta []byte) error {
	return rcConnError("WriteMeta", this.write(mr, meta))
}

func (this *RCConn) WriteMetaStart() error {
	if this.qp == nil {
		return errWriteOnClosedConn
	}

	// Post send for rendezvous start
	if err := this.qp.PostSendImm(immRendezvousStart, nil); err != nil {
		this.closeAfterError(err)
		return err
	}
	return nil
}

func (this *RCConn) write(mr *MemoryRegion, meta []byte) error {
	if this.qp == nil {
		return errWriteOnClosedConn
	}

	// wait for reply
	var reply rendezvousReply
	for {
		wc, err := this.poll()
		if err != nil {
			if this.writeTimeoutFatal {
				this.closeAfterError(err)
				return fmt.Errorf("receive reply poll: %v", makeTimeoutFatal(err))
			}
			return err
		}
		// poll can return a nil wc in case of keepalive
		if wc == nil {
			continue
		}

		if !wc.Success() {
			err := fmt.Errorf("receive reply: error wc: %s", wc)
			this.closeAfterError(nil)
			return err
		}

		if wc.Send() {
			// start has been sent
			continue
		}

		if !wc.Receive() || wc.ImmData() != immRendezvousReply {
			err := fmt.Errorf("receive reply: unexpected wc: %s", wc)
			this.closeAfterError(nil)
			return err
		}

		recvMr := wc.MemoryRegion()
		reply = *(*rendezvousReply)(recvMr.Ptr())
		// Post receive for rendezvous reply again
		if err := this.qp.PostReceive(recvMr); err != nil {
			this.closeAfterError(err)
			return err
		}
		break
	}

	// Post RDMA write with reply parameters
	if err := this.qp.PostWrite(mr, reply.remoteAddr, reply.rkey); err != nil {
		this.closeAfterError(err)
		return err
	}

	for {
		wc, err := this.poll()
		if err != nil {
			this.closeAfterError(err)
			return fmt.Errorf("rdma write poll: %v", makeTimeoutFatal(err))
		}
		// poll can return a nil wc in case of keepalive
		if wc == nil {
			continue
		}
		if !wc.Success() {
			err := fmt.Errorf("rdma write: error wc: %s", wc)
			this.closeAfterError(nil)
			return err
		}

		// this is a hack. we shouldn't have to deal with this
		// work completion at this stage. we should have remained in
		// the previous state until both the start and the reply
		// completed.
		if wc.Send() {
			// start has been sent
			continue
		}

		if !wc.Write() {
			err := fmt.Errorf("rdma write: unexpected wc: %s", wc)
			this.closeAfterError(nil)
			return err
		}
		break
	}

	if meta != nil {
		n := copy(this.meta.Bytes(), meta)
		if n != len(meta) {
			panic("ib: buffer too short for meta")
		}
	}

	// Post send for rendezvous finish
	if err := this.qp.PostSendImm(immRendezvousFinish, this.meta); err != nil {
		this.closeAfterError(err)
		return err
	}

	for {
		wc, err := this.poll()
		if err != nil {
			this.closeAfterError(err)
			return fmt.Errorf("send finish poll: %v", makeTimeoutFatal(err))
		}
		// poll can return a nil wc in case of keepalive
		if wc == nil {
			continue
		}
		if !wc.Success() {
			err := fmt.Errorf("send finish: error wc: %s", wc)
			this.closeAfterError(err)
			return err
		}
		if !wc.Send() {
			err := fmt.Errorf("send finish: unexpected wc: %s", wc)
			this.closeAfterError(err)
			return err
		}
		break
	}

	return nil
}

func (this *RCConn) Close() error {
	return this.closeImpl(false)
}

// closeAfterError closes the RCConn after an error has occured. This
// also closes the memory regions used for some of the protocol
// messages, so errors that refer to work completions should be
// formatted before this function is called.
func (this *RCConn) closeAfterError(err error) {
	closeErr := this.closeImpl(true)
	if closeErr != nil {
		// an error has already occurred, so there is no good way to
		// handle a second error
		panic(fmt.Errorf("ib: RCConn.closeAfterError: %v (%v)", closeErr, err))
	}
}

func (this *RCConn) closeImpl(broken bool) error {
	// Allow multiple closes, because read and write errors also cause
	// Close to be called.
	if this.qp == nil {
		return nil
	}

	// Send a message to shut down the remote state machine. This
	// introduces some delay if both ends shut down simultaneously,
	// but our current protocols don't usually do this.
	err := this.qp.PostSendImm(immClose, nil)
	if err != nil {
		broken = true
	}
	// If the connection isn't broken yet, keep polling until all the
	// sends, including the close, is complete.
	for !broken && this.qp.Sending() {
		_, err := this.qp.Poll(this.timeout)
		if err != nil {
			break
		}
	}

	if err := this.qp.Close(); err != nil {
		return err
	}
	this.qp = nil

	// memory regions must be closed after the QP
	if this.meta != nil {
		if err := this.meta.Close(); err != nil {
			return err
		}
		this.meta = nil
	}

	for _, mr := range this.messages {
		if err := mr.Close(); err != nil {
			return err
		}
	}
	this.messages = nil

	return nil
}
