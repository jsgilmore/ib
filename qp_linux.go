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

//#include <infiniband/verbs.h>
import "C"

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"syscall"
	"time"
	"unsafe"
)

type QueuePair interface {
	Qpn() uint32
	Psn() uint32
	Reset() error
	Init() error
	ReadyToReceive(destLid uint16, destQpn, destPsn uint32) error
	ReadyToSend() error
	Error() error
	Close() error
	PostSend(mr *MemoryRegion) error
	PostSendImm(imm uint32, mr *MemoryRegion) error
	PostReceive(mr *MemoryRegion) error
	PostWrite(mr *MemoryRegion, remoteAddr uint64, rkey uint32) error
	PostKeepalive() error
	Setup(destLid uint16, destQpn, destPsn uint32, messages []*MemoryRegion) error
	Poll(nsec int64) (*WorkCompletion, error)
	Query() *QPAttr
	Sending() bool
}

type QPAttr struct {
	attr     C.struct_ibv_qp_attr
	initAttr C.struct_ibv_qp_init_attr
}

// workRequests ensures that there is a reference to each memory
// region being used, even after it has been posted. This obviates the
// need for a mapper like in Go's syscall.Mmap.

type queuePair struct {
	iface        *Interface
	qp           *C.struct_ibv_qp
	port         uint8
	psn          uint32
	pollfd       []Pollfd
	workRequests map[C.uint64_t]workRequest
}

func (iface *Interface) createCompletionQueue(cqe int) *C.struct_ibv_cq {
	compChannel := C.ibv_create_comp_channel(iface.ctx)
	if compChannel == nil {
		panic("ibv_create_comp_channel: failure")
	}
	if err := syscall.SetNonblock(int(compChannel.fd), true); err != nil {
		panic(err)
	}
	cq := C.ibv_create_cq(iface.ctx, C.int(cqe), nil, compChannel, 0)
	if cq != nil {
		return cq
	}
	errno := C.ibv_destroy_comp_channel(compChannel)
	if errno != 0 {
		panic(newError("ibv_destroy_comp_channel", errno))
	}
	return nil
}

// The spec says:
//
// An unsignaled Work Request that completed successfully is confirmed
// when all of the following rules are met:
// - A Work Completion is retrieved from the same CQ that is
// associated with the Send Queue to which the unsignaled Work Request
// was submitted.
// - That Work Completion corresponds to a subsequent Work Request on
// the same Send Queue as the unsignaled Work Request.
//
// This means that if we only do unsignaled sends on a CQ, even if we
// do signaled receives, the Send Queue associated with the CQ will
// fill up because the unsignaled Work Requests are not confirmed.

func (iface *Interface) NewQueuePair(cqe int) (QueuePair, error) {
	cq := iface.createCompletionQueue(cqe)
	if cq == nil {
		return nil, newError("ibv_create_cq", -1)
	}

	initAttr := C.struct_ibv_qp_init_attr{}
	initAttr.send_cq = cq
	initAttr.recv_cq = cq
	initAttr.cap.max_send_wr = C.uint32_t(cqe)
	initAttr.cap.max_recv_wr = C.uint32_t(cqe)
	initAttr.cap.max_send_sge = 1
	initAttr.cap.max_recv_sge = 1
	initAttr.cap.max_inline_data = 64
	initAttr.qp_type = C.IBV_QPT_RC

	// Make everything signaled. This avoids the problem with inline
	// sends filling up the send queue of the CQ.
	initAttr.sq_sig_all = 1

	cqp := C.ibv_create_qp(iface.pd, &initAttr)
	if cqp == nil {
		return nil, newError("ibv_create_qp", -1)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	// mask psn to make sure it isn't too big
	psn := rng.Uint32() & 0xffffff

	pollfd := make([]Pollfd, 1)
	pollfd[0].Fd = int32(cq.channel.fd)
	pollfd[0].Events = POLLIN
	workRequests := make(map[C.uint64_t]workRequest)
	qp := &queuePair{iface, cqp, iface.port, psn, pollfd, workRequests}

	// Reset and Init QP here instead so that Close (which transitions
	// QP to Error state) can be called immediately after this
	// function returns. This happened when writeReadQPParams returned
	// an error in newRCConn.

	if err := qp.Reset(); err != nil {
		// no reason for this to fail
		panic(err)
	}

	if err := qp.Init(); err != nil {
		// no reason for this to fail
		panic(err)
	}

	return qp, nil
}

func destroyCompletionQueue(cq *C.struct_ibv_cq) error {
	if cq == nil {
		return nil
	}
	channel := cq.channel
	// CQ must be destroyed before completion channel
	errno := C.ibv_destroy_cq(cq)
	if errno != 0 {
		return newError("ibv_destroy_cq", errno)
	}
	if channel != nil {
		errno := C.ibv_destroy_comp_channel(channel)
		if errno != 0 {
			return newError("ibv_destroy_comp_channel", errno)
		}
	}
	return nil
}

var errQPAlreadyClosed = errors.New("ib: queuePair: already closed")

func (qp *queuePair) Close() error {
	if qp.qp == nil {
		return errQPAlreadyClosed
	}

	// Transition QP to error state. Queue processing is stopped.
	// Work Requests pending or in process are completed in error,
	// when possible.
	if err := qp.Error(); err != nil {
		return err
	}

	send_cq := qp.qp.send_cq
	recv_cq := qp.qp.recv_cq
	errno := C.ibv_destroy_qp(qp.qp)
	if errno != 0 {
		return newError("ibv_destroy_qp", errno)
	}
	qp.qp = nil

	if send_cq != nil {
		if err := destroyCompletionQueue(send_cq); err != nil {
			return err
		}
	}
	if recv_cq != send_cq {
		if err := destroyCompletionQueue(recv_cq); err != nil {
			return err
		}
	}

	return newError("ibv_destroy_qp", errno)
}

func (qp *queuePair) Qpn() uint32 {
	return uint32(qp.qp.qp_num)
}

func (qp *queuePair) Psn() uint32 {
	return qp.psn
}

func (qp *queuePair) modify(attr *C.struct_ibv_qp_attr, mask int) error {
	errno := C.ibv_modify_qp(qp.qp, attr, C.int(mask))
	return newError("ibv_modify_qp", errno)
}

func (qp *queuePair) Reset() error {
	attr := C.struct_ibv_qp_attr{}
	attr.qp_state = C.IBV_QPS_RESET
	mask := C.IBV_QP_STATE
	return qp.modify(&attr, mask)
}

func (qp *queuePair) Init() error {
	attr := C.struct_ibv_qp_attr{}
	attr.qp_state = C.IBV_QPS_INIT
	attr.pkey_index = 0
	attr.port_num = C.uint8_t(qp.port)
	// allow RDMA write
	attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE
	mask := C.IBV_QP_STATE | C.IBV_QP_PKEY_INDEX | C.IBV_QP_PORT | C.IBV_QP_ACCESS_FLAGS
	return qp.modify(&attr, mask)
}

func (qp *queuePair) ReadyToReceive(destLid uint16, destQpn, destPsn uint32) error {
	attr := C.struct_ibv_qp_attr{}
	attr.qp_state = C.IBV_QPS_RTR
	attr.path_mtu = C.IBV_MTU_2048
	attr.dest_qp_num = C.uint32_t(destQpn)
	attr.rq_psn = C.uint32_t(destPsn)
	// this must be > 0 to avoid IBV_WC_REM_INV_REQ_ERR
	attr.max_dest_rd_atomic = 1
	// Minimum RNR NAK timer (range 0..31)
	attr.min_rnr_timer = 26
	attr.ah_attr.is_global = 0
	attr.ah_attr.dlid = C.uint16_t(destLid)
	attr.ah_attr.sl = 0
	attr.ah_attr.src_path_bits = 0
	attr.ah_attr.port_num = C.uint8_t(qp.port)
	mask := C.IBV_QP_STATE | C.IBV_QP_AV | C.IBV_QP_PATH_MTU | C.IBV_QP_DEST_QPN |
		C.IBV_QP_RQ_PSN | C.IBV_QP_MAX_DEST_RD_ATOMIC | C.IBV_QP_MIN_RNR_TIMER
	return qp.modify(&attr, mask)
}

func (qp *queuePair) ReadyToSend() error {
	attr := C.struct_ibv_qp_attr{}
	attr.qp_state = C.IBV_QPS_RTS
	// Local ack timeout for primary path.
	// Timeout is calculated as 4.096e-6*(2**attr.timeout) seconds.
	attr.timeout = 14
	// Retry count (7 means forever)
	attr.retry_cnt = 6
	// RNR retry (7 means forever)
	attr.rnr_retry = 6
	attr.sq_psn = C.uint32_t(qp.psn)
	// this must be > 0 to avoid IBV_WC_REM_INV_REQ_ERR
	attr.max_rd_atomic = 1
	mask := C.IBV_QP_STATE | C.IBV_QP_TIMEOUT | C.IBV_QP_RETRY_CNT | C.IBV_QP_RNR_RETRY |
		C.IBV_QP_SQ_PSN | C.IBV_QP_MAX_QP_RD_ATOMIC
	return qp.modify(&attr, mask)
}

func (qp *queuePair) Error() error {
	attr := C.struct_ibv_qp_attr{}
	attr.qp_state = C.IBV_QPS_ERR
	mask := C.IBV_QP_STATE
	return qp.modify(&attr, mask)
}

func (qp *queuePair) PostSend(mr *MemoryRegion) error {
	return qp.PostSendImm(0, mr)
}

func (qp *queuePair) PostSendImm(imm uint32, mr *MemoryRegion) error {
	if qp.qp == nil {
		return errQPAlreadyClosed
	}

	var sendWr C.struct_ibv_send_wr
	var bad *C.struct_ibv_send_wr
	if imm > 0 {
		sendWr.opcode = C.IBV_WR_SEND_WITH_IMM
		// always send inline if there is immediate data
		sendWr.send_flags = C.IBV_SEND_INLINE
		sendWr.imm_data = C.uint32_t(imm)
	} else {
		sendWr.opcode = C.IBV_WR_SEND
		sendWr.send_flags = C.IBV_SEND_SIGNALED
	}

	if mr != nil {
		var sge C.struct_ibv_sge
		sendWr.sg_list = &sge
		sendWr.num_sge = 1
		mr.populateSge(qp.iface.pd, &sge)
	} else {
		// send inline if there is no memory region to send
		sendWr.send_flags = C.IBV_SEND_INLINE
	}

	sendWr.wr_id = C.uint64_t(uintptr(unsafe.Pointer(&sendWr)))
	qp.workRequests[sendWr.wr_id] = workRequest{mr, &sendWr, nil, false}

	errno := C.ibv_post_send(qp.qp, &sendWr, &bad)
	return newError("ibv_post_send", errno)
}

type workRequest struct {
	mr        *MemoryRegion
	sendWr    *C.struct_ibv_send_wr
	recvWr    *C.struct_ibv_recv_wr
	keepalive bool
}

func (qp *queuePair) PostReceive(mr *MemoryRegion) error {
	if qp.qp == nil {
		return errQPAlreadyClosed
	}

	var recvWr C.struct_ibv_recv_wr
	var sge C.struct_ibv_sge
	var bad *C.struct_ibv_recv_wr
	recvWr.sg_list = &sge
	recvWr.num_sge = 1
	mr.populateSge(qp.iface.pd, &sge)

	recvWr.wr_id = C.uint64_t(uintptr(unsafe.Pointer(&recvWr)))
	qp.workRequests[recvWr.wr_id] = workRequest{mr, nil, &recvWr, false}

	errno := C.ibv_post_recv(qp.qp, &recvWr, &bad)
	return newError("ibv_post_recv", errno)
}

type rdma struct {
	remoteAddr uint64
	rkey       uint32
}

func (qp *queuePair) PostWrite(mr *MemoryRegion, remoteAddr uint64, rkey uint32) error {
	if qp.qp == nil {
		return errQPAlreadyClosed
	}

	var sendWr C.struct_ibv_send_wr
	var sge C.struct_ibv_sge
	var bad *C.struct_ibv_send_wr
	sendWr.opcode = C.IBV_WR_RDMA_WRITE
	sendWr.send_flags = C.IBV_SEND_SIGNALED
	sendWr.sg_list = &sge
	sendWr.num_sge = 1
	mr.populateSge(qp.iface.pd, &sge)
	r := (*rdma)(unsafe.Pointer(&sendWr.wr))
	r.remoteAddr = remoteAddr
	r.rkey = rkey

	sendWr.wr_id = C.uint64_t(uintptr(unsafe.Pointer(&sendWr)))
	qp.workRequests[sendWr.wr_id] = workRequest{mr, &sendWr, nil, false}

	errno := C.ibv_post_send(qp.qp, &sendWr, &bad)
	return newError("ibv_post_send", errno)
}

// Zero-length RDMA write.
func (qp *queuePair) PostKeepalive() error {
	if qp.qp == nil {
		return errQPAlreadyClosed
	}

	var sendWr C.struct_ibv_send_wr
	var bad *C.struct_ibv_send_wr
	sendWr.opcode = C.IBV_WR_RDMA_WRITE
	sendWr.send_flags = C.IBV_SEND_SIGNALED

	sendWr.wr_id = C.uint64_t(uintptr(unsafe.Pointer(&sendWr)))
	qp.workRequests[sendWr.wr_id] = workRequest{nil, &sendWr, nil, true}

	errno := C.ibv_post_send(qp.qp, &sendWr, &bad)
	return newError("ibv_post_send", errno)
}

func (qp *queuePair) Setup(destLid uint16, destQpn, destPsn uint32, messages []*MemoryRegion) error {
	for _, mr := range messages {
		if err := qp.PostReceive(mr); err != nil {
			return err
		}
	}
	if err := qp.ReadyToReceive(destLid, destQpn, destPsn); err != nil {
		return err
	}
	return qp.ReadyToSend()
}

func (qp *queuePair) pollCompletionQueue(wc *C.struct_ibv_wc) (int, error) {
	cq := qp.qp.send_cq
	nwc := C.ibv_poll_cq(cq, 1, wc)
	if nwc < 0 {
		return 0, newError("ibv_poll_cq", -1)
	} else if nwc == 0 {
		return 0, nil
	}
	return 1, nil
}

type WorkCompletion struct {
	mr        *MemoryRegion
	wc        C.struct_ibv_wc
	keepalive bool
}

func (wc *WorkCompletion) String() string {
	s := C.GoString(C.ibv_wc_status_str(wc.wc.status))
	return fmt.Sprintf("WorkCompletion{%s,wc:%+v,keepalive:%v,%v}", s, wc.wc, wc.keepalive, wc.mr)
}

func (wc *WorkCompletion) MemoryRegion() *MemoryRegion {
	return wc.mr
}

func (wc *WorkCompletion) Success() bool {
	return wc.wc.status == C.IBV_WC_SUCCESS
}

func (wc *WorkCompletion) Send() bool {
	return wc.wc.opcode == C.IBV_WC_SEND
}

func (wc *WorkCompletion) Write() bool {
	return wc.wc.opcode == C.IBV_WC_RDMA_WRITE
}

func (wc *WorkCompletion) Receive() bool {
	return wc.wc.opcode&C.IBV_WC_RECV != 0
}

func (wc *WorkCompletion) ImmData() uint32 {
	return uint32(wc.wc.imm_data)
}

func (wc *WorkCompletion) Keepalive() bool {
	return wc.keepalive
}

func (qp *queuePair) newWorkCompletion(wc *C.struct_ibv_wc) *WorkCompletion {
	// Inline sends (wr_id=0) must be signaled to prevent the send
	// queue from filling up, but their completions are only useful if
	// there is an error.
	if wc.wr_id == 0 {
		if wc.status != C.IBV_WC_SUCCESS {
			return &WorkCompletion{nil, *wc, false}
		}
		return nil
	}
	wr, ok := qp.workRequests[wc.wr_id]
	if !ok {
		panic("invalid work completion")
	}
	delete(qp.workRequests, wc.wr_id)
	return &WorkCompletion{wr.mr, *wc, wr.keepalive}
}

// Poll timeout is specified in milliseconds. If the returned work
// completion and error are both nil, the caller should poll again.
func (qp *queuePair) Poll(nsec int64) (*WorkCompletion, error) {
	if qp.qp == nil {
		return nil, errQPAlreadyClosed
	}

	if qp.qp.send_cq != qp.qp.recv_cq {
		panic("send_cq != recv_cq not implemented")
	}

	var wc C.struct_ibv_wc
	if nwc, err := qp.pollCompletionQueue(&wc); err != nil {
		return nil, err
	} else if nwc > 0 {
		return qp.newWorkCompletion(&wc), nil
	}

	cq := qp.qp.send_cq
	if errno := C.ibv_req_notify_cq(cq, 0); errno != 0 {
		return nil, newError("ibv_req_notify_cq", errno)
	}

	if nwc, err := qp.pollCompletionQueue(&wc); err != nil {
		return nil, err
	} else if nwc > 0 {
		return qp.newWorkCompletion(&wc), nil
	}

	qp.pollfd[0].Revents = 0
	// nsec>>20 is a quick conversion to milliseconds
	n, err := Poll(qp.pollfd, nsec>>20)
	if err != nil {
		return nil, os.NewSyscallError("poll", err)
	}
	if n > 0 {
		var evcq *C.struct_ibv_cq
		var evctx unsafe.Pointer
		errno := C.ibv_get_cq_event(cq.channel, &evcq, &evctx)
		if errno != 0 {
			return nil, newError("ibv_get_cq_event", errno)
		}
		C.ibv_ack_cq_events(evcq, 1)
		// caller should poll again
		return nil, nil
	}

	return nil, syscall.EAGAIN
}

func (qp *queuePair) Query() *QPAttr {
	if qp.qp == nil {
		return nil
	}
	var attr QPAttr
	errno := C.ibv_query_qp(qp.qp, &attr.attr, (1<<28)-1, &attr.initAttr)
	if errno != 0 {
		return nil
	}
	return &attr
}

func (qp *queuePair) Sending() bool {
	for _, wr := range qp.workRequests {
		if wr.sendWr != nil {
			return true
		}
	}
	return false
}
