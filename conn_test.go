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

package ib

import (
	"fmt"
	"github.com/jsgilmore/shm"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var tmpfsBuf, hugeBuf shm.Buffer

func init() {
	if !SetupOptional() {
		return
	}
	var err error
	tmpfsBuf, err = shm.NewBufferTmpfs(2<<20, shm.PROT_RDWR)
	if err != nil {
		panic(err)
	}
	hugeBuf, err = shm.NewBufferHugepages(64<<20, shm.PROT_RDWR)
	if err != nil {
		panic(err)
	}
}

func randomMemory(rng *rand.Rand) *MemoryRegion {
	var mr *MemoryRegion
	var err error
	options := 3
	if tmpfsBuf != nil && hugeBuf != nil {
		options += 2
	}
	switch rng.Intn(options) {
	case 0:
		mr, err = AllocateMemory(4096)
	case 1:
		mr, err = AllocateMemory(65536)
	case 2:
		mr, err = AllocateMemory(1048576)
	case 3:
		mr, err = RegisterMemory(tmpfsBuf.Bytes())
	case 4:
		mr, err = RegisterMemory(hugeBuf.Bytes())
	default:
		panic("invalid mode")
	}
	if err != nil {
		panic(err)
	}
	return mr
}

func tmpfsMemory(rng *rand.Rand) *MemoryRegion {
	mr, err := RegisterMemory(tmpfsBuf.Bytes())
	if err != nil {
		panic(err)
	}
	return mr
}

func hugepagesMemory(rng *rand.Rand) *MemoryRegion {
	mr, err := RegisterMemory(hugeBuf.Bytes())
	if err != nil {
		panic(err)
	}
	return mr
}

const maxTimeout = 1e9

type regionFunc func(*rand.Rand) *MemoryRegion

func testConn(c *RCConn, rng *rand.Rand, newRegion regionFunc) {
	c.SetTimeout(rng.Int63n(maxTimeout))
	var err error
	var errError string
	nmessages := rng.Intn(10)
	for i := 0; i < nmessages; i++ {
		mr := newRegion(rng)
		switch rng.Intn(3) {
		case 0:
			fmt.Printf("reading %v\n", mr)
			// Because timeout errors also continue the loop, two
			// readers connected to each other will also eventually
			// make progress.
			err = c.Read(mr)
		case 1:
			fmt.Printf("writing %v\n", mr)
			err = c.Write(mr)
		case 2:
			fmt.Printf("sleeping for up to maxTimeout\n")
			time.Sleep(time.Duration(rng.Int63n(maxTimeout)))
		default:
			panic("invalid mode")
		}
		if err != nil {
			// Save error string now so that it doesn't refer to a
			// closed memory region when it is printed later.
			errError = err.Error()
		}
		if err := mr.Close(); err != nil {
			panic(err)
		}
		if err != nil {
			if t, ok := err.(timeout); ok && t.Timeout() {
				continue
			}
			break
		}
	}
	if err == nil {
		fmt.Printf("SUCCESSFUL EXCHANGE of %d MESSAGES\n", nmessages)
	} else {
		fmt.Printf("%v\n", errError)
	}
	if err := c.Close(); err != nil {
		panic("close failed: " + err.Error())
	}
}

func testConnParams(nlisteners, ndialers, nconn int, newRegion regionFunc, t *testing.T) {
	ibAddrs := []net.Addr{}
	for _, addr := range InterfaceAddrs() {
		if iface := InterfaceForAddr(addr); iface == nil {
			fmt.Printf("iface is nil\n")
			continue
		} else if !iface.Active() {
			fmt.Printf("iface is not active\n")
			continue
		}
		ibAddrs = append(ibAddrs, addr)
	}
	if len(ibAddrs) == 0 {
		t.Skip("no interfaces to test with")
	}

	// set up listeners
	listeners := []*RCListener{}
	for _, addr := range ibAddrs {
		raddr := &net.TCPAddr{IP: addr.(*net.IPNet).IP}
		fmt.Printf("listeners for %v\n", raddr)
		for k := 0; k < nlisteners; k++ {
			l, err := ListenRC(raddr)
			if err != nil {
				panic("ListenRC failed: " + err.Error())
			}
			listeners = append(listeners, l)
		}
	}

	// start dialers
	var dialWg sync.WaitGroup
	for kk := 0; kk < ndialers; kk++ {
		dialWg.Add(1)
		k := kk
		go func() {
			defer dialWg.Done()
			rng := rand.New(rand.NewSource(int64(k)))
			for j := 0; j < nconn; j++ {
				addr := ibAddrs[rng.Intn(len(ibAddrs))]
				laddr := &net.TCPAddr{IP: addr.(*net.IPNet).IP}
				l := listeners[rng.Intn(len(listeners))]
				raddr := l.Addr().(*net.TCPAddr)

				fmt.Printf("dialing %v -> %v\n", laddr, raddr)
				c, err := DialRC(laddr, raddr)
				if err != nil {
					panic("DialRC failed: " + err.Error())
				}
				fmt.Printf("connected\n")
				testConn(c, rng, newRegion)
				fmt.Printf("dialer %d conn %d DONE\n", k, j)
			}
		}()
	}

	var acceptedConns uint32
	// accept on listeners
	var listenWg sync.WaitGroup
	for kk, listener := range listeners {
		listenWg.Add(1)
		k := kk
		l := listener
		go func() {
			defer listenWg.Done()
			rng := rand.New(rand.NewSource(int64(k)))
			for j := 0; true; j++ {
				fmt.Printf("Accepting on %v\n", l.Addr())
				c, err := l.Accept()
				if err != nil {
					break
				}
				fmt.Printf("Accepted\n")
				testConn(c, rng, newRegion)
				fmt.Printf("listener %d conn %d DONE\n", k, j)
				atomic.AddUint32(&acceptedConns, 1)
			}
		}()
	}

	fmt.Printf("waiting for dialers\n")
	dialWg.Wait()

	fmt.Printf("closing listeners\n")
	for _, l := range listeners {
		if err := l.Close(); err != nil {
			panic("listener close failed: " + err.Error())
		}
	}

	fmt.Printf("waiting for listeners\n")
	listenWg.Wait()

	if ndialers*nconn != int(acceptedConns) {
		panic("accepted conns != expected conns")
	}
}

func TestConn1(t *testing.T) {
	testConnParams(1, 1, 1, randomMemory, t)
}

func TestConn4(t *testing.T) {
	if testing.Short() {
		return
	}
	testConnParams(1, 2, 2, randomMemory, t)
}

func TestConn50(t *testing.T) {
	if testing.Short() {
		return
	}
	testConnParams(5, 5, 10, randomMemory, t)
}

func TestTmpfs(t *testing.T) {
	if testing.Short() || !SetupOptional() {
		t.Skip("skipping test")
	}
	testConnParams(5, 5, 5, tmpfsMemory, t)
}

func TestHugepages(t *testing.T) {
	if testing.Short() || !SetupOptional() {
		t.Skip("skipping test")
	}
	testConnParams(5, 5, 5, hugepagesMemory, t)
}
