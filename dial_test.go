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
	"net"
	"sync"
	"testing"
	"time"
)

func populateRegion(mr *MemoryRegion) {
	b := mr.Bytes()
	for i := 0; i < len(b); i++ {
		b[i] = byte(i)
	}
}

func checkRegion(mr *MemoryRegion) bool {
	b := mr.Bytes()
	for i := 0; i < len(b); i++ {
		if b[i] != byte(i) {
			return false
		}
	}
	return true
}

func rwMrLen() int {
	if testing.Short() {
		return 1024 * 1024
	}
	return 64 * 1024 * 1024
}

func reader(c *RCConn, n int) {
	t := int64(200e6)
	if testing.Short() {
		t = 10e6
	}
	c.SetTimeout(t)

	for i := 0; i < n; i++ {
		mr, err := AllocateMemory(rwMrLen())
		if err != nil {
			panic(err)
		}

		// Note for test parameter tweakers: if number of loop
		// iterations is small enough and there is a short timeout,
		// this test can fail to read data into the memory region.
		readDone := false
		const ntimeouts = 10
		for j := 0; j < ntimeouts; j++ {
			if err := c.Read(mr); err == nil {
				fmt.Printf("Read into %v\n", mr)
				readDone = true
				break
			} else {
				if t, ok := err.(timeout); ok && t.Timeout() {
					fmt.Printf("read timed out, looping\n")
					continue
				}
				panic("reader read failed: " + err.Error())
			}
		}

		if !readDone {
			panic(fmt.Errorf("read not done with %d timeouts of %d ns", ntimeouts, t))
		}

		if !checkRegion(mr) {
			panic("memory region data invalid")
		}

		if err := mr.Close(); err != nil {
			panic(err)
		}
	}

	fmt.Printf("reader closing\n")
	if err := c.Close(); err != nil {
		panic(err)
	}
	fmt.Printf("reader closed\n")
}

func writer(c *RCConn, n int) {
	c.SetTimeout(5e9)

	for i := 0; i < n; i++ {
		mr, err := AllocateMemory(rwMrLen())
		if err != nil {
			panic(err)
		}
		populateRegion(mr)
		// sleep a bit so that read will send keepalives
		if testing.Short() {
			time.Sleep(30e6)
		} else {
			time.Sleep(600e6)
		}
		if err := c.Write(mr); err != nil {
			panic("writer write failed: " + err.Error())
		}
		fmt.Printf("Wrote from %v\n", mr)
		if err := mr.Close(); err != nil {
			panic(err)
		}
	}

	fmt.Printf("writer closing\n")
	if err := c.Close(); err != nil {
		panic(err)
	}
	fmt.Printf("writer closed\n")
}

// nops must be bigger than 16 to expose the ENOMEM error from
// ibv_post_send with unsignaled inline sends
const nops = 20

func client(wg *sync.WaitGroup, laddr, raddr *net.TCPAddr) {
	defer wg.Done()
	c, err := DialRC(laddr, raddr)
	if err != nil {
		panic(err)
	}
	reader(c, nops)
}

func server(wg *sync.WaitGroup, l *RCListener) {
	defer wg.Done()
	c, err := l.Accept()
	if err != nil {
		panic(err)
	}
	writer(c, nops)
}

// This test takes longer to tear down if the reader closes before the
// writer. Don't remember why exactly, but this is the expected
// behaviour due to the way keepalives and RCConn.Close works.
func TestListenDial(t *testing.T) {
	ibAddrs := InterfaceAddrs()
	var wg sync.WaitGroup
	for i, addri := range ibAddrs {
		if iface := InterfaceForAddr(addri); iface == nil || !iface.Active() {
			fmt.Printf("Skipping %v\n", addri)
			continue
		}
		laddr := &net.TCPAddr{IP: addri.(*net.IPNet).IP}
		for _, addrj := range ibAddrs[i:] {
			if iface := InterfaceForAddr(addrj); iface == nil || !iface.Active() {
				fmt.Printf("Skipping %v\n", addrj)
				continue
			}
			raddr := &net.TCPAddr{IP: addrj.(*net.IPNet).IP}
			l, err := ListenRC(raddr)
			if err != nil {
				panic(err)
			}
			// get port
			raddr = l.Addr().(*net.TCPAddr)
			fmt.Printf("laddr=%+v raddr=%v\n", laddr, raddr)
			wg.Add(1)
			go server(&wg, l)
			wg.Add(1)
			go client(&wg, laddr, raddr)
			wg.Wait()
		}

		if testing.Short() {
			break
		}
	}
}

// run this test with GOMAXPROCS>1 for extra fun
func xTestListenDialLoop(t *testing.T) {
	for k := 0; k < 10; k++ {
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				TestListenDial(t)
			}()
		}
		wg.Wait()
	}
}
