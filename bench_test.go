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
	"net"
	"sync"
	"testing"
)

func benchReader(wg *sync.WaitGroup, c *RCConn, mr *MemoryRegion, n int) {
	defer wg.Done()
	c.SetTimeout(1e9)
	for i := 0; i < n; i++ {
		if err := c.Read(mr); err != nil {
			panic(err)
		}
	}
	if err := c.Close(); err != nil {
		panic(err)
	}
}

func benchWriter(wg *sync.WaitGroup, c *RCConn, mr *MemoryRegion, n int) {
	defer wg.Done()
	c.SetTimeout(1e9)
	for i := 0; i < n; i++ {
		if err := c.Write(mr); err != nil {
			panic(err)
		}
	}
	if err := c.Close(); err != nil {
		panic(err)
	}
}

const benchMult = 100
const benchMrLen = 64 * 1024 * 1024

func BenchmarkThroughput(b *testing.B) {
	laddr, raddr := chooseInterfaces(b)
	sendMr, err := AllocateMemory(benchMrLen)
	if err != nil {
		panic(err)
	}
	b.SetBytes(benchMult * int64(len(sendMr.Bytes())))
	populateRegion(sendMr)

	recvMr, err := AllocateMemory(sendMr.Len())
	if err != nil {
		panic(err)
	}

	l, err := ListenRC(raddr)
	if err != nil {
		panic(err)
	}
	raddr = l.Addr().(*net.TCPAddr)

	ch := make(chan *RCConn)
	go func() {
		c, err := DialRC(laddr, raddr)
		if err != nil {
			panic(err)
		}
		ch <- c
	}()
	srvc, err := l.Accept()
	if err != nil {
		panic(err)
	}
	clic := <-ch

	n := benchMult * b.N

	b.ResetTimer()
	b.StartTimer()

	var wg sync.WaitGroup
	wg.Add(1)
	go benchReader(&wg, srvc, recvMr, n)
	wg.Add(1)
	go benchWriter(&wg, clic, sendMr, n)
	wg.Wait()

	b.StopTimer()

	if err := l.Close(); err != nil {
		panic(err)
	}
	if err := recvMr.Close(); err != nil {
		panic(err)
	}
	if err := sendMr.Close(); err != nil {
		panic(err)
	}
}
