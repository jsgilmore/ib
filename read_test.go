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
	"testing"
)

func chooseInterfaces(tb testing.TB) (*net.TCPAddr, *net.TCPAddr) {
	ibAddrs := InterfaceAddrs()
	var addri, addrj net.Addr
	for i := 0; i < len(ibAddrs); i++ {
		if iface := InterfaceForAddr(ibAddrs[i]); iface == nil || !iface.Active() {
			continue
		}
		addri = ibAddrs[i]
	}
	for j := len(ibAddrs) - 1; j >= 0; j-- {
		if iface := InterfaceForAddr(ibAddrs[j]); iface == nil || !iface.Active() {
			continue
		}
		addrj = ibAddrs[j]
	}
	if addri == nil || addrj == nil {
		tb.Skip("no interfaces to test with")
	}
	laddr := &net.TCPAddr{IP: addri.(*net.IPNet).IP}
	raddr := &net.TCPAddr{IP: addrj.(*net.IPNet).IP}
	return laddr, raddr
}

func connPair(laddr, raddr *net.TCPAddr) (*RCConn, *RCConn) {
	l, err := ListenRC(raddr)
	if err != nil {
		panic(err)
	}
	defer checkClose(l)
	// conn channel
	cc := make(chan *RCConn)
	go func() {
		c, err := l.Accept()
		if err != nil {
			panic(err)
		}
		cc <- c
	}()
	c, err := DialRC(laddr, l.Addr().(*net.TCPAddr))
	if err != nil {
		panic(err)
	}
	return c, <-cc
}

func TestWriteAfterClose(t *testing.T) {
	laddr, raddr := chooseInterfaces(t)
	c1, c2 := connPair(laddr, raddr)
	mr, err := AllocateMemory(8192)
	if err != nil {
		panic(err)
	}
	if err := c2.Close(); err != nil {
		panic(err)
	}
	c1.SetTimeout(10e9)
	fmt.Printf("writing\n")
	err = c1.Write(mr)
	fmt.Printf("write returned\n")
	if err == nil {
		panic("expected a write error")
	}
	fmt.Printf("write error: %+v\n", err)
	if err := c1.Close(); err != nil {
		panic(err)
	}
	if err := mr.Close(); err != nil {
		panic(err)
	}
}

func TestReadAfterClose(t *testing.T) {
	laddr, raddr := chooseInterfaces(t)
	c1, c2 := connPair(laddr, raddr)
	mr, err := AllocateMemory(8192)
	if err != nil {
		panic(err)
	}
	if err := c2.Close(); err != nil {
		panic(err)
	}
	c1.SetTimeout(10e9)
	for i := 0; i < 100; i++ {
		fmt.Printf("reading\n")
		err = c1.Read(mr)
		fmt.Printf("read returned\n")
		if err == nil {
			panic("expected a read error")
		}
		if t, ok := err.(timeout); ok && t.Timeout() {
			err = nil
			continue
		} else {
			break
		}
	}
	if err == nil {
		panic("expected a non-timeout read error")
	}
	fmt.Printf("read error: %+v\n", err)
	if err := c1.Close(); err != nil {
		panic(err)
	}
	if err := mr.Close(); err != nil {
		panic(err)
	}
}
