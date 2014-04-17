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
	"sync"
	"testing"
)

func TestInterfaces(t *testing.T) {
	for _, iface := range interfaces {
		fmt.Printf("%+v active=%v lid=%d\n", iface, iface.Active(), iface.Lid())
	}
}

func TestInterfaceAddrs(t *testing.T) {
	addrs := InterfaceAddrs()
	for _, addr := range addrs {
		iface := InterfaceForAddr(addr)
		if iface == nil {
			continue
		}
		fmt.Printf("%v -> %+v guid=%x\n", addr, iface, []byte(iface.guid))
	}
}

func TestCompletionChannels(t *testing.T) {
	var wg sync.WaitGroup
	ibAddrs := InterfaceAddrs()
	for _, addr := range ibAddrs {
		iface := InterfaceForAddr(addr)
		if iface == nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				cq := iface.createCompletionQueue(5)
				if cq == nil {
					panic("ibv_create_cq: failure")
				}
				fmt.Printf("cq.channel=%+v\n", cq.channel)
				if err := destroyCompletionQueue(cq); err != nil {
					panic(err)
				}
			}
		}()
	}
	wg.Wait()
}

func init() {
	Initialize()
}
