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

// workaround for http://code.google.com/p/go/issues/detail?id=3027
//#define inline
//#define static

//#include <infiniband/verbs.h>
//#cgo linux LDFLAGS: -libverbs
import "C"

import (
	"fmt"
	"net"
	"os"
	"unsafe"
)

type pdsSet map[*C.struct_ibv_pd]bool

var interfaces []*Interface

var guidToInterface = make(map[string]*Interface)

var pds pdsSet

type Interface struct {
	ctx        *C.struct_ibv_context
	pd         *C.struct_ibv_pd
	port       uint8
	deviceAttr *C.struct_ibv_device_attr
	guid       net.HardwareAddr
}

func (iface *Interface) Lid() uint16 {
	var portAttr C.struct_ibv_port_attr
	errno := C.ibv_query_port(iface.ctx, C.uint8_t(iface.port), &portAttr)
	if errno != 0 {
		return 0
	}
	return uint16(portAttr.lid)
}

func (iface *Interface) Active() bool {
	var portAttr C.struct_ibv_port_attr
	errno := C.ibv_query_port(iface.ctx, C.uint8_t(iface.port), &portAttr)
	if errno != 0 {
		return false
	}
	return portAttr.state == C.IBV_PORT_ACTIVE
}

func newInterfaces(ctx *C.struct_ibv_context) {
	var deviceAttr C.struct_ibv_device_attr
	errno := C.ibv_query_device(ctx, &deviceAttr)
	if errno != 0 {
		return
	}

	pd := C.ibv_alloc_pd(ctx)
	if pd == nil {
		panic(newError("ibv_alloc_pd", -1))
	}
	pds[pd] = true

	for port := C.uint8_t(1); port <= deviceAttr.phys_port_cnt; port++ {
		var portAttr C.struct_ibv_port_attr
		errno := C.ibv_query_port(ctx, port, &portAttr)
		if errno != 0 {
			continue
		}

		var gid C.union_ibv_gid
		errno = C.ibv_query_gid(ctx, port, 0, &gid)
		if errno != 0 {
			continue
		}
		// last 8 bytes of GID is the GUID
		guid := net.HardwareAddr(gid[8:])
		iface := &Interface{ctx, pd, uint8(port), &deviceAttr, guid}
		interfaces = append(interfaces, iface)
		guidToInterface[string([]byte(guid))] = iface
	}
}

func InterfaceForAddr(addr net.Addr) *Interface {
	var ip net.IP
	if ipAddr, ok := addr.(*net.IPNet); ok {
		ip = ipAddr.IP
	} else if tcpAddr, ok := addr.(*net.TCPAddr); ok {
		ip = tcpAddr.IP
	} else {
		return nil
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}

	var ibIface *Interface
	for i := range ifaces {
		iface := &ifaces[i]

		// hack until ifi_type in ifinfomsg is available in Go
		if len(iface.HardwareAddr) != 20 {
			continue
		}

		guidKey := string([]byte(iface.HardwareAddr[12:]))
		var ok bool
		ibIface, ok = guidToInterface[guidKey]
		if !ok || iface == nil {
			continue
		}

		ifaceAddrs, err := iface.Addrs()
		if err != nil || len(ifaceAddrs) == 0 {
			continue
		}
		found := false
		for _, ifaceAddr := range ifaceAddrs {
			ifaceIPNet := ifaceAddr.(*net.IPNet)
			if ifaceIPNet.IP.To4() == nil {
				continue
			}
			if ip.Equal(ifaceIPNet.IP) {
				found = true
				break
			}
		}
		if found {
			break
		} else {
			ibIface = nil
		}
	}
	return ibIface
}

func InterfaceAddrs() []net.Addr {
	if pds == nil {
		panic("ib not initialzied")
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}

	var ibAddrs []net.Addr
	for i := range ifaces {
		iface := &ifaces[i]
		if len(iface.HardwareAddr) != 20 {
			continue
		}
		ifaceAddrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range ifaceAddrs {
			ipAddr := addr.(*net.IPNet)
			if ipAddr.IP.To4() == nil {
				continue
			}
			ibAddrs = append(ibAddrs, ipAddr)
		}
	}
	return ibAddrs
}

func walkDevices(callback func(device *C.struct_ibv_device)) {
	var numDevices C.int
	deviceList, err := C.ibv_get_device_list(&numDevices)
	if err != nil {
		return
	}
	defer C.ibv_free_device_list(deviceList)
	devicePtr := deviceList
	device := *devicePtr
	for device != nil {
		callback(device)
		prevDevicePtr := uintptr(unsafe.Pointer(devicePtr))
		sizeofPtr := uintptr(unsafe.Sizeof(devicePtr))
		devicePtr = (**C.struct_ibv_device)(unsafe.Pointer(prevDevicePtr + sizeofPtr))
		device = *devicePtr
	}
	return
}

func handleAsyncEvents(ctx *C.struct_ibv_context) {
	var event C.struct_ibv_async_event
	errno := C.ibv_get_async_event(ctx, &event)
	if errno != 0 {
		panic(newError("ibv_get_async_event", errno))
	}
	C.ibv_ack_async_event(&event)
	// ignore most async events
	switch event.event_type {
	case C.IBV_EVENT_CQ_ERR:
		panic("Async event: CQ overrun")
	case C.IBV_EVENT_QP_FATAL:
	case C.IBV_EVENT_QP_ACCESS_ERR:
	case C.IBV_EVENT_COMM_EST:
	case C.IBV_EVENT_CLIENT_REREGISTER:
	default:
		panic(fmt.Sprintf("Async event: %+v", event))
	}
}

func Initialize() {
	if pds != nil {
		panic("ib already initialzied")
	}

	pds = make(pdsSet, 0)
	walkDevices(func(device *C.struct_ibv_device) {
		ctx := C.ibv_open_device(device)
		if ctx == nil {
			panic("ibv_open_device: failure")
		}
		newInterfaces(ctx)
		go handleAsyncEvents(ctx)
	})

	// skip memlock check if there is no IB hardware
	if len(pds) == 0 {
		return
	}

	rlim, err := Getrlimit(RLIMIT_MEMLOCK)
	if err != nil {
		panic(os.NewSyscallError("getrlimit", err))
	}
	const maxUint64 = 1<<64 - 1
	if rlim.Cur != uint64(maxUint64) || rlim.Max != uint64(maxUint64) {
		panic("ib: MEMLOCK rlimit is not unlimited")
	}
}
