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
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

const (
	IBV_ACCESS_LOCAL_WRITE  = C.IBV_ACCESS_LOCAL_WRITE
	IBV_ACCESS_REMOTE_WRITE = C.IBV_ACCESS_REMOTE_WRITE
)

type mrMap map[*C.struct_ibv_pd]*C.struct_ibv_mr

type MemoryRegion struct {
	mrs   mrMap
	buf   []byte
	unmap bool
}

func (this *MemoryRegion) String() string {
	if this.buf == nil {
		return "MemoryRegion@closed"
	}
	return fmt.Sprintf("MemoryRegion@%x[%d]", &this.buf[0], len(this.buf))
}

func AllocateMemory(size int) (*MemoryRegion, error) {
	const mrProt = syscall.PROT_READ | syscall.PROT_WRITE
	const mrFlags = syscall.MAP_PRIVATE | syscall.MAP_ANONYMOUS
	buf, err := syscall.Mmap(-1, 0, size, mrProt, mrFlags)
	if err != nil {
		return nil, os.NewSyscallError("mmap", err)
	}
	return register(buf, true)
}

func RegisterMemory(buf []byte) (*MemoryRegion, error) {
	return register(buf, false)
}

var errRegisterInvalidBuf = errors.New("ib: register: invalid buffer")

func register(buf []byte, unmap bool) (*MemoryRegion, error) {
	if len(buf) == 0 || len(buf) != cap(buf) {
		return nil, errRegisterInvalidBuf
	}
	const writeAccess = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
	mrs := make(mrMap, len(pds))
	for pd := range pds {
		// there seems to be some kind of limit at 32 GB where
		// ibv_reg_mr hits an internal ENOMEM
		cmr := C.ibv_reg_mr(pd, unsafe.Pointer(&buf[0]), C.size_t(len(buf)), writeAccess)
		if cmr == nil {
			return nil, newError("ibv_reg_mr", -1)
		}
		mrs[pd] = cmr
	}
	mr := &MemoryRegion{mrs, buf, unmap}
	runtime.SetFinalizer(mr, (*MemoryRegion).finalize)
	return mr, nil
}

func (mr *MemoryRegion) finalize() {
	panic("finalized unclosed memory region")
}

func (mr *MemoryRegion) Bytes() []byte {
	return mr.buf
}

func (mr *MemoryRegion) Ptr() unsafe.Pointer {
	return unsafe.Pointer(&mr.buf[0])
}

func (mr *MemoryRegion) Len() int {
	return len(mr.buf)
}

func (mr *MemoryRegion) RemoteKey(pd *C.struct_ibv_pd) uint32 {
	ibvMr := mr.mrs[pd]
	if ibvMr == nil {
		return 0
	}
	return uint32(ibvMr.rkey)
}

func (mr *MemoryRegion) Close() error {
	for pd, cmr := range mr.mrs {
		errno := C.ibv_dereg_mr(cmr)
		if errno != 0 {
			panic(newError("ibv_dereg_mr", errno))
		}
		delete(mr.mrs, pd)
	}
	mr.mrs = nil

	if mr.unmap && mr.buf != nil {
		err := syscall.Munmap(mr.buf)
		if err != nil {
			return os.NewSyscallError("munmap", err)
		}
		mr.buf = nil
	}

	runtime.SetFinalizer(mr, nil)

	return nil
}

func (mr *MemoryRegion) populateSge(pd *C.struct_ibv_pd, sge *C.struct_ibv_sge) {
	ibvMr := mr.mrs[pd]
	if ibvMr == nil {
		panic("invalid memory region")
	}
	sge.addr = C.uint64_t(uintptr(ibvMr.addr))
	sge.length = C.uint32_t(ibvMr.length)
	sge.lkey = ibvMr.lkey
}
