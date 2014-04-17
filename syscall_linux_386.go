// Created by cgo -godefs - DO NOT EDIT
// cgo -godefs syscall_linux.go

package ib

import (
	"syscall"
	"unsafe"
)

const (
	POLLIN    = 0x1
	POLLPRI   = 0x2
	POLLOUT   = 0x4
	POLLRDHUP = 0x2000
	POLLERR   = 0x8
	POLLHUP   = 0x10
	POLLNVAL  = 0x20
)

const (
	RLIM_INFINITY     = 0xffffffff
	RLIMIT_AS         = 0x9
	RLIMIT_CORE       = 0x4
	RLIMIT_CPU        = 0x0
	RLIMIT_DATA       = 0x2
	RLIMIT_FSIZE      = 0x1
	RLIMIT_MEMLOCK    = 0x8
	RLIMIT_MSGQUEUE   = 0xc
	RLIMIT_NICE       = 0xd
	RLIMIT_NOFILE     = 0x7
	RLIMIT_NPROC      = 0x6
	RLIMIT_RSS        = 0x5
	RLIMIT_RTPRIO     = 0xe
	RLIMIT_SIGPENDING = 0xb
	RLIMIT_STACK      = 0x3
)

type Pollfd struct {
	Fd      int32
	Events  int16
	Revents int16
}
type Rlimit struct {
	Cur uint32
	Max uint32
}

func Getrlimit(resource int) (rlim Rlimit, err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_GETRLIMIT, uintptr(resource), uintptr(unsafe.Pointer(&rlim)), 0)
	if e1 != 0 {
		err = e1
	}
	return
}

func Poll(fds []Pollfd, timeout int64) (n int, err error) {
	r0, _, e1 := syscall.Syscall(syscall.SYS_POLL, uintptr(unsafe.Pointer(&fds[0])), uintptr(len(fds)), uintptr(timeout))
	n = int(r0)
	if e1 != 0 {
		err = e1
	}
	return
}
