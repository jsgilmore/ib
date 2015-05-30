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

// +build ignore

package ib

//#define _GNU_SOURCE
//#include <sys/poll.h>
//#include <sys/resource.h>
//#include <sys/socket.h>
//#include <errno.h>
import "C"

import (
	"syscall"
	"unsafe"
)

const (
	POLLIN    = C.POLLIN
	POLLPRI   = C.POLLPRI
	POLLOUT   = C.POLLOUT
	POLLRDHUP = C.POLLRDHUP
	POLLERR   = C.POLLERR
	POLLHUP   = C.POLLHUP
	POLLNVAL  = C.POLLNVAL
)

const (
	RLIM_INFINITY     = C.RLIM_INFINITY
	RLIMIT_AS         = C.RLIMIT_AS
	RLIMIT_CORE       = C.RLIMIT_CORE
	RLIMIT_CPU        = C.RLIMIT_CPU
	RLIMIT_DATA       = C.RLIMIT_DATA
	RLIMIT_FSIZE      = C.RLIMIT_FSIZE
	RLIMIT_MEMLOCK    = C.RLIMIT_MEMLOCK
	RLIMIT_MSGQUEUE   = C.RLIMIT_MSGQUEUE
	RLIMIT_NICE       = C.RLIMIT_NICE
	RLIMIT_NOFILE     = C.RLIMIT_NOFILE
	RLIMIT_NPROC      = C.RLIMIT_NPROC
	RLIMIT_RSS        = C.RLIMIT_RSS
	RLIMIT_RTPRIO     = C.RLIMIT_RTPRIO
	RLIMIT_SIGPENDING = C.RLIMIT_SIGPENDING
	RLIMIT_STACK      = C.RLIMIT_STACK
)

type Pollfd C.struct_pollfd
type Rlimit C.struct_rlimit

func Getrlimit(resource int) (rlim Rlimit, err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_GETRLIMIT, uintptr(resource), uintptr(unsafe.Pointer(&rlim)), 0)
	if e1 != 0 {
		err = e1
	}
	return
}

func Poll(fds []Pollfd, timeout int64) (n int, err error) {
	for {
		r0, _, e1 := syscall.Syscall(syscall.SYS_POLL, uintptr(unsafe.Pointer(&fds[0])), uintptr(len(fds)), uintptr(timeout))
		n = int(r0)
		switch e1 {
		case 0:
			return
		case C.EINTR:
			// Retry system call if it returns EINTR
		default:
			err = e1
			return
		}
	}
}
