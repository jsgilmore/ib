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

import "C"

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"syscall"
)

func newError(name string, errno C.int) error {
	if errno > 0 {
		return os.NewSyscallError(name, syscall.Errno(errno))
	}
	if errno < 0 {
		// generic error for functions that don't set errno
		return errors.New(name + ": failure")
	}
	return nil
}

func readQPParams(r io.Reader) (destLid uint16, destQpn, destPsn uint32, err error) {
	err = binary.Read(r, binary.LittleEndian, &destLid)
	if err != nil {
		return
	}
	err = binary.Read(r, binary.LittleEndian, &destQpn)
	if err != nil {
		return
	}
	err = binary.Read(r, binary.LittleEndian, &destPsn)
	return
}

func writeQPParams(w io.Writer, lid uint16, qpn, psn uint32) (err error) {
	err = binary.Write(w, binary.LittleEndian, &lid)
	if err != nil {
		return
	}
	err = binary.Write(w, binary.LittleEndian, &qpn)
	if err != nil {
		return
	}
	err = binary.Write(w, binary.LittleEndian, &psn)
	return
}

func readWriteQPParams(rw io.ReadWriter, lid uint16, qpn, psn uint32) (destLid uint16, destQpn, destPsn uint32, err error) {
	destLid, destQpn, destPsn, err = readQPParams(rw)
	if err != nil {
		return
	}
	err = writeQPParams(rw, lid, qpn, psn)
	return
}

func writeReadQPParams(rw io.ReadWriter, lid uint16, qpn, psn uint32) (destLid uint16, destQpn, destPsn uint32, err error) {
	err = writeQPParams(rw, lid, qpn, psn)
	if err != nil {
		return
	}
	destLid, destQpn, destPsn, err = readQPParams(rw)
	return
}

type closer interface {
	Close() error
}

func checkClose(c closer) {
	if err := c.Close(); err != nil {
		panic(err)
	}
}
