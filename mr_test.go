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
	"github.com/jsgilmore/shm"
	"testing"
)

func TestShmMemoryRegion(t *testing.T) {
	if !SetupOptional() {
		t.Skip("skipping TestShmMemoryRegion")
		return
	}
	buf, err := shm.NewBufferTmpfs(64<<20, shm.PROT_RDWR)
	if err != nil {
		panic(err)
	}
	mr, err := RegisterMemory(buf.Bytes())
	if err != nil {
		panic(err)
	}
	checkClose(mr)
	checkClose(buf)

	buf, err = shm.NewBufferHugepages(64<<20, shm.PROT_RDWR)
	if err != nil {
		panic(err)
	}
	mr, err = RegisterMemory(buf.Bytes())
	if err != nil {
		panic(err)
	}
	checkClose(mr)
	checkClose(buf)
}
