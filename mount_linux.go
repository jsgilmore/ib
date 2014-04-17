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

import (
	"github.com/jsgilmore/mount"
	"os"
)

const mountDir = "/run/next/test/mnt"

var setupDone = false

func SetupOptional() bool {
	if setupDone {
		return true
	}
	if !mount.InMountNamespace() {
		return false
	}
	mount.MountNamespace()
	setup()
	setupDone = true
	return true
}

func setup() {
	// zero-sized directory just for mounts
	if err := os.MkdirAll(mountDir, 0777); err != nil {
		panic(err)
	}
	if err := mount.MountTmpfs(mountDir, 0); err != nil {
		panic(err)
	}

	if err := mount.MountTmpfs("/dev/shm", 14<<30); err != nil {
		panic(err)
	}
	if err := mount.MountHugetlbfs("/dev/hugepages", 2<<20, 4<<30); err != nil {
		panic(err)
	}
	setupDone = true
}
