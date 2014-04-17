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
	"testing"
)

func TestNewQPClose(t *testing.T) {
	for _, iface := range guidToInterface {
		qp, err := iface.NewQueuePair(10)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%+v\n", qp.Query())
		// this failed when NewQueuePair didn't Reset and Init the QP
		if err := qp.Close(); err != nil {
			panic(err)
		}
	}
}
