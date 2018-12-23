// Copyright 2019 siddontang All Rights Reserved.
//
// Licensed under the MIT License;
// License can be found in the LICENSES/go-mysql-LICENSE.
//
// Copyright 2019 ByteWatch All Rights Reserved.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prog

import (
	"fmt"
)

type Position struct {
	Name     string
	Pos      uint32
	ServerID uint32
}

func (p Position) Compare(o Position) int {
	if p.ServerID != o.ServerID {
		panic(fmt.Sprintf("unsupported comparison between different server id: %d != %d", p.ServerID, o.ServerID))
	}
	// First compare binlog name
	if p.Name > o.Name {
		return 1
	} else if p.Name < o.Name {
		return -1
	} else {
		// Same binlog file, compare position
		if p.Pos > o.Pos {
			return 1
		} else if p.Pos < o.Pos {
			return -1
		} else {
			return 0
		}
	}
}

func (p Position) String() string {
	return fmt.Sprintf("%s:%d", p.Name, p.Pos)
}
