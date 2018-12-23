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
	"github.com/siddontang/go-mysql/mysql"
)

type Progress struct {
	Pos  Position
	Gset mysql.GTIDSet
}

func (o *Progress) Update(pos Position) {
	o.Pos = pos
}

func (o *Progress) UpdateGTIDSet(gset mysql.GTIDSet) {
	o.Gset = gset
}

func (o *Progress) Position() Position {
	return o.Pos
}

func (o *Progress) GTIDSet() mysql.GTIDSet {
	if o.Gset == nil {
		return nil
	}
	return o.Gset.Clone()
}

func (o *Progress) Clone() Progress {
	var p Progress
	p.Pos = o.Pos
	if o.Gset != nil {
		p.Gset = o.Gset.Clone()
	}
	return p
}

func (o *Progress) IsZero() bool {
	if o.Gset != nil {
		if o.Gset.String() == "" {
			return true
		} else {
			return false
		}
	}

	if o.Pos.Name == "" && o.Pos.Pos == 0 {
		return true
	}

	return false
}

// Use file&pos to compare, can not use gtid_set, because
// gtid_set may not contain each other
func (o *Progress) Compare(p *Progress) int {

	if o.IsZero() {
		if p.IsZero() {
			return 0
		}
		return -1
	}

	if p.IsZero() {
		return 1
	}

	return o.Pos.Compare(p.Pos)
}

func (o *Progress) String() string {
	if o.Gset != nil {
		return o.Gset.String()
	}
	return o.Pos.String()
}
