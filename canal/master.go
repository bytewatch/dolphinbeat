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

package canal

import (
	"github.com/bytewatch/dolphinbeat/canal/prog"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/mysql"
	"sync"
)

type masterInfo struct {
	progress prog.Progress
	sync.RWMutex
}

func (m *masterInfo) Progress() prog.Progress {
	m.RLock()
	defer m.RUnlock()
	return m.progress.Clone()
}

func (m *masterInfo) UpdateProgress(progress prog.Progress) {
	m.Lock()
	m.progress = progress
	m.Unlock()
}

func (m *masterInfo) Update(pos prog.Position) {
	log.Debugf("update master position %s", pos)

	m.Lock()
	m.progress.Update(pos)
	m.Unlock()

}

func (m *masterInfo) UpdateGTIDSet(gset mysql.GTIDSet) {
	log.Debugf("update master gtid set %s", gset)

	m.Lock()
	m.progress.UpdateGTIDSet(gset)
	m.Unlock()
}

func (m *masterInfo) Position() prog.Position {
	m.RLock()
	defer m.RUnlock()
	return m.progress.Position()
}

func (m *masterInfo) GTIDSet() mysql.GTIDSet {
	m.RLock()
	defer m.RUnlock()
	return m.progress.GTIDSet()
}
