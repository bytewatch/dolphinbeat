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

package ckp

import (
	"encoding/json"
	"github.com/bytewatch/dolphinbeat/canal/prog"
	"time"
)

func makeProgress(name string, pos uint32) prog.Progress {
	return prog.Progress{
		Pos: prog.Position{Name: name, Pos: pos},
	}
}

func makeKafkaCkp() *Checkpoint {
	ckp := NewCheckpoint().SetProgress(makeProgress("mysql-bin.000002", 99))
	ckp.SetStringCtx("topic", "topic_test").SetIntCtx("partition", 0).SetIntCtx("offset", 102333)
	return ckp
}

func makeEsCkp() *Checkpoint {
	ckp := NewCheckpoint().SetProgress(makeProgress("mysql-bin.000002", 100))
	return ckp
}

func makeData() []byte {
	data := &Data{
		Time: time.Now().Truncate(0),
		Ckps: map[string]*Checkpoint{
			"kafka": makeKafkaCkp(),
			"es":    makeEsCkp(),
		},
	}
	buf, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	return buf
}

type MockStorage struct {
}

func NewMockStorage() *MockStorage {
	return &MockStorage{}
}

func (o *MockStorage) Save(data []byte) error {
	return nil
}

func (o *MockStorage) Load() ([]byte, error) {
	return makeData(), nil
}
func (o *MockStorage) Close() error {
	return nil
}
