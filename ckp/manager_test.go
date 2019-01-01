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
	"github.com/bytewatch/dolphinbeat/canal/prog"
	"github.com/stretchr/testify/require"
	"testing"
)

func makeMockProgress(name string, pos uint32) prog.Progress {
	return prog.Progress{
		Pos: prog.Position{Name: name, Pos: pos},
	}
}

func TestGetMinProgress(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.Storage = StorageType_Mock
	manager, err := NewCkpManager(cfg)
	require.Nil(t, err)

	ckper1 := NewMockCheckpointer(makeMockProgress("mysql-bin.000002", 99))
	ckper2 := NewMockCheckpointer(makeMockProgress("mysql-bin.000002", 199))
	manager.RegisterCheckpointer("kafka1", ckper1)
	manager.RegisterCheckpointer("kafka2", ckper2)
	require.Equal(t, makeMockProgress("mysql-bin.000002", 99), manager.GetMinProgress())
}

func TestGetCheckpoint(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.Storage = StorageType_Mock
	manager, err := NewCkpManager(cfg)
	require.Nil(t, err)

	ckper := NewMockCheckpointer(makeMockProgress("mysql-bin.000002", 99))
	manager.RegisterCheckpointer("kafka", ckper)
	require.Equal(t, ckper.Checkpoint(), manager.GetCheckpoint("kafka"))
}
