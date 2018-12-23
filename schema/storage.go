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

package schema

import (
	"github.com/bytewatch/dolphinbeat/canal/prog"
)

type SchemaStorage interface {
	// SaveSnapshot will be called when schema tracker decides to save a snapshot
	SaveSnapshot(data []byte, pos prog.Position) error

	// SaveSnapshot will be called when schema tracker decides to save a ddl statement
	SaveStatement(db string, statement string, pos prog.Position) error

	// LoadLastSnapshot will be called when schema tracker need to restore snapshot, as base data
	LoadLastSnapshot() ([]byte, prog.Position, error)

	// LoadNextStatement will be called when schema tracker need to restore a ddl statement, as delta data
	LoadNextStatement(prePos prog.Position) (string, string, prog.Position, error)

	// Reset will be called to get a empty storage
	Reset() error
}
