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

package sink

import (
	"context"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/bytewatch/dolphinbeat/canal"
	"github.com/bytewatch/dolphinbeat/ckp"
)

type Sink interface {
	canal.EventHandler
	// Checkpoint will be called periodically, sink need to expose
	// it's progress and corresponding ctx, and these info will
	// be persisted in ZooKeeper
	ckp.Checkpointer

	// Initialize will be called at process start, you need to
	// recover state from the point that checkpointed last time
	Initialize(ctx context.Context, checkpoint *ckp.Checkpoint) error

	// You need to report error by this channel.
	Err() <-chan error

	Close() error
}

type Factory func(name string, primCfg PrimitiveConfig, logger Logger) (Sink, error)

var sinkReg = map[string]Factory{}

func RegisterSink(tp string, f Factory) {
	sinkReg[tp] = f
}

func NewSink(tp string, name string, primCfg toml.Primitive, logger Logger) (Sink, error) {
	factory, ok := sinkReg[tp]
	if !ok {
		return nil, fmt.Errorf("unknow sink type: %s", tp)
	}
	return factory(name, PrimitiveConfig(primCfg), logger)
}
