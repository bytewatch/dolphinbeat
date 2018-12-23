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

package stdout

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/bytewatch/dolphinbeat/canal/prog"
	"github.com/bytewatch/dolphinbeat/ckp"
	"github.com/bytewatch/dolphinbeat/sink"
	"os"
	"sync"
)

var nl = []byte("\n")

type StdoutSink struct {
	name string

	cfg *Config
	l   sink.Logger

	opCh chan *Operation

	writer *bufio.Writer

	// The binlog progress that we have synced into file
	progress prog.Progress

	errCh  chan error
	cancel context.CancelFunc

	sync.Mutex
}

func newStdoutSink(name string, cfg *Config, l sink.Logger) *StdoutSink {
	ctx, cancel := context.WithCancel(context.Background())
	sink := &StdoutSink{
		name:   name,
		cfg:    cfg,
		l:      l,
		opCh:   make(chan *Operation, 5120),
		writer: bufio.NewWriterSize(os.Stdout, 10*1204),
		errCh:  make(chan error, 1),
		cancel: cancel,
	}

	go sink.run(ctx)
	return sink
}

func (o *StdoutSink) Checkpoint() *ckp.Checkpoint {
	o.Lock()
	defer o.Unlock()
	return ckp.NewCheckpoint().SetProgress(o.progress)
}

func (o *StdoutSink) Initialize(ctx context.Context, ckp *ckp.Checkpoint) error {
	o.progress = ckp.GetProgress()
	return nil
}

func (o *StdoutSink) Err() <-chan error {
	return o.errCh
}

func (o *StdoutSink) Close() error {
	o.l.Warnf("closing stdout sink")

	o.cancel()
	for range o.errCh {
		// drain
	}

	close(o.opCh)

	return nil
}

func (o *StdoutSink) run(ctx context.Context) {
	defer close(o.errCh)

	for {
		select {
		case op := <-o.opCh:
			data, err := json.MarshalIndent(op, "", "  ")
			if err != nil {
				o.l.Errorf("json marshal error: %s", err)
				o.errCh <- err
				return
			}
			err = o.writeBuffer(data)
			if err != nil {
				o.l.Errorf("write buffer error: %s", err)
				o.errCh <- err
				return
			}

			if op.p != nil {
				// This op is end of trx, we need advance our progress
				func() {
					o.Lock()
					defer o.Unlock()
					o.progress = *op.p
				}()
			}
			err = o.writeBuffer(nl)
			o.writer.Flush()
			if err != nil {
				o.l.Errorf("write buffer error: %s", err)
				o.errCh <- err
				return
			}

		case <-ctx.Done():
			o.l.Warnf("context canceled")
			return
		}
	}
}

func (c *StdoutSink) writeBuffer(buf []byte) error {
	written := 0
	for written < len(buf) {
		n, err := c.writer.Write(buf[written:])
		if err != nil {
			return err
		}

		written += n
	}
	return nil
}
