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

package client

import (
	"bytes"
	"compress/zlib"
	"context"
	"fmt"
	"github.com/bytewatch/dolphinbeat/sink/kafka/protocol"
	"github.com/golang/protobuf/proto"
	"io"
)

// InputArg holds kafka message data and offset.User send InputArg to the decoder,
// and the decoder will decode kafka message and return OutputResult to user.
type InputArg struct {
	Data   []byte
	Offset int64
}

// OutputResult holds operations decoded from kafka message, seq info, offset info and commit info.
// Once user has handled/consumed this OutputResult, user can save/commit the commit info. After user restart,
// user can continue consume kafka from offset at CommitOffset+1.
type OutputResult struct {
	Ops          []*protocol.Operation
	BeginSeq     uint64
	EndSeq       uint64
	BeginOffset  int64
	EndOffset    int64
	CommitOffset int64
	CommitSeq    uint64
}

type groupInfo struct {
	groupId     uint64
	beginSeq    uint64
	endSeq      uint64
	beginOffset int64
	endOffset   int64
	compression protocol.Compression
}

type OperationDecoder struct {
	curProducerId uint64
	curSeq        uint64

	curGroup  groupInfo
	fragments [][]byte

	// If we have received all fragments of cur group, we need to dispatch
	needDispatch bool

	logger Logger

	inputCh  <-chan *InputArg
	outputCh chan *OutputResult

	errCh  chan error
	cancel context.CancelFunc
}

// NewOperationDecoder need to know last CommitSeq of last OutputResult user has handled/consumed,
// it can be 0 if user first time create decoder, or user didn't save CommitSeq.
func NewOperationDecoder(cfg *Config) (*OperationDecoder, error) {
	ctx, cancel := context.WithCancel(context.Background())
	decoder := OperationDecoder{
		inputCh:  cfg.InputCh,
		curSeq:   cfg.LastCommitSeq,
		outputCh: make(chan *OutputResult, 1024),
		errCh:    make(chan error, 1),
		logger:   DefaultLogger,
		cancel:   cancel,
	}

	if cfg.Logger != nil {
		decoder.logger = cfg.Logger
	}

	go decoder.run(ctx)
	return &decoder, nil
}

func (o *OperationDecoder) Close() error {
	o.cancel()
	for range o.errCh {
		// drain
	}
	o.fragments = nil
	return nil
}

func (o *OperationDecoder) Output() <-chan *OutputResult {
	return o.outputCh
}

func (o *OperationDecoder) Err() <-chan error {
	return o.errCh
}

func (o *OperationDecoder) run(ctx context.Context) {
	defer close(o.errCh)

	// When we quit, send a nil outputResult
	defer close(o.outputCh)

	for {
		select {
		case arg := <-o.inputCh:
			if arg == nil {
				// The inputCh has been closed, need to quit
				return
			}

			err := o.feed(arg)
			if err != nil {
				o.errCh <- err
				return
			}
			if o.needDispatch {
				err := o.dispatch()
				if err != nil {
					o.errCh <- err
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}

}

func (o *OperationDecoder) feed(arg *InputArg) error {
	marshaledMsg := arg.Data
	offset := arg.Offset

	msg := protocol.Message{}
	err := proto.Unmarshal(marshaledMsg, &msg)
	if err != nil {
		return fmt.Errorf("unmarshal protocol.Message error: %s", err)
	}

	if o.curSeq == 0 {
		// Init decode's curSeq
		o.curSeq = msg.Seq - 1
	}

	if msg.Seq != o.curSeq+1 {
		// ignore this duplicated message, or disordered message
		o.logger.Printf("ignore unexpected message, offset: %d, seq: %d, expected seq: %d", offset, msg.Seq, o.curSeq+1)
		return nil
	}

	if msg.ProducerId != o.curProducerId && len(o.fragments) > 0 {
		// If the kafka is produced by different producer,
		// we drop the uncompleted fragments
		o.logger.Printf("meet a new producer, drop uncompleted fragments, size: %d", len(o.fragments))
		o.fragments = nil
	}

	o.curSeq = msg.Seq

	if len(o.fragments) == 0 {
		o.curGroup.groupId = msg.GroupId
		o.curGroup.beginSeq = msg.Seq
		o.curGroup.beginOffset = offset
		o.curGroup.compression = msg.Compression
	} else {
		if msg.GroupId != o.curGroup.groupId {
			// The current group is not done, we can't meet a new group.
			// Just panic, only bug will come into here
			o.logger.Printf("unexcepted new group of fragments, offset: %d, seq: %d", offset, msg.Seq)
			panic("unexcepted new group of fragments")
		}
	}

	o.fragments = append(o.fragments, msg.Payload)

	if !msg.MoreFragment {
		// This is the last fragment of current group, we can marshal now
		o.curGroup.endSeq = msg.Seq
		o.curGroup.endOffset = offset
		o.needDispatch = true
	}

	return nil
}

func (o *OperationDecoder) dispatch() error {
	marshaledPayload := o.fragments[0]
	totalSize := 0
	for _, fragment := range o.fragments {
		totalSize += len(fragment)
	}
	if len(o.fragments) > 1 {
		// There are fragments, need to merge into one buf
		marshaledPayload = make([]byte, totalSize)
		offset := 0
		for _, fragment := range o.fragments {
			copy(marshaledPayload[offset:], fragment)
			offset += len(fragment)
		}
	}

	ops, err := o.unmarshalOps(marshaledPayload, o.curGroup.compression)
	if err != nil {
		return err
	}

	// Clear cached fragments
	o.fragments = nil
	o.needDispatch = false

	result := &OutputResult{
		BeginSeq:     o.curGroup.beginSeq,
		EndSeq:       o.curGroup.endSeq,
		Ops:          ops,
		BeginOffset:  o.curGroup.beginOffset,
		EndOffset:    o.curGroup.endOffset,
		CommitOffset: o.curGroup.endOffset,
		CommitSeq:    o.curSeq,
	}

	select {
	case o.outputCh <- result:
	}

	return nil
}

func (o *OperationDecoder) unmarshalOps(data []byte, compression protocol.Compression) ([]*protocol.Operation, error) {
	if compression == protocol.Compression_ZLIB {
		rb := bytes.NewBuffer(data)
		decompressor, err := zlib.NewReader(rb)
		if err != nil {
			return nil, fmt.Errorf("new zlib reader error: %s", err)
		}
		defer decompressor.Close()
		wb := new(bytes.Buffer)
		_, err = io.Copy(wb, decompressor)
		if err != nil {
			return nil, fmt.Errorf("decompress error: %s", err)
		}
		data = wb.Bytes()
	}

	payload := protocol.Payload{}
	err := proto.Unmarshal(data, &payload)
	if err != nil {
		return nil, fmt.Errorf("unmarshal protocol.Payload error: %s", err)
	}

	return payload.Ops, nil
}
