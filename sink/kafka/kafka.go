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

package kafka

import (
	"bytes"
	"compress/zlib"
	"context"
	"errors"
	"github.com/badgerodon/collections/stack"
	"github.com/bytewatch/dolphinbeat/canal/prog"
	"github.com/bytewatch/dolphinbeat/ckp"
	"github.com/bytewatch/dolphinbeat/sink"
	"github.com/bytewatch/dolphinbeat/sink/kafka/client/go"
	"github.com/bytewatch/dolphinbeat/sink/kafka/protocol"
	"github.com/golang/protobuf/proto"
	"github.com/siddontang/go-mysql/mysql"
	"gopkg.in/Shopify/sarama.v1"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

const (
	maxOffset = int64(^uint64(0) >> 1)
)

type msgMetadata struct {
	seq uint64
	p   *prog.Progress
}

type KafkaSink struct {
	cfg *Config
	l   sink.Logger

	opCh chan *Operation

	// The kafka offset that have synced into kafka, we need to scan
	// kafka from ackedOffset+1 to recover ackedSeq and ackedProgress, after restart.
	// It can less than zero, means not to scan kafka after restart
	ackedOffset int64
	// The seq that have synced into kafka,
	// so the first seq after restart is ackedSeq+1
	ackedSeq uint64
	// The binlog progress that we have synced into kafka
	ackedProgress prog.Progress

	pid uint64

	// Last seq of constructed kafka msg
	seq      uint64
	producer sarama.AsyncProducer

	maxPayloadSize int

	// Compression type
	compression protocol.Compression
	compressor  *zlib.Writer

	errCh  chan error
	cancel context.CancelFunc

	sync.Mutex
}

func newKafkaSink(name string, cfg *Config, l sink.Logger) (*KafkaSink, error) {

	kafkaVersion, err := sarama.ParseKafkaVersion("0.10.1.0")
	if err != nil {
		return nil, err
	}

	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Version = kafkaVersion
	kafkaCfg.Producer.RequiredAcks = sarama.WaitForAll
	// Specify partitioner manually
	kafkaCfg.Producer.Partitioner = sarama.NewManualPartitioner
	kafkaCfg.Producer.Return.Successes = true
	kafkaCfg.Producer.Return.Errors = true
	producer, err := sarama.NewAsyncProducer(strings.Split(cfg.BrokerList, ","), kafkaCfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	sink := &KafkaSink{
		cfg:            cfg,
		l:              l,
		pid:            uint64(os.Getpid()),
		opCh:           make(chan *Operation, 5120),
		producer:       producer,
		maxPayloadSize: 1 * 1024 * 1024,
		errCh:          make(chan error, 1),
		cancel:         cancel,
	}

	if cfg.MaxPayloadSize > 0 {
		sink.maxPayloadSize = cfg.MaxPayloadSize
	}

	if cfg.Compression == Compression_ZLIB {
		sink.compression = protocol.Compression_ZLIB
		sink.compressor = zlib.NewWriter(ioutil.Discard)
	}

	go sink.run(ctx)

	return sink, nil
}

func (o *KafkaSink) Checkpoint() *ckp.Checkpoint {
	o.Lock()
	defer o.Unlock()
	return ckp.NewCheckpoint().SetProgress(o.ackedProgress).SetUintCtx("acked_seq", o.ackedSeq).SetIntCtx("acked_offset", o.ackedOffset)
}

func (o *KafkaSink) Initialize(ctx context.Context, ckp *ckp.Checkpoint) error {
	o.Lock()
	defer o.Unlock()

	defer func() {
		// Restore seq as ackedSeq
		o.seq = o.ackedSeq
	}()

	o.ackedProgress = ckp.GetProgress()
	o.ackedOffset = ckp.GetIntCtx("acked_offset", maxOffset)
	o.ackedSeq = ckp.GetUintCtx("acked_seq", 0)

	o.l.Infof("properties before recover, progress: %s，acked_offset: %d, acked_seq: %d", o.ackedProgress, o.ackedOffset, o.ackedSeq)

	// Scan kafka from ackedOffset+1
	if o.ackedOffset < -1 {
		o.l.Infof("skip recover, because ackedOffset is less than -1")
		return nil
	}

	kafkaVersion, _ := sarama.ParseKafkaVersion("0.10.1.0")
	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Version = kafkaVersion
	cli, err := sarama.NewClient(strings.Split(o.cfg.BrokerList, ","), kafkaCfg)
	if err != nil {
		o.l.Errorf("new client error: %s", err)
		return err
	}
	defer cli.Close()

	// Get kafka high water mark to check whether we don't need to scan kafka
	hwm, err := cli.GetOffset(o.cfg.Topic, o.cfg.Partition, sarama.OffsetNewest)
	if err != nil {
		o.l.Errorf("get high water mark offset error: %s", err)
		return err
	}
	o.l.Infof("kafka high water: %d, topic: %s, partition: %d", hwm, o.cfg.Topic, o.cfg.Partition)

	if o.ackedOffset == maxOffset {
		// This means we are in first time running
		o.ackedOffset = hwm - 1
	}

	if hwm < o.ackedOffset+1 {
		o.l.Infof("invalid high water mark, it should not less than o.ackedOffset+1")
		return errors.New("invalid kafka topic high water mark")
	}

	if hwm == o.ackedOffset+1 {
		o.l.Infof("there is nothing to scan in kafka to recover")
		return nil
	}

	return o.recover(ctx, cli)

}

func (o *KafkaSink) recover(ctx context.Context, cli sarama.Client) error {
	inputCh := make(chan *client.InputArg, 1024)
	cfg := client.Config{InputCh: inputCh, LastCommitSeq: o.ackedSeq, Logger: o.l}
	decoder, err := client.NewOperationDecoder(&cfg)
	if err != nil {
		o.l.Errorf("new decoder error: %s", err)
		return err
	}
	defer decoder.Close()

	consumer, err := sarama.NewConsumerFromClient(cli)
	if err != nil {
		o.l.Errorf("new consumer error: %s", err)
		return err
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(o.cfg.Topic, o.cfg.Partition, o.ackedOffset+1)
	if err != nil {
		o.l.Errorf("new partition consumer error: %s", err)
		return err
	}
	defer partitionConsumer.Close()

	messagesCh := partitionConsumer.Messages()
	for {
		select {
		case msg := <-messagesCh:
			inputCh <- &client.InputArg{Data: msg.Value, Offset: msg.Offset}
			if msg.Offset == partitionConsumer.HighWaterMarkOffset()-1 {
				// We have received all kafka msgs
				close(inputCh)
				messagesCh = nil
			}

		case outputResult := <-decoder.Output():
			if outputResult == nil {
				// Decoder quited
				if err := <-decoder.Err(); err != nil {
					o.l.Errorf("decoder quited with error: %s", err)
					return err
				}
				// We have decode all kafka msgs
				o.l.Warnf("properties after recover, progress: %s，acked_offset: %d, acked_seq: %d", o.ackedProgress, o.ackedOffset, o.ackedSeq)
				return nil
			}
			o.ackedOffset = outputResult.CommitOffset
			o.ackedSeq = outputResult.CommitSeq
			for _, op := range outputResult.Ops {
				if op.Header.Type == protocol.OperationType_COMMIT ||
					op.Header.Type == protocol.OperationType_DDL {
					o.ackedProgress = o.makeProgress(op.Progress)
				}
			}

		case <-ctx.Done():
			o.l.Warnf("context canceled")
			return ctx.Err()
		}

	}

	return nil
}

func (o *KafkaSink) Err() <-chan error {
	return o.errCh
}

func (o *KafkaSink) Close() error {
	o.l.Warnf("closing kafka sink")

	o.cancel()
	for range o.errCh {
		// drain
	}

	close(o.opCh)

	o.producer.Close()

	if o.compressor != nil {
		o.compressor.Close()
	}

	return nil
}

func (o *KafkaSink) run(ctx context.Context) {
	defer close(o.errCh)

	var needProduce bool
	var ops []*protocol.Operation
	rowCount := 0
	continuousEmptyTrxCount := 0

	for {
		needProduce = false
		select {
		case op := <-o.opCh:
			ops = append(ops, op.op)
			if op.op.Header.Type == protocol.OperationType_BEGIN {
				rowCount = 0
			}

			if op.op.Header.Type == protocol.OperationType_INSERT ||
				op.op.Header.Type == protocol.OperationType_UPDATE ||
				op.op.Header.Type == protocol.OperationType_DELETE {
				rowCount += len(op.op.Rows)
			}

			if op.op.Header.Type == protocol.OperationType_COMMIT && rowCount == 0 {
				// This is a empty trx, try to produce later when we gather enough
				continuousEmptyTrxCount++
				o.Lock()
				o.ackedProgress = *op.p
				o.Unlock()
			}

			if op.op.Header.Type == protocol.OperationType_ROTATE ||
				op.op.Header.Type == protocol.OperationType_DDL {
				needProduce = true
			}

			if op.op.Header.Type == protocol.OperationType_COMMIT && (rowCount != 0 || continuousEmptyTrxCount >= 1000) {
				needProduce = true
			}

			if needProduce {
				// This op is end of trx, we need to send all ops
				err := o.produce(ops, op.p)
				if err != nil {
					o.l.Errorf("produce msg to kafka error: %s", err)
					o.errCh <- err
					return
				}
				ops = nil
				continuousEmptyTrxCount = 0
			}

		case producerMsg := <-o.producer.Successes():
			metadata := producerMsg.Metadata.(*msgMetadata)
			o.l.Debugf("acked kafka msg, seq: %d, offset: %d", metadata.seq, producerMsg.Offset)
			if metadata.seq != o.ackedSeq+1 {
				o.l.Panicf("acked seq is not unexcepted: %d, excepted: %d", metadata.seq, o.ackedSeq+1)
			}
			o.Lock()
			o.ackedOffset = producerMsg.Offset
			o.ackedSeq = metadata.seq
			if metadata.p != nil {
				o.ackedProgress = *metadata.p
			}
			o.Unlock()

		case producerErr := <-o.producer.Errors():
			o.l.Errorf("reported kafka error: %s", producerErr.Err)

		case <-ctx.Done():
			o.l.Warnf("context canceled")
			return
		}
	}
}

func (o *KafkaSink) produce(ops []*protocol.Operation, p *prog.Progress) error {
	stack := stack.New()
	stack.Push(ops)

	for stack.Len() != 0 {
		poped := stack.Pop().([]*protocol.Operation)
		payload, err := o.marshalOps(poped)
		if err != nil {
			return err
		}

		if len(payload) > o.maxPayloadSize {
			o.l.Infof("ops's size is too large, need to divide into two small ops")
			len := len(poped)
			if len != 1 {
				stack.Push(poped[len/2:])
				stack.Push(poped[:len/2])
				continue
			}

			// This op is too big, need to make fragments further.
			err = o.doProduce(payload, p)
			if err != nil {
				return err
			}
		}
		err = o.doProduce(payload, p)
		if err != nil {
			return err
		}

	}
	return nil
}

func (o *KafkaSink) newMessage(groupId uint64, payload []byte) *protocol.Message {
	o.seq++
	return &protocol.Message{
		ProducerId:  o.pid,
		Seq:         o.seq,
		GroupId:     groupId,
		Compression: o.compression,
		Payload:     payload,
	}
}

// doProduce send payload of bytes to kafka. If payload is too large to send,
// doProduce will make fragments, like IPv4 fragments.
func (o *KafkaSink) doProduce(payload []byte, p *prog.Progress) error {
	var marshaledMsgs [][]byte
	var msgMetadatas []*msgMetadata

	groupId := o.seq + 1
	if len(payload) < o.maxPayloadSize {
		msg := o.newMessage(groupId, payload)
		data, err := proto.Marshal(msg)
		if err != nil {
			o.l.Errorf("marshal protocol.Message error: %s", err)
			return err
		}
		marshaledMsgs = append(marshaledMsgs, data)
		msgMetadatas = append(msgMetadatas, &msgMetadata{seq: msg.Seq, p: p})
	} else {
		o.l.Infof("payload's size is too large, need to make fragments")
		// We need to make fragments for this big payload
		var fragment []byte
		var msg *protocol.Message
		for payload != nil {
			if len(payload) > o.maxPayloadSize {
				fragment = payload[:o.maxPayloadSize]
				payload = payload[o.maxPayloadSize:]
				msg = o.newMessage(groupId, fragment)
				msg.MoreFragment = true
			} else {
				// This is the last fragment
				fragment = payload
				payload = nil
				msg = o.newMessage(groupId, fragment)
			}

			data, err := proto.Marshal(msg)
			if err != nil {
				o.l.Errorf("marshal protocol.Message error: %s", err)
				return err
			}
			marshaledMsgs = append(marshaledMsgs, data)
			msgMetadatas = append(msgMetadatas, &msgMetadata{seq: msg.Seq, p: p})
		}
	}

	for i, data := range marshaledMsgs {
		var producerMsg *sarama.ProducerMessage
		producerMsg = &sarama.ProducerMessage{Topic: o.cfg.Topic, Partition: o.cfg.Partition}
		producerMsg.Value = sarama.ByteEncoder(data)
		producerMsg.Metadata = msgMetadatas[i]
		select {
		case o.producer.Input() <- producerMsg:
		}
	}

	return nil

}

// Return the bytes of protocol.Payload
func (o *KafkaSink) marshalOps(ops []*protocol.Operation) ([]byte, error) {
	var payload protocol.Payload
	payload.Ops = ops
	data, err := proto.Marshal(&payload)
	if err != nil {
		o.l.Errorf("marshal protocol.Operation error: %s", err)
		return nil, err
	}

	// Compress data
	if o.compression == protocol.Compression_ZLIB {
		var wb bytes.Buffer
		o.compressor.Reset(&wb)
		_, err = o.compressor.Write(data)
		if err != nil {
			o.l.Errorf("compress data error: %s", err)
			return nil, err
		}
		err = o.compressor.Close()
		if err != nil {
			o.l.Errorf("compress data error: %s", err)
			return nil, err
		}
		data = wb.Bytes()
	}

	return data, nil
}

func (o *KafkaSink) makeProgress(p *protocol.Progress) prog.Progress {
	var progress prog.Progress

	pos := prog.Position{Name: p.LogName, Pos: p.LogPos, ServerID: p.ServerId}
	progress.Update(pos)

	if p.Gset != "" {
		gset, err := mysql.ParseGTIDSet(p.Flavor, p.Gset)
		if err != nil {
			o.l.Panicf("parse gtid set error: %s, gtid: %s", err, p.Gset)
		}
		progress.UpdateGTIDSet(gset)
	}
	return progress
}
