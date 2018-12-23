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
	"github.com/bytewatch/dolphinbeat/canal"
	"github.com/bytewatch/dolphinbeat/canal/prog"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/replication"
)

func (o *StdoutSink) OnRotate(h *replication.EventHeader, e *replication.RotateEvent) error {
	op := makeRotateOp(h, e)
	o.opCh <- op
	return nil
}

func (o *StdoutSink) OnDDL(h *replication.EventHeader, e *replication.QueryEvent, p prog.Progress) error {
	op := makeDDLOp(h, e, p)
	o.opCh <- op
	return nil
}

func (o *StdoutSink) OnQuery(h *replication.EventHeader, e *replication.QueryEvent) error {
	op := makeQueryOp(h, e)
	o.opCh <- op
	return nil
}

func (o *StdoutSink) OnBegin(h *replication.EventHeader) error {
	op := makeBeginOp(h)
	o.opCh <- op
	return nil
}

func (o *StdoutSink) OnCommit(h *replication.EventHeader, progress prog.Progress) error {
	op := makeCommitOp(h, progress)
	o.opCh <- op
	return nil
}

func (o *StdoutSink) OnRow(h *replication.EventHeader, e *canal.RowsEvent) error {
	var op *Operation
	switch e.Action {
	case canal.InsertAction:
		op = makeInsertOp(h, e)
	case canal.DeleteAction:
		op = makeDeleteOp(h, e)
	case canal.UpdateAction:
		op = makeUpdateOp(h, e)
	default:
		return errors.Errorf("invalid rows action %s", e.Action)
	}

	o.opCh <- op

	return nil
}

func (o *StdoutSink) OnGTID(h *replication.EventHeader, e *canal.GtidEvent) error {
	op := makeGtidOp(h, e)
	o.opCh <- op
	return nil
}

func (o *StdoutSink) String() string {
	return "StdoutSinkEventHandler"
}
