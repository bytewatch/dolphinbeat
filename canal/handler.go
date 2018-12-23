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
	"github.com/siddontang/go-mysql/replication"
)

type EventHandler interface {
	OnBegin(h *replication.EventHeader) error
	OnCommit(h *replication.EventHeader, p prog.Progress) error

	OnQuery(h *replication.EventHeader, e *replication.QueryEvent) error
	OnDDL(h *replication.EventHeader, e *replication.QueryEvent, p prog.Progress) error
	OnRow(h *replication.EventHeader, e *RowsEvent) error

	OnGTID(h *replication.EventHeader, e *GtidEvent) error

	OnRotate(h *replication.EventHeader, e *replication.RotateEvent) error

	String() string
}

// `SetEventHandler` registers the sync handler, you must register your
// own handler before starting Canal.
func (c *Canal) SetEventHandler(h EventHandler) {
	c.eventHandler = h
}

type DummyEventHandler struct {
}

func (o *DummyEventHandler) OnBegin(h *replication.EventHeader) error                   { return nil }
func (o *DummyEventHandler) OnCommit(h *replication.EventHeader, p prog.Progress) error { return nil }

func (o *DummyEventHandler) OnQuery(h *replication.EventHeader, e *replication.QueryEvent) error {
	return nil
}

func (o *DummyEventHandler) OnDDL(h *replication.EventHeader, e *replication.QueryEvent, p prog.Progress) error {
	return nil
}

func (o *DummyEventHandler) OnRow(h *replication.EventHeader, e *RowsEvent) error {
	return nil
}

func (o *DummyEventHandler) OnGTID(h *replication.EventHeader, e *GtidEvent) error {
	return nil
}

func (o *DummyEventHandler) OnRotate(h *replication.EventHeader, e *replication.RotateEvent) error {
	return nil
}

func (o *DummyEventHandler) String() string { return "DummyEventHandler" }
