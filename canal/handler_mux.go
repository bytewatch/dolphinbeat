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
	"fmt"
	"github.com/bytewatch/dolphinbeat/canal/prog"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/replication"
	"regexp"
)

type EventHandlerConfig struct {
	// You need tell canal the progress that you have synced,
	// so canal can know the evnets are duplicated or not for you,
	// and filter the duplicated events.
	// However, if current MySQL's server_id is not the same with
	// the server_id of this progress, canal can NOT recognize duplicated events.
	Progress     prog.Progress
	IncludeTable []string
	ExcludeTable []string
}

type EventHandlerWrapper struct {
	// Whether we have skip all duplicated event/trx
	catchUp  bool
	progress prog.Progress

	includeTableRegex []*regexp.Regexp
	excludeTableRegex []*regexp.Regexp
	ignoredTableCache map[string]bool

	h EventHandler
}

// IsIgnored tells whether this handler need to ignore the data of specified table
func (o *EventHandlerWrapper) IsIgnored(database string, table string) bool {

	if len(o.includeTableRegex) == 0 && len(o.excludeTableRegex) == 0 {
		return false
	}

	key := fmt.Sprintf("%s.%s", database, table)

	ignored, ok := o.ignoredTableCache[key]
	if ok {
		return ignored
	}

	ignored = true
	for _, exp := range o.includeTableRegex {
		if exp.MatchString(key) {
			ignored = false
			break
		}
	}

	for _, exp := range o.excludeTableRegex {
		if exp.MatchString(key) {
			ignored = true
			break
		}
	}
	o.ignoredTableCache[key] = ignored
	return ignored
}

// IsEventDuplicated tell whether this handler has received the event at sometime before
func (o *EventHandlerWrapper) IsEventDuplicated(serverID uint32, logName string, logPos uint32) bool {
	// TODO: Support filter by gtid of current evnet group
	if o.catchUp {
		return false
	}
	pos := prog.Position{
		ServerID: serverID,
		Name:     logName,
		Pos:      logPos,
	}

	if o.progress.IsZero() ||
		pos.ServerID != o.progress.Position().ServerID ||
		pos.Compare(o.progress.Position()) > 0 {
		// If server_id is different, just treat as new event, although event's content may be duplicated
		o.catchUp = true
		return false
	}

	log.Warnf("duplicated event need to skip: %s, handler: %s", pos, o.h.String())

	return true
}

type HandlerMux struct {
	handlers map[string]*EventHandlerWrapper

	serverID uint32
	logName  string
}

func NewHandlerMux() *HandlerMux {
	return &HandlerMux{
		handlers: make(map[string]*EventHandlerWrapper),
	}
}

func (o *HandlerMux) RegisterEventHandler(h EventHandler, cfg EventHandlerConfig) {
	name := h.String()
	var includeTableRegex []*regexp.Regexp
	var excludeTableRegex []*regexp.Regexp

	for _, v := range cfg.IncludeTable {
		exp := regexp.MustCompile(v)
		includeTableRegex = append(includeTableRegex, exp)
	}
	for _, v := range cfg.ExcludeTable {
		exp := regexp.MustCompile(v)
		excludeTableRegex = append(excludeTableRegex, exp)
	}

	handler := &EventHandlerWrapper{
		progress:          cfg.Progress,
		includeTableRegex: includeTableRegex,
		excludeTableRegex: excludeTableRegex,
		ignoredTableCache: make(map[string]bool),
		h:                 h,
	}
	o.handlers[name] = handler
}

func (o *HandlerMux) OnRotate(h *replication.EventHeader, e *replication.RotateEvent) error {
	var err error
	if h.Timestamp == 0 {
		// We get current Master's server_id from fake rotate event.
		// Update server id and log name
		o.serverID = h.ServerID
		o.logName = string(e.NextLogName)
	}

	for name, handler := range o.handlers {
		err = handler.h.OnRotate(h, e)
		if err != nil {
			log.Errorf("handle rotate event error: %s, handler: %s", err, name)
			return err
		}
	}
	return nil
}

func (o *HandlerMux) OnBegin(h *replication.EventHeader) error {
	var err error
	for name, handler := range o.handlers {
		if handler.IsEventDuplicated(o.serverID, o.logName, h.LogPos) {
			continue
		}
		err = handler.h.OnBegin(h)
		if err != nil {
			log.Errorf("handle begin event error: %s, handler: %s", err, name)
			return err
		}
	}
	return nil
}

func (o *HandlerMux) OnCommit(h *replication.EventHeader, p prog.Progress) error {
	var err error
	for name, handler := range o.handlers {
		if handler.IsEventDuplicated(o.serverID, o.logName, h.LogPos) {
			continue
		}
		err = handler.h.OnCommit(h, p)
		if err != nil {
			log.Errorf("handle commit event error: %s, handler: %s", err, name)
			return err
		}
	}
	return nil
}

func (o *HandlerMux) OnQuery(h *replication.EventHeader, e *replication.QueryEvent) error {
	return nil
}

func (o *HandlerMux) OnDDL(h *replication.EventHeader, e *replication.QueryEvent, p prog.Progress) error {
	var err error
	for name, handler := range o.handlers {
		if handler.IsEventDuplicated(o.serverID, o.logName, h.LogPos) {
			continue
		}
		err = handler.h.OnDDL(h, e, p)
		if err != nil {
			log.Errorf("handle ddl event error: %s, handler: %s", err, name)
			return err
		}
	}
	return nil
}

func (o *HandlerMux) OnRow(h *replication.EventHeader, e *RowsEvent) error {
	var err error
	for name, handler := range o.handlers {
		if handler.IsEventDuplicated(o.serverID, o.logName, h.LogPos) {
			continue
		}
		if handler.IsIgnored(e.Table.Database, e.Table.Name) {
			continue
		}
		err = handler.h.OnRow(h, e)
		if err != nil {
			log.Errorf("handle row event error: %s, handler: %s", err, name)
			return err
		}
	}

	return nil
}

func (o *HandlerMux) OnGTID(h *replication.EventHeader, e *GtidEvent) error {
	var err error
	for name, handler := range o.handlers {
		if handler.IsEventDuplicated(o.serverID, o.logName, h.LogPos) {
			continue
		}
		err = handler.h.OnGTID(h, e)
		if err != nil {
			log.Errorf("handle gtid event error: %s, handler: %s", err, name)
			return err
		}
	}
	return nil
}

func (o *HandlerMux) String() string {
	return "handler_multiplexer"
}
