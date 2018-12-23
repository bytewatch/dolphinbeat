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
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/mysql"
	"strconv"
)

type Progress struct {
	Flavor   string `json:"flavor"`
	ServerID uint32 `json:"server_id"`
	Name     string `json:"name"`
	Pos      uint32 `json:"pos"`
	Gset     string `json:"gset"`
}

func (o *Progress) SetProgress(p prog.Progress) {
	pos := p.Position()
	gset := p.GTIDSet()

	o.Name = pos.Name
	o.Pos = pos.Pos
	o.ServerID = pos.ServerID

	if gset != nil {
		o.Gset = gset.String()
		switch gset.(type) {
		case *mysql.MysqlGTIDSet:
			o.Flavor = mysql.MySQLFlavor
		case *mysql.MariadbGTIDSet:
			o.Flavor = mysql.MariaDBFlavor
		}
	}
}

func (o *Progress) GetProgress() prog.Progress {
	var progress prog.Progress

	pos := prog.Position{Name: o.Name, Pos: o.Pos, ServerID: o.ServerID}
	progress.Update(pos)

	if o.Flavor != "" {
		gset, err := mysql.ParseGTIDSet(o.Flavor, o.Gset)
		if err != nil {
			log.Panicf("parse gtid set error: %s, gtid: %s", err, o.Gset)
		}
		progress.UpdateGTIDSet(gset)
	}
	return progress
}

type Checkpoint struct {
	Progress
	Ctx map[string]string `json:"ctx"`
}

func NewCheckpoint() *Checkpoint {
	return &Checkpoint{
		Ctx: make(map[string]string),
	}
}

type Checkpointer interface {
	Checkpoint() *Checkpoint
}

func (o *Checkpoint) SetProgress(p prog.Progress) *Checkpoint {
	o.Progress.SetProgress(p)
	return o
}

func (o *Checkpoint) SetIntCtx(key string, value int64) *Checkpoint {
	o.Ctx[key] = strconv.FormatInt(value, 10)
	return o
}

func (o *Checkpoint) GetIntCtx(key string, defValue int64) int64 {
	str, ok := o.Ctx[key]
	if !ok {
		return defValue
	}
	v, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		log.Panicf("parse int ctx error: %s, ctx: %s", err, str)
	}
	return v
}

func (o *Checkpoint) SetUintCtx(key string, value uint64) *Checkpoint {
	o.Ctx[key] = strconv.FormatUint(value, 10)
	return o
}

func (o *Checkpoint) GetUintCtx(key string, defValue uint64) uint64 {
	str, ok := o.Ctx[key]
	if !ok {
		return defValue
	}
	v, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		log.Panicf("parse uint ctx error: %s, ctx: %s", err, str)
	}
	return v
}

func (o *Checkpoint) SetStringCtx(key string, value string) *Checkpoint {
	o.Ctx[key] = value
	return o
}

func (o *Checkpoint) GetStringCtx(key string, defValue string) string {
	str, ok := o.Ctx[key]
	if !ok {
		return defValue
	}
	return str
}
