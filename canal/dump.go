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
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/mysql"
	"time"
)

type dumpParseHandler struct {
	c    *Canal
	name string
	pos  uint64
	gset mysql.GTIDSet
}

func (h *dumpParseHandler) BinLog(name string, pos uint64) error {
	h.name = name
	h.pos = pos
	log.Infof("parsed binlog pos from MySQL backup: %s:%d", name, pos)
	return nil
}

func (h *dumpParseHandler) Gtid(gtid string) error {
	gset, err := mysql.ParseGTIDSet(h.c.cfg.Flavor, gtid)
	if err != nil {
		return errors.Trace(err)
	}
	h.gset = gset
	log.Infof("parsed gtid from MySQL backup: %s", gset.String())
	return nil
}

func (h *dumpParseHandler) DDL(db string, statement string) error {
	if err := h.c.ctx.Err(); err != nil {
		return err
	}
	if err := h.c.tracker.Exec(db, statement); err != nil {
		return err
	}
	return nil
}

func (h *dumpParseHandler) Data(db string, table string, values []string) error {
	if err := h.c.ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (c *Canal) AddDumpDatabases(dbs ...string) {
	if c.dumper == nil {
		return
	}

	c.dumper.AddDatabases(dbs...)
}

func (c *Canal) AddDumpTables(db string, tables ...string) {
	if c.dumper == nil {
		return
	}

	c.dumper.AddTables(db, tables...)
}

func (c *Canal) AddDumpIgnoreTables(db string, tables ...string) {
	if c.dumper == nil {
		return
	}

	c.dumper.AddIgnoreTables(db, tables...)
}

func (c *Canal) dump() error {
	if c.dumper == nil {
		return errors.New("mysqldump does not exist")
	}

	h := &dumpParseHandler{c: c}

	serverID, err := c.GetMasterServerID()
	if err != nil {
		return errors.Trace(err)
	}

	// If we can not get master's binlog position info from mysqldump bakcup output,
	// we need to get this by query from master, but this position info may be INCONSISTENT with backup!
	// However, we only backup schema, no data, the  position will consistent with schema,
	// as long as there is no DDL when we taking backup
	if c.cfg.Dump.SkipMasterData {
		pos, err := c.GetMasterPos()
		if err != nil {
			return errors.Trace(err)
		}
		h.name = pos.Name
		h.pos = uint64(pos.Pos)

		if c.cfg.GtidEnabled {
			gset, err := c.GetMasterGTIDSet()
			if err != nil {
				return errors.Trace(err)
			}
			h.gset = gset
		}
	}

	start := time.Now()
	log.Info("try dump MySQL and parse")
	if err := c.dumper.DumpAndParse(h); err != nil {
		log.Errorf("dump MySQL and parse error: %s", err)
		return errors.Trace(err)
	}

	pos := prog.Position{Name: h.name, Pos: uint32(h.pos), ServerID: serverID}
	c.master.Update(pos)
	if h.gset != nil {
		c.master.UpdateGTIDSet(h.gset)
	}
	log.Infof("dump MySQL and parse OK, use %0.2f seconds, start binlog replication at %s",
		time.Now().Sub(start).Seconds(), c.master.Progress())
	return nil
}

func (c *Canal) tryDump() error {
	var err error

	progress := c.master.Progress()

	if !progress.IsZero() {
		// we will sync with binlog name and position
		log.Infof("skip dump, use last binlog replication progress: %s", progress)
		return nil
	}

	if c.dumper == nil {
		log.Info("skip dump, no mysqldump")
		return nil
	}

	// Reset schema info
	err = c.tracker.Reset()
	if err != nil {
		return err
	}

	err = c.dump()
	if err != nil {
		return err
	}

	if c.cfg.GtidEnabled && c.master.GTIDSet() == nil {
		return errors.New("gtid set not found from backup, the mysqldump's version may be incorrect")
	}
	// Tell schema tracker to make a snapshot
	err = c.tracker.Persist(c.master.Position())
	if err != nil {
		return err
	}

	return nil
}
