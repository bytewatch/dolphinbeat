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
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/bytewatch/dolphinbeat/canal/prog"
	"github.com/juju/errors"
	"github.com/satori/go.uuid"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/hack"
)

var (
	expSingleComment1   = regexp.MustCompile("--[^\\r\\n]*")
	expSingleComment2   = regexp.MustCompile("^#[^\\r\\n]*")
	expNewLine          = regexp.MustCompile("[\\r\\n]+")
	expMultiLineComment = regexp.MustCompile("/\\*[\\w\\W]*?\\*/")
	expSpace            = regexp.MustCompile("[\\s]+")
	expSpaceAtBegin     = regexp.MustCompile("^[\\s]+")
)

func (c *Canal) startSyncer() (*replication.BinlogStreamer, error) {
	if !c.cfg.GtidEnabled {
		pos := c.master.Position()
		s, err := c.syncer.StartSync(mysql.Position{Name: pos.Name, Pos: pos.Pos})
		if err != nil {
			return nil, errors.Errorf("start sync replication at binlog %v error %v", pos, err)
		}
		log.Infof("start sync binlog at binlog file %v", pos)
		return s, nil
	} else {
		gset := c.master.GTIDSet()
		if gset == nil {
			return nil, errors.Errorf("start sync replication at GTID nil !?")
		}
		s, err := c.syncer.StartSyncGTID(gset)
		if err != nil {
			return nil, errors.Errorf("start sync replication at GTID set %v error %v", gset, err)
		}
		log.Infof("start sync binlog at GTID set %v", gset)
		return s, nil
	}
}

func (c *Canal) runSyncBinlog() error {
	s, err := c.startSyncer()
	if err != nil {
		return err
	}

	savePos := false
	progress := c.master.Progress()
	pos := progress.Position()

	for {
		ev, err := s.GetEvent(c.ctx)
		if err != nil {
			return errors.Trace(err)
		}
		savePos = false

		//next binlog pos
		pos.Pos = ev.Header.LogPos

		// We only save position with XIDEvent and DDL.
		// For RowsEvent, we can't save the position until meeting XIDEvent
		// which tells the whole transaction is over.
		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			log.Infof("rotate to %s:%d, is fake: %v", e.NextLogName, e.Position, ev.Header.Timestamp == 0)
			if ev.Header.Timestamp == 0 {
				// We get current Master's server_id from fake rotate event.
				// If server_id changed, we trigger a hook
				if pos.ServerID != 0 && pos.ServerID != ev.Header.ServerID {
					log.Warnf("mysql server_id changes from %d to from %d", pos.ServerID, ev.Header.ServerID)
					err = c.runBeforeServerIDChangeHook(pos.ServerID, ev.Header.ServerID)
					if err != nil {
						log.Errorf("run before_schema_change hook error: %s", err)
						return errors.Trace(err)
					}
				}
				pos.ServerID = ev.Header.ServerID
				pos.Name = string(e.NextLogName)
			}
			if err = c.eventHandler.OnRotate(ev.Header, e); err != nil {
				return errors.Trace(err)
			}
		case *replication.RowsEvent:
			// we only focus row based event
			err = c.handleRowsEvent(ev)
			if err != nil {
				log.Errorf("handle rows event error: %s", err)
				return errors.Trace(err)
			}
			c.counterLock.Lock()
			c.iudCounter++
			c.counterLock.Unlock()
		case *replication.XIDEvent:
			progress.Update(pos)
			if e.GSet != nil {
				progress.UpdateGTIDSet(e.GSet)
			}
			savePos = true
			// try to save the position later
			if err := c.eventHandler.OnCommit(ev.Header, progress); err != nil {
				return errors.Trace(err)
			}
		case *replication.MariadbGTIDEvent:
			// try to save the GTID later
			var event GtidEvent
			event.Gtid, err = mysql.ParseMariadbGTIDSet(e.GTID.String())
			if err != nil {
				return errors.Trace(err)
			}
			if err := c.eventHandler.OnGTID(ev.Header, &event); err != nil {
				return errors.Trace(err)
			}
		case *replication.GTIDEvent:
			var event GtidEvent
			event.LastCommitted = e.LastCommitted
			event.SequenceNumber = e.SequenceNumber
			u, _ := uuid.FromBytes(e.SID)
			event.Gtid, err = mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d", u.String(), e.GNO))
			if err != nil {
				return errors.Trace(err)
			}
			if err := c.eventHandler.OnGTID(ev.Header, &event); err != nil {
				return errors.Trace(err)
			}
		case *replication.QueryEvent:
			if strings.Compare(strings.ToUpper(hack.String(e.Query)), "BEGIN") == 0 {
				if err = c.eventHandler.OnBegin(ev.Header); err != nil {
					return errors.Trace(err)
				}
				continue
			}
			if strings.Compare(strings.ToUpper(hack.String(e.Query)), "COMMIT") == 0 {
				progress.Update(pos)
				if e.GSet != nil {
					progress.UpdateGTIDSet(e.GSet)
				}
				savePos = true
				if err = c.eventHandler.OnCommit(ev.Header, progress); err != nil {
					return errors.Trace(err)
				}
				continue
			}

			trimed := TrimStatement(hack.String(e.Query))

			if !IsDdlOrDclStatement(trimed) {
				// It is a normal query, we handle normal query here, but it is not very perfect,
				// because we haven't pass IntVar,RandVar events to eventHandler
				if err = c.eventHandler.OnQuery(ev.Header, e); err != nil {
					return errors.Trace(err)
				}
				continue
			}

			progress.Update(pos)
			if e.GSet != nil {
				progress.UpdateGTIDSet(e.GSet)
			}
			savePos = true

			isDdl, err := c.IsDdlStatement(trimed)
			if err != nil {
				return errors.Trace(err)
			}
			if isDdl {
				// This statement changes schema definition,  we need track this ddl.
				db := hack.String(e.Schema)
				if strings.HasPrefix(trimed, "CREATE DATABASE") || strings.HasPrefix(trimed, "DROP DATABASE") ||
					strings.HasPrefix(trimed, "CREATE SCHEMA") || strings.HasPrefix(trimed, "DROP SCHEMA") {
					db = ""
				}
				if err = c.trackDDL(db, hack.String(e.Query), progress.Position()); err != nil {
					return err
				}
			}

			if err = c.eventHandler.OnDDL(ev.Header, e, progress); err != nil {
				return errors.Trace(err)
			}
			c.counterLock.Lock()
			c.ddlCounter++
			c.counterLock.Unlock()
		default:
			continue
		}

		if savePos {
			c.master.UpdateProgress(progress)
			c.counterLock.Lock()
			c.trxCounter++
			c.counterLock.Unlock()
		}
	}

	return nil
}

func (c *Canal) trackDDL(db string, query string, pos prog.Position) error {
	// Before track the ddl, we need to ensure all dml events before has synced,
	// because once we have change the schmea, we don't know the old schema info
	// related with old dml events.
	err := c.runBeforeSchemaChangeHook(db, query)
	if err != nil {
		log.Errorf("run before_schema_change hook error: %s", err)
		return errors.Trace(err)
	}

	for {
		err = c.tracker.ExecAndPersist(db, query, pos)
		if err == nil {
			return nil
		}
		if err != nil {
			var skip bool
			log.Errorf("exec and persist error: %s", err)
			skip, err = c.runOnSchemaChangeFailedHook(db, query, err)
			if err != nil {
				log.Errorf("run on_schema_change_failed error: %s", err)
				return errors.Trace(err)
			}

			if skip {
				log.Warnf("skip a ddl statement: %s", query)
				return nil
			}
			log.Warnf("try to execute ddl again...")
		}
	}

	return nil

}

func (c *Canal) handleRowsEvent(e *replication.BinlogEvent) error {
	ev := e.Event.(*replication.RowsEvent)

	// Caveat: table may be altered at runtime.
	schema := string(ev.Table.Schema)
	table := string(ev.Table.Table)

	t, err := c.GetTableDef(schema, table)
	if err != nil {
		if err != ErrExcludedTable {
			return err
		}
		return nil
	}
	var action string
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		action = InsertAction
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		action = DeleteAction
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		action = UpdateAction
	default:
		return errors.Errorf("%s not supported now", e.Header.EventType)
	}

	var event *RowsEvent
	event = newRowsEvent(t, action, ev.Rows)
	return c.eventHandler.OnRow(e.Header, event)
}

func (c *Canal) FlushBinlog() error {
	_, err := c.Execute("FLUSH BINARY LOGS")
	return errors.Trace(err)
}

func (c *Canal) WaitUntilPos(pos prog.Position, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	for {
		select {
		case <-timer.C:
			return errors.Errorf("wait position %v too long > %s", pos, timeout)
		default:
			err := c.FlushBinlog()
			if err != nil {
				return errors.Trace(err)
			}
			curPos := c.master.Position()
			if curPos.ServerID != pos.ServerID || curPos.Compare(pos) < 0 {
				log.Debugf("master pos is %v, wait catching %v", curPos, pos)
				time.Sleep(100 * time.Millisecond)
			}
			return nil
		}
	}

	return nil
}

func (c *Canal) GetMasterPos() (prog.Position, error) {
	serverID, err := c.GetMasterServerID()
	if err != nil {
		return prog.Position{}, errors.Trace(err)
	}

	rr, err := c.Execute("SHOW MASTER STATUS")
	if err != nil {
		return prog.Position{}, errors.Trace(err)
	}

	name, _ := rr.GetString(0, 0)
	pos, _ := rr.GetInt(0, 1)

	return prog.Position{Name: name, Pos: uint32(pos), ServerID: uint32(serverID)}, nil
}

func (c *Canal) GetMasterGTIDSet() (mysql.GTIDSet, error) {
	query := ""
	switch c.cfg.Flavor {
	case mysql.MariaDBFlavor:
		query = "SELECT @@GLOBAL.gtid_current_pos"
	default:
		query = "SELECT @@GLOBAL.GTID_EXECUTED"
	}
	rr, err := c.Execute(query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	gx, err := rr.GetString(0, 0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	gset, err := mysql.ParseGTIDSet(c.cfg.Flavor, gx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return gset, nil
}

func (c *Canal) CatchMasterPos(timeout time.Duration) error {
	pos, err := c.GetMasterPos()
	if err != nil {
		return errors.Trace(err)
	}

	return c.WaitUntilPos(pos, timeout)
}

func (c *Canal) GetMasterServerID() (uint32, error) {
	query := "SELECT @@GLOBAL.server_id"
	rr, err := c.Execute(query)
	if err != nil {
		return 0, errors.Trace(err)
	}
	serverID, err := rr.GetInt(0, 0)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return uint32(serverID), nil
}

func (c *Canal) IsDdlStatement(statement string) (bool, error) {
	return c.tracker.IsDdl(statement)
}

func TrimStatement(statement string) string {
	trimed := statement
	trimed = expSingleComment1.ReplaceAllLiteralString(trimed, "")
	trimed = expSingleComment2.ReplaceAllLiteralString(trimed, "")
	trimed = expNewLine.ReplaceAllLiteralString(trimed, " ")
	trimed = expMultiLineComment.ReplaceAllLiteralString(trimed, " ")
	trimed = expSpace.ReplaceAllLiteralString(trimed, " ")
	trimed = expSpaceAtBegin.ReplaceAllLiteralString(trimed, "")
	return strings.ToUpper(trimed)

}

func IsDdlOrDclStatement(statement string) bool {
	samples := []string{"CREATE", "DROP", "ALTER", "RENAME", "TRUNCATE", "GRANT", "REVOKE", "FLUSH"}
	for _, sample := range samples {
		if strings.HasPrefix(statement, sample) {
			return true
		}
	}
	return false

}
