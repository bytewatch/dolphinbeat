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

package schema

import (
	"fmt"
	"github.com/bytewatch/ddl-executor"
	"github.com/bytewatch/dolphinbeat/canal/prog"
	"github.com/siddontang/go-log/log"
)

var HAHealthCheckSchema = "mysql.ha_health_check"

type SchemaTracker struct {
	cfg *TrackerConfig

	curPos prog.Position

	executor *executor.Executor

	storage SchemaStorage
}

func NewSchemaTracker(cfg *TrackerConfig) (*SchemaTracker, error) {
	var err error
	var storage SchemaStorage

	switch cfg.Storage {
	case StorageType_Boltdb:
		storage, err = NewBoltdbStorage(cfg.Dir + "/schema.dat")
	case StorageType_Mysql:
		storage, err = NewMysqlStorage(cfg.Addr, cfg.User, cfg.Password, cfg.Database)
	default:
		err = fmt.Errorf("unknown storage type: %s", cfg.Storage)
	}
	if err != nil {
		log.Errorf("new storage error: %s", err)
		return nil, err
	}

	// Restore schema from storage, into memory
	snapshot, pos, err := storage.LoadLastSnapshot()
	if err != nil {
		log.Errorf("load last snapshot from storage error: %s", err)
		return nil, err
	}

	executor := executor.NewExecutor(&executor.Config{
		CharsetServer:       cfg.CharsetServer,
		LowerCaseTableNames: true,
		NeedAtomic:          true,
	})
	err = executor.Restore(snapshot)
	if err != nil {
		log.Errorf("set snapshot to executor error: %s", err)
		return nil, err
	}

	// TODO: Replay all statements

	tracker := &SchemaTracker{
		cfg:      cfg,
		executor: executor,
		storage:  storage,
		curPos:   pos,
	}

	return tracker, nil
}

func (o *SchemaTracker) IsDdl(sql string) (bool, error) {
	return o.executor.IsDdl(sql)
}

// Before Persist is called, must ensure the binlog events of last server_id is in ckp
func (o *SchemaTracker) Persist(pos prog.Position) error {
	snapshot, err := o.executor.Snapshot()
	if err != nil {
		log.Errorf("get executor snapshot error: %s", err)
		return err
	}
	err = o.storage.SaveSnapshot(snapshot, pos)
	if err != nil {
		log.Errorf("save snapshot error: %s", err)
		return err
	}

	log.Infof("save snapshot succeed, server_id: %d, pos: %s", pos.ServerID, pos.String())
	o.curPos = pos

	return nil
}

// Exec ddl statement, and persistent the schema info into storage
func (o *SchemaTracker) ExecAndPersist(db string, statement string, pos prog.Position) error {
	var err error

	// When server_id is equal, we start to check whether this ddl we have executed already.
	// When server_id is not equal, we have no way to check, just treat this ddl as new one.
	if pos.ServerID == o.curPos.ServerID &&
		pos.Compare(o.curPos) == 0 {
		log.Warnf("this statement has been executed before: %s", pos.String())
		return nil
	}

	err = o.Exec(db, statement)
	if err != nil {
		return err
	}

	if o.needTriggerSnapshot() {
		snapshot, err := o.executor.Snapshot()
		if err != nil {
			log.Errorf("get executor snapshot error: %s", err)
			return err
		}
		err = o.storage.SaveSnapshot(snapshot, pos)
		if err != nil {
			log.Errorf("save snapshot error: %s", err)
			return err
		}
		log.Infof("save snapshot succeed, server_id: %d, pos: %s", pos.ServerID, pos.String())
	} else {
		o.storage.SaveStatement(db, statement, pos)
		if err != nil {
			log.Errorf("save statement error: %s", err)
			return err
		}
		log.Infof("save statement succeed, server_id: %d, pos: %s", pos.ServerID, pos.String())
	}

	o.curPos = pos

	return nil
}

// Exec ddl statement, but don't persistent the schema info
func (o *SchemaTracker) Exec(db string, statement string) error {
	var err error
	if db != "" {
		sql := "USE " + db
		err = o.executor.Exec(sql)
		if err != nil {
			log.Errorf("execute sql error: %s, sql: %s", err, sql)
			return err
		}
	}

	log.Infof("executing sql: %s", statement)
	err = o.executor.Exec(statement)
	if err != nil {
		log.Errorf("execute sql error: %s, sql: %s", err, statement)
		return err
	}

	return nil
}

func (o *SchemaTracker) GetTableDef(db string, table string) (*executor.TableDef, error) {
	t, err := o.executor.GetTableDef(db, table)
	if err != nil {
		// work around : RDS HAHeartBeat
		// ref : https://github.com/alibaba/canal/blob/master/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L385
		// issue : https://github.com/alibaba/canal/issues/222
		// This is a common error in RDS that canal can't get HAHealthCheckSchema's meta, so we mock a table meta.
		// If canal just skip and log error, as RDS HA heartbeat interval is very short, so too many HAHeartBeat errors will be logged.
		key := fmt.Sprintf("%s.%s", db, table)
		if key == HAHealthCheckSchema {
			// mock ha_health_check meta
			ta := &executor.TableDef{
				Database: db,
				Name:     table,
				Columns:  make([]*executor.ColumnDef, 0, 2),
			}
			ta.Columns = append(ta.Columns, &executor.ColumnDef{
				Name:      "id",
				Type:      "bigint(20)",
				InnerType: executor.TypeLong,
			}, &executor.ColumnDef{
				Name:      "type",
				Type:      "char(1)",
				InnerType: executor.TypeVarString,
			})
			return ta, nil
		}
		return nil, err
	}
	return t, nil
}

func (o *SchemaTracker) GetDatabases() []string {
	return o.executor.GetDatabases()
}

func (o *SchemaTracker) GetTables(db string) ([]string, error) {
	return o.executor.GetTables(db)
}

// Reset executor and storage
func (o *SchemaTracker) Reset() error {
	o.executor.Reset()
	err := o.storage.Reset()
	if err != nil {
		log.Errorf("reset storage error: %s", err)
		return err
	}
	return nil
}

func (o *SchemaTracker) needTriggerSnapshot() bool {
	return true
}

func (o *SchemaTracker) replayNextStatement() {
}

func (o *SchemaTracker) replayAllStatements() {
}
