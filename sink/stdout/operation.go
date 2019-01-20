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
	"github.com/bytewatch/dolphinbeat/schema"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/hack"
)

type OperationType string

const (
	OpType_Rotate OperationType = "rotate"
	OpType_Gtid                 = "gtid"
	OpType_Begin                = "begin"
	OpType_Commit               = "commit"
	OpType_Insert               = "insert"
	OpType_Update               = "update"
	OpType_Delete               = "delete"
	OpType_Query                = "query"
	OpType_DDL                  = "ddl"
)

type Row struct {
	Before []interface{} `json:"before,omitempty"`
	After  []interface{} `json:"after,omitempty"`
}

type Column struct {
	Name      string           `json:"name"`
	SqlType   string           `json:"sql_type"`
	InnerType byte             `json:"inner_type"`
	Unsigned  bool             `json:"unsigned"`
	Key       schema.IndexType `json:"key"`
	Charset   string           `json:"charset"`
}

type Table struct {
	Database string    `json:"database"`
	Name     string    `json:"name"`
	Columns  []*Column `json:"columns"`
}

type Progress struct {
	Flavor   string `json:"flavor"`
	ServerID uint32 `json:"server_id"`
	LogName  string `json:"log_name"`
	LogPos   uint32 `json:"log_pos"`
	Gset     string `json:"gset"`
}

type OperationHeader struct {
	ServerID  uint32        `json:"server_id"`
	Type      OperationType `json:"type"`
	Timestamp uint32        `json:"timestamp"`
	LogPos    uint32        `json:"log_pos"`
}

type Operation struct {
	Header *OperationHeader `json:"header"`

	// NextLogName and NextLogPos field is for Rotate operation
	NextLogName string `json:"next_log_name,omitempty"`
	NextLogPos  uint32 `json:"next_log_pos,omitempty"`

	// Gtid field is for GTID operation
	Gtid string `json:"gtid,omitempty"`

	// Database and Statement filed is for QUERY/DDL operation
	Database  string `json:"database,omitempty"`
	Statement string `json:"statement,omitempty"`

	// Table and Rows field is for INSERT/UPDATE/DELETE operation
	Table *Table `json:"table,omitempty"`
	Rows  []*Row `json:"rows,omitempty"`

	// Progress field is for COMMIT/DDL operation,
	// represents the GTID_SET or file&pos
	Progress *Progress `json:"progress,omitempty"`
	p        *prog.Progress
}

func makeInsertOp(h *replication.EventHeader, e *canal.RowsEvent) *Operation {
	op := Operation{
		Header: makeHeader(OpType_Insert, h),
		Table:  makeTableDef(e.Table),
	}
	for _, values := range e.Rows {
		var row Row
		row.After = values
		op.Rows = append(op.Rows, &row)
	}

	return &op

}

func makeDeleteOp(h *replication.EventHeader, e *canal.RowsEvent) *Operation {
	op := Operation{
		Header: makeHeader(OpType_Delete, h),
		Table:  makeTableDef(e.Table),
	}
	for _, values := range e.Rows {
		var row Row
		row.Before = values
		op.Rows = append(op.Rows, &row)
	}

	return &op
}

func makeUpdateOp(h *replication.EventHeader, e *canal.RowsEvent) *Operation {

	op := Operation{
		Header: makeHeader(OpType_Update, h),
		Table:  makeTableDef(e.Table),
	}
	for i := 0; i < len(e.Rows); i += 2 {
		var row Row
		row.Before = e.Rows[i]
		row.After = e.Rows[i+1]
		op.Rows = append(op.Rows, &row)
	}

	return &op
}

func makeQueryOp(h *replication.EventHeader, e *replication.QueryEvent) *Operation {
	return &Operation{
		Header:    makeHeader(OpType_Query, h),
		Statement: hack.String(e.Query),
	}
}

func makeDDLOp(h *replication.EventHeader, e *replication.QueryEvent, p prog.Progress) *Operation {
	return &Operation{
		Header:    makeHeader(OpType_DDL, h),
		Statement: hack.String(e.Query),
		Progress:  makeProgress(p),
		p:         &p,
	}
}

func makeGtidOp(h *replication.EventHeader, e *canal.GtidEvent) *Operation {
	return &Operation{
		Header: makeHeader(OpType_Gtid, h),
		Gtid:   e.Gtid.String(),
	}
}

func makeBeginOp(h *replication.EventHeader) *Operation {
	return &Operation{
		Header: makeHeader(OpType_Begin, h),
	}

}

func makeCommitOp(h *replication.EventHeader, p prog.Progress) *Operation {
	return &Operation{
		Header:   makeHeader(OpType_Commit, h),
		Progress: makeProgress(p),
		p:        &p,
	}

}

func makeRotateOp(h *replication.EventHeader, e *replication.RotateEvent) *Operation {
	return &Operation{
		Header:      makeHeader(OpType_Rotate, h),
		NextLogName: string(e.NextLogName),
		NextLogPos:  uint32(e.Position),
	}

}

func makeProgress(p prog.Progress) *Progress {
	var progress Progress
	pos := p.Position()
	gset := p.GTIDSet()

	progress.LogName = pos.Name
	progress.LogPos = pos.Pos
	progress.ServerID = pos.ServerID

	if gset != nil {
		progress.Gset = gset.String()
		switch gset.(type) {
		case *mysql.MysqlGTIDSet:
			progress.Flavor = mysql.MySQLFlavor
		case *mysql.MariadbGTIDSet:
			progress.Flavor = mysql.MariaDBFlavor
		}
	}

	return &progress
}

func makeTableDef(t *schema.TableDef) *Table {
	var table Table
	table.Name = t.Name
	table.Database = t.Database
	for _, c := range t.Columns {
		var column Column
		column.Name = c.Name
		column.SqlType = c.Type
		column.InnerType = c.InnerType
		column.Charset = c.Charset
		column.Unsigned = c.Unsigned
		column.Key = c.Key
		table.Columns = append(table.Columns, &column)
	}
	return &table
}

func makeHeader(tp OperationType, h *replication.EventHeader) *OperationHeader {
	return &OperationHeader{
		ServerID:  h.ServerID,
		Type:      tp,
		LogPos:    h.LogPos,
		Timestamp: h.Timestamp,
	}
}
