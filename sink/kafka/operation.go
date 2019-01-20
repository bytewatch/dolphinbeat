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
	"fmt"
	"github.com/bytewatch/dolphinbeat/canal"
	"github.com/bytewatch/dolphinbeat/canal/prog"
	"github.com/bytewatch/dolphinbeat/schema"
	"github.com/bytewatch/dolphinbeat/sink/kafka/protocol"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/hack"
)

type Operation struct {
	op *protocol.Operation
	p  *prog.Progress
}

func makeInsertOp(h *replication.EventHeader, e *canal.RowsEvent) *Operation {
	op := protocol.Operation{
		Header: makeHeader(protocol.OperationType_INSERT, h),
		Table:  makeTableDef(e.Table),
	}
	for _, values := range e.Rows {
		var row protocol.Row
		row.After = makeColumns(values)
		op.Rows = append(op.Rows, &row)
	}

	return &Operation{op: &op}
}

func makeDeleteOp(h *replication.EventHeader, e *canal.RowsEvent) *Operation {
	op := protocol.Operation{
		Header: makeHeader(protocol.OperationType_DELETE, h),
		Table:  makeTableDef(e.Table),
	}
	for _, values := range e.Rows {
		var row protocol.Row
		row.Before = makeColumns(values)
		op.Rows = append(op.Rows, &row)
	}

	return &Operation{op: &op}
}

func makeUpdateOp(h *replication.EventHeader, e *canal.RowsEvent) *Operation {
	op := protocol.Operation{
		Header: makeHeader(protocol.OperationType_UPDATE, h),
		Table:  makeTableDef(e.Table),
	}
	for i := 0; i < len(e.Rows); i += 2 {
		var row protocol.Row
		row.Before = makeColumns(e.Rows[i])
		row.After = makeColumns(e.Rows[i+1])
		op.Rows = append(op.Rows, &row)
	}

	return &Operation{op: &op}
}

func makeQueryOp(h *replication.EventHeader, e *replication.QueryEvent) *Operation {
	op := protocol.Operation{
		Header:    makeHeader(protocol.OperationType_QUERY, h),
		Statement: hack.String(e.Query),
	}
	return &Operation{op: &op}
}

func makeDDLOp(h *replication.EventHeader, e *replication.QueryEvent, p prog.Progress) *Operation {
	op := protocol.Operation{
		Header:    makeHeader(protocol.OperationType_DDL, h),
		Statement: hack.String(e.Query),
		Progress:  makeProgress(p),
	}
	return &Operation{op: &op, p: &p}
}

func makeGtidOp(h *replication.EventHeader, e *canal.GtidEvent) *Operation {
	op := protocol.Operation{
		Header: makeHeader(protocol.OperationType_GTID, h),
		Gtid:   e.Gtid.String(),
	}
	return &Operation{op: &op}
}

func makeBeginOp(h *replication.EventHeader) *Operation {
	op := protocol.Operation{
		Header: makeHeader(protocol.OperationType_BEGIN, h),
	}
	return &Operation{op: &op}

}

func makeCommitOp(h *replication.EventHeader, p prog.Progress) *Operation {
	op := protocol.Operation{
		Header:   makeHeader(protocol.OperationType_COMMIT, h),
		Progress: makeProgress(p),
	}
	return &Operation{op: &op, p: &p}

}

func makeRotateOp(h *replication.EventHeader, e *replication.RotateEvent) *Operation {
	op := protocol.Operation{
		Header:      makeHeader(protocol.OperationType_ROTATE, h),
		NextLogName: string(e.NextLogName),
		NextLogPos:  uint32(e.Position),
	}
	return &Operation{op: &op}
}

func makeProgress(p prog.Progress) *protocol.Progress {
	var progress protocol.Progress
	pos := p.Position()
	gset := p.GTIDSet()

	progress.LogName = pos.Name
	progress.LogPos = pos.Pos
	progress.ServerId = pos.ServerID

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

func makeHeader(tp protocol.OperationType, h *replication.EventHeader) *protocol.OperationHeader {
	return &protocol.OperationHeader{
		ServerId:  h.ServerID,
		Type:      tp,
		LogPos:    h.LogPos,
		Timestamp: h.Timestamp,
	}
}

func makeValue(value interface{}) string {
	if v, ok := value.([]byte); ok {
		return fmt.Sprintf("%s", v)
	}
	return fmt.Sprintf("%v", value)
}

func makeColumns(values []interface{}) []*protocol.Column {
	var columns []*protocol.Column
	for _, value := range values {
		var column protocol.Column
		if value == nil {
			column.IsNull = true
			column.Value = ""
		} else {
			column.Value = makeValue(value)
		}
		columns = append(columns, &column)
	}
	return columns
}

func makeTableDef(t *schema.TableDef) *protocol.TableDef {
	var table protocol.TableDef
	table.Name = t.Name
	table.Database = t.Database
	for _, c := range t.Columns {
		var column protocol.ColumnDef
		column.Name = c.Name
		column.SqlType = c.Type
		column.InnerType = protocol.InnerType(c.InnerType)
		column.Charset = c.Charset
		column.Unsigned = c.Unsigned
		column.Key = string(c.Key)
		table.Columns = append(table.Columns, &column)
	}
	return &table
}
