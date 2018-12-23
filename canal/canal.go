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
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bytewatch/ddl-executor"
	"github.com/bytewatch/dolphinbeat/canal/prog"
	"github.com/bytewatch/dolphinbeat/dump"
	"github.com/bytewatch/dolphinbeat/schema"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

// Canal can sync your MySQL data into everywhere, like Elasticsearch, Redis, etc...
// MySQL must open row format for binlog
type Canal struct {
	m sync.Mutex

	cfg *Config

	master     *masterInfo
	dumper     *dump.Dumper
	dumped     bool
	dumpDoneCh chan struct{}
	syncer     *replication.BinlogSyncer

	eventHandler EventHandler
	observer     Observer

	connLock sync.Mutex
	conn     *client.Conn

	tracker *schema.SchemaTracker

	tableLock       sync.RWMutex
	tableMatchCache map[string]bool

	includeTableRegex []*regexp.Regexp
	excludeTableRegex []*regexp.Regexp

	counterLock sync.Mutex
	trxCounter  uint64
	iudCounter  uint64
	ddlCounter  uint64

	ctx    context.Context
	cancel context.CancelFunc

	errCh chan error
}

// canal will retry fetching unknown table's meta after UnknownTableRetryPeriod
var UnknownTableRetryPeriod = time.Second * time.Duration(10)
var ErrExcludedTable = errors.New("excluded table meta")

func NewCanal(cfg *Config) (*Canal, error) {
	var err error

	c := new(Canal)
	c.cfg = cfg

	c.ctx, c.cancel = context.WithCancel(context.Background())

	c.dumpDoneCh = make(chan struct{})
	c.eventHandler = &DummyEventHandler{}

	c.master = &masterInfo{}

	c.errCh = make(chan error, 1)

	if err = c.prepareTracker(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = c.prepareDumper(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = c.prepareSyncer(); err != nil {
		return nil, errors.Trace(err)
	}

	if err := c.checkBinlogRowFormat(); err != nil {
		return nil, errors.Trace(err)
	}

	// init table filter
	if n := len(c.cfg.IncludeTableRegex); n > 0 {
		c.includeTableRegex = make([]*regexp.Regexp, n)
		for i, val := range c.cfg.IncludeTableRegex {
			reg, err := regexp.Compile(val)
			if err != nil {
				return nil, errors.Trace(err)
			}
			c.includeTableRegex[i] = reg
		}
	}

	if n := len(c.cfg.ExcludeTableRegex); n > 0 {
		c.excludeTableRegex = make([]*regexp.Regexp, n)
		for i, val := range c.cfg.ExcludeTableRegex {
			reg, err := regexp.Compile(val)
			if err != nil {
				return nil, errors.Trace(err)
			}
			c.excludeTableRegex[i] = reg
		}
	}

	if c.includeTableRegex != nil || c.excludeTableRegex != nil {
		c.tableMatchCache = make(map[string]bool)
	}

	return c, nil
}

func (c *Canal) prepareDumper() error {
	var err error
	dumpPath := c.cfg.Dump.ExecutionPath
	if len(dumpPath) == 0 {
		// ignore mysqldump, use binlog only
		return nil
	}

	if c.dumper, err = dump.NewDumper(dumpPath,
		c.cfg.Addr, c.cfg.User, c.cfg.Password); err != nil {
		return errors.Trace(err)
	}

	if c.dumper == nil {
		//no mysqldump, use binlog only
		return nil
	}

	dbs := c.cfg.Dump.Databases
	tables := c.cfg.Dump.Tables
	tableDB := c.cfg.Dump.TableDB

	if len(tables) == 0 {
		c.dumper.AddDatabases(dbs...)
	} else {
		c.dumper.AddTables(tableDB, tables...)
	}

	charset := c.cfg.Charset
	c.dumper.SetCharset(charset)

	c.dumper.SetGtidEnabled(c.cfg.GtidEnabled)

	c.dumper.SetWhere(c.cfg.Dump.Where)
	c.dumper.SkipMasterData(c.cfg.Dump.SkipMasterData)
	c.dumper.SetMaxAllowedPacket(c.cfg.Dump.MaxAllowedPacketMB)
	// Use hex blob for mysqldump
	c.dumper.SetHexBlob(true)

	for _, ignoreTable := range c.cfg.Dump.IgnoreTables {
		if seps := strings.Split(ignoreTable, ","); len(seps) == 2 {
			c.dumper.AddIgnoreTables(seps[0], seps[1])
		}
	}

	if c.cfg.Dump.DiscardErr {
		c.dumper.SetErrOut(ioutil.Discard)
	} else {
		c.dumper.SetErrOut(os.Stderr)
	}

	return nil
}

func (c *Canal) prepareTracker() error {
	var err error
	trackerCfg := &schema.TrackerConfig{
		CharsetServer: "utf8",
		Storage:       c.cfg.Tracker.Storage,
		Dir:           c.cfg.Tracker.Dir,
		Addr:          c.cfg.Tracker.Addr,
		User:          c.cfg.Tracker.User,
		Password:      c.cfg.Tracker.Password,
		Database:      c.cfg.Tracker.Database,
	}
	c.tracker, err = schema.NewSchemaTracker(trackerCfg)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Start will first try to dump all data from MySQL master `mysqldump`,
// then sync from the binlog position in the dump data.
// It will run forever until meeting an error or Canal closed.
func (c *Canal) Start(p prog.Progress) error {
	c.master.UpdateProgress(p)
	go c.run()
	return nil
}

// Dump all data from MySQL master `mysqldump`, ignore sync binlog.
func (c *Canal) Dump() error {
	if c.dumped {
		return errors.New("the method Dump can't be called twice")
	}
	c.dumped = true
	defer close(c.dumpDoneCh)
	return c.dump()
}

func (c *Canal) run() {
	defer close(c.errCh)

	if !c.dumped {
		c.dumped = true

		err := c.tryDump()
		close(c.dumpDoneCh)

		if err != nil {
			log.Errorf("canal dump mysql err: %v", err)
			c.errCh <- errors.Trace(err)
			return
		}
	}

	if err := c.runSyncBinlog(); err != nil {
		log.Errorf("canal start sync binlog err: %v", err)
		c.errCh <- errors.Trace(err)
		return
	}

	return
}

func (c *Canal) Close() {
	log.Infof("closing canal")

	c.m.Lock()
	defer c.m.Unlock()

	c.cancel()
	for range c.errCh {
		// drain
	}

	c.connLock.Lock()
	c.conn.Close()
	c.conn = nil
	c.connLock.Unlock()
	c.syncer.Close()

	//c.eventHandler.OnPosSynced(c.master.Position(), true)
}

func (c *Canal) WaitDumpDone() <-chan struct{} {
	return c.dumpDoneCh
}

func (c *Canal) Err() <-chan error {
	return c.errCh
}

func (c *Canal) checkTableMatch(key string) bool {
	// no filter, return true
	if c.tableMatchCache == nil {
		return true
	}

	c.tableLock.RLock()
	rst, ok := c.tableMatchCache[key]
	c.tableLock.RUnlock()
	if ok {
		// cache hit
		return rst
	}
	matchFlag := false
	// check include
	if c.includeTableRegex != nil {
		for _, reg := range c.includeTableRegex {
			if reg.MatchString(key) {
				matchFlag = true
				break
			}
		}
	}
	// check exclude
	if matchFlag && c.excludeTableRegex != nil {
		for _, reg := range c.excludeTableRegex {
			if reg.MatchString(key) {
				matchFlag = false
				break
			}
		}
	}
	c.tableLock.Lock()
	c.tableMatchCache[key] = matchFlag
	c.tableLock.Unlock()
	return matchFlag
}

func (c *Canal) GetTableDef(db string, table string) (*executor.TableDef, error) {
	key := fmt.Sprintf("%s.%s", db, table)
	// if table is excluded, return error and skip parsing event or dump
	if !c.checkTableMatch(key) {
		return nil, ErrExcludedTable
	}
	return c.tracker.GetTableDef(db, table)
}

func (c *Canal) GetDatabases() []string {
	return c.tracker.GetDatabases()
}

func (c *Canal) GetTables(db string) ([]string, error) {
	return c.tracker.GetTables(db)
}

func (c *Canal) ExecDDL(db string, statement string) error {
	return c.tracker.Exec(db, statement)
}

// Check MySQL binlog row image, must be in FULL, MINIMAL, NOBLOB
func (c *Canal) CheckBinlogRowImage(image string) error {
	// need to check MySQL binlog row image? full, minimal or noblob?
	// now only log
	if c.cfg.Flavor == mysql.MySQLFlavor {
		if res, err := c.Execute(`SHOW GLOBAL VARIABLES LIKE "binlog_row_image"`); err != nil {
			return errors.Trace(err)
		} else {
			// MySQL has binlog row image from 5.6, so older will return empty
			rowImage, _ := res.GetString(0, 1)
			if rowImage != "" && !strings.EqualFold(rowImage, image) {
				return errors.Errorf("MySQL uses %s binlog row image, but we want %s", rowImage, image)
			}
		}
	}

	return nil
}

func (c *Canal) checkBinlogRowFormat() error {
	res, err := c.Execute(`SHOW GLOBAL VARIABLES LIKE "binlog_format";`)
	if err != nil {
		return errors.Trace(err)
	} else if f, _ := res.GetString(0, 1); f != "ROW" {
		return errors.Errorf("binlog must ROW format, but %s now", f)
	}

	return nil
}

func (c *Canal) prepareSyncer() error {
	seps := strings.Split(c.cfg.Addr, ":")
	if len(seps) != 2 {
		return errors.Errorf("invalid mysql addr format %s, must host:port", c.cfg.Addr)
	}

	port, err := strconv.ParseUint(seps[1], 10, 16)
	if err != nil {
		return errors.Trace(err)
	}

	cfg := replication.BinlogSyncerConfig{
		ServerID:        c.cfg.ServerID,
		Flavor:          c.cfg.Flavor,
		Host:            seps[0],
		Port:            uint16(port),
		User:            c.cfg.User,
		Password:        c.cfg.Password,
		Charset:         c.cfg.Charset,
		HeartbeatPeriod: c.cfg.HeartbeatPeriod,
		ReadTimeout:     c.cfg.ReadTimeout,
		UseDecimal:      c.cfg.UseDecimal,
		ParseTime:       c.cfg.ParseTime,
		SemiSyncEnabled: c.cfg.SemiSyncEnabled,
	}

	c.syncer = replication.NewBinlogSyncer(cfg)

	return nil
}

// Execute a SQL
func (c *Canal) Execute(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
	c.connLock.Lock()
	defer c.connLock.Unlock()

	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if c.conn == nil {
			c.conn, err = client.Connect(c.cfg.Addr, c.cfg.User, c.cfg.Password, "")
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		rr, err = c.conn.Execute(cmd, args...)
		if err != nil && !mysql.ErrorEqual(err, mysql.ErrBadConn) {
			return
		} else if mysql.ErrorEqual(err, mysql.ErrBadConn) {
			c.conn.Close()
			c.conn = nil
			continue
		} else {
			return
		}
	}
	return
}

func (c *Canal) SyncedProgress() prog.Progress {
	return c.master.Progress()
}

func (c *Canal) TrxCount() uint64 {
	c.counterLock.Lock()
	defer c.counterLock.Unlock()
	return c.trxCounter
}

func (c *Canal) IudCount() uint64 {
	c.counterLock.Lock()
	defer c.counterLock.Unlock()
	return c.iudCounter
}

func (c *Canal) DDLCount() uint64 {
	c.counterLock.Lock()
	defer c.counterLock.Unlock()
	return c.ddlCounter
}
