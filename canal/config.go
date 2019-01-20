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
	"io/ioutil"
	"math/rand"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/bytewatch/dolphinbeat/schema"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
)

type DumpConfig struct {
	// mysqldump execution path, like mysqldump or /usr/bin/mysqldump, etc...
	// If not set, ignore using mysqldump.
	ExecutionPath string `toml:"mysqldump"`

	// Will override Databases, tables is in database table_db
	Tables  []string `toml:"tables"`
	TableDB string   `toml:"table_db"`

	Databases []string `toml:"dbs"`

	// Ignore table format is db.table
	IgnoreTables []string `toml:"ignore_tables"`

	// Dump only selected records. Quotes are mandatory
	Where string `toml:"where"`

	// If true, discard error msg, else, output to stderr
	DiscardErr bool `toml:"discard_err"`

	// Set true to skip --master-data if we have no privilege to do
	// 'FLUSH TABLES WITH READ LOCK'
	SkipMasterData bool `toml:"skip_master_data"`

	// Set to change the default max_allowed_packet size
	MaxAllowedPacketMB int `toml:"max_allowed_packet_mb"`
}

type TrackerConfig struct {
	// The charset_set_server of source mysql, we need
	// this charset to handle ddl statement
	CharsetServer string `toml:"charset_server"`

	// Storage type to store schema data, may be boltdb or mysql
	Storage string `toml:"storage"`

	// Boltdb file path to store data
	Dir string `toml:"dir"`

	// MySQL info to connect
	Addr     string `toml:"addr"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	Database string `toml:"database"`
}

type Config struct {
	Addr     string `toml:"addr"`
	User     string `toml:"user"`
	Password string `toml:"password"`

	Charset string `toml:"charset"`

	GtidEnabled bool `toml:"gtid_enabled"`

	ServerID        uint32        `toml:"server_id"`
	Flavor          string        `toml:"flavor"`
	HeartbeatPeriod time.Duration `toml:"heartbeat_period"`
	ReadTimeout     time.Duration `toml:"read_timeout"`

	// IncludeTableRegex or ExcludeTableRegex should contain database name
	// Only a table which matches IncludeTableRegex and dismatches ExcludeTableRegex will be processed
	// eg, IncludeTableRegex : [".*\\.canal"], ExcludeTableRegex : ["mysql\\..*"]
	//     this will include all database's 'canal' table, except database 'mysql'
	// Default IncludeTableRegex and ExcludeTableRegex are empty, this will include all tables
	IncludeTableRegex []string `toml:"include_table_regex"`
	ExcludeTableRegex []string `toml:"exclude_table_regex"`

	// discard row event without table meta
	DiscardNoMetaRowEvent bool `toml:"discard_no_meta_row_event"`

	Dump DumpConfig `toml:"dump"`

	Tracker TrackerConfig `toml:"schema_tracker"`

	UseDecimal bool `toml:"use_decimal"`
	ParseTime  bool `toml:"parse_time"`

	// SemiSyncEnabled enables semi-sync or not.
	SemiSyncEnabled bool `toml:"semi_sync_enabled"`
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}

func NewDefaultConfig() *Config {
	c := new(Config)

	c.Addr = "127.0.0.1:3306"
	c.User = "root"
	c.Password = ""
	c.Charset = mysql.DEFAULT_CHARSET

	c.GtidEnabled = true

	rand.Seed(time.Now().Unix())
	c.ServerID = uint32(rand.Intn(1000)) + 1001

	c.Flavor = "mysql"

	c.Dump.ExecutionPath = "mysqldump"
	c.Dump.DiscardErr = true

	c.Tracker.Storage = schema.StorageType_Boltdb
	c.Tracker.Dir = "."

	return c
}
