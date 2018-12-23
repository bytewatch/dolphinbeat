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

package main

import (
	"github.com/BurntSushi/toml"
)

type LogConfig struct {
	Dir   string
	Level string
}

type CheckpointConfig struct {
	// The interval to save checkpoint info
	Interval int `toml:"interval"`
	// Storage type to store checkpoint info
	Storage string `toml:"storage"`
	Dir     string `toml:"dir"`
	// Zookeeper info to store checkpoint info
	ZkHosts string `toml:"zk_hosts"`
	ZkPath  string `toml:"zk_path"`
}

type TrackerConfig struct {
	CharsetServer string `toml:"charset_server"`
	// Storage type to store schema info
	Storage string `toml:"storage"`

	Dir string `toml:"dir"`

	// MySQL info to connect
	Addr     string `toml:"addr"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	Database string `toml:"database"`
}

type SinkConfig struct {
	Type         string   `toml:"type"`
	Name         string   `toml:"name"`
	Enabled      bool     `toml:"enabled"`
	IncludeTable []string `toml:"include_table"`
	ExcludeTable []string `toml:"exclude_table"`

	Cfg toml.Primitive `toml:"cfg"`
}

type HttpServerConfig struct {
	Addr string `toml:"addr"`
}

type ElectionConfig struct {
	Enabled bool `toml:"enabled"`
	// Zookeeper info to store ephemeral node
	ZkHosts string `toml:"zk_hosts"`
	ZkPath  string `toml:"zk_path"`
	Lease   int    `toml:"lease"`
}

type Config struct {
	MysqlAddr     string `toml:"mysql_addr"`
	MysqlUser     string `toml:"mysql_user"`
	MysqlPassword string `toml:"mysql_password"`
	MysqlCharset  string `toml:"mysql_charset"`

	ServerID uint32 `toml:"server_id"`
	Flavor   string `toml:"flavor"`

	GtidEnabled bool `toml:"gtid_enabled"`

	Log LogConfig `toml:"log"`

	Checkpoint CheckpointConfig `toml:"checkpoint"`
	Tracker    TrackerConfig    `toml:"schema_tracker"`

	DumpExec       string `toml:"mysqldump"`
	SkipMasterData bool   `toml:"skip_master_data"`

	Sinks []SinkConfig `toml:"sink"`

	HttpServer HttpServerConfig `toml:"http_server"`
	Election   ElectionConfig   `toml:"election"`
}

func (o *Application) initConfig() error {
	var cfg Config
	_, err := toml.DecodeFile(o.cfgPath, &cfg)
	if err != nil {
		return err
	}
	o.cfg = &cfg
	return nil
}
