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
	"github.com/samuel/go-zookeeper/zk"
	"github.com/siddontang/go-log/log"
	"path/filepath"
	"strings"
	"time"
)

type ZookeeperStorage struct {
	path string
	conn *zk.Conn
}

func NewZookeeperStorage(hosts string, path string) (*ZookeeperStorage, error) {
	var err error

	conn, _, err := zk.Connect(strings.Split(hosts, ","), 40*time.Second, zk.WithLogInfo(false))
	if err != nil {
		log.Errorf("connect zookeeper node error: %s, hosts: %s", err, hosts)
		return nil, err
	}

	storage := ZookeeperStorage{
		path: path,
		conn: conn,
	}

	err = createNodeIfNotExists(conn, path, []byte{})
	if err != nil {
		log.Errorf("create zookeeper node error: %s, path: %s", err, path)
		return nil, err
	}

	return &storage, err
}

func (o *ZookeeperStorage) Close() error {
	if o.conn != nil {
		o.conn.Close()
	}
	return nil
}

func (o *ZookeeperStorage) Save(data []byte) error {
	_, err := o.conn.Set(o.path, data, -1)
	if err != nil {
		log.Errorf("set zookeeper node error: %s", err)
		return err
	}

	return nil
}

func (o *ZookeeperStorage) Load() ([]byte, error) {
	data, _, err := o.conn.Get(o.path)
	if err != nil {
		log.Errorf("get zookeeper node error: %s", err)
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	return data, nil
}

func createNodeIfNotExists(conn *zk.Conn, path string, data []byte) error {
	exists, _, err := conn.Exists(path)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	dir := filepath.Dir(path)
	if dir != "/" {
		err = createNodeIfNotExists(conn, dir, []byte{})
		if err != nil {
			return err
		}
	}

	_, err = conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
}
