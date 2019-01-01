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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/boltdb/bolt"
	"github.com/bytewatch/dolphinbeat/canal/prog"
	"github.com/juju/errors"
	"strconv"
	"strings"
	"time"
)

// The key of blotdb
type Key struct {
	Name uint32
	Pos  uint32
}

// The value of blotdb
type Value struct {
	ServerID  uint32
	Name      string
	Pos       uint32
	Snapshot  []byte
	Database  string
	Statement string
	Time      time.Time
}

type BoltdbStorage struct {
	path        string
	curServerID uint32

	db *bolt.DB
}

func NewBoltdbStorage(path string) (*BoltdbStorage, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	serverID := uint64(0)

	err = db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists([]byte("meta"))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}
		v := meta.Get([]byte("server_id"))
		if v != nil {
			serverID, err = strconv.ParseUint(string(v), 10, 64)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	storage := &BoltdbStorage{
		path:        path,
		db:          db,
		curServerID: uint32(serverID),
	}

	return storage, nil
}

// Save snapshot data at bucket 'pos.serverID'
func (o *BoltdbStorage) SaveSnapshot(data []byte, pos prog.Position) error {
	err := o.db.Update(func(tx *bolt.Tx) error {
		var err error

		serverIDBytes := []byte(strconv.FormatUint(uint64(pos.ServerID), 10))

		if o.curServerID != pos.ServerID {
			// If we switch to a new server_id, we need to save this server_id into meta
			meta := tx.Bucket([]byte("meta"))
			err := meta.Put([]byte("server_id"), serverIDBytes)
			if err != nil {
				return err
			}

			// If we switch to a new server_id, we need to ensure the new bucket is empty
			bucket := tx.Bucket([]byte("data")).Bucket(serverIDBytes)
			if bucket != nil {
				err = tx.Bucket([]byte("data")).DeleteBucket(serverIDBytes)
				if err != nil {
					return err
				}
			}
			_, err = tx.Bucket([]byte("data")).CreateBucket(serverIDBytes)
			if err != nil {
				return err
			}
		}

		// Make a sortable key base on pos
		key, err := makeKey(pos)
		if err != nil {
			return err
		}
		value, err := makeValue(data, "", "", pos)
		if err != nil {
			return err
		}

		// Save snapshot
		bucket := tx.Bucket([]byte("data")).Bucket(serverIDBytes)
		err = bucket.Put(key, value)
		if err != nil {
			return err
		}

		// Save the key of last snapshot into meta
		meta := tx.Bucket([]byte("meta"))
		err = meta.Put([]byte("last_snapshot"), key)
		if err != nil {
			return err
		}

		if o.curServerID != pos.ServerID {
			o.curServerID = pos.ServerID
		}

		// Purge the expired data
		return o.purge(tx)
	})

	if err != nil {
		return err
	}

	return nil
}

func (o *BoltdbStorage) LoadLastSnapshot() ([]byte, prog.Position, error) {
	var pos prog.Position
	var value Value
	var data []byte

	err := o.db.View(func(tx *bolt.Tx) error {
		if o.curServerID == 0 {
			// Maybe this is in initial startup
			return nil
		}

		meta := tx.Bucket([]byte("meta"))
		key := meta.Get([]byte("last_snapshot"))

		serverIDBytes := []byte(strconv.FormatUint(uint64(o.curServerID), 10))

		bucket := tx.Bucket([]byte("data")).Bucket(serverIDBytes)
		if bucket == nil {
			return errors.Errorf("the bucket of server_id: %d is missing", o.curServerID)
		}
		valueBytes := bucket.Get([]byte(key))
		err := json.Unmarshal(valueBytes, &value)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, pos, err
	}

	pos.ServerID = value.ServerID
	pos.Name = value.Name
	pos.Pos = value.Pos
	data = value.Snapshot

	return data, pos, nil
}

func (o *BoltdbStorage) SaveStatement(db string, statement string, pos prog.Position) error {
	return nil
}

func (o *BoltdbStorage) LoadNextStatement(prePos prog.Position) (string, string, prog.Position, error) {
	var pos prog.Position
	return "", "", pos, nil
}

func (o *BoltdbStorage) Reset() error {
	err := o.db.Update(func(tx *bolt.Tx) error {
		// Delete meta bucket if exists
		if tx.Bucket([]byte("meta")) != nil {
			err := tx.DeleteBucket([]byte("meta"))
			if err != nil {
				return err
			}
		}
		// Re-create meta bucket
		_, err := tx.CreateBucketIfNotExists([]byte("meta"))
		if err != nil {
			return err
		}

		// Delete data bucket if exists
		if tx.Bucket([]byte("data")) != nil {
			err := tx.DeleteBucket([]byte("data"))
			if err != nil {
				return err
			}
		}
		// Re-create data bucket
		_, err = tx.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}

		// Reset current server_id
		o.curServerID = 0
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

// Purge the snapshot or statement before the last snapshot
func (o *BoltdbStorage) purge(tx *bolt.Tx) error {
	if o.curServerID == 0 {
		// Maybe this is in initial startup
		return nil
	}

	meta := tx.Bucket([]byte("meta"))
	key := meta.Get([]byte("last_snapshot"))

	serverIDBytes := []byte(strconv.FormatUint(uint64(o.curServerID), 10))
	bucket := tx.Bucket([]byte("data")).Bucket(serverIDBytes)
	if bucket == nil {
		return errors.Errorf("the bucket of server_id: %d is missing", o.curServerID)
	}

	c := bucket.Cursor()
	k, _ := c.Seek(key)
	if k == nil {
		return errors.Errorf("the k-v of key: %d is missing", key)
	}

	for {
		k, v := c.Prev()
		if k == nil {
			break
		}

		var value Value
		err := json.Unmarshal(v, &value)
		if err != nil {
			return err
		}
		if time.Now().Sub(value.Time) >= 7*24*time.Hour {
			bucket.Delete(k)
		}

	}

	return nil
}

// Make a sortable bytes slice, used as key of blotdb key-value
func makeKey(pos prog.Position) ([]byte, error) {
	var key Key
	splited := strings.Split(pos.Name, ".")
	if len(splited) == 1 {
		return nil, errors.New("parse binlog name error")
	}
	binlog, err := strconv.ParseUint(splited[len(splited)-1], 10, 32)
	if err != nil {
		return nil, err
	}

	key.Name = uint32(binlog)
	key.Pos = pos.Pos

	keyBytes, err := marshal(key)
	if err != nil {
		return nil, err
	}

	return keyBytes, nil
}

func makeValue(snapshot []byte, db string, statement string, pos prog.Position) ([]byte, error) {
	var value Value
	value.ServerID = pos.ServerID
	value.Name = pos.Name
	value.Pos = pos.Pos
	value.Snapshot = snapshot
	value.Database = db
	value.Statement = statement
	value.Time = time.Now()

	buf, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return buf, err
}

func marshal(v interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unmarshal(b []byte, v interface{}) error {
	buffer := bytes.NewBuffer(b)
	err := binary.Read(buffer, binary.BigEndian, v)
	if err != nil {
		return err
	}
	return nil
}
