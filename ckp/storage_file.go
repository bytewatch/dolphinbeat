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
	"fmt"
	"github.com/siddontang/go-log/log"
	"io"
	"io/ioutil"
	"os"
	"time"
)

type FileStorage struct {
	path string

	// We need two file, because truncating file and writing into file is not atomic
	fw0 *os.File
	fw1 *os.File

	// 0 means path0, 1 means path1
	nextToWrite int
}

func NewFileStorage(path string) (*FileStorage, error) {
	var err error

	fw0, err := os.OpenFile(path+".0", os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return nil, err
	}

	fw1, err := os.OpenFile(path+".1", os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return nil, err
	}

	storage := FileStorage{
		path: path,
		fw0:  fw0,
		fw1:  fw1,
	}

	return &storage, err
}

func (o *FileStorage) Close() error {
	if o.fw0 != nil {
		o.fw0.Close()
	}
	if o.fw1 != nil {
		o.fw1.Close()
	}
	return nil
}

// Save syncs data into one of two file, alternately.
func (o *FileStorage) Save(data []byte) error {
	var err error

	fw := o.fw0
	if o.nextToWrite == 1 {
		fw = o.fw1
	}

	err = fw.Truncate(int64(len(data)))
	if err != nil {
		log.Errorf("truncate file error: %s", err)
		return err
	}
	_, err = fw.Seek(0, io.SeekStart)
	if err != nil {
		log.Errorf("seek file error: %s", err)
		return err
	}

	_, err = fw.Write(data)
	if err != nil {
		log.Errorf("write file error: %s", err)
		return err
	}

	err = fw.Sync()
	if err != nil {
		log.Errorf("sync file error: %s", err)
		return err
	}

	o.nextToWrite = (o.nextToWrite + 1) % 2

	return nil
}

// Load read data from all two files, and return newer one,
// return nil data when read empty content and no error happened.
func (o *FileStorage) Load() ([]byte, error) {
	var latestData []byte
	var latestTime time.Time
	files := []*os.File{o.fw0, o.fw1}
	errCnt := 0
	for i, file := range files {
		file.Seek(0, io.SeekStart)
		data, err := ioutil.ReadAll(file)
		if err != nil {
			log.Errorf("read checkpoint file error: %s, path: %s", err, fmt.Sprintf("%s.%d", o.path, i))
			errCnt++
			continue
		}

		if len(data) == 0 {
			continue
		}

		stat, err := file.Stat()
		if err != nil {
			log.Errorf("get checkpoint file modtime error: %s, path: %s", err, fmt.Sprintf("%s.%d", o.path, i))
			errCnt++
			continue
		}
		modTime := stat.ModTime()

		if latestData == nil {
			latestData = data
			latestTime = modTime
		} else if data != nil {
			if modTime.After(latestTime) {
				// This file is newer
				latestData = data
				latestTime = modTime
			}
		}
	}

	if errCnt == len(files) {
		return nil, fmt.Errorf("load checkpoint error from all two files")
	}

	return latestData, nil
}
