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
	"github.com/siddontang/go-log/log"
	"io"
)

func (o *Application) initLog() error {
	var err error
	dir := "."
	level := "info"
	if o.cfg.Log.Dir != "" {
		dir = o.cfg.Log.Dir
	}
	if o.cfg.Log.Level != "" {
		level = o.cfg.Log.Level
	}

	o.logWriter, err = log.NewRotatingFileHandler(dir+"/dolphinbeat.log", 33554432, 10)
	if err != nil {
		log.Errorf("failed to create file: %s", err)
		return err
	}

	handler, _ := log.NewStreamHandler(o.logWriter)
	o.logger = log.NewDefault(handler)
	log.SetDefaultLogger(o.logger)
	log.SetLevelByName(level)

	return nil
}

func (o *Application) uninitLog() {
	if o.logger != nil {
		o.logger.Close()
	}

	//Close Writer
	if o.logWriter != nil {
		if wc, ok := o.logWriter.(io.Closer); ok {
			wc.Close()
		}
	}

}
