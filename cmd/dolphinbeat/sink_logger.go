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
	"fmt"
	"github.com/siddontang/go-log/log"
	"os"
)

type SinkLogger struct {
	flag string
	l    *log.Logger
}

func NewSinkLogger(name string, logger *log.Logger) *SinkLogger {
	return &SinkLogger{
		flag: fmt.Sprintf("[%s] ", name),
		l:    logger,
	}
}

func (o *SinkLogger) Fatalf(format string, args ...interface{}) {
	o.l.Output(2, log.LevelFatal, o.flag+fmt.Sprintf(format, args...))
	os.Exit(1)
}

func (o *SinkLogger) Panicf(format string, args ...interface{}) {
	msg := o.flag + fmt.Sprintf(format, args...)
	o.l.Output(2, log.LevelError, msg)
	panic(msg)
}

func (o *SinkLogger) Printf(format string, args ...interface{}) {
	o.l.Output(2, log.LevelInfo, o.flag+fmt.Sprintf(format, args...))
}

func (o *SinkLogger) Debugf(format string, args ...interface{}) {
	o.l.Output(2, log.LevelDebug, o.flag+fmt.Sprintf(format, args...))
}

func (o *SinkLogger) Errorf(format string, args ...interface{}) {
	o.l.Output(2, log.LevelError, o.flag+fmt.Sprintf(format, args...))
}

func (o *SinkLogger) Infof(format string, args ...interface{}) {
	o.l.Output(2, log.LevelInfo, o.flag+fmt.Sprintf(format, args...))
}

func (o *SinkLogger) Warnf(format string, args ...interface{}) {
	o.l.Output(2, log.LevelWarn, o.flag+fmt.Sprintf(format, args...))
}
