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
	"context"
	"fmt"
	"github.com/bytewatch/dolphinbeat/canal"
	"github.com/bytewatch/dolphinbeat/canal/prog"
	"github.com/bytewatch/dolphinbeat/ckp"
	"github.com/bytewatch/dolphinbeat/sink"
	"github.com/bytewatch/dolphinbeat/util"
	"github.com/bytewatch/election"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"io"
	"strings"
	"sync"
	"time"
)

type Application struct {
	cfg     *Config
	cfgPath string

	handlerMux *canal.HandlerMux
	canal      *canal.Canal
	ckpManager *ckp.CkpManager
	sinks      []sink.Sink

	startProgress prog.Progress

	logWriter io.Writer
	logger    *log.Logger

	httpServer *HttpServer
	election   *election.Election

	// The failed DDL statement if any
	// Protected by mutex
	failedDb     string
	failedDDL    string
	failedReason string
	retryDDLCh   chan struct{}

	ctx    context.Context
	cancel context.CancelFunc

	// When app quited, will trigger an event on this channel
	errCh chan error

	sync.Mutex
}

func NewApplication() *Application {
	return &Application{
		retryDDLCh: make(chan struct{}, 1),
		errCh:      make(chan error, 1),
	}
}

func (o *Application) Version() string {
	return fmt.Sprintf("%s %s built at %s", util.Version, util.GitHash, util.BuildTS)
}

func (o *Application) Usage() string {
	return `DolphinBeat.
	This is a program that pretends to be a MySQL slave, parses binlog from master, and pushs incremental update data into different sinks.
	Supported sinks are: stdout, kafka...

Usage:
	./dolphinbeat [--cfg=<path>] 
	./dolphinbeat -h | --help
	./dolphinbeat --version

Options:
	--cfg=<path>  config file path [default: ../etc/dolphinbeat.toml].`
}

func (o *Application) SetOpts(m map[string]interface{}) {
	o.cfgPath = m["--cfg"].(string)
}

func (o *Application) Close() {
	log.Warn("application closing")
	o.cancel()
	for range o.errCh {
	}
}

func (o *Application) initHttpServer() error {
	var err error
	o.httpServer, err = NewHttpServer(&o.cfg.HttpServer, o)
	if err != nil {
		return err
	}
	return nil
}

func (o *Application) uninitHttpServer() error {
	return o.httpServer.Close()
}

func (o *Application) initElection() error {
	var err error
	cfg := election.Config{
		ZkHosts: strings.Split(o.cfg.Election.ZkHosts, ","),
		ZkPath:  o.cfg.Election.ZkPath,
		Lease:   o.cfg.Election.Lease,
		Logger:  o.logger,
	}
	o.election, err = election.NewElection(&cfg)
	if err != nil {
		return err
	}
	return nil
}

func (o *Application) uninitElection() error {
	return o.election.Close()
}

// Init checkpoint manager, which saves binlog progress into to file or zookeeper
func (o *Application) initCkpManager() error {
	var err error
	cfg := ckp.NewDefaultConfig()
	cfg.Storage = o.cfg.Checkpoint.Storage
	cfg.ZkHosts = o.cfg.Checkpoint.ZkHosts
	cfg.ZkPath = o.cfg.Checkpoint.ZkPath
	if o.cfg.Checkpoint.Interval != 0 {
		cfg.Interval = o.cfg.Checkpoint.Interval
	}
	if o.cfg.Checkpoint.Dir != "" {
		cfg.Dir = o.cfg.Checkpoint.Dir
	}
	o.ckpManager, err = ckp.NewCkpManager(cfg)
	if err != nil {
		return err
	}
	return nil
}

func (o *Application) uninitCkpManager() {
	o.ckpManager.Close()
}

// Init each sink, and register it into checkpoint manager,
// so that checkpoint manager will save sink's progress periodcally.
func (o *Application) initSinks() error {
	// Create a handler multiplexer
	o.handlerMux = canal.NewHandlerMux()

	for _, sinkCfg := range o.cfg.Sinks {
		if !sinkCfg.Enabled {
			continue
		}
		sinkLogger := NewSinkLogger(sinkCfg.Name, o.logger)
		log.Infof("initialize sink: %s, type: %s, include_table: %v, exclude_table: %v",
			sinkCfg.Name, sinkCfg.Type, sinkCfg.IncludeTable, sinkCfg.ExcludeTable)
		sink, err := sink.NewSink(sinkCfg.Type, sinkCfg.Name, sinkCfg.Cfg, sinkLogger)
		if err != nil {
			return err
		}

		// Get last saved checkpoint of this sink
		ckp := o.ckpManager.GetCheckpoint(sinkCfg.Name)

		// Initialize this sink, and do recovery job inside if necessary
		err = sink.Initialize(o.ctx, ckp)
		if err != nil {
			log.Errorf("initialize sink error: %s", err)
			return err
		}
		o.ckpManager.RegisterCheckpointer(sinkCfg.Name, sink)

		// Get the recovered checkpoint of this sink
		var progress prog.Progress
		if ckp := o.ckpManager.GetCheckpoint(sinkCfg.Name); ckp != nil {
			progress = ckp.GetProgress()
		}
		handlerCfg := canal.EventHandlerConfig{
			Progress:     progress,
			IncludeTable: sinkCfg.IncludeTable,
			ExcludeTable: sinkCfg.ExcludeTable,
		}
		// Add each sink (event handler) into handlerMux
		o.handlerMux.RegisterEventHandler(sink, handlerCfg)

		o.sinks = append(o.sinks, sink)
	}

	// Start to save ckp info with specified time interval
	err := o.ckpManager.Start()
	if err != nil {
		return err
	}

	return nil
}

func (o *Application) uninitSinks() {
	for _, sink := range o.sinks {
		sink.Close()
	}
}

// Init canal library, and start from min progress among all sinks
func (o *Application) initCanal() error {
	var err error

	cfg := canal.NewDefaultConfig()
	cfg.Addr = o.cfg.MysqlAddr
	cfg.User = o.cfg.MysqlUser
	cfg.Password = o.cfg.MysqlPassword
	cfg.Charset = o.cfg.MysqlCharset
	cfg.Flavor = o.cfg.Flavor
	cfg.GtidEnabled = o.cfg.GtidEnabled

	cfg.HeartbeatPeriod = 200 * time.Millisecond
	cfg.ReadTimeout = 300 * time.Millisecond

	cfg.Dump.ExecutionPath = o.cfg.DumpExec
	cfg.Dump.SkipMasterData = o.cfg.SkipMasterData
	cfg.Dump.DiscardErr = false

	cfg.Tracker.CharsetServer = o.cfg.Tracker.CharsetServer
	cfg.Tracker.Storage = o.cfg.Tracker.Storage
	cfg.Tracker.Dir = o.cfg.Tracker.Dir
	cfg.Tracker.Addr = o.cfg.Tracker.Addr
	cfg.Tracker.User = o.cfg.Tracker.User
	cfg.Tracker.Password = o.cfg.Tracker.Password
	cfg.Tracker.Database = o.cfg.Tracker.Database

	o.canal, err = canal.NewCanal(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	// Register a hook, will triggered before mysql schema changes
	o.canal.RegisterBeforeSchemaChangeHook(o.beforeSchemaChange)
	// Register a hook, will triggered on DDL failed
	o.canal.RegisterOnSchemaChangeFailedHook(o.onSchemaChangeFailed)
	// Register a hook, will triggered before mysql server_id changes
	o.canal.RegisterBeforeServerIDChangeHook(o.beforeServerIDChange)

	o.canal.SetEventHandler(o.handlerMux)

	o.startProgress = o.ckpManager.GetMinProgress()
	log.Infof("start progress in checkpoints is: %s", o.startProgress)
	o.canal.Start(o.startProgress)

	return nil
}

func (o *Application) uninitCanal() {
	o.canal.Close()
}

func (o *Application) mergeSinksErr() <-chan error {
	var wg sync.WaitGroup
	// We must ensure that the output channel has the capacity to
	// hold as many errors as there are error channels.
	out := make(chan error, len(o.sinks))

	output := func(c <-chan error) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(o.sinks))
	for _, sink := range o.sinks {
		go output(sink.Err())
	}
	// Start a goroutine to close out once all the output goroutines
	// are done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func (o *Application) Run() error {
	var err error
	defer close(o.errCh)

	o.ctx, o.cancel = context.WithCancel(context.Background())

	err = o.initConfig()
	if err != nil {
		log.Errorf("init cfg error: %s", err)
		return err
	}

	err = o.initLog()
	if err != nil {
		log.Errorf("init log error: %s", err)
		return err
	}
	defer o.uninitLog()

	log.Infof("application started, version is: %s", o.Version())
	defer log.Info("application quitted")

	err = o.initHttpServer()
	if err != nil {
		log.Errorf("prepare http server error: %s", err)
		return err
	}
	defer o.uninitHttpServer()

	if o.cfg.Election.Enabled {
		err = o.initElection()
		if err != nil {
			log.Errorf("prepare election error: %s", err)
			return err
		}
		defer o.uninitElection()
		becomeLeaderCh := make(chan struct{})
		go func() {
			for {
				select {
				case <-o.election.Notify():
					if o.election.IsLeader() {
						becomeLeaderCh <- struct{}{}
						return
					}
				case <-o.ctx.Done():
					return
				}
			}
		}()
		log.Infof("wait to become leader")
		select {
		case <-becomeLeaderCh:
			log.Infof("I am leader now")
		case err = <-o.httpServer.Err():
			log.Warnf("http server quit with error: %s", err)
			return err
		case <-o.ctx.Done():
			log.Warnf("context canceled")
			return nil
		}
	}

	err = o.initCkpManager()
	if err != nil {
		log.Errorf("prepare ckp manager error: %s", err)
		return err
	}
	defer o.uninitCkpManager()

	err = o.initSinks()
	if err != nil {
		log.Errorf("prepare sinks error: %s", err)
		return err
	}
	defer o.uninitSinks()

	err = o.initCanal()
	if err != nil {
		log.Errorf("prepare canal error: %s", err)
		return err
	}
	defer o.uninitCanal()

	o.initMetrics()

	sinksErrCh := o.mergeSinksErr()

	var electionErrCh <-chan error
	if o.cfg.Election.Enabled {
		electionErrCh = o.election.Err()
	}

	select {
	case err = <-o.httpServer.Err():
		log.Warnf("http server quit with error: %s", err)
	case err = <-electionErrCh:
		log.Warnf("election quit with error: %s", err)
	case err = <-sinksErrCh:
		log.Warnf("sinks quit with error: %s", err)
	case err = <-o.canal.Err():
		log.Warnf("canal quit with error: %s", err)
	case err = <-o.ckpManager.Err():
		log.Warnf("ckp manager quit with error: %s", err)
	case <-o.ctx.Done():
		log.Warnf("context canceled")
	}

	return err
}
