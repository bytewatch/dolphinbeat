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
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
	"net/http/pprof"
)

type HttpServer struct {
	cfg   *HttpServerConfig
	s     *http.Server
	errCh chan error
}

func newServeMux(app *Application) *http.ServeMux {
	router := mux.NewRouter()

	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	router.HandleFunc("/debug/pprof/trace", pprof.Trace)

	router.Handle("/metrics", prometheus.Handler())

	router.HandleFunc("/status", app.handleStatus)
	router.HandleFunc("/schema", app.handleSchema)
	router.HandleFunc("/schema/{db}", app.handleSchema)
	router.HandleFunc("/schema/{db}/{table}", app.handleSchema)

	router.HandleFunc("/ddl/failed", app.handleFailedDDL)
	router.HandleFunc("/ddl/retry", app.handleRetryDDL)
	router.HandleFunc("/ddl/exec", app.handleExecDDL)

	mux := http.NewServeMux()
	mux.Handle("/", router)

	return mux
}

func NewHttpServer(cfg *HttpServerConfig, app *Application) (*HttpServer, error) {
	mux := newServeMux(app)

	addr := cfg.Addr
	if addr == "" {
		addr = ":8080"
	}

	s := &http.Server{Addr: addr, Handler: mux}

	server := HttpServer{
		cfg:   cfg,
		s:     s,
		errCh: make(chan error, 1),
	}

	go server.run()
	return &server, nil
}

func (o *HttpServer) Err() <-chan error {
	return o.errCh
}

func (o *HttpServer) Close() error {
	o.s.Shutdown(nil)
	for range o.errCh {
	}
	return nil
}

func (o *HttpServer) run() {
	defer close(o.errCh)
	err := o.s.ListenAndServe()
	if err != nil {
		o.errCh <- err
		return
	}
}
