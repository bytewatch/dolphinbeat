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
	"encoding/json"
	"errors"
	"github.com/bytewatch/dolphinbeat/util"
	"github.com/gorilla/mux"
	"github.com/siddontang/go-log/log"
	"net/http"
)

type StatusRes struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
	BuildTS string `json:"build_ts"`
	Mode    string `json:"mode"`
}

type FailedDDLRes struct {
	Db        string `json:"db"`
	Statement string `json:"statement"`
	Reason    string `json:"reason"`
	Tips      string `json:"tips"`
}

func writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	_, err = w.Write([]byte(err.Error()))
	if err != nil {
		log.Errorf("write http response error: %s", err)
	}
}

func writeData(w http.ResponseWriter, v interface{}) {
	if v == nil {
		w.WriteHeader(http.StatusOK)
		return
	}

	data, err := json.Marshal(v)
	if err != nil {
		log.Errorf("json marshal error: %s", err)
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(data)
	if err != nil {
		log.Errorf("write http response error: %s", err)
	}
}

func (o *Application) handleStatus(w http.ResponseWriter, req *http.Request) {
	mode := "init"
	if !o.cfg.Election.Enabled {
		mode = "standalone"
	} else {
		if o.election != nil {
			if o.election.IsLeader() {
				mode = "leader"
			} else {
				mode = "follower"
			}
		}
	}
	res := &StatusRes{
		Version: util.Version,
		GitHash: util.GitHash,
		BuildTS: util.BuildTS,
		Mode:    mode,
	}
	writeData(w, res)
}

func (o *Application) handleSchema(w http.ResponseWriter, req *http.Request) {

	var dbName, tableName string
	var ok bool

	params := mux.Vars(req)
	if dbName, ok = params["db"]; !ok {
		// Print all databases' name
		writeData(w, o.canal.GetDatabases())
		return
	}

	if tableName, ok = params["table"]; !ok {
		// Print all tables' name in this database
		tables, err := o.canal.GetTables(dbName)
		if err != nil {
			writeError(w, err)
			return
		}
		writeData(w, tables)
		return
	}

	// Print table info a specified table name
	tableDef, err := o.canal.GetTableDef(dbName, tableName)
	if err != nil {
		writeError(w, err)
		return
	}
	writeData(w, tableDef)

	return
}

func (o *Application) handleFailedDDL(w http.ResponseWriter, req *http.Request) {
	o.Lock()
	defer o.Unlock()
	if o.failedDDL != "" {
		res := &FailedDDLRes{
			Db:        o.failedDb,
			Statement: o.failedDDL,
			Reason:    o.failedReason,
			Tips:      "You can try 'curl .../ddl/exec  --data-urlencode 'statement=...' ' to fix, and then 'curl .../ddl/retry' to tell me to retry",
		}
		writeData(w, res)
	}
}

func (o *Application) handleRetryDDL(w http.ResponseWriter, req *http.Request) {
	o.Lock()
	if o.failedDDL == "" {
		o.Unlock()
		writeError(w, errors.New("no failed ddl"))
		return
	}
	o.Unlock()

	o.retryDDLCh <- struct{}{}
	writeData(w, nil)

}

func (o *Application) handleExecDDL(w http.ResponseWriter, req *http.Request) {
	statement := req.FormValue("statement")
	if statement == "" {
		writeError(w, errors.New("param \"statement\" is empty"))
		return
	}
	err := o.canal.ExecDDL("", statement)
	if err != nil {
		writeError(w, err)
		log.Errorf("execute sql error: %s", err)
		return
	}
	log.Info("execute sql succeed")
	writeData(w, nil)
}
