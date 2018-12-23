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
	"github.com/prometheus/client_golang/prometheus"
)

var (
	failedDDLCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "failed_ddl_total",
			Help: "Counter of failed DDL.",
		})
)

func (o *Application) initMetrics() {
	trxCounter := prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "trx_total",
			Help: "Counter of trx handled by dolphinbeat.",
		}, func() float64 { return float64(o.canal.TrxCount()) })
	iudCounter := prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "iud_total",
			Help: "Counter of insert/update/delete events handled by dolphinbeat.",
		}, func() float64 { return float64(o.canal.TrxCount()) })
	ddlCounter := prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "ddl_total",
			Help: "Counter of DDL handled by dolphinbeat.",
		}, func() float64 { return float64(o.canal.TrxCount()) })

	prometheus.MustRegister(trxCounter)
	prometheus.MustRegister(iudCounter)
	prometheus.MustRegister(ddlCounter)

	prometheus.MustRegister(failedDDLCounter)
}
