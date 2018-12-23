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
	"github.com/docopt/docopt-go"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	app := NewApplication()

	opts, err := docopt.ParseArgs(app.Usage(), nil, app.Version())
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse command line options error: %s\n", err)
	}
	app.SetOpts(opts)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func(c chan os.Signal) {
		s := <-c
		fmt.Fprintf(os.Stderr, "signal received: %d\n", s)
		app.Close()
	}(sc)

	err = app.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "app run error: %s\n", err)
		return
	}
}
