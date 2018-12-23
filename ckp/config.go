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

type Config struct {
	// storage type to store checkpoint data, may be file or zookeeper
	Storage string

	// the interval to save data
	Interval int

	// file path to store data
	Dir string

	// zookeeper info to store data
	ZkHosts string
	ZkPath  string
}

func NewDefaultConfig() *Config {
	return &Config{
		Storage:  StorageType_File,
		Dir:      ".",
		Interval: 10,
	}
}
