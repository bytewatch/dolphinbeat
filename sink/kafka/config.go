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

package kafka

const (
	Compression_NONE = "none"
	Compression_ZLIB = "zlib"
	Compression_GZIP = "gzip"
)

type Config struct {
	Encoder        string `toml:"encoder"`
	BrokerList     string `toml:"broker_list"`
	Topic          string `toml:"topic"`
	Partition      int32  `toml:"partition"`
	MaxPayloadSize int    `toml:"max_payload_size"`
	Compression    string `toml:"compression"`
}
