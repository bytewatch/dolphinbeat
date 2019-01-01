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
	"sync"
)

type MockStorage struct {
	data []byte
	sync.Mutex
}

func NewMockStorage() *MockStorage {
	return &MockStorage{}
}

func (o *MockStorage) Save(data []byte) error {
	o.Lock()
	defer o.Unlock()
	o.data = data
	return nil
}

func (o *MockStorage) Load() ([]byte, error) {
	o.Lock()
	defer o.Unlock()
	return o.data, nil
}
func (o *MockStorage) Close() error {
	return nil
}
