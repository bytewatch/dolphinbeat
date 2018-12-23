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

package canal

type Observer struct {
	BeforeSchemaChange   func(string, string) error
	OnSchemaChangeFailed func(string, string, error) (bool, error)
	BeforeServerIDChange func(uint32, uint32) error
}

// Register a hook that will be called before schema change
func (c *Canal) RegisterBeforeSchemaChangeHook(fn func(string, string) error) {
	c.observer.BeforeSchemaChange = fn
}

// Register a hook that will be called on DDL failed
func (c *Canal) RegisterOnSchemaChangeFailedHook(fn func(string, string, error) (bool, error)) {
	c.observer.OnSchemaChangeFailed = fn
}

// Register a hook that will be called before server_id change
func (c *Canal) RegisterBeforeServerIDChangeHook(fn func(uint32, uint32) error) {
	c.observer.BeforeServerIDChange = fn
}

func (c *Canal) runBeforeSchemaChangeHook(db string, statement string) error {
	if c.observer.BeforeSchemaChange == nil {
		return nil
	}
	return c.observer.BeforeSchemaChange(db, statement)
}

func (c *Canal) runOnSchemaChangeFailedHook(db string, statement string, err error) (bool, error) {
	if c.observer.OnSchemaChangeFailed == nil {
		return false, err
	}
	return c.observer.OnSchemaChangeFailed(db, statement, err)
}

func (c *Canal) runBeforeServerIDChangeHook(old uint32, new uint32) error {
	if c.observer.BeforeServerIDChange == nil {
		return nil
	}
	return c.observer.BeforeServerIDChange(old, new)
}
