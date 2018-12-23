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

// Before canal execute ddl, we need ensure all dml events has been synced
func (o *Application) beforeSchemaChange(db string, statement string) error {
	if o.canal.TrxCount() == 0 {
		// progress has not changed, don't need to wait
		return nil
	}
	return o.ckpManager.WaitUntil(o.ctx, o.canal.SyncedProgress())
}

// When canal execute ddl failed, we need app tell canal what to do next, skip it or not?
func (o *Application) onSchemaChangeFailed(db string, statement string, err error) (bool, error) {
	failedDDLCounter.Inc()

	o.Lock()
	o.failedDb = db
	o.failedDDL = statement
	o.failedReason = err.Error()
	o.Unlock()

	select {
	case <-o.retryDDLCh:
	case <-o.ctx.Done():
		return false, o.ctx.Err()
	}

	o.Lock()
	o.failedDb = ""
	o.failedDDL = ""
	o.failedReason = ""
	o.Unlock()

	return false, nil
}

// Before canal process events of new server_id, we need ensure all events of old server_id have been synced
func (o *Application) beforeServerIDChange(old uint32, new uint32) error {
	if o.canal.TrxCount() == 0 {
		// progress has not changed, don't need to wait
		return nil
	}

	progress := o.canal.SyncedProgress()
	err := o.ckpManager.WaitUntil(o.ctx, progress)
	if err != nil {
		return err
	}
	// Record this progress as aligned progress by all sink
	err = o.ckpManager.SetAlignedProgress(progress)
	return err
}
