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
	"context"
	"encoding/json"
	"fmt"
	"github.com/bytewatch/dolphinbeat/canal/prog"
	"github.com/siddontang/go-log/log"
	"sync"
	"time"
)

type Data struct {
	Time            time.Time              `json:"time"`
	AlignedProgress *Progress              `json:"aligned_progress,omitempty"`
	Ckps            map[string]*Checkpoint `json:"ckps"`
}

type CkpManager struct {
	cfg *Config

	// Checkpoint info of each Checkpointer
	ckps map[string]*Checkpoint
	reg  map[string]Checkpointer

	// All Checkpointer(sink)'s progress are more advanced than alignedProgress at least.
	// alignedProgress is uesed when can not retrive min progress by comparision.
	alignedProgress *Progress

	storage CkpStorage

	errCh  chan error
	cancel context.CancelFunc

	sync.Mutex
}

func NewCkpManager(cfg *Config) (*CkpManager, error) {
	var err error
	m := &CkpManager{
		cfg:   cfg,
		reg:   make(map[string]Checkpointer),
		ckps:  make(map[string]*Checkpoint),
		errCh: make(chan error, 1),
	}

	switch cfg.Storage {
	case StorageType_File:
		m.storage, err = NewFileStorage(cfg.Dir + "/checkpoint.json")
	case StorageType_Zookeeper:
		m.storage, err = NewZookeeperStorage(cfg.ZkHosts, cfg.ZkPath)
	case StorageType_Mock:
		m.storage = NewMockStorage()
	default:
		err = fmt.Errorf("unknown storage type: %s", cfg.Storage)
	}
	if err != nil {
		return nil, err
	}

	buf, err := m.storage.Load()
	if err != nil {
		log.Errorf("storage load error: %s", err)
		m.storage.Close()
		return nil, err
	}

	if buf == nil {
		log.Warn("checkpoint data loaded from storage is empty")
	} else {
		var data Data
		err = json.Unmarshal(buf, &data)
		if err != nil {
			log.Errorf("unmarshal checkpoint content error: %s", err)
		}
		m.alignedProgress = data.AlignedProgress
		m.ckps = data.Ckps
	}

	return m, nil
}

func (o *CkpManager) Start() error {
	o.Lock()
	defer o.Unlock()

	// Save checkpoint info at start, helps to check whether have problems in storage.
	buf, err := o.serializeData()
	if err != nil {
		return err
	}
	err = o.storage.Save(buf)
	if err != nil {
		log.Errorf("save checkpoint error: %s", err)
		return err
	}

	var ctx context.Context
	ctx, o.cancel = context.WithCancel(context.Background())
	go o.run(ctx)
	return nil
}

// Clean any ckp in the o.ckps that is not in use
func (o *CkpManager) clean() {
	var serverID uint32
	oneServerID := true

	for name, ckp := range o.ckps {
		if _, ok := o.reg[name]; !ok {
			delete(o.ckps, name)
			continue
		}
		if serverID == 0 {
			serverID = ckp.ServerID
			continue
		}
		if serverID != 0 && ckp.ServerID != serverID {
			oneServerID = false
		}
	}

	if oneServerID {
		// All Checkpointer(sink)'s server_id are same, not need alignedProgress
		o.alignedProgress = nil
	}
}

func (o *CkpManager) SetAlignedProgress(p prog.Progress) error {
	o.Lock()
	defer o.Unlock()
	o.alignedProgress = &Progress{}
	o.alignedProgress.SetProgress(p)
	buf, err := o.serializeData()
	if err != nil {
		return err
	}
	err = o.storage.Save(buf)
	if err != nil {
		log.Errorf("save checkpoint error: %s", err)
		return err
	}
	return nil
}

func (o *CkpManager) GetMinProgress() prog.Progress {
	// If using gtid, we can not compare by gitd_set, because each Progress's gtid_set may
	// neither contain each other.  And, we can not compare by file&pos when server_ids
	// of each Progress may be different. So, we use last aligned Progress as min Progress,
	// when server_ids of each Progress may be different

	o.Lock()
	defer o.Unlock()

	// Clean any ckp that is not used by any sink
	o.clean()

	var minProgress prog.Progress
	for _, ckp := range o.ckps {
		progress := ckp.GetProgress()
		if progress.IsZero() {
			continue
		}
		if minProgress.IsZero() {
			minProgress = progress
			continue
		}
		if progress.Pos.ServerID != minProgress.Pos.ServerID {
			// If Progress's server_id is not equal, we use last aligned Progress
			// as min Progress
			log.Warn("different server_id found in ckps, will use last aligned progress as min progress")
			if o.alignedProgress == nil {
				log.Panicf("last aligned progress is nil")
			}
			return o.alignedProgress.GetProgress()
		}
		if progress.Compare(&minProgress) < 0 {
			minProgress = progress
		}
	}
	return minProgress
}

func (o *CkpManager) GetCheckpoint(name string) *Checkpoint {
	o.Lock()
	defer o.Unlock()

	var ckp *Checkpoint
	ckp, ok := o.ckps[name]
	if !ok {
		log.Warnf("checkpoint name not found: %s", name)
		return NewCheckpoint()
	}
	return ckp
}

func (o *CkpManager) serializeData() ([]byte, error) {
	data := &Data{Time: time.Now(), AlignedProgress: o.alignedProgress, Ckps: o.ckps}
	buf, err := json.Marshal(data)
	if err != nil {
		o.Unlock()
		log.Errorf("marshal checkpoint content error: %s", err)
		return buf, err
	}
	return buf, nil

}

// WaitUntil wait all sinks to reach a specified progress
func (o *CkpManager) WaitUntil(ctx context.Context, progress prog.Progress) error {
	for {
		allDone := true
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			o.Lock()
			for name, ckper := range o.reg {
				ckp := ckper.Checkpoint()
				o.ckps[name] = ckp
				p := ckp.GetProgress()
				// If server_id is different, we treat it as not done, need to wait
				done := p.Position().ServerID == progress.Position().ServerID &&
					p.Compare(&progress) >= 0
				if !done {
					allDone = false
					break
				}
			}
			if allDone {
				log.Infof("wait util all sinks to catch progress succeed: %s", progress)
				buf, err := o.serializeData()
				if err != nil {
					o.Unlock()
					return err
				}
				err = o.storage.Save(buf)
				o.Unlock()
				return err
			}
			o.Unlock()
			log.Infof("wait util all sinks to catch progress: %s", progress)
			time.Sleep(100 * time.Millisecond)
		}
	}

	return nil
}

func (o *CkpManager) RegisterCheckpointer(name string, ckper Checkpointer) error {
	o.Lock()
	defer o.Unlock()

	if o.reg[name] != nil {
		return fmt.Errorf("checkpointer exists already: %s", name)
	}
	o.reg[name] = ckper
	o.ckps[name] = ckper.Checkpoint()
	return nil
}

func (o *CkpManager) Err() <-chan error {
	return o.errCh
}

func (o *CkpManager) Close() error {
	log.Warnf("closing ckp manager")
	if o.cancel != nil {
		o.cancel()
		for range o.errCh {
			// drain
		}
	}

	if o.storage != nil {
		o.storage.Close()
	}

	return nil
}

func (o *CkpManager) run(ctx context.Context) {
	defer close(o.errCh)

	ticker := time.NewTicker(time.Duration(o.cfg.Interval) * time.Second)
	tick := 0
	for {
		select {
		case <-ticker.C:
			o.Lock()
			for name, ckper := range o.reg {
				ckp := ckper.Checkpoint()
				o.ckps[name] = ckp
			}
			buf, err := o.serializeData()
			if err != nil {
				o.Unlock()
				o.errCh <- err
				return
			}
			if tick%50 == 0 {
				// Print checkpoint info into log every 50 tick
				log.Infof("checkpoint info: %s", buf)
			}
			err = o.storage.Save(buf)
			o.Unlock()

			if err != nil {
				log.Errorf("save checkpoint error: %s", err)
				o.errCh <- err
				return
			}
			tick++

		case <-ctx.Done():
			log.Warnf("context canceled")
			return
		}
	}
}
