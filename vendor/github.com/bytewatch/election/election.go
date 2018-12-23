package election

import (
	"context"
	"github.com/samuel/go-zookeeper/zk"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type Role string

const (
	Role_LEADER   Role = "leader"
	Role_FOLLOWER Role = "follower"
	Role_UNKNOWN  Role = "unkown"
)

type Election struct {
	conn *zk.Conn

	// Protected by mutex
	connected bool

	// The election role
	// Protected by mutex
	role Role

	// Triggered when there are session events or node events happen
	eventCh <-chan zk.Event

	// Triggered when election role changed or session status change
	notifyCh chan struct{}

	nodeName string

	cancel context.CancelFunc

	errCh chan error

	logger Logger

	sync.Mutex
}

func NewElection(cfg *Config) (*Election, error) {
	var err error
	var conn *zk.Conn

	o := &Election{
		connected: false,
		role:      Role_UNKNOWN,
		notifyCh:  make(chan struct{}, 1),
		errCh:     make(chan error, 1),
	}

	o.logger = DefaultLogger
	if cfg.Logger != nil {
		o.logger = cfg.Logger
	}

	defer func() {
		if err == nil {
			return
		}
		if conn != nil {
			conn.Close()
		}
	}()

	conn, eventCh, err := zk.Connect(cfg.ZkHosts, time.Duration(cfg.Lease)*time.Second, zk.WithLogInfo(false))
	if err != nil {
		o.logger.Printf("connect zookeeper error: %s, addrs: %v", err, cfg.ZkHosts)
		return nil, err
	}

	nodesPath := cfg.ZkPath
	err = createNodeIfNotExists(conn, nodesPath, []byte(""))
	if err != nil {
		o.logger.Printf("create node error: %s, path: %s", err, nodesPath)
		return nil, err
	}

	// Set children wath at nodesPath
	_, _, _, err = conn.ChildrenW(nodesPath)
	if err != nil {
		o.logger.Printf("children watcherror: %s", err)
		return nil, err
	}

	// Create ephemeral node, and this will trigger our election logic later
	nodeName, err := createEphemeralSequential(conn, nodesPath, []byte(""))
	if err != nil {
		o.logger.Printf("create ephemeral node error: %s", err)
		return nil, err
	}

	o.logger.Printf("created ephemeral node: %s", nodeName)

	var ctx context.Context
	ctx, cancel := context.WithCancel(context.Background())

	o.conn = conn
	o.eventCh = eventCh
	o.nodeName = nodeName
	o.cancel = cancel

	go o.run(ctx)
	return o, nil
}

func (o *Election) Notify() <-chan struct{} {
	return o.notifyCh
}

func (o *Election) getRole() Role {
	o.Lock()
	defer o.Unlock()
	return o.role
}

func (o *Election) setRole(role Role) {
	o.Lock()
	defer o.Unlock()
	o.role = role
}

func (o *Election) setConnected(connected bool) {
	o.Lock()
	defer o.Unlock()
	o.connected = connected
}

func (o *Election) IsConnected() bool {
	o.Lock()
	defer o.Unlock()
	return o.connected
}

func (o *Election) IsLeader() bool {
	role := o.getRole()
	return role == Role_LEADER
}

func (o *Election) handleSessionEvent(event zk.Event) {
	switch event.State {
	case zk.StateHasSession:
		o.setConnected(true)
		if o.getRole() == Role_UNKNOWN {
			o.logger.Printf("I become follower")
			o.setRole(Role_FOLLOWER)
			o.notifyCh <- struct{}{}
		}
	case zk.StateDisconnected:
		o.logger.Printf("zookeeper connection disconnected")
		o.setConnected(false)
		o.notifyCh <- struct{}{}
	case zk.StateExpired:
		// When session expired, we can do nothing to recover session.
		// Panic and quit application as fast as we can.
		o.logger.Printf("zookeeper session expired, abort now")
		panic("zookeeper session expired")

	default:

	}
}

func (o *Election) getCandidates(path string) []string {
	// Get children list and set watch again
	candidates, _, _, err := o.conn.ChildrenW(path)
	if err != nil {
		o.logger.Printf("get zookeeper children error: %s", err)
		panic("get zookeeper children error")
	}
	return candidates
}

func (o *Election) handleChildrenEvent(event zk.Event) {
	mySeq := o.getNodeSeq(o.nodeName)

	candidates := o.getCandidates(event.Path)

	minSeq := o.getNodeSeq(candidates[0])
	for i := range candidates {
		seq := o.getNodeSeq(candidates[i])
		if seq <= minSeq {
			minSeq = seq
		}
	}

	o.logger.Printf("min seq of all candidates: %d, my seq: %d", minSeq, mySeq)

	if minSeq == mySeq {
		if !o.IsLeader() {
			o.logger.Printf("I become leader")
			o.setRole(Role_LEADER)
			o.notifyCh <- struct{}{}
		}
		return
	}

	// My seq is not the minimum
	if o.IsLeader() {
		o.logger.Printf("I am leader, but the minimum node is not mine, abort now")
		panic("unexpected election status")
	}

}

func (o *Election) run(ctx context.Context) {
	defer close(o.errCh)

	for {
		select {
		case event := <-o.eventCh:
			switch event.Type {
			case zk.EventSession:
				go o.handleSessionEvent(event)
			case zk.EventNodeChildrenChanged:
				go o.handleChildrenEvent(event)
			default:
			}
		case <-ctx.Done():
			o.logger.Printf("context canceled")
			return
		}
	}
}

func (o *Election) Err() <-chan error {
	return o.errCh
}

func (o *Election) getNodeSeq(nodeName string) int {
	suffix := nodeName[len(nodeName)-10:]
	seq, err := strconv.Atoi(suffix)
	if err != nil || seq < 0 {
		o.logger.Printf("parse zookeeper node seq err: %s, abort now", err)
		panic("parse zookeeper node seq err")
	}
	return seq
}

func (o *Election) Close() error {

	o.cancel()

	for range o.errCh {
	}

	if o.conn != nil {
		o.conn.Close()
	}

	return nil
}

func createNodeIfNotExists(conn *zk.Conn, path string, data []byte) error {
	exists, _, err := conn.Exists(path)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	dir := filepath.Dir(path)
	if dir != "/" {
		err = createNodeIfNotExists(conn, dir, []byte{})
		if err != nil {
			return err
		}
	}

	_, err = conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
}

func createEphemeralSequential(conn *zk.Conn, dir string, data []byte) (string, error) {
	if dir[len(dir)-1] != '/' {
		// Make sure to create node under dir
		dir += "/"
	}
	nodeName, err := conn.CreateProtectedEphemeralSequential(dir, data, zk.WorldACL(zk.PermAll))
	if err != nil {
		return "", err
	}
	return nodeName, nil
}
