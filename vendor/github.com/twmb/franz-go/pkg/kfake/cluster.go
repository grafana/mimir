package kfake

import (
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// TODO
//
// * Add raft and make the brokers independent
//
// * Support multiple replicas -- we just pass this through

type (

	// Cluster is a mock Kafka broker cluster.
	Cluster struct {
		cfg cfg

		controller *broker
		bs         []*broker

		adminCh      chan func()
		reqCh        chan clientReq
		watchFetchCh chan *watchFetch

		controlMu          sync.Mutex
		control            map[int16][]controlFn
		keepCurrentControl atomic.Bool
		currentBroker      atomic.Pointer[broker]

		data   data
		pids   pids
		groups groups
		sasls  sasls
		bcfgs  map[string]*string

		die  chan struct{}
		dead atomic.Bool
	}

	broker struct {
		c     *Cluster
		ln    net.Listener
		node  int32
		bsIdx int
	}

	controlFn func(kmsg.Request) (kmsg.Response, error, bool)
)

// MustCluster is like NewCluster, but panics on error.
func MustCluster(opts ...Opt) *Cluster {
	c, err := NewCluster(opts...)
	if err != nil {
		panic(err)
	}
	return c
}

// NewCluster returns a new mocked Kafka cluster.
func NewCluster(opts ...Opt) (*Cluster, error) {
	cfg := cfg{
		nbrokers:        3,
		logger:          new(nopLogger),
		clusterID:       "kfake",
		defaultNumParts: 10,

		minSessionTimeout: 6 * time.Second,
		maxSessionTimeout: 5 * time.Minute,

		sasls: make(map[struct{ m, u string }]string),
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if len(cfg.ports) > 0 {
		cfg.nbrokers = len(cfg.ports)
	}

	c := &Cluster{
		cfg: cfg,

		adminCh:      make(chan func()),
		reqCh:        make(chan clientReq, 20),
		watchFetchCh: make(chan *watchFetch, 20),
		control:      make(map[int16][]controlFn),

		data: data{
			id2t:      make(map[uuid]string),
			t2id:      make(map[string]uuid),
			treplicas: make(map[string]int),
			tcfgs:     make(map[string]map[string]*string),
		},
		bcfgs: make(map[string]*string),

		die: make(chan struct{}),
	}
	c.data.c = c
	c.groups.c = c
	var err error
	defer func() {
		if err != nil {
			c.Close()
		}
	}()

	for mu, p := range cfg.sasls {
		switch mu.m {
		case saslPlain:
			if c.sasls.plain == nil {
				c.sasls.plain = make(map[string]string)
			}
			c.sasls.plain[mu.u] = p
		case saslScram256:
			if c.sasls.scram256 == nil {
				c.sasls.scram256 = make(map[string]scramAuth)
			}
			c.sasls.scram256[mu.u] = newScramAuth(saslScram256, p)
		case saslScram512:
			if c.sasls.scram512 == nil {
				c.sasls.scram512 = make(map[string]scramAuth)
			}
			c.sasls.scram512[mu.u] = newScramAuth(saslScram512, p)
		default:
			return nil, fmt.Errorf("unknown SASL mechanism %v", mu.m)
		}
	}
	cfg.sasls = nil

	if cfg.enableSASL && c.sasls.empty() {
		c.sasls.scram256 = map[string]scramAuth{
			"admin": newScramAuth(saslScram256, "admin"),
		}
	}

	for i := 0; i < cfg.nbrokers; i++ {
		var port int
		if len(cfg.ports) > 0 {
			port = cfg.ports[i]
		}
		var ln net.Listener
		ln, err = newListener(port, c.cfg.tls)
		if err != nil {
			return nil, err
		}
		b := &broker{
			c:     c,
			ln:    ln,
			node:  int32(i),
			bsIdx: len(c.bs),
		}
		c.bs = append(c.bs, b)
		go b.listen()
	}
	c.controller = c.bs[len(c.bs)-1]
	go c.run()

	seedTopics := make(map[string]int32)
	for _, sts := range cfg.seedTopics {
		p := sts.p
		if p < 1 {
			p = int32(cfg.defaultNumParts)
		}
		for _, t := range sts.ts {
			seedTopics[t] = p
		}
	}
	for t, p := range seedTopics {
		c.data.mkt(t, int(p), -1, nil)
	}
	return c, nil
}

// ListenAddrs returns the hostports that the cluster is listening on.
func (c *Cluster) ListenAddrs() []string {
	var addrs []string
	c.admin(func() {
		for _, b := range c.bs {
			addrs = append(addrs, b.ln.Addr().String())
		}
	})
	return addrs
}

// Close shuts down the cluster.
func (c *Cluster) Close() {
	if c.dead.Swap(true) {
		return
	}
	close(c.die)
	for _, b := range c.bs {
		b.ln.Close()
	}
}

func newListener(port int, tc *tls.Config) (net.Listener, error) {
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, err
	}
	if tc != nil {
		l = tls.NewListener(l, tc)
	}
	return l, nil
}

func (b *broker) listen() {
	defer b.ln.Close()
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			return
		}

		cc := &clientConn{
			c:      b.c,
			b:      b,
			conn:   conn,
			respCh: make(chan clientResp, 2),
		}
		go cc.read()
		go cc.write()
	}
}

func (c *Cluster) run() {
	for {
		var creq clientReq
		var w *watchFetch

		select {
		case creq = <-c.reqCh:
		case w = <-c.watchFetchCh:
			if w.cleaned {
				continue // already cleaned up, this is an extraneous timer fire
			}
			w.cleanup(c)
			creq = w.creq
		case <-c.die:
			return
		case fn := <-c.adminCh:
			// Run a custom request in the context of the cluster
			fn()
			continue
		}

		kreq := creq.kreq
		kresp, err, handled := c.tryControl(kreq, creq.cc.b)
		if handled {
			goto afterControl
		}

		if c.cfg.enableSASL {
			if allow := c.handleSASL(creq); !allow {
				err = errors.New("not allowed given SASL state")
				goto afterControl
			}
		}

		switch k := kmsg.Key(kreq.Key()); k {
		case kmsg.Produce:
			kresp, err = c.handleProduce(creq.cc.b, kreq)
		case kmsg.Fetch:
			kresp, err = c.handleFetch(creq, w)
		case kmsg.ListOffsets:
			kresp, err = c.handleListOffsets(creq.cc.b, kreq)
		case kmsg.Metadata:
			kresp, err = c.handleMetadata(kreq)
		case kmsg.OffsetCommit:
			kresp, err = c.handleOffsetCommit(creq)
		case kmsg.OffsetFetch:
			kresp, err = c.handleOffsetFetch(creq)
		case kmsg.FindCoordinator:
			kresp, err = c.handleFindCoordinator(kreq)
		case kmsg.JoinGroup:
			kresp, err = c.handleJoinGroup(creq)
		case kmsg.Heartbeat:
			kresp, err = c.handleHeartbeat(creq)
		case kmsg.LeaveGroup:
			kresp, err = c.handleLeaveGroup(creq)
		case kmsg.SyncGroup:
			kresp, err = c.handleSyncGroup(creq)
		case kmsg.DescribeGroups:
			kresp, err = c.handleDescribeGroups(creq)
		case kmsg.ListGroups:
			kresp, err = c.handleListGroups(creq)
		case kmsg.SASLHandshake:
			kresp, err = c.handleSASLHandshake(creq)
		case kmsg.ApiVersions:
			kresp, err = c.handleApiVersions(kreq)
		case kmsg.CreateTopics:
			kresp, err = c.handleCreateTopics(creq.cc.b, kreq)
		case kmsg.DeleteTopics:
			kresp, err = c.handleDeleteTopics(creq.cc.b, kreq)
		case kmsg.DeleteRecords:
			kresp, err = c.handleDeleteRecords(creq.cc.b, kreq)
		case kmsg.InitProducerID:
			kresp, err = c.handleInitProducerID(kreq)
		case kmsg.OffsetForLeaderEpoch:
			kresp, err = c.handleOffsetForLeaderEpoch(creq.cc.b, kreq)
		case kmsg.DescribeConfigs:
			kresp, err = c.handleDescribeConfigs(creq.cc.b, kreq)
		case kmsg.AlterConfigs:
			kresp, err = c.handleAlterConfigs(creq.cc.b, kreq)
		case kmsg.AlterReplicaLogDirs:
			kresp, err = c.handleAlterReplicaLogDirs(creq.cc.b, kreq)
		case kmsg.DescribeLogDirs:
			kresp, err = c.handleDescribeLogDirs(creq.cc.b, kreq)
		case kmsg.SASLAuthenticate:
			kresp, err = c.handleSASLAuthenticate(creq)
		case kmsg.CreatePartitions:
			kresp, err = c.handleCreatePartitions(creq.cc.b, kreq)
		case kmsg.DeleteGroups:
			kresp, err = c.handleDeleteGroups(creq)
		case kmsg.IncrementalAlterConfigs:
			kresp, err = c.handleIncrementalAlterConfigs(creq.cc.b, kreq)
		case kmsg.OffsetDelete:
			kresp, err = c.handleOffsetDelete(creq)
		case kmsg.DescribeUserSCRAMCredentials:
			kresp, err = c.handleDescribeUserSCRAMCredentials(kreq)
		case kmsg.AlterUserSCRAMCredentials:
			kresp, err = c.handleAlterUserSCRAMCredentials(creq.cc.b, kreq)
		default:
			err = fmt.Errorf("unahndled key %v", k)
		}

	afterControl:
		if kresp == nil && err == nil { // produce request with no acks, or hijacked group request
			continue
		}

		select {
		case creq.cc.respCh <- clientResp{kresp: kresp, corr: creq.corr, err: err, seq: creq.seq}:
		case <-c.die:
			return
		}
	}
}

// Control is a function to call on any client request the cluster handles.
//
// If the control function returns true, then either the response is written
// back to the client or, if there the control function returns an error, the
// client connection is closed. If both returns are nil, then the cluster will
// loop continuing to read from the client and the client will likely have a
// read timeout at some point.
//
// Controlling a request drops the control function from the cluster, meaning
// that a control function can only control *one* request. To keep the control
// function handling more requests, you can call KeepControl within your
// control function.
//
// It is safe to add new control functions within a control function. Control
// functions are not called concurrently.
func (c *Cluster) Control(fn func(kmsg.Request) (kmsg.Response, error, bool)) {
	c.controlMu.Lock()
	defer c.controlMu.Unlock()
	c.control[-1] = append(c.control[-1], fn)
}

// Control is a function to call on a specific request key that the cluster
// handles.
//
// If the control function returns true, then either the response is written
// back to the client or, if there the control function returns an error, the
// client connection is closed. If both returns are nil, then the cluster will
// loop continuing to read from the client and the client will likely have a
// read timeout at some point.
//
// Controlling a request drops the control function from the cluster, meaning
// that a control function can only control *one* request. To keep the control
// function handling more requests, you can call KeepControl within your
// control function.
//
// It is safe to add new control functions within a control function.
func (c *Cluster) ControlKey(key int16, fn func(kmsg.Request) (kmsg.Response, error, bool)) {
	c.controlMu.Lock()
	defer c.controlMu.Unlock()
	c.control[key] = append(c.control[key], fn)
}

// KeepControl marks the currently running control function to be kept even if
// you handle the request and return true. This can be used to continuously
// control requests without needing to re-add control functions manually.
func (c *Cluster) KeepControl() {
	c.keepCurrentControl.Swap(true)
}

// CurrentNode is solely valid from within a control function; it returns
// the broker id that the request was received by.
// If there's no request currently inflight, this returns -1.
func (c *Cluster) CurrentNode() int32 {
	if b := c.currentBroker.Load(); b != nil {
		return b.node
	}
	return -1
}

func (c *Cluster) tryControl(kreq kmsg.Request, b *broker) (kresp kmsg.Response, err error, handled bool) {
	c.currentBroker.Store(b)
	defer c.currentBroker.Store(nil)
	c.controlMu.Lock()
	defer c.controlMu.Unlock()
	if len(c.control) == 0 {
		return nil, nil, false
	}
	kresp, err, handled = c.tryControlKey(kreq.Key(), kreq, b)
	if !handled {
		kresp, err, handled = c.tryControlKey(-1, kreq, b)
	}
	return kresp, err, handled
}

func (c *Cluster) tryControlKey(key int16, kreq kmsg.Request, b *broker) (kresp kmsg.Response, err error, handled bool) {
	for i, fn := range c.control[key] {
		kresp, err, handled = c.callControl(key, kreq, fn)
		if handled {
			// fn may have called Control, ControlKey, or KeepControl,
			// all of which will append to c.control; refresh the slice.
			fns := c.control[key]
			c.control[key] = append(fns[:i], fns[i+1:]...)
			return
		}
	}
	return
}

func (c *Cluster) callControl(key int16, req kmsg.Request, fn controlFn) (kresp kmsg.Response, err error, handled bool) {
	c.keepCurrentControl.Swap(false)
	c.controlMu.Unlock()
	defer func() {
		c.controlMu.Lock()
		if handled && c.keepCurrentControl.Swap(false) {
			c.control[key] = append(c.control[key], fn)
		}
	}()
	return fn(req)
}

// Various administrative requests can be passed into the cluster to simulate
// real-world operations. These are performed synchronously in the goroutine
// that handles client requests.

func (c *Cluster) admin(fn func()) {
	ofn := fn
	wait := make(chan struct{})
	fn = func() { ofn(); close(wait) }
	c.adminCh <- fn
	<-wait
}

// MoveTopicPartition simulates the rebalancing of a partition to an alternative
// broker. This returns an error if the topic, partition, or node does not exit.
func (c *Cluster) MoveTopicPartition(topic string, partition int32, nodeID int32) error {
	var err error
	c.admin(func() {
		var br *broker
		for _, b := range c.bs {
			if b.node == nodeID {
				br = b
				break
			}
		}
		if br == nil {
			err = fmt.Errorf("node %d not found", nodeID)
			return
		}
		pd, ok := c.data.tps.getp(topic, partition)
		if !ok {
			err = errors.New("topic/partition not found")
			return
		}
		pd.leader = br
	})
	return err
}

// AddNode adds a node to the cluster. If nodeID is -1, the next node ID is
// used. If port is 0 or negative, a random port is chosen. This returns the
// added node ID and the port used, or an error if the node already exists or
// the port cannot be listened to.
func (c *Cluster) AddNode(nodeID int32, port int) (int32, int, error) {
	var err error
	c.admin(func() {
		if nodeID >= 0 {
			for _, b := range c.bs {
				if b.node == nodeID {
					err = fmt.Errorf("node %d already exists", nodeID)
					return
				}
			}
		} else if len(c.bs) > 0 {
			// We go one higher than the max current node ID. We
			// need to search all nodes because a person may have
			// added and removed a bunch, with manual ID overrides.
			nodeID = c.bs[0].node
			for _, b := range c.bs[1:] {
				if b.node > nodeID {
					nodeID = b.node
				}
			}
			nodeID++
		} else {
			nodeID = 0
		}
		if port < 0 {
			port = 0
		}
		var ln net.Listener
		if ln, err = newListener(port, c.cfg.tls); err != nil {
			return
		}
		_, strPort, _ := net.SplitHostPort(ln.Addr().String())
		port, _ = strconv.Atoi(strPort)
		b := &broker{
			c:     c,
			ln:    ln,
			node:  nodeID,
			bsIdx: len(c.bs),
		}
		c.bs = append(c.bs, b)
		c.cfg.nbrokers++
		c.shufflePartitionsLocked()
		go b.listen()
	})
	return nodeID, port, err
}

// RemoveNode removes a ndoe from the cluster. This returns an error if the
// node does not exist.
func (c *Cluster) RemoveNode(nodeID int32) error {
	var err error
	c.admin(func() {
		for i, b := range c.bs {
			if b.node == nodeID {
				if len(c.bs) == 1 {
					err = errors.New("cannot remove all brokers")
					return
				}
				b.ln.Close()
				c.cfg.nbrokers--
				c.bs[i] = c.bs[len(c.bs)-1]
				c.bs[i].bsIdx = i
				c.bs = c.bs[:len(c.bs)-1]
				c.shufflePartitionsLocked()
				return
			}
		}
		err = fmt.Errorf("node %d not found", nodeID)
	})
	return err
}

// ShufflePartitionLeaders simulates a leader election for all partitions: all
// partitions have a randomly selected new leader and their internal epochs are
// bumped.
func (c *Cluster) ShufflePartitionLeaders() {
	c.admin(func() {
		c.shufflePartitionsLocked()
	})
}

func (c *Cluster) shufflePartitionsLocked() {
	c.data.tps.each(func(_ string, _ int32, p *partData) {
		var leader *broker
		if len(c.bs) == 0 {
			leader = c.noLeader()
		} else {
			leader = c.bs[rand.Intn(len(c.bs))]
		}
		p.leader = leader
		p.epoch++
	})
}
