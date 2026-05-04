// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// AgentResolver maps a (topic, partition) pair to a destination agent NodeID.
// Returns ok=false when no agent is currently assigned to the partition.
type AgentResolver func(topic string, partition int32) (nodeID int32, ok bool)

// ClusterRecordBuffer routes records to per-agent buffers (AgentRecordBuffer)
// according to AgentResolver. Each agent owns its own batching state and
// linger timer; the cluster only manages routing and lifecycle.
//
// The cluster uses a sync.RWMutex for the agents map: the hot path
// (Add to an existing agent) takes only RLock, so concurrent Adds for
// different agents do not contend with each other. Creating the buffer for
// an agent that hasn't been seen before takes a brief write lock; once
// created, the buffer is reused.
type ClusterRecordBuffer struct {
	linger        time.Duration
	maxBatchBytes int32
	resolve       AgentResolver
	flush         FlushFunc
	metrics       *metrics

	mu           sync.RWMutex
	agentBuffers map[int32]*AgentRecordBuffer
	closed       bool
}

// NewClusterRecordBuffer returns a ClusterRecordBuffer ready to accept
// records. flush is forwarded to every per-agent buffer the cluster lazily
// creates. resolve is consulted on every Add to bucket each record under a
// destination agent.
func NewClusterRecordBuffer(linger time.Duration, maxBatchBytes int32, resolve AgentResolver, flush FlushFunc, m *metrics) *ClusterRecordBuffer {
	return &ClusterRecordBuffer{
		linger:        linger,
		maxBatchBytes: maxBatchBytes,
		resolve:       resolve,
		flush:         flush,
		metrics:       m,
		agentBuffers:  make(map[int32]*AgentRecordBuffer),
	}
}

// Add buffers records into per-agent batches and arranges for done to fire
// exactly once after every agent contributing to this Add has acknowledged
// its share of the batch. done is called with the first error observed
// across the participating agents (or nil on full success). If any record's
// (topic, partition) does not resolve to an agent, Add fails the whole call
// synchronously via done and buffers nothing. Add returns immediately; the
// caller is expected to block on done.
func (c *ClusterRecordBuffer) Add(records []*kgo.Record, done func(error)) {
	if len(records) == 0 {
		done(nil)
		return
	}

	type agentDispatch struct {
		buffer  *AgentRecordBuffer
		records []*kgo.Record
	}

	// Resolve every record to its destination buffer up-front. Bailing
	// synchronously on the first failure means by the time we start
	// dispatching every destination is known to be valid — no partial
	// dispatch is possible.
	dispatchByAgent := make(map[int32]*agentDispatch)
	for _, r := range records {
		nodeID, ok := c.resolve(r.Topic, r.Partition)
		if !ok {
			done(fmt.Errorf("no agent assigned for topic %q partition %d", r.Topic, r.Partition))
			return
		}

		dispatch, found := dispatchByAgent[nodeID]
		if !found {
			buffer, err := c.agentRecordBufferFor(nodeID)
			if err != nil {
				done(err)
				return
			}
			dispatch = &agentDispatch{buffer: buffer}
			dispatchByAgent[nodeID] = dispatch
		}

		dispatch.records = append(dispatch.records, r)
	}

	// Add the records to the respective per-agent buffer and keep track of completion.
	completion := newAddCompletion(int32(len(dispatchByAgent)), done)
	for _, d := range dispatchByAgent {
		d.buffer.Add(d.records, completion.report)
	}
}

// Close flushes every per-agent buffer in parallel and waits for all of them
// to drain. Subsequent Add calls fail with errBufferClosed. Idempotent.
func (c *ClusterRecordBuffer) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true

	// Take ownership of the map and drop it from the cluster. Subsequent
	// Add / agentRecordBufferFor calls observe c.closed and never touch it.
	agentBuffers := c.agentBuffers
	c.agentBuffers = nil
	c.mu.Unlock()

	var wg sync.WaitGroup
	for _, a := range agentBuffers {
		wg.Add(1)
		go func(a *AgentRecordBuffer) {
			defer wg.Done()
			a.Close()
		}(a)
	}
	wg.Wait()
}

// agentRecordBufferFor returns the per-agent buffer for nodeID, creating it
// on first call. Returns an error when the cluster is closed (and would in
// the future cover any other reason a buffer cannot be supplied).
func (c *ClusterRecordBuffer) agentRecordBufferFor(nodeID int32) (*AgentRecordBuffer, error) {
	// Hot path: read lock for an already-known agent.
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, errBufferClosed
	}
	a, ok := c.agentBuffers[nodeID]
	c.mu.RUnlock()
	if ok {
		return a, nil
	}

	// Cold path: create on first record for this agent.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errBufferClosed
	}
	if a, ok := c.agentBuffers[nodeID]; ok {
		return a, nil
	}
	a = NewAgentRecordBuffer(nodeID, c.linger, c.maxBatchBytes, c.flush, c.metrics)
	c.agentBuffers[nodeID] = a
	return a, nil
}

// addCompletion fans-in N agent acknowledgements into a single done callback.
// Each agent's flush calls report once; done fires after the Nth report,
// with the first error observed (or nil on full success).
type addCompletion struct {
	pending  atomic.Int32
	once     sync.Once
	done     func(error)
	mu       sync.Mutex
	firstErr error
}

func newAddCompletion(pending int32, done func(error)) *addCompletion {
	c := &addCompletion{done: done}
	c.pending.Store(pending)
	return c
}

func (c *addCompletion) report(err error) {
	if err != nil {
		c.mu.Lock()
		if c.firstErr == nil {
			c.firstErr = err
		}
		c.mu.Unlock()
	}
	if c.pending.Add(-1) == 0 {
		c.once.Do(func() { c.done(c.firstErr) })
	}
}
