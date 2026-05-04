// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"fmt"
	"sync"
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
// exactly once with one of:
//   - nil, when every agent contributing to this Add has acknowledged its
//     share of the batch;
//   - the first error observed across the participating agents;
//   - ctx.Err() if ctx is canceled before the above happens.
//
// ctx governs only the caller's wait on done — it does NOT cancel the
// underlying produce. Records are committed to the batch as soon as they are
// buffered; cancelling ctx detaches this caller from the result but the
// batch still flushes normally. As a consequence, when done fires with
// ctx.Err() the records may still land successfully on the broker.
//
// If any record's (topic, partition) does not resolve to an agent, or the
// cluster is closed, Add fails the whole call synchronously via done and
// buffers nothing. Add returns immediately; the caller is expected to block
// on done.
func (c *ClusterRecordBuffer) Add(ctx context.Context, records []*kgo.Record, done func(error)) {
	if len(records) == 0 {
		done(nil)
		return
	}
	if err := ctx.Err(); err != nil {
		// Pre-canceled fast path: fail without buffering or dispatching.
		// Distinct from a mid-flight cancel (which still buffers and lets
		// the batch flush in the background); this matters because the
		// caller may otherwise observe a duplicate from a "no-op" call.
		done(err)
		return
	}

	type agentDispatch struct {
		buffer  *AgentRecordBuffer
		records []*kgo.Record
	}

	// Resolve every record to its destination buffer up-front. Bailing
	// synchronously on the first failure means by the time we start
	// dispatching every destination is known to be valid — no partial
	// dispatch is possible. Synchronous failures bypass the completion
	// fan-in and call done directly because no agent has been told to
	// dispatch yet.
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

	// Add the records to the respective per-agent buffer and keep track of
	// completion. The completion type owns the once-notifyDone + ctx-watch
	// wiring so that done fires exactly once on whichever happens first:
	// every agent's flush completing, or ctx being canceled.
	comp := newCompletion(ctx, int32(len(dispatchByAgent)), done)
	for _, d := range dispatchByAgent {
		d.buffer.Add(d.records, comp.reportResult)
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
