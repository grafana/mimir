package wgo

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// ClusterRecordBuffer is the cluster-wide entry point for buffering produce
// records. It owns the routing and lifecycle of per-agent batches; each
// AgentRecordBuffer owns its own state, linger timer, and flush loop.
//
// The buffer is strategy-free: callers hand partition-grouped work
// pre-stamped with a destination NodeID (via routedTopicPartitionRecords) and the
// buffer just bins them by agent. The
// routing decision is owned upstream, where the PartitionAssignmentStrategy
// is consulted; the buffer's job is purely batching.
//
// Batching per agent has two consequences that matter:
//
//   - One ProduceRequest can carry records for many partitions, which is
//     the whole point of choosing a per-agent grouping for Warpstream:
//     fewer wire round-trips and less per-batch hedging overhead.
//   - The per-partition leader bookkeeping that vanilla Kafka clients need
//     (one in-flight cap per leader, one batch per leader, per-leader
//     ordering) becomes irrelevant. Warpstream agents are stateless;
//     "leader" is just a routing decision we make ourselves.
//
// Concurrency is biased to the hot path: the agents map is guarded by a
// sync.RWMutex so concurrent Adds to different agents take RLock and never
// contend with each other. Creating a per-agent buffer is a one-time write
// lock per new NodeID; subsequent Adds reuse it. bufferedBytes /
// bufferedRecords are atomic counters that mirror franz-go's
// BufferedProduceBytes / BufferedProduceRecords semantics — they reflect
// "records the producer is responsible for", not "records the caller is
// waiting on", which is why they are decremented on actual flush completion
// (not on caller ctx-cancel).
//
// These counters are for observability only: the buffer enforces no upper
// bound on buffered bytes or records. Bounding in-flight produce volume is
// intentionally the caller's responsibility — a caller that needs
// backpressure must cap its own outstanding produces rather than rely on
// the buffer to reject work.
type ClusterRecordBuffer struct {
	linger        time.Duration
	maxBatchBytes int32
	flush         AgentFlushFunc
	metrics       *metrics

	// bufferedBytes is the total size in bytes of every record waiting
	// between Add and flush completion. A caller ctx-cancel detaches the
	// caller but does not decrement this; only the flush completing does.
	bufferedBytes atomic.Int64

	// bufferedRecords is the count of records waiting between Add and flush
	// completion. A caller ctx-cancel does not decrement this; only the
	// flush completing does.
	bufferedRecords atomic.Int64

	mu           sync.RWMutex
	agentBuffers map[int32]*AgentRecordBuffer
	closed       bool
}

// NewClusterRecordBuffer returns a buffer that lazily spawns one
// AgentRecordBuffer per NodeID seen via Add, all sharing flush.
func NewClusterRecordBuffer(linger time.Duration, maxBatchBytes int32, flush AgentFlushFunc, m *metrics) *ClusterRecordBuffer {
	return &ClusterRecordBuffer{
		linger:        linger,
		maxBatchBytes: maxBatchBytes,
		flush:         flush,
		metrics:       m,
		agentBuffers:  make(map[int32]*AgentRecordBuffer),
	}
}

// Add buffers partition-grouped work for produce. Each entry's done fires
// exactly once with the partition's final outcome (nil on success, an
// error on terminal failure, or ctx.Err() if ctx is canceled before the
// producer resolves the partition). Cancelling ctx detaches the caller
// but does not stop the in-flight produce.
func (c *ClusterRecordBuffer) Add(ctx context.Context, partitions []promisedRoutedTopicPartitionRecords) {
	if len(partitions) == 0 {
		return
	}

	// Wrap each partition's done with two layers:
	//   - a once-fire that delivers either the produce outcome or
	//     ctx.Err() (whichever first) to the original done;
	//   - an accounting hook that decrements the buffered counters on
	//     actual flush completion regardless of whether ctx already fired.
	wrapped := make([]promisedRoutedTopicPartitionRecords, len(partitions))
	for i, p := range partitions {
		payloadBytes := p.recordPayloadBytes()
		recCount := int64(len(p.records))
		c.bufferedBytes.Add(payloadBytes)
		c.bufferedRecords.Add(recCount)

		var (
			origDone = p.done

			// origDoneFired gates the original done ("ctx-cancel vs flush" race).
			origDoneFired atomic.Bool

			// ourDoneFired gates the accounting decrement (flush only).
			ourDoneFired atomic.Bool
		)

		fireOrigDone := func(res ProduceResult) {
			if origDoneFired.CompareAndSwap(false, true) {
				origDone(res)
			}
		}

		// AfterFunc's callback always runs in a separate goroutine and
		// never races with the stopCtxWatch assignment below — done()
		// can only fire via addToBuffers further down (synchronous on
		// pre-canceled ctx) or via the flush (later still), so
		// stopCtxWatch is already bound by the time it's read.
		stopCtxWatch := context.AfterFunc(ctx, func() {
			fireOrigDone(ProduceResult{err: ctx.Err()})
		})

		p.done = func(res ProduceResult) {
			if ourDoneFired.CompareAndSwap(false, true) {
				stopCtxWatch()
				c.bufferedBytes.Add(-payloadBytes)
				c.bufferedRecords.Add(-recCount)
			}
			fireOrigDone(res)
		}
		wrapped[i] = p
	}

	c.addToBuffers(ctx, wrapped)
}

// addToBuffers bins partition groups by destination and dispatches them to
// the matching per-agent buffer.
func (c *ClusterRecordBuffer) addToBuffers(ctx context.Context, partitions []promisedRoutedTopicPartitionRecords) {
	if len(partitions) == 0 {
		return
	}
	if err := ctx.Err(); err != nil {
		// Pre-canceled fast path: fail every partition without dispatching.
		// Distinct from a mid-flight cancel (which still buffers and lets
		// the batch flush in the background); this matters because the
		// caller may otherwise observe a duplicate from a "no-op" call.
		for _, p := range partitions {
			p.done(ProduceResult{err: err})
		}
		return
	}

	// Stamp record timestamps here, to keep it consistent with franz-go.
	now := time.Now()
	for _, p := range partitions {
		for _, r := range p.records {
			ensureRecordTimestamp(r, now)
		}
	}

	byAgent := make(map[int32][]promisedRoutedTopicPartitionRecords)
	for _, p := range partitions {
		byAgent[p.nodeID] = append(byAgent[p.nodeID], p)
	}
	for nodeID, agentPartitions := range byAgent {
		buffer, err := c.agentRecordBufferFor(nodeID)
		if err != nil {
			for _, p := range agentPartitions {
				p.done(ProduceResult{err: err})
			}
			continue
		}
		buffer.Add(agentPartitions)
	}
}

// BufferedBytes returns the bytes of all records awaiting ack.
func (c *ClusterRecordBuffer) BufferedBytes() int64 {
	return c.bufferedBytes.Load()
}

// BufferedRecords returns the count of records awaiting ack.
func (c *ClusterRecordBuffer) BufferedRecords() int64 {
	return c.bufferedRecords.Load()
}

// Close flushes every per-agent buffer in parallel and waits for them to
// drain. Subsequent Adds fail with errBufferClosed. Idempotent.
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
// on first call. Errors with errBufferClosed if the cluster is closed.
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
