package wgo

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ClusterBuffer is the cluster-wide entry point for buffering produce items. It
// owns the routing and lifecycle of per-agent batches; each AgentBuffer owns its
// own state, linger timer, and flush loop.
//
// The buffer is strategy-free: callers hand items pre-stamped with a destination
// nodeID and the buffer just bins them by agent. The routing decision is owned
// upstream, where the PartitionAssignmentStrategy is consulted; the buffer's job
// is purely batching.
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
// lock per new nodeID; subsequent Adds reuse it. bufferedBytes /
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
type ClusterBuffer[W routedBatch[W]] struct {
	linger        time.Duration
	batchMaxBytes int32
	flush         AgentFlushFunc[W]
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
	agentBuffers map[int32]*AgentBuffer[W]
	closed       bool
}

// NewClusterBuffer returns a buffer that lazily spawns one AgentBuffer per
// nodeID seen via Add, all sharing flush. It registers the buffered-producer
// gauges on reg.
func NewClusterBuffer[W routedBatch[W]](linger time.Duration, batchMaxBytes int32, flush AgentFlushFunc[W], m *metrics, reg prometheus.Registerer) *ClusterBuffer[W] {
	c := &ClusterBuffer[W]{
		linger:        linger,
		batchMaxBytes: batchMaxBytes,
		flush:         flush,
		metrics:       m,
		agentBuffers:  make(map[int32]*AgentBuffer[W]),
	}

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "buffered_produce_records_total",
		Help: "Number of records currently buffered awaiting acknowledgement.",
	}, func() float64 { return float64(c.BufferedRecords()) })
	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "buffered_produce_bytes",
		Help: "Bytes of records currently buffered awaiting acknowledgement.",
	}, func() float64 { return float64(c.BufferedBytes()) })

	return c
}

// Add buffers items for produce. Each item's done fires exactly once with its
// final outcome (nil on success, an error on terminal failure, or ctx.Err() if
// ctx is canceled before the producer resolves the partition). Cancelling ctx
// detaches the caller but does not stop the in-flight produce.
func (c *ClusterBuffer[W]) Add(ctx context.Context, items []promised[W]) {
	if len(items) == 0 {
		return
	}

	// Wrap each item's done with two layers:
	//   - a once-fire that delivers either the produce outcome or
	//     ctx.Err() (whichever first) to the original done;
	//   - an accounting hook that decrements the buffered counters on
	//     actual flush completion regardless of whether ctx already fired.
	wrapped := make([]promised[W], len(items))
	for i, p := range items {
		payloadBytes := p.item.payloadBytes()
		recCount := int64(p.item.recordCount())
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

// addToBuffers bins items by destination and dispatches them to the matching
// per-agent buffer.
func (c *ClusterBuffer[W]) addToBuffers(ctx context.Context, items []promised[W]) {
	if len(items) == 0 {
		return
	}
	if err := ctx.Err(); err != nil {
		// Pre-canceled fast path: fail every item without dispatching.
		// Distinct from a mid-flight cancel (which still buffers and lets
		// the batch flush in the background); this matters because the
		// caller may otherwise observe a duplicate from a "no-op" call.
		for _, p := range items {
			p.done(ProduceResult{err: err})
		}
		return
	}

	byAgent := make(map[int32][]promised[W])
	for _, p := range items {
		nodeID := p.item.getNodeID()
		byAgent[nodeID] = append(byAgent[nodeID], p)
	}
	for nodeID, agentItems := range byAgent {
		buffer, err := c.agentBufferFor(nodeID)
		if err != nil {
			for _, p := range agentItems {
				p.done(ProduceResult{err: err})
			}
			continue
		}
		buffer.Add(agentItems)
	}
}

// BufferedBytes returns the bytes of all records awaiting ack.
func (c *ClusterBuffer[W]) BufferedBytes() int64 {
	return c.bufferedBytes.Load()
}

// BufferedRecords returns the count of records awaiting ack.
func (c *ClusterBuffer[W]) BufferedRecords() int64 {
	return c.bufferedRecords.Load()
}

// Close flushes every per-agent buffer in parallel and waits for them to
// drain. Subsequent Adds fail with errBufferClosed. Idempotent.
func (c *ClusterBuffer[W]) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true

	// Take ownership of the map and drop it from the cluster. Subsequent
	// Add / agentBufferFor calls observe c.closed and never touch it.
	agentBuffers := c.agentBuffers
	c.agentBuffers = nil
	c.mu.Unlock()

	var wg sync.WaitGroup
	for _, a := range agentBuffers {
		wg.Add(1)
		go func(a *AgentBuffer[W]) {
			defer wg.Done()
			a.Close()
		}(a)
	}
	wg.Wait()
}

// agentBufferFor returns the per-agent buffer for nodeID, creating it on first
// call. Errors with errBufferClosed if the cluster is closed.
func (c *ClusterBuffer[W]) agentBufferFor(nodeID int32) (*AgentBuffer[W], error) {
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

	// Cold path: create on first item for this agent.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errBufferClosed
	}
	if a, ok := c.agentBuffers[nodeID]; ok {
		return a, nil
	}
	a = NewAgentBuffer[W](nodeID, c.linger, c.batchMaxBytes, c.flush, c.metrics)
	c.agentBuffers[nodeID] = a
	return a, nil
}
