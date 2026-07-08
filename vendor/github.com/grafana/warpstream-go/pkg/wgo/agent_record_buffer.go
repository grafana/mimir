package wgo

import (
	"context"
	"errors"
	"sync"
	"time"
)

// errBufferClosed is returned to Add callers attempting to enqueue after Close.
var errBufferClosed = errors.New("record buffer is closed")

// AgentFlushFunc produces one agent's batch of wire items — all targeting
// nodeID, at most one entry per (topic, partition) — and returns the outcome. A
// successful outcome must carry a non-nil resp; an error-only or zero
// ProduceResult counts as a failure.
type AgentFlushFunc[W routedBatch[W]] func(ctx context.Context, nodeID int32, wire []W) ProduceResult

// AgentBuffer accumulates items targeted at one Warpstream agent and flushes
// them as a single ProduceRequest on linger expiry, batch-size overflow, or
// Close. It exists because Warpstream's stateless model lets a single Produce
// request to one agent carry batches for many partitions — the natural unit of
// batching here is "items bound for this agent", independent of how many topics
// or partitions they span.
//
// The buffer is intentionally narrow: it knows nothing about routing,
// resolvers, hedging, or other agents. ClusterBuffer owns those concerns. This
// split keeps the per-agent contention surface small (one mutex per agent) and
// keeps the multi-agent fan-in logic out of the hot per-item path. Concurrent
// Adds for *different* agents never touch the same mutex.
//
// Two design choices stand out:
//
//   - Linger bounds batching on the steady path: the typical deployment has
//     many concurrent client processes producing to the same Warpstream
//     cluster, and lingering lets us amortise the Produce request cost. A
//     batch still flushes early when adding the next item would exceed the
//     byte cap, so linger is an upper bound on batching delay, not a floor.
//   - Each flush runs in its own goroutine so the buffer can immediately
//     start accumulating the next batch. This means multiple Produce
//     requests to the same agent can be in flight concurrently. There is
//     no per-agent in-flight cap by design: Warpstream agents are
//     stateless, ordering between requests doesn't matter for the produce
//     contract this client exposes, and the cap that would matter (memory
//     pressure) is enforced by the caller upstream.
type AgentBuffer[W routedBatch[W]] struct {
	nodeID        int32
	linger        time.Duration
	batchMaxBytes int32
	flush         AgentFlushFunc[W]
	metrics       *metrics

	mu                    sync.Mutex
	nextProduceItems      []promised[W]
	nextProduceRecords    int
	nextProduceWireBytes  int64
	nextProduceFlushTimer *time.Timer
	closed                bool

	flushWG        sync.WaitGroup
	flushCtx       context.Context
	cancelFlushCtx context.CancelFunc
}

// NewAgentBuffer returns a buffer for items destined to nodeID. flush runs in a
// background goroutine when a batch is ready.
func NewAgentBuffer[W routedBatch[W]](nodeID int32, linger time.Duration, batchMaxBytes int32, flush AgentFlushFunc[W], m *metrics) *AgentBuffer[W] {
	ctx, cancel := context.WithCancel(context.Background())
	return &AgentBuffer[W]{
		nodeID:         nodeID,
		linger:         linger,
		batchMaxBytes:  batchMaxBytes,
		flush:          flush,
		metrics:        m,
		flushCtx:       ctx,
		cancelFlushCtx: cancel,
	}
}

// Add buffers items for produce. Each item's done fires exactly once with its
// terminal outcome.
func (a *AgentBuffer[W]) Add(items []promised[W]) {
	incoming := 0
	for i := range items {
		incoming += items[i].item.recordCount()
	}
	if incoming == 0 {
		return
	}

	// Split any item whose own RecordBatch would exceed batchMaxBytes into
	// chunks that each fit, so no single flushed per-partition batch can be
	// rejected MessageTooLarge. Chunks are added one at a time below, and
	// because each is <= batchMaxBytes the overflow check keeps
	// nextProduceWireBytes (and therefore every per-partition batch) within the cap.
	chunks := make([]promised[W], 0, len(items))
	for _, p := range items {
		chunks = append(chunks, splitPromisedRoutedBatchByBatchMaxBytes(p, a.batchMaxBytes)...)
	}

	a.mu.Lock()
	if a.closed {
		// Closed buffer: there is no flush to carry the outcome, so resolve
		// every item's done synchronously with errBufferClosed.
		a.mu.Unlock()
		for _, p := range chunks {
			p.done(ProduceResult{err: errBufferClosed})
		}
		return
	}

	for i := range chunks {
		a.addToNextProduceLocked(chunks[i])
	}
	if a.nextProduceRecords > 0 && a.nextProduceFlushTimer == nil {
		a.nextProduceFlushTimer = time.AfterFunc(a.linger, a.timerFlush)
	}
	a.mu.Unlock()
}

// addToNextProduceLocked appends one item (already <= batchMaxBytes) to the
// pending produce, flushing first when it wouldn't fit. Items are never merged
// here: same-partition entries are coalesced into one wire batch at flush time
// (startFlushLocked), which keeps each item's done untouched. Caller must hold
// a.mu.
func (a *AgentBuffer[W]) addToNextProduceLocked(p promised[W]) {
	addBytes := p.item.wireBytes()
	if a.nextProduceRecords > 0 && a.nextProduceWireBytes+addBytes > int64(a.batchMaxBytes) {
		a.startFlushLocked()
	}
	a.nextProduceItems = append(a.nextProduceItems, p)
	a.nextProduceRecords += p.item.recordCount()
	a.nextProduceWireBytes += addBytes
}

// Close flushes the pending produce and waits for every in-flight flush to
// report. Subsequent Adds fail with errBufferClosed. Idempotent.
func (a *AgentBuffer[W]) Close() {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.closed = true
	a.startFlushLocked()
	a.mu.Unlock()

	a.flushWG.Wait()
	a.cancelFlushCtx()
}

// timerFlush is invoked by the linger timer.
func (a *AgentBuffer[W]) timerFlush() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.startFlushLocked()
}

// startFlushLocked dispatches the pending produce to flush in a goroutine and
// resets buffer state so the next produce can accumulate immediately. No-op
// if nothing is pending. Caller must hold a.mu.
func (a *AgentBuffer[W]) startFlushLocked() {
	if a.nextProduceRecords == 0 {
		return
	}
	if a.nextProduceFlushTimer != nil {
		// Stop's return value is intentionally ignored: a concurrently-firing
		// timer's callback (timerFlush) blocks on a.mu and, once it acquires
		// it, finds the buffer empty and is a no-op via startFlushLocked.
		a.nextProduceFlushTimer.Stop()
		a.nextProduceFlushTimer = nil
	}
	entries := a.nextProduceItems
	a.nextProduceItems = nil
	a.nextProduceRecords = 0
	a.nextProduceWireBytes = 0

	// Merge same-partition items into one wire entry: the produce/hedge path
	// keys per-partition state by (topic, partition) and rejects duplicates
	// (see produceResultAccumulator). Completion still fans out to every
	// original entry's done, so the merged wire view drops done.
	wire := mergePromisedRoutedBatchByTopicPartition(entries)

	a.flushWG.Add(1)
	go func() {
		defer a.flushWG.Done()
		a.metrics.lingerFlushesTotal.Inc()
		res := a.flush(a.flushCtx, a.nodeID, wire)
		for _, e := range entries {
			e.done(res)
		}
	}()
}
