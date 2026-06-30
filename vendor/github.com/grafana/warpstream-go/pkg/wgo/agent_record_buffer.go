package wgo

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// errBufferClosed is returned to Add callers attempting to enqueue after Close.
var errBufferClosed = errors.New("record buffer is closed")

// AgentFlushFunc is invoked by AgentRecordBuffer when a batch is ready to
// send. The function only produces the batch and returns the resulting
// ProduceResult. A successful outcome must carry a non-nil resp; an error-only
// (or zero) ProduceResult is treated as a failure by completion handling.
//
// The partitions it receives satisfy two guarantees:
//
//   - All belong to nodeID — the agent that owns the AgentRecordBuffer doing
//     the flush — because the buffer bins incoming routed entries by nodeID.
//   - At most one entry per (topic, partition). The buffer coalesces
//     same-partition records into a single entry before the call, because the
//     produce/hedge path keys per-partition state by (topic, partition) and
//     rejects duplicates (see produceResultAccumulator).
type AgentFlushFunc func(ctx context.Context, nodeID int32, partitions []routedTopicPartitionRecords) ProduceResult

// AgentRecordBuffer accumulates records targeted at one Warpstream agent and
// flushes them as a single ProduceRequest on linger expiry, batch-size
// overflow, or Close. It exists because Warpstream's stateless model lets a
// single Produce request to one agent carry batches for many partitions —
// the natural unit of batching here is "records bound for this agent",
// independent of how many topics or partitions they span.
//
// The buffer is intentionally narrow: it knows nothing about routing,
// resolvers, hedging, or other agents. ClusterRecordBuffer owns those
// concerns. This split keeps the per-agent contention surface small (one
// mutex per agent) and keeps the multi-agent fan-in logic out of the hot
// per-record path. Concurrent Adds for *different* agents never touch the
// same mutex.
//
// Two design choices stand out:
//
//   - Linger bounds batching on the steady path: the typical deployment has
//     many concurrent client processes producing to the same Warpstream
//     cluster, and lingering lets us amortise the Produce request cost. A
//     batch still flushes early when adding the next record would exceed the
//     byte cap, so linger is an upper bound on batching delay, not a floor.
//   - Each flush runs in its own goroutine so the buffer can immediately
//     start accumulating the next batch. This means multiple Produce
//     requests to the same agent can be in flight concurrently. There is
//     no per-agent in-flight cap by design: Warpstream agents are
//     stateless, ordering between requests doesn't matter for the produce
//     contract this client exposes, and the cap that would matter (memory
//     pressure) is enforced by the caller upstream.
type AgentRecordBuffer struct {
	nodeID        int32
	linger        time.Duration
	batchMaxBytes int32
	flush         AgentFlushFunc
	metrics       *metrics

	mu                        sync.Mutex
	nextProduceFirstTimestamp int64
	nextProducePartitions     []promisedRoutedTopicPartitionRecords
	nextProduceRecords        int
	nextProduceWireBytes      int64
	nextProduceFlushTimer     *time.Timer
	closed                    bool

	flushWG        sync.WaitGroup
	flushCtx       context.Context
	cancelFlushCtx context.CancelFunc
}

// NewAgentRecordBuffer returns a buffer for records destined to nodeID.
// flush runs in a background goroutine when a batch is ready.
func NewAgentRecordBuffer(nodeID int32, linger time.Duration, batchMaxBytes int32, flush AgentFlushFunc, m *metrics) *AgentRecordBuffer {
	ctx, cancel := context.WithCancel(context.Background())
	return &AgentRecordBuffer{
		nodeID:         nodeID,
		linger:         linger,
		batchMaxBytes:  batchMaxBytes,
		flush:          flush,
		metrics:        m,
		flushCtx:       ctx,
		cancelFlushCtx: cancel,
	}
}

// Add buffers partition groups for produce. Each group's done fires exactly
// once with its terminal outcome.
func (a *AgentRecordBuffer) Add(partitions []promisedRoutedTopicPartitionRecords) {
	incoming := 0
	for _, p := range partitions {
		incoming += len(p.records)
	}
	if incoming == 0 {
		return
	}

	// Split any group whose own RecordBatch would exceed batchMaxBytes into
	// chunks that each fit, so no single flushed per-partition batch can be
	// rejected MessageTooLarge. Chunks are then added one at a time below, and
	// because each is <= batchMaxBytes the overflow check keeps nextProduceWireBytes
	// (and therefore every per-partition batch) within the cap.
	chunks := make([]promisedRoutedTopicPartitionRecords, 0, len(partitions))
	for _, p := range partitions {
		chunks = append(chunks, splitPromisedRoutedTopicPartitionRecordsByBatchMaxBytes(p, a.batchMaxBytes)...)
	}

	a.mu.Lock()
	if a.closed {
		// Closed buffer: there is no flush to carry the outcome, so resolve
		// every group's done synchronously with errBufferClosed.
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

// addToNextProduceLocked appends one group (already <= batchMaxBytes) to the
// pending produce, flushing first when it wouldn't fit. Groups are never merged
// here: same-partition entries are coalesced into one wire batch at flush time
// (startFlushLocked), which keeps each group's done untouched. Caller must hold
// a.mu.
func (a *AgentRecordBuffer) addToNextProduceLocked(p promisedRoutedTopicPartitionRecords) {
	addBytes, firstTS := a.computeAddCostLocked(p.records)
	if a.nextProduceRecords > 0 && a.nextProduceWireBytes+addBytes > int64(a.batchMaxBytes) {
		// Re-cost after the forced flush: the batch overhead and offsetDelta
		// values reset, so the original addBytes no longer applies.
		a.startFlushLocked()
		addBytes, firstTS = a.computeAddCostLocked(p.records)
	}

	if a.nextProduceRecords == 0 {
		// Fresh produce: anchor timestamp.
		a.nextProduceFirstTimestamp = firstTS
	}
	a.nextProducePartitions = append(a.nextProducePartitions, p)
	a.nextProduceRecords += len(p.records)
	a.nextProduceWireBytes += addBytes
}

// computeAddCostLocked returns the additional wire bytes that appending records
// to the pending produce would contribute, plus the firstTimestamp that anchors
// the computation (the pending produce's existing anchor when non-empty, the
// first incoming record's timestamp otherwise). Includes the
// recordBatchHeaderBytes overhead when the pending produce is empty so the
// caller can add the result to a zeroed counter. Caller must hold a.mu.
func (a *AgentRecordBuffer) computeAddCostLocked(records []*kgo.Record) (int64, int64) {
	fresh := a.nextProduceRecords == 0
	firstTS := a.nextProduceFirstTimestamp
	if fresh && len(records) > 0 {
		firstTS = records[0].Timestamp.UnixMilli()
	}

	var bytes int64
	if fresh {
		bytes = recordBatchHeaderBytes
	}
	offset := int32(a.nextProduceRecords)
	for _, rec := range records {
		tsDelta := rec.Timestamp.UnixMilli() - firstTS
		bytes += recordEstimateBytes(rec, offset, tsDelta)
		offset++
	}
	return bytes, firstTS
}

// Close flushes the pending produce and waits for every in-flight FlushFunc
// to report. Subsequent Adds fail with errBufferClosed. Idempotent.
func (a *AgentRecordBuffer) Close() {
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
func (a *AgentRecordBuffer) timerFlush() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.startFlushLocked()
}

// startFlushLocked dispatches the pending produce to flush in a goroutine and
// resets buffer state so the next produce can accumulate immediately. No-op
// if nothing is pending. Caller must hold a.mu.
func (a *AgentRecordBuffer) startFlushLocked() {
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
	entries := a.nextProducePartitions
	a.nextProducePartitions = nil
	a.nextProduceRecords = 0
	a.nextProduceWireBytes = 0
	a.nextProduceFirstTimestamp = 0

	// Merge same-partition records into one wire entry: the produce/hedge path
	// keys per-partition state by (topic, partition) and rejects duplicates
	// (see produceResultAccumulator). Completion still fans out to every
	// original entry's done, so the merged wire view drops done.
	wire := mergePromisedRoutedTopicPartitionRecordsByTopicPartition(entries)

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
