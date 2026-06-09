// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

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
// send. All partitions in a single call are guaranteed to belong to nodeID
// — the agent that owns the AgentRecordBuffer doing the flush — because the
// buffer bins incoming routed entries by nodeID and never merges across
// bins.
//
// The function is responsible only for producing the batch and returning
// the resulting ProduceResult. The buffer fires each routed entry's done
// callback uniformly with that outcome after this returns; callers that
// resolve per-partition outcomes internally (e.g. the Hedger) must make
// their done callbacks idempotent so the buffer's redundant fire is a no-op.
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
//   - The linger period is always honoured. The typical deployment has
//     many concurrent client processes producing to the same Warpstream
//     cluster and enforcing linger allow us to amortise the Produce requests
//     cost.
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
	maxBatchBytes int32
	flush         AgentFlushFunc
	metrics       *metrics

	mu                     sync.Mutex
	bufferedFirstTimestamp int64
	bufferedPartitions     []routedTopicPartitionRecords
	bufferedRecords        int
	bufferedWireBytes      int32
	bufferedFlushTimer     *time.Timer
	closed                 bool

	flushWG        sync.WaitGroup
	flushCtx       context.Context
	cancelFlushCtx context.CancelFunc
}

// NewAgentRecordBuffer returns a buffer for records destined to nodeID.
// flush runs in a background goroutine when a batch is ready.
func NewAgentRecordBuffer(nodeID int32, linger time.Duration, maxBatchBytes int32, flush AgentFlushFunc, m *metrics) *AgentRecordBuffer {
	ctx, cancel := context.WithCancel(context.Background())
	return &AgentRecordBuffer{
		nodeID:         nodeID,
		linger:         linger,
		maxBatchBytes:  maxBatchBytes,
		flush:          flush,
		metrics:        m,
		flushCtx:       ctx,
		cancelFlushCtx: cancel,
	}
}

// Add buffers partition groups. Incoming groups are merged into existing
// buffered groups when they share (topic, partition, tried); this keeps
// fresh Produce calls for the same partition coalesced into one wire batch
// on flush, with every contributing caller's done chained together. Each
// partition's done fires exactly once via the FlushFunc handler when its
// terminal outcome is known (or synchronously with errBufferClosed if the
// buffer is closed).
func (a *AgentRecordBuffer) Add(partitions []routedTopicPartitionRecords) {
	incoming := 0
	for _, p := range partitions {
		incoming += len(p.records)
	}
	if incoming == 0 {
		return
	}

	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		for _, p := range partitions {
			p.done(ProduceResult{err: errBufferClosed})
		}
		return
	}

	addBytes, firstTS := a.computeAddCostLocked(partitions)
	if a.bufferedRecords > 0 && a.bufferedWireBytes+addBytes > a.maxBatchBytes {
		// Re-cost after the forced flush: the batch overhead and offsetDelta
		// values reset, so the original addBytes no longer applies.
		a.startFlushLocked()
		addBytes, firstTS = a.computeAddCostLocked(partitions)
	}

	if a.bufferedRecords == 0 {
		// Fresh batch: anchor timestamp.
		a.bufferedFirstTimestamp = firstTS
	}
	a.mergeInLocked(partitions)
	a.bufferedRecords += incoming
	a.bufferedWireBytes += addBytes
	if a.bufferedFlushTimer == nil {
		a.bufferedFlushTimer = time.AfterFunc(a.linger, a.timerFlush)
	}
	a.mu.Unlock()
}

// mergeInLocked appends incoming partition groups to bufferedPartitions,
// merging into an existing entry when (topic, partition, tried) matches.
// On merge, records are appended and the two dones are chained so every
// caller's completion fires when the merged group resolves. Caller must
// hold a.mu.
func (a *AgentRecordBuffer) mergeInLocked(partitions []routedTopicPartitionRecords) {
	for _, in := range partitions {
		merged := false
		for i := range a.bufferedPartitions {
			ex := &a.bufferedPartitions[i]
			if ex.mergeableWith(&in) {
				ex.records = append(ex.records, in.records...)
				ex.done = chainDones(ex.done, in.done)
				merged = true
				break
			}
		}
		if !merged {
			cp := in
			cp.records = append([]*kgo.Record(nil), in.records...)
			a.bufferedPartitions = append(a.bufferedPartitions, cp)
		}
	}
}

// computeAddCostLocked returns the additional wire bytes the incoming partitions
// would contribute if appended to the current batch, plus the firstTimestamp
// that anchors the computation (the batch's existing anchor when non-empty,
// the first incoming record's timestamp otherwise). Includes the
// recordBatchHeaderBytes overhead when the batch is empty so the caller can
// simply add the result to a zeroed counter. Caller must hold a.mu.
func (a *AgentRecordBuffer) computeAddCostLocked(partitions []routedTopicPartitionRecords) (int32, int64) {
	fresh := a.bufferedRecords == 0
	firstTS := a.bufferedFirstTimestamp
	if fresh {
		for _, p := range partitions {
			if len(p.records) > 0 {
				firstTS = p.records[0].Timestamp.UnixMilli()
				break
			}
		}
	}

	var bytes int32
	if fresh {
		bytes = recordBatchHeaderBytes
	}
	offset := int32(a.bufferedRecords)
	for _, p := range partitions {
		for _, rec := range p.records {
			tsDelta := rec.Timestamp.UnixMilli() - firstTS
			bytes += recordEstimateBytes(rec, offset, tsDelta)
			offset++
		}
	}
	return bytes, firstTS
}

// Close flushes the pending batch and waits for every in-flight FlushFunc
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

// startFlushLocked dispatches the pending batch to flush in a goroutine and
// resets buffer state so the next batch can accumulate immediately. No-op
// if nothing is buffered. Caller must hold a.mu.
func (a *AgentRecordBuffer) startFlushLocked() {
	if a.bufferedRecords == 0 {
		return
	}
	if a.bufferedFlushTimer != nil {
		// Stop's return value is intentionally ignored: a concurrently-firing
		// timer's callback (timerFlush) blocks on a.mu and, once it acquires
		// it, finds the buffer empty and is a no-op via startFlushLocked.
		a.bufferedFlushTimer.Stop()
		a.bufferedFlushTimer = nil
	}
	partitions := a.bufferedPartitions
	a.bufferedPartitions = nil
	a.bufferedRecords = 0
	a.bufferedWireBytes = 0
	a.bufferedFirstTimestamp = 0

	a.flushWG.Add(1)
	go func() {
		defer a.flushWG.Done()
		a.metrics.lingerFlushesTotal.Inc()
		res := a.flush(a.flushCtx, a.nodeID, partitions)
		for _, p := range partitions {
			p.done(res)
		}
	}()
}
