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

// FlushFunc is invoked by AgentRecordBuffer when a batch is ready to send.
// records carries every buffered record for nodeID at the moment of the
// flush; they may span multiple topics and partitions. The implementation
// must call done(err) exactly once after the batch has been durably stored
// or has failed.
type FlushFunc func(ctx context.Context, nodeID int32, records []*kgo.Record, done func(error))

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
	flush         FlushFunc
	metrics       *metrics

	mu                 sync.Mutex
	bufferedRecords    []*kgo.Record
	bufferedWireBytes  int32
	bufferedCallbacks  []func(error)
	bufferedFlushTimer *time.Timer
	closed             bool

	flushWG sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewAgentRecordBuffer returns a buffer for records destined to nodeID.
// flush runs in a background goroutine when a batch is ready.
func NewAgentRecordBuffer(nodeID int32, linger time.Duration, maxBatchBytes int32, flush FlushFunc, m *metrics) *AgentRecordBuffer {
	ctx, cancel := context.WithCancel(context.Background())
	return &AgentRecordBuffer{
		nodeID:        nodeID,
		linger:        linger,
		maxBatchBytes: maxBatchBytes,
		flush:         flush,
		metrics:       m,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Add appends records to the pending batch and fires flushDone once after
// that batch is acked. Overflowing maxBatchBytes flushes the existing batch
// inline first; closed buffers fail flushDone with errBufferClosed.
func (a *AgentRecordBuffer) Add(records []*kgo.Record, flushDone func(error)) {
	if len(records) == 0 {
		flushDone(nil)
		return
	}
	bytes := wireBytesOf(records)

	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		flushDone(errBufferClosed)
		return
	}
	if len(a.bufferedRecords) > 0 && a.bufferedWireBytes+bytes > a.maxBatchBytes {
		a.startFlushLocked()
	}
	a.bufferedRecords = append(a.bufferedRecords, records...)
	a.bufferedWireBytes += bytes
	a.bufferedCallbacks = append(a.bufferedCallbacks, flushDone)
	if a.bufferedFlushTimer == nil {
		a.bufferedFlushTimer = time.AfterFunc(a.linger, a.timerFlush)
	}
	a.mu.Unlock()
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
	a.cancel()
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
	if len(a.bufferedRecords) == 0 {
		return
	}
	if a.bufferedFlushTimer != nil {
		// Stop's return value is intentionally ignored: a concurrently-firing
		// timer's callback (timerFlush) blocks on a.mu and, once it acquires
		// it, finds bufferedRecords empty and is a no-op via startFlushLocked.
		a.bufferedFlushTimer.Stop()
		a.bufferedFlushTimer = nil
	}
	records := a.bufferedRecords
	callbacks := a.bufferedCallbacks
	a.bufferedRecords = nil
	a.bufferedWireBytes = 0
	a.bufferedCallbacks = nil

	a.flushWG.Add(1)
	go func() {
		defer a.flushWG.Done()
		ack := make(chan error, 1)
		a.flush(a.ctx, a.nodeID, records, func(err error) { ack <- err })
		err := <-ack
		for _, flushDone := range callbacks {
			flushDone(err)
		}
		a.metrics.lingerFlushesTotal.Inc()
	}()
}

// wireBytesOf returns an upper-bound estimate of the wire-encoded size of
// records. Used only for the batch-size cap, where exact accuracy doesn't
// matter — only that we never significantly under-count.
func wireBytesOf(records []*kgo.Record) int32 {
	const recordOverheadEstimate = 24 // ~upper bound on Length+Attributes+TimestampDelta+OffsetDelta+(varint lengths)
	var n int32
	for _, r := range records {
		n += recordOverheadEstimate + int32(len(r.Key)) + int32(len(r.Value))
		for _, h := range r.Headers {
			n += 8 + int32(len(h.Key)) + int32(len(h.Value))
		}
	}
	return n
}
