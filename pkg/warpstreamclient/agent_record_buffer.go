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

// AgentRecordBuffer accumulates records destined for one agent and invokes
// FlushFunc when:
//   - the linger timer fires;
//   - adding records would push the batch above maxBatchBytes; or
//   - Close is called.
//
// AgentRecordBuffer is concurrency-safe via a single per-instance mutex and
// has no knowledge of routing, resolvers, or the existence of other agents.
//
// The linger period is always honoured; there is no synchronous-flush bypass.
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

// NewAgentRecordBuffer returns a buffer ready to accept records destined for
// nodeID. flush is invoked from a background goroutine when a batch is ready
// to send.
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

// Add appends records to the pending batch and arranges for flushDone to
// fire once after the batch containing these records is acked. If appending
// would push the batch above maxBatchBytes the existing batch is flushed
// inline before the new records are appended. If the buffer is closed
// flushDone fires synchronously with errBufferClosed.
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

// Close flushes the pending batch and waits until every in-flight FlushFunc
// has completed and every flushDone has fired. Subsequent Add calls fail
// with errBufferClosed. Idempotent.
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

// startFlushLocked snapshots the pending batch and dispatches it to the
// FlushFunc in a background goroutine. Resets the buffer state so a fresh
// batch can begin accumulating immediately. No-op if there is nothing
// buffered.
//
// Caller must hold a.mu.
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

// wireBytesOf returns an approximate wire-encoded size of records, used as a
// stable upper-bound estimate for the batch-size cap. Exact accuracy is not
// required; what matters is that the estimate is monotone and never
// underestimates badly enough to allow batches significantly above
// maxBatchBytes.
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
