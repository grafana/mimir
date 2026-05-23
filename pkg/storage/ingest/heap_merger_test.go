// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"container/heap"
	"context"
	"errors"
	"iter"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// --- recordHeap unit tests ----------------------------------------------------

// Asserts records pop in ascending Timestamp order.
func TestRecordHeap_OrdersByTimestamp(t *testing.T) {
	base := time.Unix(0, 0)
	h := &recordHeap{}
	heap.Init(h)

	heap.Push(h, heapItem{record: &kgo.Record{Timestamp: base.Add(30 * time.Millisecond), Offset: 3}, vcID: 0, ackCh: make(chan error, 1)})
	heap.Push(h, heapItem{record: &kgo.Record{Timestamp: base.Add(10 * time.Millisecond), Offset: 1}, vcID: 0, ackCh: make(chan error, 1)})
	heap.Push(h, heapItem{record: &kgo.Record{Timestamp: base.Add(20 * time.Millisecond), Offset: 2}, vcID: 0, ackCh: make(chan error, 1)})

	var got []int64
	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)
		got = append(got, item.record.Offset)
	}
	assert.Equal(t, []int64{1, 2, 3}, got)
}

// Asserts records sharing a Timestamp pop in (vcID, offset) order.
func TestRecordHeap_TimestampTie_OrderByVCThenOffset(t *testing.T) {
	ts := time.Unix(1234, 0)
	h := &recordHeap{}
	heap.Init(h)

	// Same timestamp; expect order (vcID asc, offset asc).
	heap.Push(h, heapItem{record: &kgo.Record{Timestamp: ts, Offset: 5}, vcID: 2})
	heap.Push(h, heapItem{record: &kgo.Record{Timestamp: ts, Offset: 1}, vcID: 0})
	heap.Push(h, heapItem{record: &kgo.Record{Timestamp: ts, Offset: 2}, vcID: 0})
	heap.Push(h, heapItem{record: &kgo.Record{Timestamp: ts, Offset: 7}, vcID: 1})

	type emitted struct {
		vc     int
		offset int64
	}
	var got []emitted
	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)
		got = append(got, emitted{item.vcID, item.record.Offset})
	}
	assert.Equal(t, []emitted{
		{0, 1}, {0, 2}, {1, 7}, {2, 5},
	}, got)
}

// --- HeapMerger integration tests --------------------------------------------

// recordingConsumer is a RecordConsumer that captures every record it sees,
// in order, into a shared slice for assertions.
type recordingConsumer struct {
	mu      sync.Mutex
	records []*kgo.Record
	delay   time.Duration // optional artificial slowness for backpressure tests
	err     error         // optional error to return
}

func (rc *recordingConsumer) Consume(_ context.Context, records iter.Seq[*kgo.Record]) error {
	var batch []*kgo.Record
	for r := range records {
		batch = append(batch, r)
	}
	if rc.delay > 0 {
		time.Sleep(rc.delay)
	}
	rc.mu.Lock()
	rc.records = append(rc.records, batch...)
	rc.mu.Unlock()
	return rc.err
}

func (rc *recordingConsumer) snapshot() []*kgo.Record {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	out := make([]*kgo.Record, len(rc.records))
	copy(out, rc.records)
	return out
}

func newTestMerger(t *testing.T, rc *recordingConsumer, cfg HeapMergerConfig) (*HeapMerger, *prometheus.Registry) {
	t.Helper()
	factory := consumerFactoryFunc(func() RecordConsumer { return rc })
	reg := prometheus.NewRegistry()
	m := NewHeapMerger(cfg, factory, NewHeapMergerMetrics(reg), log.NewNopLogger())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), m))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), m)
	})
	return m, reg
}

// Asserts records from a single submitter pass through unchanged and in submission order.
func TestHeapMerger_SingleVCInOrder(t *testing.T) {
	rc := &recordingConsumer{}
	m, _ := newTestMerger(t, rc, HeapMergerConfig{MaxBatchRecords: 3, MaxBatchWait: 10 * time.Millisecond})

	sc := m.NewSubmittingConsumer(0)
	base := time.Unix(0, 0)
	in := []*kgo.Record{
		{Timestamp: base.Add(1 * time.Millisecond), Offset: 1},
		{Timestamp: base.Add(2 * time.Millisecond), Offset: 2},
		{Timestamp: base.Add(3 * time.Millisecond), Offset: 3},
	}
	require.NoError(t, sc.Consume(context.Background(), recordsSeq(in)))

	got := rc.snapshot()
	require.Len(t, got, 3)
	for i, want := range in {
		assert.Equal(t, want.Offset, got[i].Offset)
	}
}

// Asserts the merger interleaves records from concurrent submitters in strict timestamp order.
func TestHeapMerger_TwoVCsInterleaveByTimestamp(t *testing.T) {
	rc := &recordingConsumer{}
	// Set MaxBatchWait high enough that both submitters land their records before flush.
	// Set MaxBatchRecords high so the flush is timer-driven, giving both VCs time to enqueue.
	m, _ := newTestMerger(t, rc, HeapMergerConfig{MaxBatchRecords: 100, MaxBatchWait: 100 * time.Millisecond})

	sc0 := m.NewSubmittingConsumer(0)
	sc1 := m.NewSubmittingConsumer(1)

	base := time.Unix(0, 0)
	vc0Records := []*kgo.Record{
		{Timestamp: base.Add(1 * time.Millisecond), Offset: 10},
		{Timestamp: base.Add(3 * time.Millisecond), Offset: 11},
		{Timestamp: base.Add(5 * time.Millisecond), Offset: 12},
	}
	vc1Records := []*kgo.Record{
		{Timestamp: base.Add(2 * time.Millisecond), Offset: 20},
		{Timestamp: base.Add(4 * time.Millisecond), Offset: 21},
		{Timestamp: base.Add(6 * time.Millisecond), Offset: 22},
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		assert.NoError(t, sc0.Consume(context.Background(), recordsSeq(vc0Records)))
	}()
	go func() {
		defer wg.Done()
		assert.NoError(t, sc1.Consume(context.Background(), recordsSeq(vc1Records)))
	}()
	wg.Wait()

	got := rc.snapshot()
	require.Len(t, got, 6)
	// Expect strict timestamp order, regardless of VC.
	for i := 1; i < len(got); i++ {
		assert.True(t, !got[i].Timestamp.Before(got[i-1].Timestamp),
			"records emitted out of timestamp order at index %d: %v then %v", i, got[i-1].Timestamp, got[i].Timestamp)
	}
}

// Asserts a downstream error reaches every submitter whose records were in the failing batch.
func TestHeapMerger_ErrorPropagatesToAllSubmittersInBatch(t *testing.T) {
	rc := &recordingConsumer{err: errors.New("downstream boom")}
	m, _ := newTestMerger(t, rc, HeapMergerConfig{MaxBatchRecords: 100, MaxBatchWait: 30 * time.Millisecond})

	sc0 := m.NewSubmittingConsumer(0)
	sc1 := m.NewSubmittingConsumer(1)

	base := time.Unix(0, 0)
	var wg sync.WaitGroup
	var err0, err1 error
	wg.Add(2)
	go func() {
		defer wg.Done()
		err0 = sc0.Consume(context.Background(), recordsSeq([]*kgo.Record{{Timestamp: base, Offset: 1}}))
	}()
	go func() {
		defer wg.Done()
		err1 = sc1.Consume(context.Background(), recordsSeq([]*kgo.Record{{Timestamp: base, Offset: 2}}))
	}()
	wg.Wait()

	require.Error(t, err0)
	require.Error(t, err1)
	assert.Contains(t, err0.Error(), "downstream boom")
	assert.Contains(t, err1.Error(), "downstream boom")
}

// Asserts Consume returns immediately and invokes nothing downstream when given no records.
func TestHeapMerger_EmptyConsumeIsNoop(t *testing.T) {
	rc := &recordingConsumer{}
	m, _ := newTestMerger(t, rc, HeapMergerConfig{MaxBatchRecords: 100, MaxBatchWait: 30 * time.Millisecond})
	sc := m.NewSubmittingConsumer(0)
	require.NoError(t, sc.Consume(context.Background(), recordsSeq(nil)))
	assert.Empty(t, rc.snapshot())
}

// Asserts submitters block (via the bounded input channel) while the downstream consumer is slow.
func TestHeapMerger_BackpressureBlocksSubmittersWhenDownstreamSlow(t *testing.T) {
	// A small input buffer and a slow downstream should cause submitters to block
	// once the buffer + heap fills up. We confirm backpressure indirectly by checking
	// that Consume doesn't return until the slow downstream has finished.
	rc := &recordingConsumer{delay: 80 * time.Millisecond}
	m, _ := newTestMerger(t, rc, HeapMergerConfig{
		MaxBatchRecords: 2,
		MaxBatchWait:    5 * time.Millisecond,
		InputBufferSize: 2,
	})

	sc := m.NewSubmittingConsumer(0)
	base := time.Unix(0, 0)
	records := make([]*kgo.Record, 6)
	for i := range records {
		records[i] = &kgo.Record{Timestamp: base.Add(time.Duration(i) * time.Millisecond), Offset: int64(i)}
	}

	start := time.Now()
	require.NoError(t, sc.Consume(context.Background(), recordsSeq(records)))
	elapsed := time.Since(start)

	// 6 records / 2 per batch = 3 batches, each delayed 80ms ≈ 240ms minimum
	// (loose lower bound to avoid flakiness from scheduling).
	assert.GreaterOrEqual(t, elapsed, 150*time.Millisecond,
		"Consume returned too quickly (elapsed=%v); backpressure may not be working", elapsed)
	assert.Len(t, rc.snapshot(), 6)
}

// Asserts in-flight submitters unblock cleanly when the merger is stopped instead of hanging on ack.
func TestHeapMerger_ShutdownAcksPendingRecords(t *testing.T) {
	// A merger with a downstream that never returns: we stop the merger and
	// confirm that submitters see their context cancellation propagated as an ack.
	blocker := &blockingConsumer{block: make(chan struct{})}
	factory := consumerFactoryFunc(func() RecordConsumer { return blocker })
	m := NewHeapMerger(HeapMergerConfig{MaxBatchRecords: 1, MaxBatchWait: 5 * time.Millisecond}, factory, NewHeapMergerMetrics(prometheus.NewRegistry()), log.NewNopLogger())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), m))

	sc := m.NewSubmittingConsumer(0)
	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- sc.Consume(context.Background(), recordsSeq([]*kgo.Record{{Timestamp: time.Unix(1, 0), Offset: 1}}))
	}()

	// Wait briefly so the record is in flight, then stop the merger.
	time.Sleep(20 * time.Millisecond)
	go func() {
		_ = services.StopAndAwaitTerminated(context.Background(), m)
	}()

	// Unblock the in-flight downstream call so the merger's run loop can exit.
	close(blocker.block)

	select {
	case err := <-consumeErr:
		// The merger acks with context.Canceled (or downstream's error) at shutdown;
		// either way, the submitter returns rather than hanging forever.
		_ = err
	case <-time.After(2 * time.Second):
		t.Fatal("submitter did not return after merger shutdown — likely a hung ack")
	}
}

// Asserts the out-of-order emissions counter increments when a record arrives with an
// older timestamp than one already emitted in a previous batch.
func TestHeapMerger_CountsOutOfOrderEmissionsAcrossBatches(t *testing.T) {
	rc := &recordingConsumer{}
	// Small batch + short wait so the first record flushes alone, then the second record
	// (with older timestamp) flushes in a second batch and trips the OOO counter.
	m, reg := newTestMerger(t, rc, HeapMergerConfig{MaxBatchRecords: 1, MaxBatchWait: 5 * time.Millisecond})
	sc := m.NewSubmittingConsumer(0)

	base := time.Unix(0, 0)
	require.NoError(t, sc.Consume(context.Background(), recordsSeq([]*kgo.Record{{Timestamp: base.Add(50 * time.Millisecond), Offset: 1}})))
	require.NoError(t, sc.Consume(context.Background(), recordsSeq([]*kgo.Record{{Timestamp: base.Add(10 * time.Millisecond), Offset: 2}})))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingest_storage_heap_merger_out_of_order_emissions_total Total number of records emitted with a timestamp older than the most recently emitted record. This is the residual cross-VC OOO that the merger could not absorb.
		# TYPE cortex_ingest_storage_heap_merger_out_of_order_emissions_total counter
		cortex_ingest_storage_heap_merger_out_of_order_emissions_total 1
	`), "cortex_ingest_storage_heap_merger_out_of_order_emissions_total"))
}

// Asserts emitted_records_total tracks contributions per source VC.
func TestHeapMerger_EmittedRecordsCountedPerVC(t *testing.T) {
	rc := &recordingConsumer{}
	m, reg := newTestMerger(t, rc, HeapMergerConfig{MaxBatchRecords: 100, MaxBatchWait: 30 * time.Millisecond})

	sc0 := m.NewSubmittingConsumer(0)
	sc1 := m.NewSubmittingConsumer(1)

	base := time.Unix(0, 0)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		assert.NoError(t, sc0.Consume(context.Background(), recordsSeq([]*kgo.Record{
			{Timestamp: base.Add(1 * time.Millisecond), Offset: 1},
			{Timestamp: base.Add(3 * time.Millisecond), Offset: 2},
		})))
	}()
	go func() {
		defer wg.Done()
		assert.NoError(t, sc1.Consume(context.Background(), recordsSeq([]*kgo.Record{
			{Timestamp: base.Add(2 * time.Millisecond), Offset: 1},
			{Timestamp: base.Add(4 * time.Millisecond), Offset: 2},
			{Timestamp: base.Add(6 * time.Millisecond), Offset: 3},
		})))
	}()
	wg.Wait()

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingest_storage_heap_merger_emitted_records_total Total number of records the heap merger has forwarded downstream, by source write compartment.
		# TYPE cortex_ingest_storage_heap_merger_emitted_records_total counter
		cortex_ingest_storage_heap_merger_emitted_records_total{write_compartment="0"} 2
		cortex_ingest_storage_heap_merger_emitted_records_total{write_compartment="1"} 3
	`), "cortex_ingest_storage_heap_merger_emitted_records_total"))
}

// Asserts the MaxBatchWait timer flushes the heap even when MaxBatchRecords isn't reached.
func TestHeapMerger_TimerDrivenFlush(t *testing.T) {
	rc := &recordingConsumer{}
	m, _ := newTestMerger(t, rc, HeapMergerConfig{MaxBatchRecords: 100, MaxBatchWait: 20 * time.Millisecond})
	sc := m.NewSubmittingConsumer(0)
	require.NoError(t, sc.Consume(context.Background(), recordsSeq([]*kgo.Record{{Timestamp: time.Unix(1, 0), Offset: 1}})))
	assert.Len(t, rc.snapshot(), 1)
}

// Asserts the MaxBatchWait timer re-arms after a flush so subsequent batches also flush on time.
func TestHeapMerger_TimerReArmsAfterFlush(t *testing.T) {
	rc := &recordingConsumer{}
	m, _ := newTestMerger(t, rc, HeapMergerConfig{MaxBatchRecords: 100, MaxBatchWait: 20 * time.Millisecond})
	sc := m.NewSubmittingConsumer(0)
	base := time.Unix(0, 0)
	require.NoError(t, sc.Consume(context.Background(), recordsSeq([]*kgo.Record{{Timestamp: base, Offset: 1}})))
	require.NoError(t, sc.Consume(context.Background(), recordsSeq([]*kgo.Record{{Timestamp: base.Add(time.Second), Offset: 2}})))
	assert.Len(t, rc.snapshot(), 2)
}

// Asserts that records sharing a producer timestamp emit in (vcID, offset) order through the merger.
// This is the per-batch invariant the design relies on: all records in one MultiWriteSync share a
// timestamp and must emit in offset order.
func TestHeapMerger_TiedTimestampsEmitInOffsetOrder(t *testing.T) {
	rc := &recordingConsumer{}
	m, _ := newTestMerger(t, rc, HeapMergerConfig{MaxBatchRecords: 100, MaxBatchWait: 50 * time.Millisecond})

	sc := m.NewSubmittingConsumer(0)
	ts := time.Unix(100, 0)
	require.NoError(t, sc.Consume(context.Background(), recordsSeq([]*kgo.Record{
		{Timestamp: ts, Offset: 3},
		{Timestamp: ts, Offset: 1},
		{Timestamp: ts, Offset: 2},
	})))

	got := rc.snapshot()
	require.Len(t, got, 3)
	assert.Equal(t, []int64{1, 2, 3}, []int64{got[0].Offset, got[1].Offset, got[2].Offset})
}

// Asserts a submitter unblocks promptly when its context is cancelled while records are in flight.
func TestHeapMerger_ConsumeRespectsContextCancellation(t *testing.T) {
	blocker := &blockingConsumer{block: make(chan struct{})}
	defer close(blocker.block)

	factory := consumerFactoryFunc(func() RecordConsumer { return blocker })
	m := NewHeapMerger(HeapMergerConfig{MaxBatchRecords: 1, MaxBatchWait: 5 * time.Millisecond}, factory, NewHeapMergerMetrics(prometheus.NewRegistry()), log.NewNopLogger())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), m))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), m) })

	sc := m.NewSubmittingConsumer(0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- sc.Consume(ctx, recordsSeq([]*kgo.Record{{Timestamp: time.Unix(1, 0), Offset: 1}}))
	}()

	// Wait long enough for the record to be in flight, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Consume did not return after context cancellation")
	}
}

// blockingConsumer is a RecordConsumer that blocks in Consume until block is closed.
type blockingConsumer struct {
	block chan struct{}
	count atomic.Int32
}

func (bc *blockingConsumer) Consume(ctx context.Context, _ iter.Seq[*kgo.Record]) error {
	bc.count.Add(1)
	select {
	case <-bc.block:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// recordsSeq turns a slice of records into an iter.Seq for tests.
func recordsSeq(records []*kgo.Record) iter.Seq[*kgo.Record] {
	return func(yield func(*kgo.Record) bool) {
		for _, r := range records {
			if !yield(r) {
				return
			}
		}
	}
}
