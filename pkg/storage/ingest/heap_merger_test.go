// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"container/heap"
	"context"
	"errors"
	"iter"
	"strings"
	"sync"
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

func TestRecordHeap_Ordering(t *testing.T) {
	// Asserts records pop in ascending Timestamp order.
	t.Run("orders by ascending timestamp", func(t *testing.T) {
		base := time.Unix(0, 0)
		h := &recordHeap{}
		heap.Init(h)

		heap.Push(h, heapItem{record: &kgo.Record{Timestamp: base.Add(30 * time.Millisecond), Offset: 3}, kafkaClusterID: 0, ackCh: make(chan error, 1)})
		heap.Push(h, heapItem{record: &kgo.Record{Timestamp: base.Add(10 * time.Millisecond), Offset: 1}, kafkaClusterID: 0, ackCh: make(chan error, 1)})
		heap.Push(h, heapItem{record: &kgo.Record{Timestamp: base.Add(20 * time.Millisecond), Offset: 2}, kafkaClusterID: 0, ackCh: make(chan error, 1)})

		var got []int64
		for h.Len() > 0 {
			item := heap.Pop(h).(heapItem)
			got = append(got, item.record.Offset)
		}
		assert.Equal(t, []int64{1, 2, 3}, got)
	})

	// Asserts records sharing a Timestamp pop in (kafkaClusterID, offset) order.
	t.Run("timestamp ties break by cluster then offset", func(t *testing.T) {
		ts := time.Unix(1234, 0)
		h := &recordHeap{}
		heap.Init(h)

		heap.Push(h, heapItem{record: &kgo.Record{Timestamp: ts, Offset: 5}, kafkaClusterID: 2})
		heap.Push(h, heapItem{record: &kgo.Record{Timestamp: ts, Offset: 1}, kafkaClusterID: 0})
		heap.Push(h, heapItem{record: &kgo.Record{Timestamp: ts, Offset: 2}, kafkaClusterID: 0})
		heap.Push(h, heapItem{record: &kgo.Record{Timestamp: ts, Offset: 7}, kafkaClusterID: 1})

		type emitted struct {
			cluster int
			offset  int64
		}
		var got []emitted
		for h.Len() > 0 {
			item := heap.Pop(h).(heapItem)
			got = append(got, emitted{item.kafkaClusterID, item.record.Offset})
		}
		assert.Equal(t, []emitted{
			{0, 1}, {0, 2}, {1, 7}, {2, 5},
		}, got)
	})
}

func TestHeapMerger_Ordering(t *testing.T) {
	// Asserts records from a single submitter pass through unchanged and in submission order.
	t.Run("single cluster preserves submission order", func(t *testing.T) {
		rc := &recordingConsumer{}
		m, _ := newTestMerger(t, rc, OrderedConsumptionConfig{MaxBatchRecords: 3, MaxBatchWait: 10 * time.Millisecond})

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
	})

	// Asserts the merger interleaves records from concurrent submitters in strict timestamp order.
	t.Run("two clusters interleave by timestamp", func(t *testing.T) {
		rc := &recordingConsumer{}
		// Set MaxBatchWait high enough that both submitters land their records before flush, and
		// MaxBatchRecords high so the flush is timer-driven, giving both clusters time to enqueue.
		m, _ := newTestMerger(t, rc, OrderedConsumptionConfig{MaxBatchRecords: 100, MaxBatchWait: 100 * time.Millisecond})

		sc0 := m.NewSubmittingConsumer(0)
		sc1 := m.NewSubmittingConsumer(1)

		base := time.Unix(0, 0)
		cluster0Records := []*kgo.Record{
			{Timestamp: base.Add(1 * time.Millisecond), Offset: 10},
			{Timestamp: base.Add(3 * time.Millisecond), Offset: 11},
			{Timestamp: base.Add(5 * time.Millisecond), Offset: 12},
		}
		cluster1Records := []*kgo.Record{
			{Timestamp: base.Add(2 * time.Millisecond), Offset: 20},
			{Timestamp: base.Add(4 * time.Millisecond), Offset: 21},
			{Timestamp: base.Add(6 * time.Millisecond), Offset: 22},
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			assert.NoError(t, sc0.Consume(context.Background(), recordsSeq(cluster0Records)))
		}()
		go func() {
			defer wg.Done()
			assert.NoError(t, sc1.Consume(context.Background(), recordsSeq(cluster1Records)))
		}()
		wg.Wait()

		got := rc.snapshot()
		require.Len(t, got, 6)
		// Expect strict timestamp order, regardless of cluster.
		for i := 1; i < len(got); i++ {
			assert.True(t, !got[i].Timestamp.Before(got[i-1].Timestamp),
				"records emitted out of timestamp order at index %d: %v then %v", i, got[i-1].Timestamp, got[i].Timestamp)
		}
	})
}

// Asserts a downstream error reaches every submitter whose records were in the failing batch.
func TestHeapMerger_ErrorPropagatesToAllSubmittersInBatch(t *testing.T) {
	rc := &recordingConsumer{err: errors.New("downstream boom")}
	m, _ := newTestMerger(t, rc, OrderedConsumptionConfig{MaxBatchRecords: 100, MaxBatchWait: 30 * time.Millisecond})

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
	m, _ := newTestMerger(t, rc, OrderedConsumptionConfig{MaxBatchRecords: 100, MaxBatchWait: 30 * time.Millisecond})
	sc := m.NewSubmittingConsumer(0)
	require.NoError(t, sc.Consume(context.Background(), recordsSeq(nil)))
	assert.Empty(t, rc.snapshot())
}

func TestHeapMerger_TimerFlush(t *testing.T) {
	// Asserts the MaxBatchWait timer flushes the heap even when MaxBatchRecords isn't reached.
	t.Run("flushes before max-batch-records is reached", func(t *testing.T) {
		rc := &recordingConsumer{}
		m, _ := newTestMerger(t, rc, OrderedConsumptionConfig{MaxBatchRecords: 100, MaxBatchWait: 20 * time.Millisecond})
		sc := m.NewSubmittingConsumer(0)
		require.NoError(t, sc.Consume(context.Background(), recordsSeq([]*kgo.Record{{Timestamp: time.Unix(1, 0), Offset: 1}})))
		assert.Len(t, rc.snapshot(), 1)
	})

	// Asserts the MaxBatchWait timer re-arms after a flush so subsequent batches also flush on time.
	t.Run("timer re-arms after a flush", func(t *testing.T) {
		rc := &recordingConsumer{}
		m, _ := newTestMerger(t, rc, OrderedConsumptionConfig{MaxBatchRecords: 100, MaxBatchWait: 20 * time.Millisecond})
		sc := m.NewSubmittingConsumer(0)
		base := time.Unix(0, 0)
		require.NoError(t, sc.Consume(context.Background(), recordsSeq([]*kgo.Record{{Timestamp: base, Offset: 1}})))
		require.NoError(t, sc.Consume(context.Background(), recordsSeq([]*kgo.Record{{Timestamp: base.Add(time.Second), Offset: 2}})))
		assert.Len(t, rc.snapshot(), 2)
	})
}

// Asserts drain() acks records still buffered in the heap at shutdown, so a submitter blocked on its ack
// unblocks (with an error) instead of hanging forever. A large batch size and long wait keep the record
// sitting in the heap — never flushed downstream — so the only thing that can ack it is drain().
func TestHeapMerger_ShutdownAcksPendingRecords(t *testing.T) {
	rc := &recordingConsumer{}
	factory := consumerFactoryFunc(func() RecordConsumer { return rc })
	m := NewHeapMerger(OrderedConsumptionConfig{MaxBatchRecords: 100, MaxBatchWait: time.Hour}, factory, NewHeapMergerMetrics(prometheus.NewRegistry()), log.NewNopLogger())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), m))

	sc := m.NewSubmittingConsumer(0)
	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- sc.Consume(context.Background(), recordsSeq([]*kgo.Record{{Timestamp: time.Unix(1, 0), Offset: 1}}))
	}()

	// Wait briefly so the record has landed in the heap (it won't flush: batch size is 100, wait is 1h),
	// then stop the merger. This makes run() take the ctx.Done() branch and call drain().
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), m))

	select {
	case err := <-consumeErr:
		// drain() acks with the run context's error, which the submitter wraps and returns.
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("submitter did not return after merger shutdown — likely a hung ack")
	}

	// The record was never forwarded downstream: it was dropped by drain(), not flushed.
	assert.Empty(t, rc.snapshot())
}

// Asserts the out-of-order emissions counter increments when a record arrives with an older timestamp
// than one already emitted in a previous batch.
func TestHeapMerger_CountsOutOfOrderEmissionsAcrossBatches(t *testing.T) {
	rc := &recordingConsumer{}
	// Small batch + short wait so the first record flushes alone, then the second record (with older
	// timestamp) flushes in a second batch and trips the OOO counter.
	m, reg := newTestMerger(t, rc, OrderedConsumptionConfig{MaxBatchRecords: 1, MaxBatchWait: 5 * time.Millisecond})
	sc := m.NewSubmittingConsumer(0)

	base := time.Unix(0, 0)
	require.NoError(t, sc.Consume(context.Background(), recordsSeq([]*kgo.Record{{Timestamp: base.Add(50 * time.Millisecond), Offset: 1}})))
	require.NoError(t, sc.Consume(context.Background(), recordsSeq([]*kgo.Record{{Timestamp: base.Add(10 * time.Millisecond), Offset: 2}})))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingest_storage_ordered_consumption_out_of_order_records_total Total number of records pushed with a timestamp older than the most recently pushed record. This is the residual cross-cluster out-of-order that ordered consumption could not absorb.
		# TYPE cortex_ingest_storage_ordered_consumption_out_of_order_records_total counter
		cortex_ingest_storage_ordered_consumption_out_of_order_records_total 1
	`), "cortex_ingest_storage_ordered_consumption_out_of_order_records_total"))
}

// recordingConsumer is a RecordConsumer that captures every record it sees, in order, into a shared
// slice for assertions.
type recordingConsumer struct {
	mu      sync.Mutex
	records []*kgo.Record
	err     error // optional error to return
}

func (rc *recordingConsumer) Consume(_ context.Context, records iter.Seq[*kgo.Record]) error {
	var batch []*kgo.Record
	for r := range records {
		batch = append(batch, r)
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

func newTestMerger(t *testing.T, rc *recordingConsumer, cfg OrderedConsumptionConfig) (*HeapMerger, *prometheus.Registry) {
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
