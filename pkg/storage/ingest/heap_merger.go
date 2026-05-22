// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"container/heap"
	"context"
	"iter"
	"slices"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

// heapItem is a single record awaiting emission by the HeapMerger, plus the
// channel used to signal back to its submitter once it has been forwarded.
type heapItem struct {
	record *kgo.Record
	vcID   int        // source compartment (for tie-breaking and observability)
	ackCh  chan error // signaled exactly once after the record has been forwarded
}

// recordHeap is a min-heap of records keyed by (record.Timestamp, vcID, offset).
// vcID and offset are deterministic tie-breakers so the emit order is stable
// when multiple records share a producer timestamp (as is the case within a
// single distributor write batch).
type recordHeap []heapItem

func (h recordHeap) Len() int { return len(h) }

// Less orders by Timestamp ascending, then vcID ascending, then offset ascending.
func (h recordHeap) Less(i, j int) bool {
	a, b := h[i].record, h[j].record
	if !a.Timestamp.Equal(b.Timestamp) {
		return a.Timestamp.Before(b.Timestamp)
	}
	if h[i].vcID != h[j].vcID {
		return h[i].vcID < h[j].vcID
	}
	return a.Offset < b.Offset
}

func (h recordHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *recordHeap) Push(x any) { *h = append(*h, x.(heapItem)) }

func (h *recordHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// HeapMergerConfig holds tunables for HeapMerger.
type HeapMergerConfig struct {
	// MaxBatchRecords is the soft upper bound on records buffered in the heap
	// before forcing a flush. A larger value gives the heap more cross-VC mixing
	// at the cost of memory and per-flush latency.
	MaxBatchRecords int

	// MaxBatchWait is the maximum time records sit in the heap before being
	// flushed even if MaxBatchRecords hasn't been reached. Keeps tail latency
	// bounded when traffic is sparse.
	MaxBatchWait time.Duration

	// InputBufferSize is the buffer size of the channel into which submitting
	// consumers push records. Capped to avoid unbounded memory if the merger
	// stalls on a slow downstream consumer.
	InputBufferSize int
}

// HeapMerger receives records from multiple VC streams via a single channel,
// maintains them in a min-heap ordered by Kafka record timestamp, and
// periodically flushes batches in-order to a downstream RecordConsumer.
// Each record's submitter is acked once the batch containing it has been
// forwarded (with the consumer's error, if any).
type HeapMerger struct {
	services.Service

	cfg             HeapMergerConfig
	input           chan heapItem
	consumerFactory consumerFactory
	logger          log.Logger
}

// NewHeapMerger constructs a HeapMerger that forwards merged batches via
// consumers produced by consumerFactory.
func NewHeapMerger(cfg HeapMergerConfig, consumerFactory consumerFactory, logger log.Logger) *HeapMerger {
	if cfg.MaxBatchRecords <= 0 {
		cfg.MaxBatchRecords = 1024
	}
	if cfg.MaxBatchWait <= 0 {
		cfg.MaxBatchWait = 50 * time.Millisecond
	}
	if cfg.InputBufferSize <= 0 {
		cfg.InputBufferSize = 2 * cfg.MaxBatchRecords
	}
	m := &HeapMerger{
		cfg:             cfg,
		input:           make(chan heapItem, cfg.InputBufferSize),
		consumerFactory: consumerFactory,
		logger:          logger,
	}
	m.Service = services.NewBasicService(nil, m.run, nil).WithName("heap-merger")
	return m
}

// NewSubmittingConsumer returns a RecordConsumer that streams its records into
// this merger from the given VC and blocks until they have all been forwarded.
func (m *HeapMerger) NewSubmittingConsumer(vcID int) RecordConsumer {
	return &submittingConsumer{merger: m, vcID: vcID}
}

func (m *HeapMerger) run(ctx context.Context) error {
	h := &recordHeap{}
	heap.Init(h)

	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	timerActive := false

	flush := func() {
		if h.Len() == 0 {
			return
		}
		if timerActive {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerActive = false
		}

		batch := make([]heapItem, h.Len())
		for i := range batch {
			batch[i] = heap.Pop(h).(heapItem)
		}

		records := make([]*kgo.Record, len(batch))
		for i, item := range batch {
			records[i] = item.record
		}

		err := m.consumerFactory.consumer().Consume(ctx, slices.Values(records))
		if err != nil {
			level.Warn(m.logger).Log("msg", "downstream consumer returned error from merged batch", "records", len(batch), "err", err)
		}

		for _, item := range batch {
			item.ackCh <- err
		}
	}

	for {
		select {
		case <-ctx.Done():
			m.drain(h, ctx.Err())
			return nil

		case item := <-m.input:
			heap.Push(h, item)
			if !timerActive {
				timer.Reset(m.cfg.MaxBatchWait)
				timerActive = true
			}
			if h.Len() >= m.cfg.MaxBatchRecords {
				flush()
			}

		case <-timer.C:
			timerActive = false
			flush()
		}
	}
}

// drain acks any remaining heap entries and any records still queued in the
// input channel with the supplied error. Used at shutdown so that submitters
// never block forever on an ack.
func (m *HeapMerger) drain(h *recordHeap, err error) {
	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)
		item.ackCh <- err
	}
	for {
		select {
		case item := <-m.input:
			item.ackCh <- err
		default:
			return
		}
	}
}

// submittingConsumer is a RecordConsumer that submits each record into the
// shared HeapMerger and blocks until the merger has forwarded all of them.
type submittingConsumer struct {
	merger *HeapMerger
	vcID   int
}

func (sc *submittingConsumer) Consume(ctx context.Context, records iter.Seq[*kgo.Record]) error {
	var batch []*kgo.Record
	for r := range records {
		batch = append(batch, r)
	}
	if len(batch) == 0 {
		return nil
	}

	ackCh := make(chan error, len(batch))
	for _, r := range batch {
		select {
		case sc.merger.input <- heapItem{record: r, vcID: sc.vcID, ackCh: ackCh}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	var firstErr error
	for range batch {
		select {
		case err := <-ackCh:
			if err != nil && firstErr == nil {
				firstErr = err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if firstErr != nil {
		return errors.Wrap(firstErr, "heap merger downstream consumer")
	}
	return nil
}
