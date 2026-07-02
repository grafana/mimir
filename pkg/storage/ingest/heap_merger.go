// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"container/heap"
	"context"
	"flag"
	"iter"
	"slices"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
)

// HeapMergerConfig holds settings for the HeapMerger.
type HeapMergerConfig struct {
	// Enabled controls whether records from all Kafka clusters are merged by Kafka record timestamp
	// before being pushed downstream. When disabled, each cluster's records are pushed independently
	// and cross-cluster ordering relies entirely on the TSDB out-of-order window.
	Enabled bool `yaml:"enabled" category:"experimental"`

	// MaxBatchRecords is the soft upper bound on records buffered in the heap before forcing a flush.
	// A larger value gives the heap more cross-cluster mixing at the cost of memory and per-flush latency.
	MaxBatchRecords int `yaml:"max_batch_records" category:"experimental"`

	// MaxBatchWait is the maximum time records sit in the heap before being flushed even if
	// MaxBatchRecords hasn't been reached. Keeps tail latency bounded when traffic is sparse.
	MaxBatchWait time.Duration `yaml:"max_batch_wait" category:"experimental"`

	// InputBufferSize is the buffer size of the channel into which submitting consumers push records.
	// Capped to avoid unbounded memory if the merger stalls on a slow downstream consumer. When 0,
	// defaults to 2x MaxBatchRecords.
	InputBufferSize int `yaml:"input_buffer_size" category:"experimental"`
}

// RegisterFlagsWithPrefix registers HeapMergerConfig flags under the given prefix.
func (cfg *HeapMergerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, prefix+"enabled", false, "Whether records from all write compartments' Kafka clusters are merged by Kafka record timestamp before being pushed. When disabled, each Kafka cluster is pushed independently and cross-cluster ordering relies on the TSDB out-of-order window. Only takes effect when compartments are enabled with more than one write compartment.")
	f.IntVar(&cfg.MaxBatchRecords, prefix+"max-batch-records", 1024, "Soft upper bound on records buffered in the heap merger before forcing a flush. Larger values give the merger more cross-cluster mixing at the cost of memory and per-flush latency.")
	f.DurationVar(&cfg.MaxBatchWait, prefix+"max-batch-wait", 50*time.Millisecond, "Maximum time records sit in the heap merger before being flushed even if max-batch-records has not been reached.")
	f.IntVar(&cfg.InputBufferSize, prefix+"input-buffer-size", 0, "Buffer size of the channel into which the per-cluster submitting consumers push records. When 0, defaults to 2x max-batch-records.")
}

type HeapMergerMetrics struct {
	outOfOrderEmits   prometheus.Counter
	batchFlushLatency prometheus.Histogram
}

// NewHeapMergerMetrics creates and registers a HeapMergerMetrics on the supplied registerer.
func NewHeapMergerMetrics(reg prometheus.Registerer) *HeapMergerMetrics {
	factory := promauto.With(reg)
	return &HeapMergerMetrics{
		outOfOrderEmits: factory.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_heap_merger_out_of_order_emissions_total",
			Help: "Total number of records emitted with a timestamp older than the most recently emitted record. This is the residual cross-cluster out-of-order that the merger could not absorb.",
		}),
		batchFlushLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingest_storage_heap_merger_batch_flush_latency_seconds",
			Help:    "Elapsed time from the first record entering a batch to the batch finishing its downstream push.",
			Buckets: prometheus.DefBuckets,
		}),
	}
}

// heapItem is a single record awaiting emission by the HeapMerger, plus the channel used to signal back
// to its submitter once it has been forwarded.
type heapItem struct {
	record         *kgo.Record
	kafkaClusterID int        // source Kafka cluster (for tie-breaking and observability)
	ackCh          chan error // signaled exactly once after the record has been forwarded
}

// recordHeap is a min-heap of records keyed by (record.Timestamp, kafkaClusterID, offset). kafkaClusterID
// and offset are deterministic tie-breakers so the emit order is stable when multiple records share a
// producer timestamp (as is the case within a single distributor write batch).
type recordHeap []heapItem

func (h recordHeap) Len() int { return len(h) }

// Less orders by Timestamp ascending, then kafkaClusterID ascending, then offset ascending.
func (h recordHeap) Less(i, j int) bool {
	a, b := h[i].record, h[j].record
	if !a.Timestamp.Equal(b.Timestamp) {
		return a.Timestamp.Before(b.Timestamp)
	}
	if h[i].kafkaClusterID != h[j].kafkaClusterID {
		return h[i].kafkaClusterID < h[j].kafkaClusterID
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

// HeapMerger receives records from multiple Kafka cluster streams via a single channel, maintains them
// in a min-heap ordered by Kafka record timestamp, and periodically flushes batches in-order to a
// downstream RecordConsumer. Each record's submitter is acked once the batch containing it has been
// forwarded (with the consumer's error, if any), so a per-cluster reader only commits its offset once
// its records have actually been pushed.
//
// The ordering is best effort: it can only reorder records that happen to be buffered in the heap at the
// same time. Any residual cross-cluster out-of-order that the heap could not absorb is still handled by
// the TSDB out-of-order window downstream.
type HeapMerger struct {
	services.Service

	cfg             HeapMergerConfig
	input           chan heapItem
	consumerFactory consumerFactory
	metrics         *HeapMergerMetrics
	logger          log.Logger
}

// NewHeapMerger constructs a HeapMerger that forwards merged batches via consumers produced by
// consumerFactory.
func NewHeapMerger(cfg HeapMergerConfig, consumerFactory consumerFactory, metrics *HeapMergerMetrics, logger log.Logger) *HeapMerger {
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
		metrics:         metrics,
		logger:          logger,
	}
	m.Service = services.NewBasicService(nil, m.run, nil).WithName("heap-merger")
	return m
}

// NewSubmittingConsumer returns a RecordConsumer that streams its records into this merger from the
// given Kafka cluster and blocks until they have all been forwarded.
func (m *HeapMerger) NewSubmittingConsumer(kafkaClusterID int) RecordConsumer {
	return &submittingConsumer{merger: m, kafkaClusterID: kafkaClusterID}
}

func (m *HeapMerger) run(ctx context.Context) error {
	h := &recordHeap{}
	heap.Init(h)

	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	timerActive := false
	var batchStart time.Time
	var lastEmittedTs time.Time

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

		// Pop the whole heap in sorted order. batch retains the heapItems so we can ack each submitter
		// (and feed the out-of-order metric) after the push; records is the flat view the consumer wants.
		batch := make([]heapItem, h.Len())
		records := make([]*kgo.Record, len(batch))
		for i := range batch {
			batch[i] = heap.Pop(h).(heapItem)
			records[i] = batch[i].record
		}

		// A non-cancellable context lets a shutdown mid-flush complete the batch rather than abort it,
		// matching SingleClusterPartitionReader's downstream path.
		err := m.consumerFactory.consumer().Consume(context.WithoutCancel(ctx), slices.Values(records))
		if err != nil {
			level.Warn(m.logger).Log("msg", "downstream consumer returned error from merged batch", "records", len(batch), "err", err)
		} else if m.metrics != nil {
			// Only count successful emissions; failures will be retried by the upstream readers.
			for _, item := range batch {
				ts := item.record.Timestamp
				if !lastEmittedTs.IsZero() && ts.Before(lastEmittedTs) {
					m.metrics.outOfOrderEmits.Inc()
				}
				if ts.After(lastEmittedTs) {
					lastEmittedTs = ts
				}
			}
		}

		if m.metrics != nil {
			m.metrics.batchFlushLatency.Observe(time.Since(batchStart).Seconds())
		}
		batchStart = time.Time{}

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
			if h.Len() == 0 {
				batchStart = time.Now()
			}
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

// drain acks any remaining heap entries and any records still queued in the input channel with the
// supplied error. Used at shutdown so that submitters never block forever on an ack.
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

// submittingConsumer is a RecordConsumer that submits each record into the shared HeapMerger and blocks
// until the merger has forwarded all of them.
type submittingConsumer struct {
	merger         *HeapMerger
	kafkaClusterID int
}

func (sc *submittingConsumer) Consume(ctx context.Context, records iter.Seq[*kgo.Record]) error {
	batch := slices.Collect(records)
	if len(batch) == 0 {
		return nil
	}

	ackCh := make(chan error, len(batch))
	for _, r := range batch {
		select {
		case sc.merger.input <- heapItem{record: r, kafkaClusterID: sc.kafkaClusterID, ackCh: ackCh}:
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
