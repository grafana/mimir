// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"container/heap"
	"context"
	"flag"
	"iter"
	"slices"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
)

// HeapMergerMetrics holds Prometheus metrics for a HeapMerger.
type HeapMergerMetrics struct {
	bufferedRecords   prometheus.Gauge
	emittedRecords    *prometheus.CounterVec
	outOfOrderEmits   prometheus.Counter
	batchFlushLatency prometheus.Histogram
	submitterWaitTime *prometheus.HistogramVec
}

// NewHeapMergerMetrics creates and registers a HeapMergerMetrics on the supplied registerer.
func NewHeapMergerMetrics(reg prometheus.Registerer) *HeapMergerMetrics {
	factory := promauto.With(reg)
	return &HeapMergerMetrics{
		bufferedRecords: factory.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_ingest_storage_heap_merger_buffered_records",
			Help: "Current number of records buffered in the heap merger awaiting emission.",
		}),
		emittedRecords: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_heap_merger_emitted_records_total",
			Help: "Total number of records the heap merger has forwarded downstream, by source write compartment.",
		}, []string{"write_compartment"}),
		outOfOrderEmits: factory.NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_heap_merger_out_of_order_emissions_total",
			Help: "Total number of records emitted with a timestamp older than the most recently emitted record. This is the residual cross-VC OOO that the merger could not absorb.",
		}),
		batchFlushLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingest_storage_heap_merger_batch_flush_latency_seconds",
			Help:    "Elapsed time from the first record entering a batch to the batch finishing its downstream push.",
			Buckets: prometheus.DefBuckets,
		}),
		submitterWaitTime: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_ingest_storage_heap_merger_submitter_wait_seconds",
			Help:    "Time a per-VC submitting consumer spent waiting for its records to be acked by the merger.",
			Buckets: prometheus.DefBuckets,
		}, []string{"write_compartment"}),
	}
}

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
	MaxBatchRecords int `yaml:"max_batch_records"`

	// MaxBatchWait is the maximum time records sit in the heap before being
	// flushed even if MaxBatchRecords hasn't been reached. Keeps tail latency
	// bounded when traffic is sparse.
	MaxBatchWait time.Duration `yaml:"max_batch_wait"`

	// InputBufferSize is the buffer size of the channel into which submitting
	// consumers push records. Capped to avoid unbounded memory if the merger
	// stalls on a slow downstream consumer.
	InputBufferSize int `yaml:"input_buffer_size"`
}

// RegisterFlagsWithPrefix registers HeapMergerConfig flags under the given prefix.
func (cfg *HeapMergerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.IntVar(&cfg.MaxBatchRecords, prefix+"max-batch-records", 1024, "Soft upper bound on records buffered in the heap merger before forcing a flush. Larger values give the merger more cross-VC mixing at the cost of memory and per-flush latency.")
	f.DurationVar(&cfg.MaxBatchWait, prefix+"max-batch-wait", 50*time.Millisecond, "Maximum time records sit in the heap merger before being flushed even if max-batch-records has not been reached.")
	f.IntVar(&cfg.InputBufferSize, prefix+"input-buffer-size", 0, "Buffer size of the channel into which per-VC submitting consumers push records. When 0, defaults to 2x max-batch-records.")
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
	metrics         *HeapMergerMetrics
	logger          log.Logger
}

// NewHeapMerger constructs a HeapMerger that forwards merged batches via
// consumers produced by consumerFactory.
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
		} else if m.metrics != nil {
			// Only count successful emissions; failures will be retried by upstream readers.
			perVC := make(map[int]int, len(batch))
			for _, item := range batch {
				perVC[item.vcID]++
				ts := item.record.Timestamp
				if !lastEmittedTs.IsZero() && ts.Before(lastEmittedTs) {
					m.metrics.outOfOrderEmits.Inc()
				}
				if ts.After(lastEmittedTs) {
					lastEmittedTs = ts
				}
			}
			for vcID, count := range perVC {
				m.metrics.emittedRecords.WithLabelValues(strconv.Itoa(vcID)).Add(float64(count))
			}
		}

		if m.metrics != nil {
			m.metrics.batchFlushLatency.Observe(time.Since(batchStart).Seconds())
			m.metrics.bufferedRecords.Set(float64(h.Len()))
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
			if m.metrics != nil {
				m.metrics.bufferedRecords.Set(float64(h.Len()))
			}
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

	start := time.Now()
	defer func() {
		if sc.merger.metrics != nil {
			sc.merger.metrics.submitterWaitTime.WithLabelValues(strconv.Itoa(sc.vcID)).Observe(time.Since(start).Seconds())
		}
	}()

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
