// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"flag"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

const (
	l2FlushReasonTimeout   = "timeout"
	l2FlushReasonSizeLimit = "size_limit"
)

// L2Config holds configuration for the L2 distributor buffer.
type L2Config struct {
	BufferDuration time.Duration `yaml:"buffer_duration"`
	MaxBufferBytes int64         `yaml:"max_buffer_bytes"`
}

// RegisterFlagsWithPrefix adds flags for L2Config to f.
func (cfg *L2Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.BufferDuration, prefix+"buffer-duration", 200*time.Millisecond, "How long to buffer per-partition requests before flushing to Warpstream.")
	f.Int64Var(&cfg.MaxBufferBytes, prefix+"max-buffer-bytes", 6*1024*1024, "Maximum buffered bytes per partition before triggering an early flush.")
}

// l2BufferKey identifies a buffer by partition and tenant.
// A single Kafka partition may carry data for multiple tenants, so we buffer them separately.
type l2BufferKey struct {
	partitionID int32
	tenantID    string
}

// l2PendingRequest represents one in-flight PushToPartition call waiting to be flushed.
type l2PendingRequest struct {
	req  *mimirpb.WriteRequest
	done chan error // closed (with error or nil) when the flush containing this request completes
}

// l2PartitionBuffer holds pending requests for one (partition, tenant) pair.
type l2PartitionBuffer struct {
	mu        sync.Mutex
	pending   []*l2PendingRequest
	bytes     int
	lastWrite time.Time
}

// drain atomically takes all pending requests and resets the buffer state.
// Returns nil if the buffer was empty.
func (b *l2PartitionBuffer) drain() []*l2PendingRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.pending) == 0 {
		return nil
	}
	pending := b.pending
	b.pending = nil
	b.bytes = 0
	return pending
}

// L2Buffer buffers incoming PushToPartition requests by (partition, tenant) and flushes them
// as merged RW2 records to Warpstream. It implements services.Service so it can be managed
// as a subservice of the distributor.
type L2Buffer struct {
	services.Service

	cfg    L2Config
	writer *ingest.Writer
	logger log.Logger

	mu      sync.Mutex
	buffers map[l2BufferKey]*l2PartitionBuffer

	// metrics
	activePartitions  prometheus.Gauge
	requestsCoalesced prometheus.Histogram
	bytesBeforeMerge  prometheus.Counter
	bytesAfterMerge   prometheus.Counter
	flushTotal        *prometheus.CounterVec
}

// NewL2Buffer creates and returns an L2Buffer ready to be started as a subservice.
func NewL2Buffer(cfg L2Config, writer *ingest.Writer, reg prometheus.Registerer, logger log.Logger) *L2Buffer {
	b := &L2Buffer{
		cfg:     cfg,
		writer:  writer,
		logger:  logger,
		buffers: make(map[l2BufferKey]*l2PartitionBuffer),

		activePartitions: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_distributor_l2_active_partitions",
			Help: "Number of (partition, tenant) pairs currently buffered in this L2 distributor.",
		}),
		requestsCoalesced: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_distributor_l2_requests_coalesced",
			Help:    "Number of PushToPartition requests merged into a single flush.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10),
		}),
		bytesBeforeMerge: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_distributor_l2_bytes_before_merge_total",
			Help: "Total bytes of input requests before symbol-table merging.",
		}),
		bytesAfterMerge: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_distributor_l2_bytes_after_merge_total",
			Help: "Total bytes of merged requests after symbol-table merging.",
		}),
		flushTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_l2_flush_total",
			Help: "Total number of partition flushes, labelled by reason.",
		}, []string{"reason"}),
	}

	// Pre-create label combinations so they appear in metrics from startup.
	b.flushTotal.WithLabelValues(l2FlushReasonTimeout)
	b.flushTotal.WithLabelValues(l2FlushReasonSizeLimit)

	b.Service = services.NewBasicService(nil, b.running, b.stopping)
	return b
}

// Push enqueues req into the buffer for (partitionID, tenant-from-ctx) and blocks until the
// flush that includes this request completes (or the context is cancelled).
func (b *L2Buffer) Push(ctx context.Context, partitionID int32, req *mimirpb.WriteRequest) error {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}

	key := l2BufferKey{partitionID: partitionID, tenantID: tenantID}

	b.mu.Lock()
	buf, ok := b.buffers[key]
	if !ok {
		buf = &l2PartitionBuffer{}
		b.buffers[key] = buf
		b.activePartitions.Inc()
	}
	b.mu.Unlock()

	pending := &l2PendingRequest{
		req:  req,
		done: make(chan error, 1),
	}

	buf.mu.Lock()
	buf.pending = append(buf.pending, pending)
	buf.bytes += req.Size()
	buf.lastWrite = time.Now()
	triggerFlush := buf.bytes >= int(b.cfg.MaxBufferBytes)
	buf.mu.Unlock()

	if triggerFlush {
		go b.flushBuffer(context.Background(), key, buf, l2FlushReasonSizeLimit)
	}

	select {
	case err := <-pending.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// running is the service loop: ticks at half the buffer duration to ensure the max wait is
// no more than one full BufferDuration.
func (b *L2Buffer) running(ctx context.Context) error {
	if b.cfg.BufferDuration <= 0 {
		<-ctx.Done()
		return nil
	}

	ticker := time.NewTicker(b.cfg.BufferDuration / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			b.flushExpiredAndEvict(ctx)
		}
	}
}

// stopping flushes all remaining buffers synchronously before the service exits.
func (b *L2Buffer) stopping(_ error) error {
	b.mu.Lock()
	keys := make([]l2BufferKey, 0, len(b.buffers))
	bufs := make([]*l2PartitionBuffer, 0, len(b.buffers))
	for k, buf := range b.buffers {
		keys = append(keys, k)
		bufs = append(bufs, buf)
	}
	b.mu.Unlock()

	var wg sync.WaitGroup
	for i, buf := range bufs {
		if pending := buf.drain(); len(pending) > 0 {
			wg.Add(1)
			go func(key l2BufferKey, pending []*l2PendingRequest) {
				defer wg.Done()
				b.flush(context.Background(), key, pending, l2FlushReasonTimeout)
			}(keys[i], pending)
		}
	}
	wg.Wait()
	return nil
}

// flushExpiredAndEvict flushes buffers that have exceeded the buffer duration or byte limit,
// and evicts buffers that have been idle for more than 2x the buffer duration.
func (b *L2Buffer) flushExpiredAndEvict(ctx context.Context) {
	now := time.Now()

	b.mu.Lock()
	type candidate struct {
		key l2BufferKey
		buf *l2PartitionBuffer
	}
	var toFlush []candidate
	var toEvict []l2BufferKey

	for key, buf := range b.buffers {
		buf.mu.Lock()
		hasPending := len(buf.pending) > 0
		age := now.Sub(buf.lastWrite)
		exceedsSize := buf.bytes >= int(b.cfg.MaxBufferBytes)
		buf.mu.Unlock()

		switch {
		case hasPending && (age >= b.cfg.BufferDuration || exceedsSize):
			toFlush = append(toFlush, candidate{key, buf})
		case !hasPending && buf.lastWrite != (time.Time{}) && age > 2*b.cfg.BufferDuration:
			toEvict = append(toEvict, key)
		}
	}

	for _, key := range toEvict {
		delete(b.buffers, key)
		b.activePartitions.Dec()
	}
	b.mu.Unlock()

	for _, c := range toFlush {
		go b.flushBuffer(ctx, c.key, c.buf, l2FlushReasonTimeout)
	}
}

// flushBuffer drains buf and flushes the collected requests.
func (b *L2Buffer) flushBuffer(ctx context.Context, key l2BufferKey, buf *l2PartitionBuffer, reason string) {
	pending := buf.drain()
	if len(pending) == 0 {
		return
	}
	b.flush(ctx, key, pending, reason)
}

// flush merges pending requests into a single RW2 write and sends it to Warpstream,
// then signals all callers.
func (b *L2Buffer) flush(ctx context.Context, key l2BufferKey, pending []*l2PendingRequest, reason string) {
	reqs := make([]*mimirpb.WriteRequest, len(pending))
	totalBytes := 0
	for i, p := range pending {
		reqs[i] = p.req
		totalBytes += p.req.Size()
	}

	b.requestsCoalesced.Observe(float64(len(pending)))
	b.bytesBeforeMerge.Add(float64(totalBytes))
	b.flushTotal.WithLabelValues(reason).Inc()

	merged, err := mergeRequests(reqs)
	if err != nil {
		level.Error(b.logger).Log("msg", "failed to merge L2 partition requests", "partition", key.partitionID, "tenant", key.tenantID, "err", err)
		signal(pending, err)
		return
	}

	b.bytesAfterMerge.Add(float64(merged.Size()))

	err = b.writer.WriteSync(ctx, key.partitionID, key.tenantID, merged)
	signal(pending, err)
}

// signal sends err to every pending request's done channel.
func signal(pending []*l2PendingRequest, err error) {
	for _, p := range pending {
		p.done <- err
	}
}
