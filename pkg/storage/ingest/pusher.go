// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const shardForSeriesBuffer = 2000 // TODO dimitarvdimitrov 2000 is arbitrary; the idea is that we don't block the goroutine calling PushToStorage while we're flushing. A linked list with a sync.Cond or something different would also work

type Pusher interface {
	PushToStorage(context.Context, *mimirpb.WriteRequest) error
}

type PusherCloser interface {
	PushToStorage(context.Context, *mimirpb.WriteRequest) error
	// Calls to close are safe and will not be called concurrenctly.
	Close() []error
}

// pusherConsumer receivers records from Kafka and pushes them to the storage.
// Each time a batch of records is received from Kafka, we instantiate a new pusherConsumer, this is to ensure we can retry if necessary and know whether we have completed that batch or not.
type pusherConsumer struct {
	fallbackClientErrSampler *util_log.Sampler
	metrics                  *pusherConsumerMetrics
	logger                   log.Logger

	kafkaConfig KafkaConfig

	pusher Pusher
}

type pusherConsumerMetrics struct {
	numTimeSeriesPerFlush prometheus.Histogram
	processingTimeSeconds prometheus.Observer
	clientErrRequests     prometheus.Counter
	serverErrRequests     prometheus.Counter
	totalRequests         prometheus.Counter
}

// newPusherConsumerMetrics creates a new pusherConsumerMetrics instance.
func newPusherConsumerMetrics(reg prometheus.Registerer) *pusherConsumerMetrics {
	errRequestsCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingest_storage_reader_records_failed_total",
		Help: "Number of records (write requests) which caused errors while processing. Client errors are errors such as tenant limits and samples out of bounds. Server errors indicate internal recoverable errors.",
	}, []string{"cause"})

	return &pusherConsumerMetrics{
		numTimeSeriesPerFlush: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                        "cortex_ingester_pusher_num_timeseries_per_flush",
			Help:                        "Number of time series per flush",
			NativeHistogramBucketFactor: 1.1,
		}),
		processingTimeSeconds: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "cortex_ingest_storage_reader_processing_time_seconds",
			Help:                            "Time taken to process a single record (write request).",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}),
		clientErrRequests: errRequestsCounter.WithLabelValues("client"),
		serverErrRequests: errRequestsCounter.WithLabelValues("server"),
		totalRequests: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_records_total",
			Help: "Number of attempted records (write requests).",
		}),
	}
}

// newPusherConsumer creates a new pusherConsumer instance.
func newPusherConsumer(pusher Pusher, kafkaCfg KafkaConfig, metrics *pusherConsumerMetrics, logger log.Logger) *pusherConsumer {
	return &pusherConsumer{
		pusher:                   pusher,
		kafkaConfig:              kafkaCfg,
		metrics:                  metrics,
		logger:                   logger,
		fallbackClientErrSampler: util_log.NewSampler(kafkaCfg.FallbackClientErrorSampleRate),
	}
}

// Consume implements the recordConsumer interface.
// It'll use a separate goroutine to unmarshal the next record while we push the current record to storage.
func (c pusherConsumer) Consume(ctx context.Context, records []record) error {
	type parsedRecord struct {
		*mimirpb.WriteRequest
		// ctx holds the tracing baggage for this record/request.
		ctx      context.Context
		tenantID string
		err      error
		index    int
	}

	recordsChannel := make(chan parsedRecord)

	// Create a cancellable context to let the unmarshalling goroutine know when to stop.
	ctx, cancel := context.WithCancelCause(ctx)

	// Now, unmarshal the records into the channel.
	go func(unmarshalCtx context.Context, records []record, ch chan<- parsedRecord) {
		defer close(ch)

		for index, r := range records {
			// Before we being unmarshalling the write request check if the context was cancelled.
			select {
			case <-unmarshalCtx.Done():
				// No more processing is needed, so we need to abort.
				return
			default:
			}

			parsed := parsedRecord{
				ctx:          r.ctx,
				tenantID:     r.tenantID,
				WriteRequest: &mimirpb.WriteRequest{},
				index:        index,
			}

			// We don't free the WriteRequest slices because they are being freed by a level below.
			err := parsed.WriteRequest.Unmarshal(r.content)
			if err != nil {
				parsed.err = fmt.Errorf("parsing ingest consumer write request: %w", err)
			}

			// Now that we're done, check again before we send it to the channel.
			select {
			case <-unmarshalCtx.Done():
				return
			case ch <- parsed:
			}
		}
	}(ctx, records, recordsChannel)

	writer := c.newStorageWriter()
	for r := range recordsChannel {
		if r.err != nil {
			level.Error(c.logger).Log("msg", "failed to parse write request; skipping", "err", r.err)
			continue
		}

		// If we get an error at any point, we need to stop processing the records. They will be retried at some point.
		err := c.pushToStorage(r.ctx, r.tenantID, r.WriteRequest, writer)
		if err != nil {
			cancel(cancellation.NewErrorf("error while pushing to storage")) // Stop the unmarshalling goroutine.
			return fmt.Errorf("consuming record at index %d for tenant %s: %w", r.index, r.tenantID, err)
		}
	}

	cancel(cancellation.NewErrorf("done unmarshalling records"))

	// We need to tell the storage writer that we're done and no more records are coming.
	// err := c.close(ctx, writer)
	spanLog := spanlogger.FromContext(ctx, log.NewNopLogger())
	errs := writer.Close()
	for eIdx := 0; eIdx < len(errs); eIdx++ {
		err := errs[eIdx]
		isServerErr := c.handlePushErr(ctx, "TODO", err, spanLog)
		if !isServerErr {
			errs[len(errs)-1], errs[eIdx] = errs[eIdx], errs[len(errs)-1]
			errs = errs[:len(errs)-1]
			eIdx--
		}
	}

	return multierror.New(errs...).Err()
}

func (c pusherConsumer) newStorageWriter() PusherCloser {
	if c.kafkaConfig.ReplayShards == 0 {
		return newSequentialStoragePusher(c.metrics, c.pusher)
	}

	return newParallelStoragePusher(c.metrics, c.pusher, c.kafkaConfig.ReplayShards, c.kafkaConfig.BatchSize, c.logger)
}

func (c pusherConsumer) pushToStorage(ctx context.Context, tenantID string, req *mimirpb.WriteRequest, writer PusherCloser) error {
	spanLog, ctx := spanlogger.NewWithLogger(ctx, c.logger, "pusherConsumer.pushToStorage")
	defer spanLog.Finish()

	processingStart := time.Now()

	// Note that the implementation of the Pusher expects the tenantID to be in the context.
	ctx = user.InjectOrgID(ctx, tenantID)
	err := writer.PushToStorage(ctx, req)

	// TODO dimitarvdimitrov processing time is flawed because it's only counting enqueuing time, not processing time.
	c.metrics.processingTimeSeconds.Observe(time.Since(processingStart).Seconds())
	c.metrics.totalRequests.Inc()

	isServerErr := c.handlePushErr(ctx, tenantID, err, spanLog)
	if isServerErr {
		return err
	}
	return nil
}

func (c pusherConsumer) handlePushErr(ctx context.Context, tenantID string, err error, spanLog *spanlogger.SpanLogger) bool {
	if err == nil {
		return false
	}
	// Only return non-client errors; these will stop the processing of the current Kafka fetches and retry (possibly).
	if !mimirpb.IsClientError(err) {
		c.metrics.serverErrRequests.Inc()
		_ = spanLog.Error(err)
		return true
	}

	c.metrics.clientErrRequests.Inc()

	// The error could be sampled or marked to be skipped in logs, so we check whether it should be
	// logged before doing it.
	if keep, reason := c.shouldLogClientError(ctx, err); keep {
		if reason != "" {
			err = fmt.Errorf("%w (%s)", err, reason)
		}
		// This error message is consistent with error message in Prometheus remote-write and OTLP handlers in distributors.
		level.Warn(spanLog).Log("msg", "detected a client error while ingesting write request (the request may have been partially ingested)", "user", tenantID, "insight", true, "err", err)
	}
	return false
}

// shouldLogClientError returns whether err should be logged.
func (c pusherConsumer) shouldLogClientError(ctx context.Context, err error) (bool, string) {
	var optional middleware.OptionalLogging
	if !errors.As(err, &optional) {
		// If error isn't sampled yet, we wrap it into our sampler and try again.
		err = c.fallbackClientErrSampler.WrapError(err)
		if !errors.As(err, &optional) {
			// We can get here if c.clientErrSampler is nil.
			return true, ""
		}
	}

	return optional.ShouldLog(ctx)
}

// sequentialStoragePusher receives mimirpb.WriteRequest which are then pushed to the storage one by one.
type sequentialStoragePusher struct {
	metrics *pusherConsumerMetrics

	pusher Pusher
}

// newSequentialStoragePusher creates a new sequentialStoragePusher instance.
func newSequentialStoragePusher(metrics *pusherConsumerMetrics, pusher Pusher) sequentialStoragePusher {
	return sequentialStoragePusher{
		metrics: metrics,
		pusher:  pusher,
	}
}

// PushToStorage implements the PusherCloser interface.
func (ssp sequentialStoragePusher) PushToStorage(ctx context.Context, wr *mimirpb.WriteRequest) error {
	// TODO: What about time??
	ssp.metrics.numTimeSeriesPerFlush.Observe(float64(len(wr.Timeseries)))
	return ssp.pusher.PushToStorage(ctx, wr)
}

// Close implements the PusherCloser interface.
func (ssp sequentialStoragePusher) Close() []error {
	return nil
}

// parallelStoragePusher receives WriteRequest which are then pushed to the storage in parallel.
// The parallelism is two-tiered which means that we first parallelize by tenantID and then by series.
type parallelStoragePusher struct {
	metrics *pusherConsumerMetrics
	logger  log.Logger

	pushers        map[string]*parallelStorageShards
	upstreamPusher Pusher
	numShards      int
	batchSize      int
}

// newParallelStoragePusher creates a new parallelStoragePusher instance.
func newParallelStoragePusher(metrics *pusherConsumerMetrics, pusher Pusher, numShards int, batchSize int, logger log.Logger) *parallelStoragePusher {
	return &parallelStoragePusher{
		logger:         log.With(logger, "component", "parallel-storage-pusher"),
		pushers:        make(map[string]*parallelStorageShards),
		upstreamPusher: pusher,
		numShards:      numShards,
		batchSize:      batchSize,
		metrics:        metrics,
	}
}

// PushToStorage implements the PusherCloser interface.
func (c parallelStoragePusher) PushToStorage(ctx context.Context, wr *mimirpb.WriteRequest) error {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to extract tenant ID from context", "err", err)
	}

	shards := c.shardsFor(userID)
	return shards.ShardWriteRequest(ctx, wr)
}

// Close implements the PusherCloser interface.
func (c parallelStoragePusher) Close() []error {
	var errs multierror.MultiError
	for _, p := range c.pushers {
		errs.Add(p.Stop())
	}
	clear(c.pushers)
	return errs
}

// shardsFor returns the parallelStorageShards for the given userID. Once created the same shards are re-used for the same userID.
// We create a shard for each tenantID to parallelize the writes.
func (c parallelStoragePusher) shardsFor(userID string) *parallelStorageShards {
	if p := c.pushers[userID]; p != nil {
		return p
	}
	// Use the same hashing function that's used for stripes in the TSDB. That way we make use of the low-contention property of stripes.
	hashLabels := labels.Labels.Hash
	p := newParallelStorageShards(c.metrics.numTimeSeriesPerFlush, c.numShards, c.batchSize, shardForSeriesBuffer, c.upstreamPusher, hashLabels)
	c.pushers[userID] = p
	return p
}

type labelsHashFunc func(labels.Labels) uint64

// parallelStorageShards is a collection of shards that are used to parallelize the writes to the storage by series.
// Each series is hashed to a shard that contains its own BatchingQueue.
type parallelStorageShards struct {
	numTimeSeriesPerFlush prometheus.Histogram

	pusher     Pusher
	hashLabels labelsHashFunc

	numShards int
	batchSize int
	capacity  int

	wg     *sync.WaitGroup
	shards []*BatchingQueue
	errs   chan error
}

// FlushableWriteRequest is a WriteRequest that can be flushed to the storage. It represents the current batch of time series that are to be flushed.
type FlushableWriteRequest struct {
	*mimirpb.WriteRequest
	context.Context
}

// newParallelStorageShards creates a new parallelStorageShards instance.
func newParallelStorageShards(numTimeSeriesPerFlush prometheus.Histogram, numShards int, batchSize int, capacity int, pusher Pusher, hashLabels labelsHashFunc) *parallelStorageShards {
	p := &parallelStorageShards{
		numShards:             numShards,
		pusher:                pusher,
		hashLabels:            hashLabels,
		capacity:              capacity,
		numTimeSeriesPerFlush: numTimeSeriesPerFlush,
		batchSize:             batchSize,
		wg:                    &sync.WaitGroup{},
		errs:                  make(chan error, numShards),
	}

	p.start()

	return p
}

// ShardWriteRequest hashes each time series in the write requests and sends them to the appropriate shard which is then handled by the current BatchingQueue in that shard.
func (p *parallelStorageShards) ShardWriteRequest(ctx context.Context, request *mimirpb.WriteRequest) error {
	var (
		builder         labels.ScratchBuilder
		nonCopiedLabels labels.Labels
		errs            multierror.MultiError
	)

	for _, ts := range request.Timeseries {
		mimirpb.FromLabelAdaptersOverwriteLabels(&builder, ts.Labels, &nonCopiedLabels)
		shard := p.hashLabels(nonCopiedLabels) % uint64(p.numShards)

		// TODO: Add metrics to measure how long are items sitting in the queue before they are flushed.
		p.shards[shard].AddToBatch(ctx, ts)

		// After each addition, check for any errors in the error channel
		select {
		case err := <-p.errs:
			errs.Add(err)
		default:
			// No errors, continue processing
		}
	}

	// ensure we drain the errors before closing the channels
	// TODO: I'm not entirely sure this is right. How can we be sure that we've drained all the errors?
	// I guess we'll know by the time we've hit Stop().
drain:
	for {
		select {
		case err := <-p.errs:
			errs.Add(err)
		default:
			// No more errors to drain, exit loop
			break drain
		}
	}

	// Return whatever errors we have now, we'll call stop eventually and collect the rest.
	// We might some data left in some of the queues in the shards, but they will be flushed eventually once Stop is called, and we're certain that no more data is coming.
	return errs.Err()
}

// Stop stops all the shards and waits for them to finish.
func (p *parallelStorageShards) Stop() error {
	for _, shard := range p.shards {
		shard.Close()
	}

	p.wg.Wait()

	close(p.errs)

	var errs multierror.MultiError
	for err := range p.errs {
		errs.Add(err)
	}

	return errs.Err()
}

// start starts the shards, each in its own goroutine.
func (p *parallelStorageShards) start() {
	shards := make([]*BatchingQueue, p.numShards)
	p.wg.Add(p.numShards)

	for i := range shards {
		shards[i] = NewBatchingQueue(p.capacity, p.batchSize)
		go p.run(shards[i])
	}

	p.shards = shards
}

// run runs the BatchingQueue for the shard.
func (p *parallelStorageShards) run(queue *BatchingQueue) {
	defer p.wg.Done()
	for wr := range queue.Channel() {
		p.numTimeSeriesPerFlush.Observe(float64(len(wr.WriteRequest.Timeseries)))
		err := p.pusher.PushToStorage(wr.Context, wr.WriteRequest)
		if err != nil {
			p.errs <- err
		}
	}
}

// BatchingQueue is a queue that batches the incoming time series according to the batch size.
// Once the batch size is reached, the batch is pushed to a channel which can be accessed through the Channel() method.
type BatchingQueue struct {
	ch           chan FlushableWriteRequest
	currentBatch FlushableWriteRequest
	batchSize    int
}

// NewBatchingQueue creates a new BatchingQueue instance.
func NewBatchingQueue(capacity int, batchSize int) *BatchingQueue {
	return &BatchingQueue{
		ch:           make(chan FlushableWriteRequest, capacity),
		currentBatch: FlushableWriteRequest{WriteRequest: &mimirpb.WriteRequest{Timeseries: mimirpb.PreallocTimeseriesSliceFromPool()}},
		batchSize:    batchSize,
	}
}

// AddToBatch adds a time series to the current batch. If the batch size is reached, the batch is pushed to the Channel().
func (q *BatchingQueue) AddToBatch(ctx context.Context, ts mimirpb.PreallocTimeseries) {
	s := &q.currentBatch
	s.Timeseries = append(s.Timeseries, ts)
	s.Context = ctx

	if len(s.Timeseries) >= q.batchSize {
		q.push(*s)
		q.currentBatch = FlushableWriteRequest{
			WriteRequest: &mimirpb.WriteRequest{Timeseries: mimirpb.PreallocTimeseriesSliceFromPool()},
		}
	}
}

// Close closes the BatchingQueue, it'll push the current branch to the channel if it's not empty.
// and then close the channel.
func (q *BatchingQueue) Close() {
	if len(q.currentBatch.Timeseries) > 0 {
		q.push(q.currentBatch)
	}

	close(q.ch)
}

// Channel returns the channel where the batches are pushed.
func (q *BatchingQueue) Channel() <-chan FlushableWriteRequest {
	return q.ch
}

func (q *BatchingQueue) push(fwr FlushableWriteRequest) {
	q.ch <- fwr
}
