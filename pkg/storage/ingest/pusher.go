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
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// batchingQueueCapacity controls how many batches can be enqueued for flushing.
// We don't want to push any batches in parallel and instead want to prepare the next one while the current one finishes, hence the buffer of 1.
// For example, if we flush 1 batch/sec, then batching 2 batches/sec doesn't make us faster.
// This is our initial assumption, and there's potential in testing with higher numbers if there's a high variability in flush times - assuming we can preserve the order of the batches. For now, we'll stick to 5.
// If there's high variability in the time to flush or in the time to batch, then this buffer might need to be increased.
const batchingQueueCapacity = 5

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
	defer func(processingStart time.Time) {
		c.metrics.processingTimeSeconds.Observe(time.Since(processingStart).Seconds())
	}(time.Now())

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
			level.Error(spanlogger.FromContext(ctx, c.logger)).Log("msg", "failed to parse write request; skipping", "err", r.err)
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
	if c.kafkaConfig.IngestionConcurrency == 0 {
		return newSequentialStoragePusher(c.metrics.storagePusherMetrics, c.pusher)
	}

	return newParallelStoragePusher(c.metrics.storagePusherMetrics, c.pusher, c.kafkaConfig.IngestionConcurrency, c.kafkaConfig.IngestionConcurrencyBatchSize, c.logger)
}

func (c pusherConsumer) pushToStorage(ctx context.Context, tenantID string, req *mimirpb.WriteRequest, writer PusherCloser) error {
	spanLog, ctx := spanlogger.NewWithLogger(ctx, c.logger, "pusherConsumer.pushToStorage")
	defer spanLog.Finish()

	// Note that the implementation of the Pusher expects the tenantID to be in the context.
	ctx = user.InjectOrgID(ctx, tenantID)
	err := writer.PushToStorage(ctx, req)

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
	metrics *storagePusherMetrics

	pusher Pusher
}

// newSequentialStoragePusher creates a new sequentialStoragePusher instance.
func newSequentialStoragePusher(metrics *storagePusherMetrics, pusher Pusher) sequentialStoragePusher {
	return sequentialStoragePusher{
		metrics: metrics,
		pusher:  pusher,
	}
}

// PushToStorage implements the PusherCloser interface.
func (ssp sequentialStoragePusher) PushToStorage(ctx context.Context, wr *mimirpb.WriteRequest) error {
	ssp.metrics.timeSeriesPerFlush.Observe(float64(len(wr.Timeseries)))
	defer func(now time.Time) {
		ssp.metrics.processingTime.WithLabelValues(requestContents(wr)).Observe(time.Since(now).Seconds())
	}(time.Now())

	return ssp.pusher.PushToStorage(ctx, wr)
}

// Close implements the PusherCloser interface.
func (ssp sequentialStoragePusher) Close() []error {
	return nil
}

// parallelStoragePusher receives WriteRequest which are then pushed to the storage in parallel.
// The parallelism is two-tiered which means that we first parallelize by tenantID and then by series.
type parallelStoragePusher struct {
	metrics *storagePusherMetrics
	logger  log.Logger

	// pushers is map["$tenant|$source"]*parallelStorageShards
	pushers        map[string]*parallelStorageShards
	upstreamPusher Pusher
	numShards      int
	batchSize      int
}

// newParallelStoragePusher creates a new parallelStoragePusher instance.
func newParallelStoragePusher(metrics *storagePusherMetrics, pusher Pusher, numShards int, batchSize int, logger log.Logger) *parallelStoragePusher {
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

	shards := c.shardsFor(userID, wr.Source)
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
func (c parallelStoragePusher) shardsFor(userID string, requestSource mimirpb.WriteRequest_SourceEnum) *parallelStorageShards {
	// Construct the string inline so that it doesn't escape to the heap. Go doesn't escape strings that are used to only look up map keys.
	// We can use "|" because that cannot be part of a tenantID in Mimir.
	if p := c.pushers[userID+"|"+requestSource.String()]; p != nil {
		return p
	}
	// Use the same hashing function that's used for stripes in the TSDB. That way we make use of the low-contention property of stripes.
	hashLabels := labels.Labels.Hash
	p := newParallelStorageShards(c.metrics, c.numShards, c.batchSize, batchingQueueCapacity, c.upstreamPusher, hashLabels)
	c.pushers[userID+"|"+requestSource.String()] = p
	return p
}

type labelsHashFunc func(labels.Labels) uint64

// parallelStorageShards is a collection of shards that are used to parallelize the writes to the storage by series.
// Each series is hashed to a shard that contains its own batchingQueue.
type parallelStorageShards struct {
	metrics *storagePusherMetrics

	pusher     Pusher
	hashLabels labelsHashFunc

	numShards int
	batchSize int
	capacity  int

	wg     *sync.WaitGroup
	shards []*batchingQueue
}

type flushableWriteRequest struct {
	// startedAt is the time when the first item was added to this request (timeseries or metadata).
	startedAt time.Time
	*mimirpb.WriteRequest
	context.Context
}

// newParallelStorageShards creates a new parallelStorageShards instance.
func newParallelStorageShards(metrics *storagePusherMetrics, numShards int, batchSize int, capacity int, pusher Pusher, hashLabels labelsHashFunc) *parallelStorageShards {
	p := &parallelStorageShards{
		numShards:  numShards,
		pusher:     pusher,
		hashLabels: hashLabels,
		capacity:   capacity,
		metrics:    metrics,
		batchSize:  batchSize,
		wg:         &sync.WaitGroup{},
	}

	p.start()

	return p
}

// ShardWriteRequest hashes each time series in the write requests and sends them to the appropriate shard which is then handled by the current batchingQueue in that shard.
// ShardWriteRequest ignores SkipLabelNameValidation because that field is only used in the distributor and not in the ingester.
func (p *parallelStorageShards) ShardWriteRequest(ctx context.Context, request *mimirpb.WriteRequest) error {
	var (
		builder         labels.ScratchBuilder
		nonCopiedLabels labels.Labels
		errs            multierror.MultiError
	)

	for _, ts := range request.Timeseries {
		mimirpb.FromLabelAdaptersOverwriteLabels(&builder, ts.Labels, &nonCopiedLabels)
		shard := p.hashLabels(nonCopiedLabels) % uint64(p.numShards)

		if err := p.shards[shard].AddToBatch(ctx, request.Source, ts); err != nil {
			// TODO: Technically, we should determine at this point what type of error it is and abort the whole push if it's a server error.
			// We'll do that in the next PR as otherwise it's too many changes right now.
			if !mimirpb.IsClientError(err) {
				return err
			}

			errs.Add(err)
		}
	}

	// Push metadata to every shard in a round-robin fashion.
	shard := 0
	for mdIdx := range request.Metadata {
		if err := p.shards[shard].AddMetadataToBatch(ctx, request.Source, request.Metadata[mdIdx]); err != nil {
			// TODO: Technically, we should determine at this point what type of error it is and abort the whole push if it's a server error.
			// We'll do that in the next PR as otherwise it's too many changes right now.
			if !mimirpb.IsClientError(err) {
				return err
			}

			errs.Add(err)
		}
		shard++
		shard %= p.numShards
	}

	// We might some data left in some of the queues in the shards, but they will be flushed eventually once Stop is called, and we're certain that no more data is coming.
	// Return whatever errors we have now, we'll call stop eventually and collect the rest.
	return errs.Err()
}

// Stop stops all the shards and waits for them to finish.
func (p *parallelStorageShards) Stop() error {
	var errs multierror.MultiError

	for _, shard := range p.shards {
		errs.Add(shard.Close())
	}

	p.wg.Wait()

	return errs.Err()
}

// start starts the shards, each in its own goroutine.
func (p *parallelStorageShards) start() {
	shards := make([]*batchingQueue, p.numShards)
	p.wg.Add(p.numShards)

	for i := range shards {
		shards[i] = newBatchingQueue(p.capacity, p.batchSize, p.metrics.batchingQueueMetrics)
		go p.run(shards[i])
	}

	p.shards = shards
}

// run runs the batchingQueue for the shard.
func (p *parallelStorageShards) run(queue *batchingQueue) {
	defer p.wg.Done()
	defer queue.Done()

	for wr := range queue.Channel() {
		p.metrics.batchAge.Observe(time.Since(wr.startedAt).Seconds())
		p.metrics.timeSeriesPerFlush.Observe(float64(len(wr.WriteRequest.Timeseries)))
		processingStart := time.Now()

		err := p.pusher.PushToStorage(wr.Context, wr.WriteRequest)

		p.metrics.processingTime.WithLabelValues(requestContents(wr.WriteRequest)).Observe(time.Since(processingStart).Seconds())
		if err != nil {
			queue.ErrorChannel() <- err
		}
	}
}

func requestContents(request *mimirpb.WriteRequest) string {
	switch {
	case len(request.Timeseries) > 0 && len(request.Metadata) > 0:
		return "timeseries_and_metadata"
	case len(request.Timeseries) > 0:
		return "timeseries"
	case len(request.Metadata) > 0:
		return "metadata"
	default:
		// This would be a bug, but at least we'd know.
		return "empty"
	}
}

// batchingQueue is a queue that batches the incoming time series according to the batch size.
// Once the batch size is reached, the batch is pushed to a channel which can be accessed through the Channel() method.
type batchingQueue struct {
	metrics *batchingQueueMetrics

	ch    chan flushableWriteRequest
	errCh chan error
	done  chan struct{}

	currentBatch flushableWriteRequest
	batchSize    int
}

// newBatchingQueue creates a new batchingQueue instance.
func newBatchingQueue(capacity int, batchSize int, metrics *batchingQueueMetrics) *batchingQueue {
	return &batchingQueue{
		metrics:      metrics,
		ch:           make(chan flushableWriteRequest, capacity),
		errCh:        make(chan error, capacity+1), // We check errs before pushing to the channel, so we need to have a buffer of at least capacity+1 so that the consumer can push all of its errors and not rely on the producer to unblock it.
		done:         make(chan struct{}),
		currentBatch: flushableWriteRequest{WriteRequest: &mimirpb.WriteRequest{Timeseries: mimirpb.PreallocTimeseriesSliceFromPool()}},
		batchSize:    batchSize,
	}
}

// AddToBatch adds a time series to the current batch. If the batch size is reached, the batch is pushed to the Channel().
// If an error occurs while pushing the batch, it returns the error and ensures the batch is pushed.
func (q *batchingQueue) AddToBatch(ctx context.Context, source mimirpb.WriteRequest_SourceEnum, ts mimirpb.PreallocTimeseries) error {
	if q.currentBatch.startedAt.IsZero() {
		q.currentBatch.startedAt = time.Now()
	}
	q.currentBatch.Timeseries = append(q.currentBatch.Timeseries, ts)
	q.currentBatch.Context = ctx
	q.currentBatch.Source = source

	return q.pushIfFull()
}

// AddMetadataToBatch adds metadata to the current batch.
func (q *batchingQueue) AddMetadataToBatch(ctx context.Context, source mimirpb.WriteRequest_SourceEnum, metadata *mimirpb.MetricMetadata) error {
	if q.currentBatch.startedAt.IsZero() {
		q.currentBatch.startedAt = time.Now()
	}
	q.currentBatch.Metadata = append(q.currentBatch.Metadata, metadata)
	q.currentBatch.Context = ctx
	q.currentBatch.Source = source

	return q.pushIfFull()
}

// Close closes the batchingQueue, it'll push the current branch to the channel if it's not empty.
// and then close the channel.
func (q *batchingQueue) Close() error {
	var errs multierror.MultiError
	if len(q.currentBatch.Timeseries)+len(q.currentBatch.Metadata) > 0 {
		if err := q.push(); err != nil {
			errs.Add(err)
		}
	}

	close(q.ch)
	<-q.done

	errs = append(errs, q.collectErrors()...)
	close(q.errCh)
	return errs.Err()
}

// Channel returns the channel where the batches are pushed.
func (q *batchingQueue) Channel() <-chan flushableWriteRequest {
	return q.ch
}

// ErrorChannel returns the channel where errors are pushed.
func (q *batchingQueue) ErrorChannel() chan<- error {
	return q.errCh
}

// Done signals the queue that there is no more data coming for both the channel and the error channel.
// It is necessary to ensure we don't close the channel before all the data is flushed.
func (q *batchingQueue) Done() {
	close(q.done)
}

func (q *batchingQueue) pushIfFull() error {
	if len(q.currentBatch.Metadata)+len(q.currentBatch.Timeseries) >= q.batchSize {
		return q.push()
	}
	return nil
}

// push pushes the current batch to the channel and resets the current batch.
// It also collects any errors that might have occurred while pushing the batch.
func (q *batchingQueue) push() error {
	errs := q.collectErrors()

	q.metrics.flushErrorsTotal.Add(float64(len(errs)))
	q.metrics.flushTotal.Inc()

	q.ch <- q.currentBatch
	q.resetCurrentBatch()

	return errs.Err()
}

// resetCurrentBatch resets the current batch to an empty state.
func (q *batchingQueue) resetCurrentBatch() {
	q.currentBatch = flushableWriteRequest{
		WriteRequest: &mimirpb.WriteRequest{Timeseries: mimirpb.PreallocTimeseriesSliceFromPool()},
	}
}

func (q *batchingQueue) collectErrors() multierror.MultiError {
	var errs multierror.MultiError

	for {
		select {
		case err := <-q.errCh:
			errs.Add(err)
		default:
			return errs
		}
	}
}
