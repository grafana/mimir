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

type Pusher interface {
	PushToStorage(context.Context, *mimirpb.WriteRequest) error
}

type PusherCloser interface {
	Pusher
	Close() []error
}

type pusherConsumer struct {
	fallbackClientErrSampler *util_log.Sampler
	metrics                  *pusherConsumerMetrics
	logger                   log.Logger

	pusher PusherCloser
}

type pusherConsumerMetrics struct {
	numTimeSeriesPerFlush prometheus.Histogram
	processingTimeSeconds prometheus.Observer
	clientErrRequests     prometheus.Counter
	serverErrRequests     prometheus.Counter
	totalRequests         prometheus.Counter
}

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

func newPusherConsumer(pusher Pusher, kafkaCfg KafkaConfig, metrics *pusherConsumerMetrics, logger log.Logger) *pusherConsumer {
	var p PusherCloser
	if kafkaCfg.ReplayShards == 0 {
		p = newNoopPusherCloser(metrics, pusher)
	} else {
		p = newMultiTenantPusher(metrics, pusher, kafkaCfg.ReplayShards, kafkaCfg.BatchSize)
	}

	return &pusherConsumer{
		metrics:                  metrics,
		logger:                   logger,
		fallbackClientErrSampler: util_log.NewSampler(kafkaCfg.FallbackClientErrorSampleRate),

		pusher: p,
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
	ctx, cancel := context.WithCancel(ctx)

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
			default:
				ch <- parsed
			}
		}
	}(ctx, records, recordsChannel)

	for r := range recordsChannel {
		if r.err != nil {
			level.Error(c.logger).Log("msg", "failed to parse write request; skipping", "err", r.err)
			continue
		}

		// If we get an error at any point, we need to stop processing the records. They will be retried at some point.
		err := c.pushToStorage(r.ctx, r.tenantID, r.WriteRequest)
		if err != nil {
			cancel()
			return fmt.Errorf("consuming record at index %d for tenant %s: %w", r.index, r.tenantID, err)
		}
	}

	cancel()
	return nil
}

func (c pusherConsumer) Close(ctx context.Context) error {
	spanLog := spanlogger.FromContext(ctx, log.NewNopLogger())
	errs := c.pusher.Close()
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

func (c pusherConsumer) pushToStorage(ctx context.Context, tenantID string, req *mimirpb.WriteRequest) error {
	spanLog, ctx := spanlogger.NewWithLogger(ctx, c.logger, "pusherConsumer.pushToStorage")
	defer spanLog.Finish()

	processingStart := time.Now()

	// Note that the implementation of the Pusher expects the tenantID to be in the context.
	ctx = user.InjectOrgID(ctx, tenantID)
	err := c.pusher.PushToStorage(ctx, req)

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

type multiTenantPusher struct {
	metrics *pusherConsumerMetrics

	pushers        map[string]*shardingPusher
	upstreamPusher Pusher
	numShards      int
	batchSize      int
}

func (c multiTenantPusher) PushToStorage(ctx context.Context, request *mimirpb.WriteRequest) error {
	user, _ := user.ExtractOrgID(ctx)
	return c.pusher(user).PushToStorage(ctx, request)
}

// TODO dimitarvdimitrov rename because this is multi-tenant sharding pusher
func newMultiTenantPusher(metrics *pusherConsumerMetrics, upstream Pusher, numShards int, batchSize int) *multiTenantPusher {
	return &multiTenantPusher{
		pushers:        make(map[string]*shardingPusher),
		upstreamPusher: upstream,
		numShards:      numShards,
		batchSize:      batchSize,
		metrics:        metrics,
	}
}

func (c multiTenantPusher) pusher(userID string) *shardingPusher {
	if p := c.pushers[userID]; p != nil {
		return p
	}
	const shardingPusherBuffer = 2000                                                                                         // TODO dimitarvdimitrov 2000 is arbitrary; the idea is that we don't block the goroutine calling PushToStorage while we're flushing. A linked list with a sync.Cond or something different would also work
	p := newShardingPusher(c.metrics.numTimeSeriesPerFlush, c.numShards, c.batchSize, shardingPusherBuffer, c.upstreamPusher) // TODO dimitarvdimitrov this ok or do we need to inject a factory here too?
	c.pushers[userID] = p
	return p
}

func (c multiTenantPusher) Close() []error {
	var errs multierror.MultiError
	for _, p := range c.pushers {
		errs.Add(p.close())
	}
	clear(c.pushers)
	return errs
}

type shardingPusher struct {
	numShards             int
	shards                []chan flushableWriteRequest
	unfilledShards        []flushableWriteRequest
	upstream              Pusher
	wg                    *sync.WaitGroup
	errs                  chan error
	batchSize             int
	numTimeSeriesPerFlush prometheus.Histogram
}

// TODO dimitarvdimitrov if this is expensive, consider having this long-lived and not Close()-ing and recreating it on every fetch, but instead calling something like Flush() on it.
func newShardingPusher(numTimeSeriesPerFlush prometheus.Histogram, numShards int, batchSize int, buffer int, upstream Pusher) *shardingPusher {
	pusher := &shardingPusher{
		numShards:             numShards,
		upstream:              upstream,
		numTimeSeriesPerFlush: numTimeSeriesPerFlush,
		batchSize:             batchSize,
		wg:                    &sync.WaitGroup{},
		errs:                  make(chan error, numShards),
		unfilledShards:        make([]flushableWriteRequest, numShards),
	}
	shards := make([]chan flushableWriteRequest, numShards)
	pusher.wg.Add(numShards)
	for i := range shards {
		pusher.unfilledShards[i].WriteRequest = &mimirpb.WriteRequest{Timeseries: mimirpb.PreallocTimeseriesSliceFromPool()}
		shards[i] = make(chan flushableWriteRequest, buffer)
		go pusher.runShard(shards[i])
	}
	go func() {
		pusher.wg.Wait()
		close(pusher.errs)
	}()

	pusher.shards = shards
	return pusher
}

func (p *shardingPusher) PushToStorage(ctx context.Context, request *mimirpb.WriteRequest) error {
	var (
		builder         labels.ScratchBuilder
		nonCopiedLabels labels.Labels
		errs            multierror.MultiError
	)
	for _, ts := range request.Timeseries {
		mimirpb.FromLabelAdaptersOverwriteLabels(&builder, ts.Labels, &nonCopiedLabels)
		shard := nonCopiedLabels.Hash() % uint64(p.numShards)

		s := p.unfilledShards[shard]
		// TODO dimitarvdimitrov support metadata and the rest of the fields; perhaps cut a new request for different values of SkipLabelNameValidation?
		s.Timeseries = append(s.Timeseries, ts)
		s.Context = ctx // retain the last context in case we have to flush it when closing shardingPusher

		if len(s.Timeseries) < p.batchSize {
			continue
		}
		p.unfilledShards[shard] = flushableWriteRequest{
			WriteRequest: &mimirpb.WriteRequest{Timeseries: mimirpb.PreallocTimeseriesSliceFromPool()},
		}

	tryPush:
		for {
			select {
			case p.shards[shard] <- s:
				break tryPush
			case err := <-p.errs:
				// we might have to first unblock the shard before we can flush to it
				errs.Add(err)
			}
		}

		// drain errors to avoid blocking the shard loop
	drainErrs:
		for {
			select {
			case err := <-p.errs:
				errs.Add(err)
			default:
				break drainErrs
			}
		}
	}
	return errs.Err()
}

type flushableWriteRequest struct {
	*mimirpb.WriteRequest
	context.Context
}

func (p *shardingPusher) runShard(toFlush chan flushableWriteRequest) {
	defer p.wg.Done()
	for wr := range toFlush {
		p.numTimeSeriesPerFlush.Observe(float64(len(wr.WriteRequest.Timeseries)))
		err := p.upstream.PushToStorage(wr.Context, wr.WriteRequest)
		if err != nil {
			p.errs <- err
		}
	}
}

func (p *shardingPusher) close() error {
	for shard, wr := range p.unfilledShards {
		if len(wr.Timeseries) > 0 {
			p.shards[shard] <- wr
		}
	}
	for _, shard := range p.shards {
		close(shard)
	}
	var errs multierror.MultiError
	for err := range p.errs {
		errs.Add(err)
	}
	return errs.Err()
}
