// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/mimirpb"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type Pusher interface {
	PushToStorage(context.Context, *mimirpb.WriteRequest) error
}

type pusherConsumer struct {
	pusher Pusher

	processingTimeSeconds prometheus.Observer
	clientErrRequests     prometheus.Counter
	serverErrRequests     prometheus.Counter
	totalRequests         prometheus.Counter

	fallbackClientErrSampler *util_log.Sampler // Fallback log message sampler client errors that are not sampled yet.
	logger                   log.Logger
}

type parsedRecord struct {
	*mimirpb.WriteRequest
	// ctx holds the tracing baggage for this record/request.
	ctx      context.Context
	tenantID string
	err      error
	index    int
}

func newPusherConsumer(p Pusher, fallbackClientErrSampler *util_log.Sampler, reg prometheus.Registerer, l log.Logger) *pusherConsumer {
	errRequestsCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingest_storage_reader_records_failed_total",
		Help: "Number of records (write requests) which caused errors while processing. Client errors are errors such as tenant limits and samples out of bounds. Server errors indicate internal recoverable errors.",
	}, []string{"cause"})

	return &pusherConsumer{
		pusher:                   p,
		logger:                   l,
		fallbackClientErrSampler: fallbackClientErrSampler,
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
	recordsChannel := make(chan parsedRecord)

	// Create a cancellable context to let the unmarshalling goroutine know when to stop.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Now, unmarshal the records into the channel.
	go func(unmarshalCtx context.Context, records []record) {
		defer close(recordsChannel)

		for index, r := range records {
			select {
			case <-unmarshalCtx.Done():
				// No more processing is needed, so we need to abort.
				return
			default:
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
				recordsChannel <- parsed
			}
		}
	}(ctx, records)

	for r := range recordsChannel {
		if r.err != nil {
			level.Error(c.logger).Log("msg", "failed to parse write request; skipping", "err", r.err)
			continue
		}

		// If we get an error at any point, we need to stop processing the records. They will be retried at some point.
		err := c.pushToStorage(r.ctx, r.tenantID, r.WriteRequest)
		if err != nil {
			cancel() // Stop the unmarshalling goroutine.
			return fmt.Errorf("consuming record at index %d for tenant %s: %w", r.index, r.tenantID, err)
		}
	}

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

	c.processingTimeSeconds.Observe(time.Since(processingStart).Seconds())
	c.totalRequests.Inc()

	if err != nil {
		// Only return non-client errors; these will stop the processing of the current Kafka fetches and retry (possibly).
		if !mimirpb.IsClientError(err) {
			c.serverErrRequests.Inc()
			return spanLog.Error(err)
		}

		c.clientErrRequests.Inc()

		// The error could be sampled or marked to be skipped in logs, so we check whether it should be
		// logged before doing it.
		if keep, reason := c.shouldLogClientError(ctx, err); keep {
			if reason != "" {
				err = fmt.Errorf("%w (%s)", err, reason)
			}
			// This error message is consistent with error message in Prometheus remote-write and OTLP handlers in distributors.
			level.Warn(spanLog).Log("msg", "detected a client error while ingesting write request (the request may have been partially ingested)", "user", tenantID, "insight", true, "err", err)
		}
	}
	return nil
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
