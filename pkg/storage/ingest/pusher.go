// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/mimirpb"
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
	logger                log.Logger
}

type parsedRecord struct {
	*mimirpb.WriteRequest
	// Context holds the tracing and cancellation data for this record/request.
	ctx      context.Context
	tenantID string
	err      error
}

func newPusherConsumer(p Pusher, reg prometheus.Registerer, l log.Logger) *pusherConsumer {
	errRequestsCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingest_storage_reader_records_failed_total",
		Help: "Number of records (write requests) which caused errors while processing. Client errors are errors such as tenant limits and samples out of bounds. Server errors indicate internal recoverable errors.",
	}, []string{"cause"})

	return &pusherConsumer{
		pusher: p,
		logger: l,
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

func (c pusherConsumer) consume(ctx context.Context, records []record) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(cancellation.NewErrorf("done consuming records"))

	recC := make(chan parsedRecord)

	// Speed up consumption by unmarhsalling the next request while the previous one is being pushed.
	go c.unmarshalRequests(ctx, records, recC)
	return c.pushRequests(recC)
}

func (c pusherConsumer) pushRequests(reqC <-chan parsedRecord) error {
	recordIdx := -1
	for wr := range reqC {
		recordIdx++
		if wr.err != nil {
			level.Error(c.logger).Log("msg", "failed to parse write request; skipping", "err", wr.err)
			continue
		}

		err := c.pushToStorage(wr.ctx, wr.tenantID, wr.WriteRequest)
		if err != nil {
			return fmt.Errorf("consuming record at index %d for tenant %s: %w", recordIdx, wr.tenantID, err)
		}
	}
	return nil
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
		if keep, reason := shouldLog(ctx, err); keep {
			if reason != "" {
				err = fmt.Errorf("%w (%s)", err, reason)
			}
			level.Warn(spanLog).Log("msg", "detected a client error while ingesting write request (the request may have been partially ingested)", "err", err, "user", tenantID)
		}
	}
	return nil
}

// The passed context is expected to be cancelled after all items in records were fully processed and are ready
// to be released. This so to guaranty the release of resources associated with each parsedRecord context.
func (c pusherConsumer) unmarshalRequests(ctx context.Context, records []record, recC chan<- parsedRecord) {
	defer close(recC)
	done := ctx.Done()

	for _, rec := range records {
		// rec.ctx contains the tracing baggage for this record, which we propagate down the call tree.
		// Since rec.ctx cancellation is disjointed from the context passed to unmarshalRequests(), the context.AfterFunc below,
		// fuses the two lifetimes together.
		recCtx, cancelRecCtx := context.WithCancelCause(rec.ctx)
		context.AfterFunc(ctx, func() {
			cancelRecCtx(context.Cause(ctx))
		})
		pRecord := parsedRecord{
			ctx:          recCtx,
			tenantID:     rec.tenantID,
			WriteRequest: &mimirpb.WriteRequest{},
		}
		// We don't free the WriteRequest slices because they are being freed by the Pusher.
		err := pRecord.WriteRequest.Unmarshal(rec.content)
		if err != nil {
			pRecord.err = fmt.Errorf("parsing ingest consumer write request: %w", err)
		}
		select {
		case <-done:
			return
		case recC <- pRecord:
		}
	}
}
