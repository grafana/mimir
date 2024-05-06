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
	p Pusher

	processingTimeSeconds prometheus.Observer
	clientErrRequests     prometheus.Counter
	serverErrRequests     prometheus.Counter
	totalRequests         prometheus.Counter
	l                     log.Logger
}

type parsedRecord struct {
	*mimirpb.WriteRequest
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
		p: p,
		l: l,
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
	err := c.pushRequests(recC)
	if err != nil {
		return err
	}
	return nil
}

func (c pusherConsumer) pushRequests(reqC <-chan parsedRecord) error {
	recordIdx := -1
	for wr := range reqC {
		recordIdx++
		if wr.err != nil {
			level.Error(c.l).Log("msg", "failed to parse write request; skipping", "err", wr.err)
			continue
		}

		ctx := user.InjectOrgID(wr.ctx, wr.tenantID)
		err := c.pushToStorage(ctx, wr.WriteRequest)
		if err != nil {
			return fmt.Errorf("consuming record at index %d for tenant %s: %w", recordIdx, wr.tenantID, err)
		}
	}
	return nil
}

func (c pusherConsumer) pushToStorage(ctx context.Context, req *mimirpb.WriteRequest) error {
	spanLog, spanCtx := spanlogger.NewWithLogger(ctx, c.l, "pusherConsumer.pushToStorage")
	defer spanLog.Finish()

	processingStart := time.Now()

	err := c.p.PushToStorage(spanCtx, req)

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

			tenantID, _ := user.ExtractOrgID(spanCtx)
			level.Warn(spanLog).Log("msg", "detected a client error while ingesting write request (the request may have been partially ingested)", "err", err, "user", tenantID)
		}
	}
	return nil
}

func (c pusherConsumer) unmarshalRequests(ctx context.Context, records []record, recC chan<- parsedRecord) {
	defer close(recC)
	done := ctx.Done()

	for _, rec := range records {
		// rec.ctx contains the tracing baggage for this record, which we propagate down the call tree.
		// But this context's cancellation is disjointed from the top-most ctx. The context.AfterFunc below,
		// is what fuses the two lifetimes together.
		recCtx, cancel := context.WithCancelCause(rec.ctx)
		context.AfterFunc(ctx, func() {
			cancel(context.Cause(ctx))
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
