// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type Pusher interface {
	Push(context.Context, *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error)
}

type pusherConsumer struct {
	p Pusher

	processingTimeSeconds prometheus.Observer
	clientErrRequests     prometheus.Counter
	totalRequests         prometheus.Counter
	l                     log.Logger
}

type parsedRecord struct {
	*mimirpb.WriteRequest
	tenantID string
	err      error
}

func newPusherConsumer(p Pusher, reg prometheus.Registerer, l log.Logger) *pusherConsumer {
	return &pusherConsumer{
		p: p,
		l: l,
		processingTimeSeconds: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Name:       "cortex_ingest_storage_reader_processing_time_seconds",
			Help:       "Time taken to process a single record (write request).",
			Objectives: latencySummaryObjectives,
			MaxAge:     time.Minute,
			AgeBuckets: 10,
		}),
		clientErrRequests: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_client_error_requests_total",
			Help: "Number of write requests which caused client errors (e.g. tenant limits, samples out of bounds).",
		}),
		totalRequests: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_requests_total",
			Help: "Number of attempted write requests.",
		}),
	}
}

func (c pusherConsumer) consume(ctx context.Context, records []record) error {
	recC := make(chan parsedRecord)
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(cancellation.NewErrorf("done consuming records"))

	// Speed up consumption by unmarhsalling the next request while the previous one is being pushed.
	go c.unmarshalRequests(ctx, records, recC)
	err := c.pushRequests(ctx, recC)
	if err != nil {
		return err
	}
	return nil
}

func (c pusherConsumer) pushRequests(ctx context.Context, reqC <-chan parsedRecord) error {
	recordIdx := -1
	for wr := range reqC {
		recordIdx++
		if wr.err != nil {
			level.Error(c.l).Log("msg", "failed to parse write request; skipping", "err", wr.err)
			continue
		}
		processingStart := time.Now()

		ctx := user.InjectOrgID(ctx, wr.tenantID)
		_, err := c.p.Push(ctx, wr.WriteRequest)

		c.processingTimeSeconds.Observe(time.Since(processingStart).Seconds())
		c.totalRequests.Inc()
		if err != nil {
			if !isClientIngesterError(err) {
				return fmt.Errorf("consuming record at index %d for tenant %s: %w", recordIdx, wr.tenantID, err)
			}
			c.clientErrRequests.Inc()
			level.Warn(c.l).Log("msg", "detected a client error while ingesting write request (the request may have been partially ingested)", "err", err, "user", wr.tenantID)
		}
	}
	return nil
}

func isClientIngesterError(err error) bool {
	stat, ok := grpcutil.ErrorToStatus(err)
	if !ok {
		// This should not be reached but in case it is, fall back to assuming it's our fault.
		return false
	}

	if details := stat.Details(); len(details) > 0 {
		if errDetails, ok := details[0].(*mimirpb.ErrorDetails); ok {
			// This is usually the case.
			return errDetails.Cause == mimirpb.BAD_DATA
		}
	}
	// This should not be reached but in case it is, fall back to assuming it's our fault.
	return false
}

func (c pusherConsumer) unmarshalRequests(ctx context.Context, records []record, recC chan<- parsedRecord) {
	defer close(recC)
	done := ctx.Done()

	for _, record := range records {
		pRecord := parsedRecord{
			tenantID:     record.tenantID,
			WriteRequest: &mimirpb.WriteRequest{},
		}
		// We don't free the WriteRequest slices because they are being freed by the Pusher.
		err := pRecord.WriteRequest.Unmarshal(record.content)
		if err != nil {
			err = errors.Wrap(err, "parsing ingest consumer write request")
			pRecord.err = err
		}
		select {
		case <-done:
			return
		case recC <- pRecord:
		}
	}
}
