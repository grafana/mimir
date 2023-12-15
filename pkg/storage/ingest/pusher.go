// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
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
	}
}

func (c pusherConsumer) consume(ctx context.Context, records []record) error {
	recC := make(chan parsedRecord)
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(cancellation.NewErrorf("done consuming records"))

	go c.unmarshalRequests(ctx, records, recC)
	err := c.pushRequests(ctx, recC)
	if err != nil {
		return err
	}
	return nil
}

func (c pusherConsumer) pushRequests(ctx context.Context, reqC <-chan parsedRecord) error {
	for wr := range reqC {
		if wr.err != nil {
			level.Error(c.l).Log("msg", "failed to parse write request; skipping", "err", wr.err)
			continue
		}
		processingStart := time.Now()

		ctx := user.InjectOrgID(ctx, wr.tenantID)
		_, err := c.p.Push(ctx, wr.WriteRequest)

		c.processingTimeSeconds.Observe(time.Since(processingStart).Seconds())
		if err != nil {
			level.Error(c.l).Log("msg", "failed to push write request; skipping", "err", err)
			// TODO move distributor's isClientError to a separate package and use that here to swallow only client errors and abort on others
			continue
		}
	}
	return nil
}

func (c pusherConsumer) unmarshalRequests(ctx context.Context, records []record, reqC chan<- parsedRecord) {
	defer close(reqC)
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
		case reqC <- pRecord:
		}
	}
}
