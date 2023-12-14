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

	"github.com/grafana/mimir/pkg/mimirpb"
)

type Pusher interface {
	Push(context.Context, *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error)
}

type pusherConsumer struct {
	p Pusher

	metrics readerMetrics
	l       log.Logger
}

type parsedRecord struct {
	*mimirpb.WriteRequest
	tenantID string
	err      error
}

func newPusherConsumer(p Pusher, metrics readerMetrics, l log.Logger) *pusherConsumer {
	return &pusherConsumer{
		p:       p,
		metrics: metrics,
		l:       l,
	}
}

func (c pusherConsumer) Consume(ctx context.Context, records []Record) error {
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

		c.metrics.processingTime.Observe(time.Since(processingStart).Seconds())
		if err != nil {
			level.Error(c.l).Log("msg", "failed to push write request; skipping", "err", err)
			// TODO move distributor's isClientError to a separate package and use that here to swallow only client errors and abort on others
			continue
		}
	}
	return nil
}

func (c pusherConsumer) unmarshalRequests(ctx context.Context, records []Record, reqC chan<- parsedRecord) {
	defer close(reqC)
	done := ctx.Done()

	for _, record := range records {
		pRecord := parsedRecord{
			tenantID:     record.TenantID,
			WriteRequest: &mimirpb.WriteRequest{},
		}
		// We don't free the WriteRequest slices because they are being freed by the Pusher.
		err := pRecord.WriteRequest.Unmarshal(record.Content)
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
