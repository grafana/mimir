package ingest

import (
	"context"

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
	l log.Logger
}

type parsedRecord struct {
	*mimirpb.WriteRequest
	tenantId string
	err      error
}

func newPusherConsumer(p Pusher, l log.Logger) *pusherConsumer {
	return &pusherConsumer{
		p: p,
		l: l,
	}
}

func (c pusherConsumer) Consume(ctx context.Context, records []Record) error {
	recC := make(chan parsedRecord)
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(cancellation.NewErrorf("done consuming records"))

	go c.unmarshalRequests(ctx, records, recC)
	err := c.consumeRequests(ctx, recC)
	if err != nil {
		return err
	}
	return nil
}

func (c pusherConsumer) consumeRequests(ctx context.Context, reqC <-chan parsedRecord) error {
	for wr := range reqC {
		if wr.err != nil {
			level.Error(c.l).Log("msg", "failed to parse write request; skipping", "err", wr.err)
			continue
		}
		ctx := user.InjectOrgID(ctx, wr.tenantId)
		_, err := c.p.Push(ctx, wr.WriteRequest)
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
			tenantId:     record.TenantID,
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
