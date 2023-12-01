package ingest

import (
	"context"

	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// Pusher is an ingester server that accepts pushes.
type Pusher interface {
	Push(context.Context, *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error)
}

type IngesterConsumer struct {
	p Pusher
}

type parsedRecord struct {
	*mimirpb.WriteRequest
	tenantId string
}

func NewIngesterConsumer(p Pusher) *IngesterConsumer {
	return &IngesterConsumer{
		p: p,
	}
}

func (c IngesterConsumer) Consume(ctx context.Context, records []Record) error {
	reqC := make(chan parsedRecord)
	errC := make(chan error)

	go c.consumeRequests(ctx, reqC, errC)
	err := c.unmarshalRequests(records, reqC, errC)
	close(reqC)
	if err != nil {
		return err
	}
	return <-errC
}

func (c IngesterConsumer) consumeRequests(ctx context.Context, reqC chan parsedRecord, errC chan error) {
	defer close(errC)
	for wr := range reqC {
		ctx = user.InjectOrgID(ctx, wr.tenantId)
		_, err := c.p.Push(ctx, wr.WriteRequest)
		if err != nil {
			errC <- errors.Wrap(err, "consuming write request")
			return
		}
	}
}

func (c IngesterConsumer) unmarshalRequests(records []Record, reqC chan parsedRecord, errC chan error) error {
	for _, record := range records {
		wr := &mimirpb.WriteRequest{}
		err := wr.Unmarshal(record.Content)
		if err != nil {
			return errors.Wrap(err, "parsing ingest consumer write request")
		}
		select {
		case err = <-errC:
			return errors.Wrap(err, "parsing ingest consumer write request")
		case reqC <- parsedRecord{WriteRequest: wr, tenantId: record.TenantID}:
		}
	}
	return nil
}
