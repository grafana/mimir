// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"

	"github.com/grafana/mimir/pkg/storage/ingest/ingestpb"
)

type WriteAgent struct {
	services.Service

	logger log.Logger
	store  *MetadataStore
}

func (a *WriteAgent) Write(ctx context.Context, wr *ingestpb.WriteRequest) (*ingestpb.WriteResponse, error) {
	return &ingestpb.WriteResponse{}, a.writeSegmentPiece(ctx, wr)
}

func NewWriteAgent(cfg Config, logger log.Logger) *WriteAgent {
	a := &WriteAgent{
		store:  NewMetadataStore(NewMetadataStorePostgresql(cfg.PostgresConfig), logger),
		logger: logger,
	}

	a.Service = services.NewBasicService(a.starting, a.running, a.stopping)
	return a
}

func (a *WriteAgent) starting(ctx context.Context) error {
	// Start dependencies.
	if err := services.StartAndAwaitRunning(ctx, a.store); err != nil {
		return err
	}

	return nil
}

func (a *WriteAgent) running(ctx context.Context) error {
	// Wait until terminated.
	select {
	case <-ctx.Done():
		return nil
	}
}

func (a *WriteAgent) stopping(_ error) error {
	// Stop dependencies.
	if err := services.StopAndAwaitTerminated(context.Background(), a.store); err != nil {
		level.Warn(a.logger).Log("msg", "failed to stop write agent dependencies", "err", err)
	}

	return nil
}

func (a *WriteAgent) writeSegmentPiece(ctx context.Context, wr *ingestpb.WriteRequest) error {
	fmt.Println("writeSegmentPiece: partition ", wr.PartitionId)
	return nil
}
