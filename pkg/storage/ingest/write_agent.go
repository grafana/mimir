// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
)

type WriteAgent struct {
	services.Service

	logger log.Logger
	store  *MetadataStore
}

func NewWriteAgent(cfg Config, logger log.Logger) *WriteAgent {
	a := &WriteAgent{
		store:  NewMetadataStore(cfg.PostgresConfig, logger),
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
	// TODO DEBUG
	//go func() {
	//	for ctx.Err() == nil {
	//		time.Sleep(time.Second)
	//		segment, err := a.store.AddSegment(ctx, 1, ulid.MustNew(uint64(time.Now().UnixMilli()), nil))
	//		fmt.Println("AddSegment() segment:", segment, "err:", err)
	//	}
	//}()
	//
	//for ctx.Err() == nil {
	//	segments := a.store.WatchSegments(ctx, 1, -1)
	//	fmt.Println("WatchSegments() segments:", segments)
	//}

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
