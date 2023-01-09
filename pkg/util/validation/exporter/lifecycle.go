package exporter

import (
	"context"

	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
)

func (oe *OverridesExporter) starting(ctx context.Context) error {
	if oe.subserviceManager == nil {
		return nil
	}

	oe.subserviceWatcher.WatchManager(oe.subserviceManager)
	if err := services.StartManagerAndAwaitHealthy(ctx, oe.subserviceManager); err != nil {
		return errors.Wrap(err, "unable to start overrides-exporter subserviceManager")
	}
	return nil
}

func (oe *OverridesExporter) running(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-oe.subserviceWatcher.Chan():
			return errors.Wrap(err, "a subservice of overrides-exporter has failed")
		}
	}
}

func (oe *OverridesExporter) stopping(error) error {
	if oe.subserviceManager == nil {
		return nil
	}

	return errors.Wrap(
		services.StopManagerAndAwaitStopped(context.Background(), oe.subserviceManager),
		"failed to stop overrides-exporter's subserviceManager",
	)
}
