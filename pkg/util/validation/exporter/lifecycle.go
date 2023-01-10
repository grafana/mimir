// SPDX-License-Identifier: AGPL-3.0-only

package exporter

import (
	"context"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
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

	// Wait until the ring client detected this instance in the ACTIVE state
	_ = level.Info(oe.logger).Log("msg", "waiting until overrides-exporter is ACTIVE in the ring")
	if err := ring.WaitInstanceState(ctx, oe.ring.client, oe.ring.lifecycler.GetInstanceID(), ring.ACTIVE); err != nil {
		return err
	}
	_ = level.Info(oe.logger).Log("msg", "overrides-exporter is ACTIVE in the ring")
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
