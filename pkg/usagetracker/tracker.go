package usagetracker

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/util/validation"
)

type UsageTracker struct {
	services.Service

	overrides *validation.Overrides
	logger    log.Logger
}

func NewUsageTracker(overrides *validation.Overrides, logger log.Logger, _ prometheus.Registerer) *UsageTracker {
	t := &UsageTracker{
		overrides: overrides,
		logger:    logger,
	}

	t.Service = services.NewBasicService(t.start, t.run, t.stop)

	return t
}

// start implements services.StartingFn.
func (t *UsageTracker) start(_ context.Context) error {
	return nil
}

// stop implements services.StoppingFn.
func (t *UsageTracker) stop(_ error) error {
	return nil
}

// run implements services.RunningFn.
func (t *UsageTracker) run(ctx context.Context) error {
	level.Info(t.logger).Log("msg", "Usage tracker service is running")

	select {
	case <-ctx.Done():
		return nil
	}
}
