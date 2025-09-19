// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/util/reactivelimiter"
)

type rejectionPrioritizer struct {
	cfg *reactivelimiter.RejectionPrioritizerConfig
	reactivelimiter.Prioritizer
}

// The distributor reactive limiter consists of a limiter for limiting requests.
// It also includes a rejectionPrioritizer which, based on recent latencies in the limiter,
// decides the priority of requests to reject.
type distributorReactiveLimiter struct {
	service services.Service

	prioritizer *rejectionPrioritizer
	limiter     reactivelimiter.BlockingLimiter
}

func newDistributorReactiveLimiter(limiterCfg *reactivelimiter.Config, prioritizerCfg *reactivelimiter.RejectionPrioritizerConfig, logger log.Logger, registerer prometheus.Registerer) *distributorReactiveLimiter {
	if !limiterCfg.Enabled {
		return nil
	}

	// Create prioritizer to prioritize the rejection threshold between limiter and read limiters
	prioritizer := &rejectionPrioritizer{
		cfg:         prioritizerCfg,
		Prioritizer: reactivelimiter.NewPrioritizer(logger),
	}

	// Capture rejection metrics from the prioritizer
	promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_distributor_rejection_rate",
		Help: "The prioritized rate at which requests should be rejected.",
	}, func() float64 {
		return prioritizer.RejectionRate()
	})
	promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_distributor_rejection_threshold",
		Help: "The priority threshold below which requests should be rejected.",
	}, func() float64 {
		return float64(prioritizer.RejectionThreshold())
	})

	// Create limiters that use prioritizer
	limiter := newPriorityLimiter(limiterCfg, prioritizer, reactivelimiter.PriorityHigh, logger, registerer)

	reactiveLimiter := &distributorReactiveLimiter{
		prioritizer: prioritizer,
		limiter:     limiter,
	}

	reactiveLimiter.service = services.NewTimerService(prioritizerCfg.CalibrationInterval, nil, reactiveLimiter.update, nil)
	return reactiveLimiter
}

func (l *distributorReactiveLimiter) update(_ context.Context) error {
	l.prioritizer.Calibrate()
	return nil
}

func (l *distributorReactiveLimiter) getService() services.Service {
	if l == nil {
		return nil
	}
	return l.service
}

func (l *distributorReactiveLimiter) getLimiter() reactivelimiter.BlockingLimiter {
	if l == nil {
		return nil
	}
	return l.limiter
}

// A limiter that acquires permits for a specific priority.
type priorityLimiter struct {
	reactivelimiter.PriorityLimiter
	priority reactivelimiter.Priority
}

// Returns a BlockingLimiter that uses a fixed priority to threshold all requests against the limiter.
func newPriorityLimiter(cfg *reactivelimiter.Config, prioritizer reactivelimiter.Prioritizer, priority reactivelimiter.Priority, logger log.Logger, registerer prometheus.Registerer) reactivelimiter.BlockingLimiter {
	if !cfg.Enabled || prioritizer == nil {
		return nil
	}

	limiter := reactivelimiter.NewPriorityLimiter(cfg, prioritizer, logger)
	registerReactiveLimiterMetrics(limiter, registerer)
	return &priorityLimiter{
		PriorityLimiter: limiter,
		priority:        priority,
	}
}

func (l *priorityLimiter) CanAcquirePermit() bool {
	return l.PriorityLimiter.CanAcquirePermit(l.priority)
}

func (l *priorityLimiter) AcquirePermit(ctx context.Context) (reactivelimiter.Permit, error) {
	return l.PriorityLimiter.AcquirePermit(ctx, l.priority)
}

func registerReactiveLimiterMetrics(limiterMetrics reactivelimiter.Metrics, r prometheus.Registerer) {
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_distributor_reactive_limiter_inflight_limit",
		Help: "Distributor reactive limiter inflight request limit.",
	}, func() float64 {
		return float64(limiterMetrics.Limit())
	})
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_distributor_reactive_limiter_inflight_requests",
		Help: "Distributor reactive limiter inflight requests.",
	}, func() float64 {
		return float64(limiterMetrics.Inflight())
	})
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_distributor_reactive_limiter_blocked_requests",
		Help: "Distributor reactive limiter blocked requests.",
	}, func() float64 {
		return float64(limiterMetrics.Blocked())
	})
}
