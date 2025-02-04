// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/util/adaptivelimiter"
)

const (
	adaptiveLimiterRequestTypeLabel = "request_type"
)

type rejectionPrioritizer struct {
	cfg *adaptivelimiter.RejectionPrioritizerConfig
	adaptivelimiter.Prioritizer
}

// The ingester adaptive limiter consists of two limiters: one for push requests and one for read requests.
// It also includes a rejectionPrioritizer which, based on recent latencies in both limiters, decides the priority of requests to reject.
type ingesterAdaptiveLimiter struct {
	services.Service

	prioritizer *rejectionPrioritizer
	push        *priorityLimiter
	read        *priorityLimiter
}

func newIngesterAdaptiveLimiter(prioritizerConfig *adaptivelimiter.RejectionPrioritizerConfig, pushConfig *adaptivelimiter.Config, readConfig *adaptivelimiter.Config, logger log.Logger, registerer prometheus.Registerer) *ingesterAdaptiveLimiter {
	if !prioritizerConfig.Enabled {
		return nil
	}

	// Create prioritizer to prioritize the rejection threshold between push and read limiters
	prioritizer := &rejectionPrioritizer{
		cfg:         prioritizerConfig,
		Prioritizer: adaptivelimiter.NewPrioritizer(logger),
	}
	promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_ingester_rejection_rate",
		Help: "Ingester rejection rate.",
	}, func() float64 {
		return prioritizer.RejectionRate()
	})
	promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_ingester_rejection_threshold",
		Help: "Ingester rejection threshold.",
	}, func() float64 {
		return float64(prioritizer.RejectionThreshold())
	})

	// Create limiters that use prioritizer
	limiter := &ingesterAdaptiveLimiter{
		prioritizer: prioritizer,
		push:        newPriorityLimiter(pushConfig, prioritizer, adaptivelimiter.PriorityHigh, "push", logger, registerer),
		read:        newPriorityLimiter(readConfig, prioritizer, adaptivelimiter.PriorityLow, "read", logger, registerer),
	}
	limiter.Service = services.NewTimerService(prioritizerConfig.CalibrationInterval, nil, limiter.update, nil)

	return limiter
}

func (l *ingesterAdaptiveLimiter) update(_ context.Context) error {
	l.prioritizer.Calibrate()
	return nil
}

// A limiter that acquires permits for a specific priority.
type priorityLimiter struct {
	limiter  adaptivelimiter.PriorityLimiter
	priority adaptivelimiter.Priority
}

func newPriorityLimiter(cfg *adaptivelimiter.Config, prioritizer adaptivelimiter.Prioritizer, priority adaptivelimiter.Priority, requestType string, logger log.Logger, registerer prometheus.Registerer) *priorityLimiter {
	if !cfg.Enabled || prioritizer == nil {
		return nil
	}

	limiter := adaptivelimiter.NewPrioritizedLimiter(cfg, prioritizer, log.With(logger, "requestType", requestType))
	registerAdaptiveLimiterMetrics(limiter, requestType, registerer)
	return &priorityLimiter{
		limiter:  limiter,
		priority: priority,
	}
}

func (l *priorityLimiter) CanAcquirePermit() bool {
	return l.limiter.CanAcquirePermit(l.priority)
}

func (l *priorityLimiter) AcquirePermit(ctx context.Context) (adaptivelimiter.Permit, error) {
	return l.limiter.AcquirePermit(ctx, l.priority)
}

func registerAdaptiveLimiterMetrics(limiter adaptivelimiter.PriorityLimiter, requestType string, r prometheus.Registerer) {
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "cortex_ingester_adaptive_limiter_inflight_limit",
		Help:        "Current inflight request limit.",
		ConstLabels: map[string]string{adaptiveLimiterRequestTypeLabel: requestType},
	}, func() float64 {
		return float64(limiter.Limit())
	})
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "cortex_ingester_adaptive_limiter_inflight_requests",
		Help:        "Current inflight request.",
		ConstLabels: map[string]string{adaptiveLimiterRequestTypeLabel: requestType},
	}, func() float64 {
		return float64(limiter.Inflight())
	})
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "cortex_ingester_adaptive_limiter_blocked_requests",
		Help:        "Current blocked request.",
		ConstLabels: map[string]string{adaptiveLimiterRequestTypeLabel: requestType},
	}, func() float64 {
		return float64(limiter.Blocked())
	})
}
