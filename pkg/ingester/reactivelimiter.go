// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/util/reactivelimiter"
)

const (
	reactiveLimiterRequestTypeLabel = "request_type"
)

type rejectionPrioritizer struct {
	cfg *reactivelimiter.RejectionPrioritizerConfig
	reactivelimiter.Prioritizer
}

// The ingester reactive limiter consists of two limiters: one for push requests and one for read requests.
// It also includes a rejectionPrioritizer which, based on recent latencies in both limiters, decides the priority of requests to reject.
type ingesterReactiveLimiter struct {
	services.Service

	prioritizer *rejectionPrioritizer
	push        *priorityLimiter
	read        *priorityLimiter
}

func newIngesterReactiveLimiter(prioritizerConfig *reactivelimiter.RejectionPrioritizerConfig, pushConfig *reactivelimiter.Config, readConfig *reactivelimiter.Config, logger log.Logger, registerer prometheus.Registerer) *ingesterReactiveLimiter {
	// Create prioritizer to prioritize the rejection threshold between push and read limiters
	prioritizer := &rejectionPrioritizer{
		cfg:         prioritizerConfig,
		Prioritizer: reactivelimiter.NewPrioritizer(logger),
	}
	promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_ingester_rejection_rate",
		Help: "Ingester prioritizer rejection rate.",
	}, func() float64 {
		return prioritizer.RejectionRate()
	})
	promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_ingester_rejection_threshold",
		Help: "Ingester prioritizer rejection threshold.",
	}, func() float64 {
		return float64(prioritizer.RejectionThreshold())
	})

	// Create limiters that use prioritizer
	limiter := &ingesterReactiveLimiter{
		prioritizer: prioritizer,
		push:        newPriorityLimiter(pushConfig, prioritizer, reactivelimiter.PriorityHigh, "push", logger, registerer),
		read:        newPriorityLimiter(readConfig, prioritizer, reactivelimiter.PriorityLow, "read", logger, registerer),
	}
	limiter.Service = services.NewTimerService(prioritizerConfig.CalibrationInterval, nil, limiter.update, nil)

	return limiter
}

func (l *ingesterReactiveLimiter) update(_ context.Context) error {
	l.prioritizer.Calibrate()
	return nil
}

// A limiter that acquires permits for a specific priority.
type priorityLimiter struct {
	limiter  reactivelimiter.PriorityLimiter
	priority reactivelimiter.Priority
}

func newPriorityLimiter(cfg *reactivelimiter.Config, prioritizer reactivelimiter.Prioritizer, priority reactivelimiter.Priority, requestType string, logger log.Logger, registerer prometheus.Registerer) *priorityLimiter {
	if !cfg.Enabled || prioritizer == nil {
		return nil
	}

	limiter := reactivelimiter.NewPriorityLimiter(cfg, prioritizer, log.With(logger, "requestType", requestType))
	registerReactiveLimiterMetrics(limiter, requestType, registerer)
	return &priorityLimiter{
		limiter:  limiter,
		priority: priority,
	}
}

func (l *priorityLimiter) CanAcquirePermit() bool {
	return l.limiter.CanAcquirePermit(l.priority)
}

func (l *priorityLimiter) AcquirePermit(ctx context.Context) (reactivelimiter.Permit, error) {
	return l.limiter.AcquirePermit(ctx, l.priority)
}

func registerReactiveLimiterMetrics(limiter reactivelimiter.PriorityLimiter, requestType string, r prometheus.Registerer) {
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "cortex_ingester_reactive_limiter_inflight_limit",
		Help:        "Ingester reactive limiter inflight request limit.",
		ConstLabels: map[string]string{reactiveLimiterRequestTypeLabel: requestType},
	}, func() float64 {
		return float64(limiter.Limit())
	})
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "cortex_ingester_reactive_limiter_inflight_requests",
		Help:        "Ingester reactive limiter inflight requests.",
		ConstLabels: map[string]string{reactiveLimiterRequestTypeLabel: requestType},
	}, func() float64 {
		return float64(limiter.Inflight())
	})
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "cortex_ingester_reactive_limiter_blocked_requests",
		Help:        "Ingester reactive limiter blocked requests.",
		ConstLabels: map[string]string{reactiveLimiterRequestTypeLabel: requestType},
	}, func() float64 {
		return float64(limiter.Blocked())
	})
}
