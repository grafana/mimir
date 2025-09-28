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

type ingesterReactiveLimiter struct {
	service services.Service

	prioritizer *rejectionPrioritizer
	push        reactivelimiter.BlockingLimiter
	read        reactivelimiter.BlockingLimiter
}

func newIngesterReactiveLimiter(prioritizerConfig *reactivelimiter.RejectionPrioritizerConfig, pushConfig *reactivelimiter.Config, readConfig *reactivelimiter.Config, logger log.Logger, registerer prometheus.Registerer) *ingesterReactiveLimiter {
	var prioritizer *rejectionPrioritizer
	var pushLimiter reactivelimiter.BlockingLimiter
	var readLimiter reactivelimiter.BlockingLimiter

	if pushConfig.Enabled && readConfig.Enabled {
		prioritizer = &rejectionPrioritizer{
			cfg:         prioritizerConfig,
			Prioritizer: reactivelimiter.NewPrioritizer(logger),
		}

		promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "cortex_ingester_rejection_rate",
			Help: "The prioritized rate at which requests should be rejected.",
		}, func() float64 {
			return prioritizer.RejectionRate()
		})
		promauto.With(registerer).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "cortex_ingester_rejection_threshold",
			Help: "The priority threshold below which requests should be rejected.",
		}, func() float64 {
			return float64(prioritizer.RejectionThreshold())
		})

		pushLimiter = newPriorityLimiter(pushConfig, prioritizer, reactivelimiter.PriorityHigh, "push", logger, registerer)
		readLimiter = newPriorityLimiter(readConfig, prioritizer, reactivelimiter.PriorityLow, "read", logger, registerer)
	} else {
		pushLimiter = newBlockingLimiter(pushConfig, "push", logger, registerer)
		readLimiter = newBlockingLimiter(readConfig, "read", logger, registerer)
	}

	limiter := &ingesterReactiveLimiter{
		prioritizer: prioritizer,
		push:        pushLimiter,
		read:        readLimiter,
	}

	if prioritizer != nil {
		limiter.service = services.NewTimerService(prioritizerConfig.CalibrationInterval, nil, limiter.update, nil)
	}

	return limiter
}

func (l *ingesterReactiveLimiter) update(_ context.Context) error {
	l.prioritizer.Calibrate()
	return nil
}

func newBlockingLimiter(cfg *reactivelimiter.Config, requestType string, logger log.Logger, registerer prometheus.Registerer) reactivelimiter.BlockingLimiter {
	if !cfg.Enabled {
		return nil
	}

	limiter := reactivelimiter.NewBlockingLimiter(cfg, log.With(logger, "request_type", requestType))
	registerReactiveLimiterMetrics(limiter, requestType, registerer)
	return limiter
}

type priorityLimiter struct {
	reactivelimiter.PriorityLimiter
	priority reactivelimiter.Priority
}

func newPriorityLimiter(cfg *reactivelimiter.Config, prioritizer reactivelimiter.Prioritizer, priority reactivelimiter.Priority, requestType string, logger log.Logger, registerer prometheus.Registerer) reactivelimiter.BlockingLimiter {
	if !cfg.Enabled || prioritizer == nil {
		return nil
	}

	limiter := reactivelimiter.NewPriorityLimiter(cfg, prioritizer, log.With(logger, "requestType", requestType))
	registerReactiveLimiterMetrics(limiter, requestType, registerer)
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

func (l *priorityLimiter) AcquirePermitWithPriority(ctx context.Context, priority int) (reactivelimiter.Permit, error) {
	priorityEnum := convertIntToPriority(priority)
	return l.PriorityLimiter.AcquirePermit(ctx, priorityEnum)
}

func (l *priorityLimiter) CanAcquirePermitWithPriority(priority int) bool {
	priorityEnum := convertIntToPriority(priority)
	return l.PriorityLimiter.CanAcquirePermit(priorityEnum)
}

func convertIntToPriority(priority int) reactivelimiter.Priority {
	switch {
	case priority >= 400:
		return reactivelimiter.PriorityVeryHigh
	case priority >= 300:
		return reactivelimiter.PriorityHigh
	case priority >= 200:
		return reactivelimiter.PriorityMedium
	case priority >= 100:
		return reactivelimiter.PriorityLow
	default:
		return reactivelimiter.PriorityVeryLow
	}
}

func registerReactiveLimiterMetrics(limiterMetrics reactivelimiter.Metrics, requestType string, r prometheus.Registerer) {
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "cortex_ingester_reactive_limiter_inflight_limit",
		Help:        "Ingester reactive limiter inflight request limit.",
		ConstLabels: map[string]string{reactiveLimiterRequestTypeLabel: requestType},
	}, func() float64 {
		return float64(limiterMetrics.Limit())
	})
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "cortex_ingester_reactive_limiter_inflight_requests",
		Help:        "Ingester reactive limiter inflight requests.",
		ConstLabels: map[string]string{reactiveLimiterRequestTypeLabel: requestType},
	}, func() float64 {
		return float64(limiterMetrics.Inflight())
	})
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "cortex_ingester_reactive_limiter_blocked_requests",
		Help:        "Ingester reactive limiter blocked requests.",
		ConstLabels: map[string]string{reactiveLimiterRequestTypeLabel: requestType},
	}, func() float64 {
		return float64(limiterMetrics.Blocked())
	})
}
