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
	service services.Service

	prioritizer *rejectionPrioritizer
	push        reactivelimiter.BlockingLimiter
	read        reactivelimiter.BlockingLimiter
}

// Returns an ingesterReactiveLimiter that uses reactivelimiter.PriorityLimiters with a Prioritizer when push and read
// limiting is enabled, else that uses reactivelimiter.BlockingLimiter if only one of these is enabled.
func newIngesterReactiveLimiter(prioritizerConfig *reactivelimiter.RejectionPrioritizerConfig, pushConfig *reactivelimiter.Config, readConfig *reactivelimiter.Config, logger log.Logger, registerer prometheus.Registerer) *ingesterReactiveLimiter {
	var prioritizer *rejectionPrioritizer
	var pushLimiter reactivelimiter.BlockingLimiter
	var readLimiter reactivelimiter.BlockingLimiter

	if pushConfig.Enabled && readConfig.Enabled {
		// Create prioritizer to prioritize the rejection threshold between push and read limiters
		prioritizer = &rejectionPrioritizer{
			cfg:         prioritizerConfig,
			Prioritizer: reactivelimiter.NewPrioritizer(logger),
		}

		// Capture rejection metrics from the prioritizer
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

		// Create limiters that use prioritizer
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

// A limiter that acquires permits for a specific priority.
type priorityLimiter struct {
	reactivelimiter.PriorityLimiter
	priority reactivelimiter.Priority
}

// Returns a BlockingLimiter that uses a fixed priority to threshold all requests against the limiter.
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
