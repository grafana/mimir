// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"

	"github.com/failsafe-go/failsafe-go/adaptivelimiter"
	"github.com/failsafe-go/failsafe-go/priority"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/util/reactivelimiter"
)

const (
	reactiveLimiterRequestTypeLabel = "request_type"
)

// The ingester reactive limiter consists of two limiters: one for push requests and one for read requests.
// It also includes a Prioritizer which, based on recent latencies in both limiters, decides the priority of requests to reject.
type ingesterReactiveLimiter struct {
	service services.Service

	prioritizer *reactivelimiter.Prioritizer
	push        reactivelimiter.ReactiveLimiter
	read        reactivelimiter.ReactiveLimiter
}

// Returns an ingesterReactiveLimiter that uses reactivelimiter.PriorityLimiters with a Prioritizer when push and read
// limiting is enabled, else that uses reactivelimiter.BlockingLimiter if only one of these is enabled.
func newIngesterReactiveLimiter(prioritizerConfig *reactivelimiter.PrioritizerConfig, pushConfig *reactivelimiter.Config, readConfig *reactivelimiter.Config, logger log.Logger, registerer prometheus.Registerer) *ingesterReactiveLimiter {
	var prioritizer *reactivelimiter.Prioritizer
	if pushConfig.Enabled && readConfig.Enabled {
		prioritizer = reactivelimiter.NewPrioritizer(logger)
		registerPrioritizerMetrics(prioritizer, registerer)
	}
	limiter := &ingesterReactiveLimiter{
		prioritizer: prioritizer,
		push:        newReactiveLimiter(pushConfig, "push", logger, registerer, priority.Low, prioritizer),
		read:        newReactiveLimiter(readConfig, "read", logger, registerer, priority.High, prioritizer),
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

func newReactiveLimiter(cfg *reactivelimiter.Config, requestType string, logger log.Logger, registerer prometheus.Registerer, requestPriority priority.Priority, prioritizer *reactivelimiter.Prioritizer) reactivelimiter.ReactiveLimiter {
	if !cfg.Enabled {
		return nil
	}

	var limiter reactivelimiter.ReactiveLimiter
	if prioritizer != nil {
		limiter = reactivelimiter.NewWithPriority(cfg, logger, requestPriority, prioritizer.Prioritizer)
	} else {
		limiter = reactivelimiter.New(cfg, logger)
	}

	registerReactiveLimiterMetrics(limiter.Metrics(), requestType, registerer)
	return limiter
}

func registerPrioritizerMetrics(prioritizer *reactivelimiter.Prioritizer, r prometheus.Registerer) {
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_ingester_rejection_rate",
		Help: "The prioritized rate at which requests should be rejected.",
	}, func() float64 {
		return prioritizer.RejectionRate()
	})
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_ingester_rejection_threshold",
		Help: "The priority threshold below which requests should be rejected.",
	}, func() float64 {
		return float64(prioritizer.RejectionThreshold())
	})
}

func registerReactiveLimiterMetrics(limiterMetrics adaptivelimiter.Metrics, requestType string, r prometheus.Registerer) {
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
		Name:        "cortex_ingester_reactive_limiter_queued_requests",
		Help:        "Ingester reactive limiter queued requests.",
		ConstLabels: map[string]string{reactiveLimiterRequestTypeLabel: requestType},
	}, func() float64 {
		return float64(limiterMetrics.Queued())
	})
}
