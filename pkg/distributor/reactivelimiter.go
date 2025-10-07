// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"

	"github.com/failsafe-go/failsafe-go/adaptivelimiter"
	"github.com/failsafe-go/failsafe-go/priority"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/util/reactivelimiter"
)

// The distributor reactive limiter consists of a limiter for limiting requests.
// It also includes a rejectionPrioritizer which, based on recent latencies in the limiter,
// decides the priority of requests to reject.
type distributorReactiveLimiter struct {
	service services.Service

	logger log.Logger

	prioritizer *reactivelimiter.Prioritizer
	limiter     reactivelimiter.ReactiveLimiter
}

func newDistributorReactiveLimiter(cfg Config, logger log.Logger, registerer prometheus.Registerer) *distributorReactiveLimiter {
	if !cfg.ReactiveLimiter.Enabled {
		return nil
	}

	limiterCfg := &cfg.ReactiveLimiter
	var (
		prioritizer *reactivelimiter.Prioritizer
		limiter     reactivelimiter.ReactiveLimiter
	)
	if cfg.RejectionPrioritizerEnabled {
		prioritizer = reactivelimiter.NewPrioritizer(logger)
		registerPrioritizerMetrics(prioritizer, registerer)

		limiter = reactivelimiter.NewWithPriority(limiterCfg, logger, priority.High, prioritizer)
		level.Info(logger).Log("msg", "a reactive limiter with prioritizer for high priorities has been created")
	} else {
		limiter = reactivelimiter.New(limiterCfg, logger)
		level.Info(logger).Log("msg", "a reactive limiter has been created")
	}

	registerReactiveLimiterMetrics(limiter.Metrics(), registerer)
	distributorLimiter := &distributorReactiveLimiter{
		prioritizer: prioritizer,
		limiter:     limiter,
		logger:      logger,
	}

	if prioritizer != nil {
		distributorLimiter.service = services.NewTimerService(cfg.RejectionPrioritizer.CalibrationInterval, nil, distributorLimiter.update, nil)
	}

	level.Info(logger).Log("msg", "a distributorReactiveLimiter limiter has been created")
	return distributorLimiter
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

func (l *distributorReactiveLimiter) getLimiter() reactivelimiter.ReactiveLimiter {
	if l == nil {
		return nil
	}
	return l.limiter
}

func (l *distributorReactiveLimiter) CanAcquirePermit() bool {
	if l == nil {
		return true
	}
	result := l.limiter.CanAcquirePermit()
	kv := l.getStat()
	kv = append(kv, "msg", "CanAcquirePermit called", "result", result)
	level.Debug(l.logger).Log(kv...)
	return result
}

func (l *distributorReactiveLimiter) AcquirePermit(ctx context.Context) (adaptivelimiter.Permit, error) {
	if l == nil {
		return nil, nil
	}
	permit, err := l.limiter.AcquirePermit(ctx)
	kv := l.getStat()
	kv = append(kv, "msg", "AcquirePermit called", "permit", fmt.Sprintf("%v", permit), "err", err)
	level.Debug(l.logger).Log(kv...)
	return permit, err
}

func (l *distributorReactiveLimiter) Reset() {
	if l == nil {
		return
	}
	l.limiter.Reset()
}

func (l *distributorReactiveLimiter) getStat() []interface{} {
	if l == nil {
		return nil
	}
	inflight := l.limiter.Metrics().Inflight()
	limit := l.limiter.Metrics().Limit()
	queued := l.limiter.Metrics().Queued()
	return []interface{}{"inflight", fmt.Sprintf("%d", inflight), "limit", fmt.Sprintf("%d", limit), "queued", fmt.Sprintf("%d", queued)}
}

func registerPrioritizerMetrics(prioritizer *reactivelimiter.Prioritizer, r prometheus.Registerer) {
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_distributor_rejection_rate",
		Help: "The prioritized rate at which requests should be rejected.",
	}, func() float64 {
		return prioritizer.RejectionRate()
	})
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_distributor_rejection_threshold",
		Help: "The priority threshold below which requests should be rejected.",
	}, func() float64 {
		return float64(prioritizer.RejectionThreshold())
	})
}

func registerReactiveLimiterMetrics(limiterMetrics adaptivelimiter.Metrics, r prometheus.Registerer) {
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
		Name: "cortex_distributor_reactive_limiter_queued_requests",
		Help: "Distributor reactive limiter queued requests.",
	}, func() float64 {
		return float64(limiterMetrics.Queued())
	})
}
