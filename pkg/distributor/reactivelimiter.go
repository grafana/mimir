// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"github.com/failsafe-go/failsafe-go/adaptivelimiter"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/util/reactivelimiter"
)

var errReactiveLimiterLimitExceeded = newReactiveLimiterExceededError(adaptivelimiter.ErrExceeded)

func newDistributorReactiveLimiter(cfg reactivelimiter.Config, logger log.Logger, registerer prometheus.Registerer) reactivelimiter.ReactiveLimiter {
	if !cfg.Enabled {
		return nil
	}

	limiter := reactivelimiter.New(&cfg, logger)
	registerReactiveLimiterMetrics(limiter.Metrics(), registerer)
	return limiter
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
