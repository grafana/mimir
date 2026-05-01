// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// metrics holds Prometheus metrics for WarpstreamClient.
// It only contains metrics not already emitted by franz-go's kprom hooks
// (which cover per-broker E2E latency, produce record/byte counts, etc.).
type metrics struct {
	hedgeAttemptsTotal           prometheus.Counter
	hedgeWinsTotal               prometheus.Counter
	hedgeAttemptsSuppressedTotal *prometheus.CounterVec

	lingerFlushesTotal prometheus.Counter
	lingerBufferBytes  prometheus.Gauge

	produceRequestsTotal       prometheus.Counter
	produceRequestsFailedTotal *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		hedgeAttemptsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "hedge_attempts_total",
			Help: "Total number of produce requests for which a hedge was sent to a secondary agent.",
		}),
		hedgeWinsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "hedge_wins_total",
			Help: "Total number of produce requests where the secondary agent responded before the primary.",
		}),
		hedgeAttemptsSuppressedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "hedge_attempts_suppressed_total",
			Help: "Total number of produce requests where hedging was suppressed.",
		}, []string{"reason"}),
		lingerFlushesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "linger_flushes_total",
			Help: "Total number of partition batch flushes triggered by the linger buffer.",
		}),
		lingerBufferBytes: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "linger_buffer_bytes",
			Help: "Current number of bytes buffered in the linger buffer awaiting flush.",
		}),
		produceRequestsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "produce_requests_total",
			Help: "Total number of produce requests attempted.",
		}),
		produceRequestsFailedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "produce_requests_failed_total",
			Help: "Total number of produce requests that failed, by failure reason.",
		}, []string{"reason"}),
	}
}
