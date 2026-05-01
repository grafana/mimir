// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"time"

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

	produceRequestsTotal                   prometheus.Counter
	produceRequestsFailedTotal             *prometheus.CounterVec
	produceRequestsRetriedTotal            *prometheus.CounterVec
	produceRequestsAttemptsBeforeSucceeded prometheus.Histogram

	produceRequestsPrimaryTotal prometheus.Counter
	produceRequestsHedgeTotal   prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		hedgeAttemptsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "hedge_attempts_total",
			Help: "Total number of produce requests for which a fanout to per-partition secondaries was dispatched. Includes both latency-triggered hedges (primary still in flight) and primary-failure retries.",
		}),
		hedgeWinsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "hedge_wins_total",
			Help: "Total number of produce requests where the per-partition secondary fanout produced the winning response. Includes both races where the secondaries beat the primary and retries where the primary had already failed.",
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
			Help: "Total number of Produce requests issued to a Warpstream agent. Each retry counts as a separate request.",
		}),
		produceRequestsPrimaryTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "produce_requests_primary_total",
			Help: "Total number of primary Produce wire requests.",
		}),
		produceRequestsHedgeTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "produce_requests_hedge_total",
			Help: "Total number of hedging and retried Produce wire requests.",
		}),
		produceRequestsFailedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "produce_requests_failed_total",
			Help: "Total number of Produce requests issued to a Warpstream agent that failed, by failure reason.",
		}, []string{"reason"}),
		produceRequestsRetriedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "produce_requests_retried_total",
			Help: "Total number of Produce request retries the retry layer issued, by reason (the failure that triggered the retry).",
		}, []string{"reason"}),
		produceRequestsAttemptsBeforeSucceeded: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "produce_requests_attempts_before_succeeded",
			Help:                            "Number of Produce attempts a successful request took (1 = succeeded on first try, N = succeeded after N-1 retries). Only successful requests are recorded.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}),
	}
}
