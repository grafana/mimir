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

	produceDirectRequestsTotal         prometheus.Counter
	produceDirectRequestsFailedTotal   *prometheus.CounterVec
	produceRequestsAttemptsSuccess     prometheus.Observer
	produceRequestsAttemptsFailure     prometheus.Observer
	produceDirectRequestLatencySuccess prometheus.Observer
	// produceDirectRequestLatencyFailure is keyed by failure reason; success
	// latency carries no reason label (see produceDirectRequestLatencySuccess).
	produceDirectRequestLatencyFailure prometheus.ObserverVec

	produceRequestsPrimaryTotal prometheus.Counter
	produceRequestsHedgeTotal   prometheus.Counter
}

// hedge suppression reasons recorded on hedgeAttemptsSuppressedTotal. These
// match the early-return paths in Hedger.shouldHedge.
const (
	hedgeSuppressedNoAgentStats   = "no_agent_stats"
	hedgeSuppressedNoClusterStats = "no_cluster_stats"
	hedgeSuppressedSlowFraction   = "slow_fraction_exceeded"
	hedgeSuppressedFaultyFraction = "faulty_fraction_exceeded"
)

func newMetrics(reg prometheus.Registerer) *metrics {
	// On failures the latency carries a "reason" label; on success the
	// reason is left empty, which Prometheus treats as the label being
	// absent for the success series.
	produceDirectRequestLatency := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "produce_direct_request_latency_seconds",
		Help:                            "Latency of a single direct Produce request to a Warpstream agent, by outcome (and by failure reason when the outcome is a failure). Each retry counts as a separate request.",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
		Buckets:                         prometheus.DefBuckets,
	}, []string{"outcome", "reason"})

	produceRequestAttempts := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "produce_requests_attempts",
		Help:                            "Number of Produce attempts a request took (1 = resolved on the primary, N = resolved after N-1 hedge waves), by outcome.",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"outcome"})

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
		produceDirectRequestsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "produce_direct_requests_total",
			Help: "Total number of direct Produce requests issued to a Warpstream agent. Each retry counts as a separate request.",
		}),
		produceRequestsPrimaryTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "produce_requests_primary_total",
			Help: "Total number of primary Produce wire requests.",
		}),
		produceRequestsHedgeTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "produce_requests_hedge_total",
			Help: "Total number of hedging and retried Produce wire requests.",
		}),
		produceDirectRequestsFailedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "produce_direct_requests_failed_total",
			Help: "Total number of direct Produce requests issued to a Warpstream agent that failed, by failure reason. Each retry counts as a separate request.",
		}, []string{"reason"}),
		produceRequestsAttemptsSuccess:     produceRequestAttempts.WithLabelValues("success"),
		produceRequestsAttemptsFailure:     produceRequestAttempts.WithLabelValues("failure"),
		produceDirectRequestLatencySuccess: produceDirectRequestLatency.WithLabelValues("success", ""),
		produceDirectRequestLatencyFailure: produceDirectRequestLatency.MustCurryWith(prometheus.Labels{"outcome": "failure"}),
	}
}
