// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	pairs     *prometheus.CounterVec
	flags     *prometheus.CounterVec
	groups    *prometheus.CounterVec
	refusals  *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	rounds    *prometheus.CounterVec
	lastRound prometheus.Gauge

	bursts        prometheus.Counter
	burstRequests *prometheus.CounterVec
	burstDuration *prometheus.HistogramVec
}

// latencyBuckets doubles from 10ms to the first bucket above timeout, so no answered request lands in +Inf.
func latencyBuckets(timeout time.Duration) []float64 {
	n := 2
	for 0.01*math.Pow(2, float64(n-1)) <= timeout.Seconds() {
		n++
	}
	return prometheus.ExponentialBuckets(0.01, 2, n)
}

func newMetrics(reg prometheus.Registerer, timeout time.Duration) *metrics {
	f := promauto.With(reg)
	latencyBuckets := latencyBuckets(timeout)
	return &metrics{
		pairs: f.NewCounterVec(prometheus.CounterOpts{
			Name: "cardinality_estimate_compare_pairs_total",
			Help: "Compared pairs of an estimate and its PromQL truth, by query, window kind, which request went first, outcome (pass, tolerated, flagged, mismatch, refusal, or answered with -estimate-only), the class of the pair's most severe group, and the estimate's read and source, empty when it didn't answer.",
		}, []string{"query", "window", "order", "outcome", "class", "read", "source"}),
		flags: f.NewCounterVec(prometheus.CounterOpts{
			Name: "cardinality_estimate_compare_flags_total",
			Help: "Answered estimates of a pair with a flag set, by query and flag: lower_bound, coverage_gap, head_min_time, missing_blocks, missing_shards, has_more or dedup.",
		}, []string{"query", "flag"}),
		groups: f.NewCounterVec(prometheus.CounterOpts{
			Name: "cardinality_estimate_compare_groups_total",
			Help: "Groups whose estimate differs from the truth, by query, outcome and class.",
		}, []string{"query", "outcome", "class"}),
		refusals: f.NewCounterVec(prometheus.CounterOpts{
			Name: "cardinality_estimate_compare_refusals_total",
			Help: "Requests of a pair that were refused or failed, by query, side (estimate, promql) and class.",
		}, []string{"query", "side", "class"}),
		duration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cardinality_estimate_compare_request_duration_seconds",
			Help:    "Client-side latency of each side of a pair that answered, once per pair, summed over the side's queries, by query, window kind and length, side, position (first, second, or alone with -estimate-only), reference (grouped, per_value, or none with -estimate-only), and the estimate's read and source.",
			Buckets: latencyBuckets,
		}, []string{"query", "window", "length", "side", "position", "reference", "read", "source"}),
		rounds: f.NewCounterVec(prometheus.CounterOpts{
			Name: "cardinality_estimate_compare_rounds_total",
			Help: "Rounds run, by result: ok, or failed when the targets could not be picked.",
		}, []string{"result"}),
		lastRound: f.NewGauge(prometheus.GaugeOpts{
			Name: "cardinality_estimate_compare_last_round_timestamp_seconds",
			Help: "When the last round finished.",
		}),
		bursts: f.NewCounter(prometheus.CounterOpts{
			Name: "cardinality_estimate_compare_bursts_total",
			Help: "Bursts of estimates sent at once.",
		}),
		burstRequests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "cardinality_estimate_compare_burst_requests_total",
			Help: "Estimates of a burst, by query and how they were answered: the HTTP status, busy for a 503 whose sources were all at their count limit, timeout, or error.",
		}, []string{"query", "status"}),
		burstDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cardinality_estimate_compare_burst_request_duration_seconds",
			Help:    "End-to-end latency of each estimate of a burst, by how it was answered.",
			Buckets: latencyBuckets,
		}, []string{"status"}),
	}
}
