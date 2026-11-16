// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	pairs    *prometheus.CounterVec
	groups   *prometheus.CounterVec
	refusals *prometheus.CounterVec
	duration *prometheus.HistogramVec
	// sketchError is the relative error of each estimated group.
	sketchError *prometheus.HistogramVec
	rounds      *prometheus.CounterVec
	lastRound   prometheus.Gauge

	bursts        prometheus.Counter
	burstRequests *prometheus.CounterVec
	burstDuration *prometheus.HistogramVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	f := promauto.With(reg)
	return &metrics{
		pairs: f.NewCounterVec(prometheus.CounterOpts{
			Name: "cardinality_estimate_compare_pairs_total",
			Help: "Compared pairs of an estimate and its PromQL truth, by query, window kind, which request went first, outcome (pass, tolerated, mismatch, refusal) and the class of the pair's most severe group.",
		}, []string{"query", "window", "order", "outcome", "class"}),
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
			Help:    "End-to-end latency of each request of a pair, by query, side and cache: cold when it went first, warm when it went second.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 14),
		}, []string{"query", "side", "cache"}),
		sketchError: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cardinality_estimate_compare_sketch_relative_error",
			Help:    "Relative error |estimate - truth| / truth of each group the route marked estimated, equal or not, by group_by: metric when grouped by __name__, label_value otherwise. Step pairs are left out.",
			Buckets: []float64{0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.07, 0.1, 0.15, 0.2, 0.5},
		}, []string{"group_by"}),
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
			Help: "Estimates of a burst, by query and how they were answered: the HTTP status, error_trailer for a 200 whose stream ended in an error, timeout, or error.",
		}, []string{"query", "status"}),
		burstDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cardinality_estimate_compare_burst_request_duration_seconds",
			Help:    "End-to-end latency of each estimate of a burst, by how it was answered.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 14),
		}, []string{"status"}),
	}
}
