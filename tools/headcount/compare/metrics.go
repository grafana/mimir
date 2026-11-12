// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	pairs     *prometheus.CounterVec
	groups    *prometheus.CounterVec
	refusals  *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	rounds    *prometheus.CounterVec
	lastRound prometheus.Gauge
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
		rounds: f.NewCounterVec(prometheus.CounterOpts{
			Name: "cardinality_estimate_compare_rounds_total",
			Help: "Rounds run, by result: ok, or failed when the targets could not be picked.",
		}, []string{"result"}),
		lastRound: f.NewGauge(prometheus.GaugeOpts{
			Name: "cardinality_estimate_compare_last_round_timestamp_seconds",
			Help: "When the last round finished.",
		}),
	}
}
