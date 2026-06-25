// SPDX-License-Identifier: AGPL-3.0-only

package subqueryspinoff

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Reasons recorded against the "reason" label of the spin-off-skipped metric when a query is not spun off.
const (
	SkippedReasonParsingFailed     = "parsing-failed"
	SkippedReasonMappingFailed     = "mapping-failed"
	SkippedReasonNoSubqueries      = "no-subqueries"
	SkippedReasonDownstreamQueries = "too-many-downstream-queries"
)

// Metrics holds the metrics recorded while spinning off subqueries from a query.
type Metrics struct {
	SpinOffAttempts           prometheus.Counter
	SpinOffSuccesses          prometheus.Counter
	SpinOffSkipped            *prometheus.CounterVec
	SpunOffSubqueries         prometheus.Counter
	SpunOffSubqueriesPerQuery prometheus.Histogram
}

// NewMetrics creates and registers the metrics used while spinning off subqueries.
func NewMetrics(registerer prometheus.Registerer) Metrics {
	m := Metrics{
		SpinOffAttempts: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_subquery_spinoff_attempts_total",
			Help: "Total number of queries the query-frontend attempted to spin-off subqueries from.",
		}),
		SpinOffSuccesses: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_subquery_spinoff_successes_total",
			Help: "Total number of queries the query-frontend successfully spun off subqueries from.",
		}),
		SpinOffSkipped: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_frontend_subquery_spinoff_skipped_total",
			Help: "Total number of queries the query-frontend skipped or failed to spin-off subqueries from.",
		}, []string{"reason"}),
		SpunOffSubqueries: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_spun_off_subqueries_total",
			Help: "Total number of subqueries that were spun off.",
		}),
		SpunOffSubqueriesPerQuery: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_frontend_spun_off_subqueries_per_query",
			Help:    "Number of subqueries spun off from a single query.",
			Buckets: prometheus.ExponentialBuckets(2, 2, 10),
		}),
	}

	// Initialize known label values.
	for _, reason := range []string{
		SkippedReasonParsingFailed,
		SkippedReasonMappingFailed,
		SkippedReasonNoSubqueries,
		SkippedReasonDownstreamQueries,
	} {
		m.SpinOffSkipped.WithLabelValues(reason)
	}

	return m
}
