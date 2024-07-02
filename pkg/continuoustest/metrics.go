// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// TestMetrics holds generic metrics tracked by tests. The common metrics are used to enforce the same
// metric names and labels to track the same information across different tests.
type TestMetrics struct {
	writesTotal                  *prometheus.CounterVec
	writesFailedTotal            *prometheus.CounterVec
	writesLatency                *prometheus.HistogramVec
	queriesTotal                 *prometheus.CounterVec
	queriesFailedTotal           *prometheus.CounterVec
	queriesLatency               *prometheus.HistogramVec
	queryResultChecksTotal       *prometheus.CounterVec
	queryResultChecksFailedTotal *prometheus.CounterVec
}

func NewTestMetrics(testName string, reg prometheus.Registerer) *TestMetrics {
	return &TestMetrics{
		writesTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_writes_total",
			Help:        "Total number of attempted write requests.",
			ConstLabels: map[string]string{"test": testName},
		}, []string{"type"}),
		writesFailedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_writes_failed_total",
			Help:        "Total number of failed write requests.",
			ConstLabels: map[string]string{"test": testName},
		}, []string{"status_code", "type"}),
		writesLatency: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:        "mimir_continuous_test_writes_request_duration_seconds",
			Help:        "Duration of the write requests.",
			Buckets:     prometheus.ExponentialBuckets(0.001, 4, 6),
			ConstLabels: map[string]string{"test": testName},
		}, []string{"type"}),
		queriesTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_queries_total",
			Help:        "Total number of attempted query requests.",
			ConstLabels: map[string]string{"test": testName},
		}, []string{"type"}),
		queriesFailedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_queries_failed_total",
			Help:        "Total number of failed query requests.",
			ConstLabels: map[string]string{"test": testName},
		}, []string{"type"}),
		queriesLatency: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:        "mimir_continuous_test_queries_request_duration_seconds",
			Help:        "Duration of the read requests.",
			Buckets:     prometheus.ExponentialBuckets(0.001, 4, 6),
			ConstLabels: map[string]string{"test": testName},
		}, []string{"type", "results_cache"}),
		queryResultChecksTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_query_result_checks_total",
			Help:        "Total number of query results checked for correctness.",
			ConstLabels: map[string]string{"test": testName},
		}, []string{"type"}),
		queryResultChecksFailedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_query_result_checks_failed_total",
			Help:        "Total number of query results failed when checking for correctness.",
			ConstLabels: map[string]string{"test": testName},
		}, []string{"type"}),
	}
}
