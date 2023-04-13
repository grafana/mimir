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
	queriesTotal                 *prometheus.CounterVec
	queriesFailedTotal           *prometheus.CounterVec
	queryResultChecksTotal       *prometheus.CounterVec
	queryResultChecksFailedTotal *prometheus.CounterVec
}

func NewTestMetrics(testName string, reg prometheus.Registerer) *TestMetrics {
	return &TestMetrics{
		writesTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_writes_total",
			Help:        "Total number of attempted write requests.",
			ConstLabels: map[string]string{"test": testName},
		}, []string{"metric_name"}),
		writesFailedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_writes_failed_total",
			Help:        "Total number of failed write requests.",
			ConstLabels: map[string]string{"test": testName},
		}, []string{"status_code", "metric_name"}),
		queriesTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_queries_total",
			Help:        "Total number of attempted query requests.",
			ConstLabels: map[string]string{"test": testName},
		}, []string{"metric_name"}),
		queriesFailedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_queries_failed_total",
			Help:        "Total number of failed query requests.",
			ConstLabels: map[string]string{"test": testName},
		}, []string{"metric_name"}),
		queryResultChecksTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_query_result_checks_total",
			Help:        "Total number of query results checked for correctness.",
			ConstLabels: map[string]string{"test": testName},
		}, []string{"metric_name"}),
		queryResultChecksFailedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_query_result_checks_failed_total",
			Help:        "Total number of query results failed when checking for correctness.",
			ConstLabels: map[string]string{"test": testName},
		}, []string{"metric_name"}),
	}
}
