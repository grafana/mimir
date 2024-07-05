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
		}, []string{"type"}),
		writesFailedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_writes_failed_total",
			Help:        "Total number of failed write requests.",
			ConstLabels: map[string]string{"test": testName},
		}, []string{"status_code", "type"}),
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

func (m *TestMetrics) InitializeCountersToZero(testType string) {
	// Note that we don't initialize writesFailedTotal as we don't want to create series for every possible status code.

	m.writesTotal.WithLabelValues(testType)
	m.queriesTotal.WithLabelValues(testType)
	m.queriesFailedTotal.WithLabelValues(testType)
	m.queryResultChecksTotal.WithLabelValues(testType)
	m.queryResultChecksFailedTotal.WithLabelValues(testType)
}
