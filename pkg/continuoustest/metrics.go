// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// TestMetrics holds generic metrics tracked by tests. The common metrics are used to enforce the same
// metric names and labels to track the same information across different tests.
type TestMetrics struct {
	writesTotal       prometheus.Counter
	writesFailedTotal *prometheus.CounterVec
}

func NewTestMetrics(testName string, reg prometheus.Registerer) *TestMetrics {
	return &TestMetrics{
		writesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_writes_total",
			Help:        "Total number of attempted write requests.",
			ConstLabels: map[string]string{"test": testName},
		}),
		writesFailedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_writes_failed_total",
			Help:        "Total number of failed write requests.",
			ConstLabels: map[string]string{"test": testName},
		}, []string{"status_code"}),
	}
}
