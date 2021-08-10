// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/tools/querytee/proxy_metrics.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querytee

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	comparisonSuccess = "success"
	comparisonFailed  = "fail"
)

type ProxyMetrics struct {
	requestDuration        *prometheus.HistogramVec
	responsesTotal         *prometheus.CounterVec
	responsesComparedTotal *prometheus.CounterVec
}

func NewProxyMetrics(registerer prometheus.Registerer) *ProxyMetrics {
	m := &ProxyMetrics{
		requestDuration: promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "cortex_querytee",
			Name:      "request_duration_seconds",
			Help:      "Time (in seconds) spent serving HTTP requests.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 0.75, 1, 1.5, 2, 3, 4, 5, 10, 25, 50, 100},
		}, []string{"backend", "method", "route", "status_code"}),
		responsesTotal: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex_querytee",
			Name:      "responses_total",
			Help:      "Total number of responses sent back to the client by the selected backend.",
		}, []string{"backend", "method", "route"}),
		responsesComparedTotal: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex_querytee",
			Name:      "responses_compared_total",
			Help:      "Total number of responses compared per route name by result.",
		}, []string{"route", "result"}),
	}

	return m
}
