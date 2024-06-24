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
	queryTeeMetricsNamespace = "cortex_querytee"
	ComparisonSuccess        = ComparisonResult("success")
	ComparisonFailed         = ComparisonResult("fail")
	ComparisonSkipped        = ComparisonResult("skip")
)

type ComparisonResult string

type ProxyMetrics struct {
	requestDuration        *prometheus.HistogramVec
	responsesTotal         *prometheus.CounterVec
	responsesComparedTotal *prometheus.CounterVec
	relativeDuration       *prometheus.HistogramVec
	proportionalDuration   *prometheus.HistogramVec
}

func NewProxyMetrics(registerer prometheus.Registerer) *ProxyMetrics {
	m := &ProxyMetrics{
		requestDuration: promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: queryTeeMetricsNamespace,
			Name:      "backend_request_duration_seconds",
			Help:      "Time (in seconds) spent serving HTTP requests.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 0.75, 1, 1.5, 2, 3, 4, 5, 10, 25, 50, 100},
		}, []string{"backend", "method", "route", "status_code"}),
		responsesTotal: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Namespace: queryTeeMetricsNamespace,
			Name:      "responses_total",
			Help:      "Total number of responses sent back to the client by the selected backend.",
		}, []string{"backend", "method", "route"}),
		responsesComparedTotal: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Namespace: queryTeeMetricsNamespace,
			Name:      "responses_compared_total",
			Help:      "Total number of responses compared per route name by result.",
		}, []string{"route", "result"}),
		relativeDuration: promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
			Namespace:                   queryTeeMetricsNamespace,
			Name:                        "backend_response_relative_duration_seconds",
			Help:                        "Time (in seconds) of secondary backend less preferred backend.",
			NativeHistogramBucketFactor: 1.1,
		}, []string{"route"}),
		proportionalDuration: promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
			Namespace:                   queryTeeMetricsNamespace,
			Name:                        "backend_response_relative_duration_proportional",
			Help:                        "Response time of secondary backend less preferred backend, as a proportion of preferred backend response time.",
			NativeHistogramBucketFactor: 1.1,
		}, []string{"route"}),
	}

	return m
}
