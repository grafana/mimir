// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	writeTeeMetricsNamespace = "cortex_writetee"
)

type ProxyMetrics struct {
	requestDuration *prometheus.HistogramVec
	responsesTotal  *prometheus.CounterVec
	errorsTotal     *prometheus.CounterVec
	bodySize        *prometheus.HistogramVec
}

func NewProxyMetrics(registerer prometheus.Registerer) *ProxyMetrics {
	m := &ProxyMetrics{
		requestDuration: promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: writeTeeMetricsNamespace,
			Name:      "backend_request_duration_seconds",
			Help:      "Time (in seconds) spent serving HTTP write requests.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 0.75, 1, 1.5, 2, 3, 4, 5, 10, 25, 50, 100},
		}, []string{"backend", "method", "route", "status_code"}),
		responsesTotal: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Namespace: writeTeeMetricsNamespace,
			Name:      "responses_total",
			Help:      "Total number of responses sent back to the client by the selected backend.",
		}, []string{"backend", "method", "route"}),
		errorsTotal: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Namespace: writeTeeMetricsNamespace,
			Name:      "backend_errors_total",
			Help:      "Total number of errors from backend requests.",
		}, []string{"backend", "method", "route", "error_type"}),
		bodySize: promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: writeTeeMetricsNamespace,
			Name:      "request_body_size_bytes",
			Help:      "Size of incoming request bodies in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1024, 2, 20), // 1KB to ~1GB
		}, []string{"route"}),
	}

	return m
}
