// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	writeTeeMetricsNamespace = "cortex_writetee"
)

type ProxyMetrics struct {
	requestDuration      *prometheus.HistogramVec
	responsesTotal       *prometheus.CounterVec
	errorsTotal          *prometheus.CounterVec
	bodySize             *prometheus.HistogramVec
	droppedRequestsTotal *prometheus.CounterVec
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
		droppedRequestsTotal: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Namespace: writeTeeMetricsNamespace,
			Name:      "dropped_requests_total",
			Help:      "Total number of requests dropped for non-preferred backends.",
		}, []string{"backend", "reason"}),
	}

	return m
}

// RecordBackendResult records metrics for a completed backend request.
// This provides consistent metric tracking for both preferred and non-preferred backends.
func (m *ProxyMetrics) RecordBackendResult(backendName, method, routeName string, elapsed time.Duration, status int, err error) {
	// Always record duration
	m.requestDuration.WithLabelValues(backendName, method, routeName, strconv.Itoa(statusCodeForMetrics(status, err))).Observe(elapsed.Seconds())

	// Record error only for actual errors (network/timeout), not for 5xx responses
	if err != nil {
		errorType := "network"
		if errors.Is(err, context.DeadlineExceeded) {
			errorType = "timeout"
		}
		m.errorsTotal.WithLabelValues(backendName, method, routeName, errorType).Inc()
	}
}

// statusCodeForMetrics returns the status code to use for metrics labels.
func statusCodeForMetrics(status int, err error) int {
	if err != nil || status <= 0 {
		return 500
	}
	return status
}
