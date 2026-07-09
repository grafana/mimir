// SPDX-License-Identifier: AGPL-3.0-only

package readtee

import (
	"context"
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	readTeeMetricsNamespace = "cortex_readtee"
)

type ProxyMetrics struct {
	requestDuration      *prometheus.HistogramVec
	responsesTotal       *prometheus.CounterVec
	errorsTotal          *prometheus.CounterVec
	droppedRequestsTotal *prometheus.CounterVec
	rewriteErrorsTotal   *prometheus.CounterVec
}

func NewProxyMetrics(registerer prometheus.Registerer) *ProxyMetrics {
	m := &ProxyMetrics{
		requestDuration: promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
			Namespace: readTeeMetricsNamespace,
			Name:      "backend_request_duration_seconds",
			Help:      "Time (in seconds) spent serving HTTP read requests.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 0.75, 1, 1.5, 2, 3, 4, 5, 10, 25, 50, 100},
		}, []string{"backend", "method", "route", "status_code"}),
		responsesTotal: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Namespace: readTeeMetricsNamespace,
			Name:      "responses_total",
			Help:      "Total number of responses sent back to the client by the selected backend.",
		}, []string{"backend", "method", "route"}),
		errorsTotal: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Namespace: readTeeMetricsNamespace,
			Name:      "backend_errors_total",
			Help:      "Total number of errors from backend requests.",
		}, []string{"backend", "method", "route", "error_type"}),
		droppedRequestsTotal: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Namespace: readTeeMetricsNamespace,
			Name:      "dropped_requests_total",
			Help:      "Total number of amplified (async fire-and-forget) requests dropped.",
		}, []string{"backend", "reason"}),
		rewriteErrorsTotal: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Namespace: readTeeMetricsNamespace,
			Name:      "rewrite_errors_total",
			Help:      "Total number of query/selector rewrite failures while preparing amplified read copies.",
		}, []string{"route"}),
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

		// Check for timeout/canceled errors: context errors (errors.Is auto-unwraps url.Error) or net.Error.Timeout()
		var netErr net.Error
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
			(errors.As(err, &netErr) && netErr.Timeout()) {
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
