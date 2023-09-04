// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	requestDuration           *prometheus.HistogramVec
	circuitBreakerTransitions *prometheus.CounterVec
	circuitBreakerResults     *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	return &Metrics{
		requestDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_ingester_client_request_duration_seconds",
			Help:    "Time spent doing Ingester requests.",
			Buckets: prometheus.ExponentialBuckets(0.001, 4, 8),
		}, []string{"operation", "status_code"}),
		circuitBreakerTransitions: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_client_circuit_breaker_transitions_total",
			Help: "Number times the circuit breaker has entered a state",
		}, []string{"state"}),
		circuitBreakerResults: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_client_circuit_breaker_results_total",
			Help: "Results of executing requests via the circuit breaker",
		}, []string{"result"}),
	}
}
