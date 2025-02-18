// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"github.com/grafana/dskit/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	requestDuration                  *prometheus.HistogramVec
	invalidClusterVerificationLabels *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	return &Metrics{
		requestDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_ingester_client_request_duration_seconds",
			Help:    "Time spent doing Ingester requests.",
			Buckets: prometheus.ExponentialBuckets(0.001, 4, 8),
		}, []string{"operation", "status_code"}),

		invalidClusterVerificationLabels: middleware.NewRequestInvalidClusterVerficationLabelsTotalCounter(reg, "cortex_ingester_client"),
	}
}
