// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/util"
)

type Metrics struct {
	requestDuration                  *prometheus.HistogramVec
	transferredBytes                 *prometheus.CounterVec
	invalidClusterVerificationLabels *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	return &Metrics{
		requestDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_ingester_client_request_duration_seconds",
			Help:    "Time spent doing Ingester requests.",
			Buckets: prometheus.ExponentialBuckets(0.001, 4, 8),
		}, []string{"operation", "status_code"}),

		transferredBytes: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_client_transferred_bytes_total",
			Help: "Total bytes transferred to/from the ingester.",
		}, []string{"ingester_zone"}),

		invalidClusterVerificationLabels: util.NewRequestInvalidClusterValidationLabelsTotalCounter(reg, "ingester", util.GRPCProtocol),
	}
}
