// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	clientLabel   = "client"
	protocolLabel = "protocol"
	methodLabel   = "method"

	GRPCProtocol = "grpc"
	HTTPProtocol = "http"
)

func NewRequestInvalidClusterValidationLabelsTotalCounter(reg prometheus.Registerer, client string, protocol string) *prometheus.CounterVec {
	return promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_client_invalid_cluster_validation_label_requests_total",
		Help: "Number of requests with invalid cluster validation label.",
		ConstLabels: map[string]string{
			clientLabel:   client,
			protocolLabel: protocol,
		},
	}, []string{methodLabel})
}

func NewInvalidClusterValidationReporter(cluster string, invalidClusterValidations *prometheus.CounterVec, logger log.Logger) middleware.InvalidClusterValidationReporter {
	return func(msg string, method string) {
		level.Warn(logger).Log("msg", msg, "method", method, "cluster_validation_label", cluster)
		invalidClusterValidations.WithLabelValues(method).Inc()
	}
}
