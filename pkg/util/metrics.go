// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
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
		Name: "cortex_client_request_invalid_cluster_validation_labels_total",
		Help: "Number of requests with invalid cluster validation label.",
		ConstLabels: map[string]string{
			clientLabel:   client,
			protocolLabel: protocol,
		},
	}, []string{methodLabel})
}

func NewOnInvalidCluster(invalidClusterValidations *prometheus.CounterVec, logger log.Logger) func(string, string, string) {
	return func(msg string, cluster string, method string) {
		level.Warn(logger).Log("msg", msg, "method", method, "clusterValidationLabel", cluster)
		invalidClusterValidations.WithLabelValues(method).Inc()
	}
}

func EmptyInvalidCluster() func(string, string, string) {
	return func(string, string, string) {}
}
