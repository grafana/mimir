// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
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
