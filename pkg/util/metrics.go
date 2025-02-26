package util

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	clientLabel         = "client"
	protocolLabel       = "protocol"
	methodLabel         = "method"
	requestClusterLabel = "request_cluster_label"
	failingSideLabel    = "failing_side"

	GRPCProtocol = "grpc"
	HTTPProtocol = "http"
)

func NewRequestInvalidClusterVerficationLabelsTotalCounter(reg prometheus.Registerer, client string, protocol string) *prometheus.CounterVec {
	return promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_client_request_invalid_cluster_verification_labels_total",
		Help: "Number of requests with invalid cluster verification label.",
		ConstLabels: map[string]string{
			clientLabel:   client,
			protocolLabel: protocol,
		},
	}, []string{methodLabel, requestClusterLabel, failingSideLabel})
}
