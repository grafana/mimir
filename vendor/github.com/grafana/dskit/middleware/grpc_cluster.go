package middleware

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/dskit/clusterutil"
	"github.com/grafana/dskit/grpcutil"
)

const (
	failureClient         = "client"
	failureServer         = "server"
	protocolLabel         = "protocol"
	methodLabel           = "method"
	requestClusterLabel   = "request_cluster_label"
	failingComponentLabel = "failing_component"
)

var (
	errNoClusterProvided = grpcutil.Status(codes.InvalidArgument, "no cluster provided").Err()
)

func NewRequestInvalidClusterVerficationLabelsTotalCounter(reg prometheus.Registerer, prefix string) *prometheus.CounterVec {
	return promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name:        fmt.Sprintf("%s_request_invalid_cluster_verification_labels_total", prefix),
		Help:        "Number of requests with invalid cluster verification label.",
		ConstLabels: nil,
	}, []string{protocolLabel, methodLabel, requestClusterLabel, failingComponentLabel})
}

// ClusterUnaryClientInterceptor propagates the given cluster info to gRPC metadata.
func ClusterUnaryClientInterceptor(cluster string, invalidCluster *prometheus.CounterVec, logger log.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if cluster == "" {
			return errNoClusterProvided
		}

		reqCluster, err := clusterutil.GetClusterFromIncomingContext(ctx, logger)
		if err != nil && !errors.Is(err, clusterutil.ErrNoClusterVerificationLabel) {
			return err
		}

		if errors.Is(err, clusterutil.ErrNoClusterVerificationLabel) || reqCluster == cluster {
			// The incoming context either contains no cluster verification label,
			// or it already contains one which is equal to the expected one, which
			// is the cluster parameter.
			// In both cases we propagate the latter to the outgoing context.
			ctx = clusterutil.PutClusterIntoOutgoingContext(ctx, cluster)
			return handleError(invoker(ctx, method, req, reply, cc, opts...), cluster, method, invalidCluster, logger)
		}

		// At this point the incoming context already contains a cluster verification label,
		// but it is different from the expected one, we increase the metrics, and return an error.
		msg := fmt.Sprintf("wrong cluster verification label in the incoming context: %s, expected: %s", reqCluster, cluster)
		if logger != nil {
			level.Warn(logger).Log("msg", msg, "clusterVerificationLabel", cluster, "requestClusterVerificationLabel", reqCluster)
		}
		if invalidCluster != nil {
			invalidCluster.WithLabelValues("grpc", method, cluster, failureClient).Inc()
		}
		return grpcutil.Status(codes.InvalidArgument, msg).Err()
	}
}

func handleError(err error, cluster string, method string, invalidCluster *prometheus.CounterVec, logger log.Logger) error {
	if err == nil {
		return nil
	}
	if stat, ok := grpcutil.ErrorToStatus(err); ok {
		details := stat.Details()
		if len(details) == 1 {
			if errDetails, ok := details[0].(*grpcutil.ErrorDetails); ok {
				if errDetails.GetCause() == grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL {
					msg := fmt.Sprintf("request rejected by the server: %s", stat.Message())
					if logger != nil {
						level.Warn(logger).Log("msg", msg, "cluster", cluster, "method", method)
					}
					if invalidCluster != nil {
						invalidCluster.WithLabelValues("grpc", method, cluster, failureServer).Inc()
					}
					return grpcutil.Status(codes.InvalidArgument, msg).Err()
				}
			}
		}
	}
	return err

}

// ClusterUnaryServerInterceptor checks if the incoming gRPC metadata contains any cluster information and if so,
// checks if the latter corresponds to the given cluster. If it is the case, the request is further propagated.
// Otherwise, an error is returned. In that case, non-nil invalidClusters counter is increased.
func ClusterUnaryServerInterceptor(cluster string, invalidClusters *prometheus.CounterVec, logger log.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if cluster == "" {
			if logger != nil {
				level.Warn(logger).Log("msg", "no cluster verification label sent to the interceptor", "method", info.FullMethod)
			}
			return nil, errNoClusterProvided
		}

		if _, ok := info.Server.(healthpb.HealthServer); ok {
			return handler(ctx, req)
		}

		reqCluster, err := clusterutil.GetClusterFromIncomingContext(ctx, logger)
		var (
			msgs   []any
			errMsg string
		)
		switch {
		case err == nil && cluster == reqCluster:
			return handler(ctx, req)
		case err == nil && cluster != reqCluster:
			msgs = []any{"msg", "rejecting request with wrong cluster verification label", "clusterVerificationLabel", cluster, "requestClusterVerificationLabel", reqCluster, "method", info.FullMethod}
			errMsg = fmt.Sprintf("server from cluster %q rejected request intended for cluster %q", cluster, reqCluster)
		case errors.Is(err, clusterutil.ErrNoClusterVerificationLabel):
			msgs = []any{"msg", "rejecting request with no cluster verification label", "clusterVerificationLabel", cluster, "method", info.FullMethod}
			errMsg = fmt.Sprintf("server from cluster %q rejected request with no cluster verification label", cluster)
		default:
			return nil, err
		}

		if logger != nil {
			level.Warn(logger).Log(msgs...)
		}
		if invalidClusters != nil {
			invalidClusters.WithLabelValues("grpc", info.FullMethod, reqCluster).Inc()
		}

		stat := grpcutil.Status(codes.FailedPrecondition, errMsg, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VERIFICATION_LABEL})
		return nil, stat.Err()
	}
}
