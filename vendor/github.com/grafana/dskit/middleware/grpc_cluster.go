package middleware

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/dskit/clusterutil"
	"github.com/grafana/dskit/grpcutil"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// ClusterUnaryClientInterceptor propagates the given cluster label to gRPC metadata, before calling the next invoker.
// If an empty cluster label, nil invalidCounter or nil logger are provided, ClusterUnaryClientInterceptor panics.
// In case of an error related to the cluster label validation, the error is returned, and invalidCounter is incremented.
func ClusterUnaryClientInterceptor(cluster string, invalidCluster *prometheus.CounterVec, logger log.Logger) grpc.UnaryClientInterceptor {
	validateClusterClientInterceptorInputParameters(cluster, invalidCluster, logger)
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = clusterutil.PutClusterIntoOutgoingContext(ctx, cluster)
		return handleClusterValidationError(invoker(ctx, method, req, reply, cc, opts...), cluster, method, invalidCluster, logger)
	}
}

func validateClusterClientInterceptorInputParameters(cluster string, invalidCluster *prometheus.CounterVec, logger log.Logger) {
	if cluster == "" {
		panic("no cluster label provided")
	}
	if invalidCluster == nil {
		panic("no invalid cluster counter provided")
	}
	if logger == nil {
		panic("no logger provided")
	}
}

func handleClusterValidationError(err error, cluster string, method string, invalidCluster *prometheus.CounterVec, logger log.Logger) error {
	if err == nil {
		return nil
	}
	if stat, ok := grpcutil.ErrorToStatus(err); ok {
		details := stat.Details()
		if len(details) == 1 {
			if errDetails, ok := details[0].(*grpcutil.ErrorDetails); ok {
				if errDetails.GetCause() == grpcutil.WRONG_CLUSTER_VALIDATION_LABEL {
					msg := fmt.Sprintf("request rejected by the server: %s", stat.Message())
					level.Warn(logger).Log("msg", msg, "method", method, "clusterValidationLabel", cluster)
					invalidCluster.WithLabelValues(method).Inc()
					return grpcutil.Status(codes.Internal, msg).Err()
				}
			}
		}
	}
	return err
}

// ClusterUnaryServerInterceptor checks if the incoming gRPC metadata contains any cluster label and if so, checks if
// the latter corresponds to the given cluster label. If it is the case, the request is further propagated.
// If an empty cluster label or nil logger are provided, ClusterUnaryServerInterceptor panics.
// If the softValidation parameter is true, errors related to the cluster label validation are logged, but not returned.
// Otherwise, an error is returned.
func ClusterUnaryServerInterceptor(cluster string, softValidation bool, logger log.Logger) grpc.UnaryServerInterceptor {
	validateClusterServerInterceptorInputParameters(cluster, logger)
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// We skip the gRPC health check.
		if _, ok := info.Server.(healthpb.HealthServer); ok {
			return handler(ctx, req)
		}

		msgs, err := checkClusterFromIncomingContext(ctx, info.FullMethod, cluster, softValidation)
		if len(msgs) > 0 {
			level.Warn(logger).Log(msgs...)
		}
		if err != nil {
			stat := grpcutil.Status(codes.FailedPrecondition, err.Error(), &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_VALIDATION_LABEL})
			return nil, stat.Err()
		}
		return handler(ctx, req)
	}
}

func validateClusterServerInterceptorInputParameters(cluster string, logger log.Logger) {
	if cluster == "" {
		panic("no cluster label provided")
	}
	if logger == nil {
		panic("no logger provided")
	}
}

func checkClusterFromIncomingContext(ctx context.Context, method string, expectedCluster string, softValidationEnabled bool) ([]any, error) {
	reqCluster, err := clusterutil.GetClusterFromIncomingContext(ctx)
	if err == nil {
		if reqCluster == expectedCluster {
			return nil, nil
		}
		var wrongClusterErr error
		if !softValidationEnabled {
			wrongClusterErr = fmt.Errorf("rejected request with wrong cluster validation label %q - it should be %q", reqCluster, expectedCluster)
		}
		return []any{"msg", "request with wrong cluster validation label", "method", method, "clusterValidationLabel", expectedCluster, "requestClusterValidationLabel", reqCluster, "softValidation", softValidationEnabled}, wrongClusterErr
	}

	if errors.Is(err, clusterutil.ErrNoClusterValidationLabel) {
		var emptyClusterErr error
		if !softValidationEnabled {
			emptyClusterErr = fmt.Errorf("rejected request with empty cluster validation label - it should be %q", expectedCluster)
		}
		return []any{"msg", "request with no cluster validation label", "method", method, "clusterValidationLabel", expectedCluster, "softValidation", softValidationEnabled}, emptyClusterErr
	}
	var rejectedRequestErr error
	if !softValidationEnabled {
		rejectedRequestErr = fmt.Errorf("rejected request: %w", err)
	}
	return []any{"msg", "detected error during cluster validation label extraction", "method", method, "clusterValidationLabel", expectedCluster, "softValidation", softValidationEnabled, "err", err}, rejectedRequestErr
}
