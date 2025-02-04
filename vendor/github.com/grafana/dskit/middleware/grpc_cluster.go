package middleware

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/dskit/grpcutil"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

const (
	MetadataClusterKey = "x-cluster"
)

// ClusterUnaryClientInterceptor propagates the given cluster info to gRPC metadata.
func ClusterUnaryClientInterceptor(cluster string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if cluster != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, MetadataClusterKey, cluster)
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// ClusterUnaryServerInterceptor checks if the incoming gRPC metadata contains any cluster information and if so,
// checks if the latter corresponds to the given cluster. If it is the case, the request is further propagated.
// Otherwise, an error is returned.
func ClusterUnaryServerInterceptor(cluster string, invalidClusters *prometheus.CounterVec, logger log.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if _, ok := info.Server.(healthpb.HealthServer); ok {
			return handler(ctx, req)
		}
		reqCluster := getClusterFromIncomingContext(ctx, logger)
		if cluster != reqCluster {
			level.Warn(logger).Log("msg", "rejecting request intended for wrong cluster",
				"cluster", cluster, "request_cluster", reqCluster, "method", info.FullMethod)
			if invalidClusters != nil {
				invalidClusters.WithLabelValues("grpc", info.FullMethod, reqCluster).Inc()
			}
			msg := fmt.Sprintf("request intended for cluster %q - this is cluster %q", reqCluster, cluster)
			stat := grpcutil.Status(codes.FailedPrecondition, msg, &grpcutil.ErrorDetails{Cause: grpcutil.WRONG_CLUSTER_NAME})
			return nil, stat.Err()
		}
		return handler(ctx, req)
	}
}

func getClusterFromIncomingContext(ctx context.Context, logger log.Logger) string {
	clusterIDs := metadata.ValueFromIncomingContext(ctx, MetadataClusterKey)
	if len(clusterIDs) != 1 {
		msg := fmt.Sprintf("gRPC metadata should contain exactly 1 value for key \"%s\", but the current set of values is %v. Returning an empty string.", MetadataClusterKey, clusterIDs)
		level.Warn(logger).Log("msg", msg)
		return ""
	}
	return clusterIDs[0]
}
