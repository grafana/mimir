// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"context"

	"github.com/grafana/dskit/ring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/tap"
)

// Used to communicate, via a context, when this is the only replica serving a request.
type onlyReplicaContextKey struct{}

const (
	onlyReplicaGrpcMdKey = "__only_replica__"
)

// OnlyReplicaClientUnaryInterceptor propagates an only-replica marker from the context to the outgoing gRPC request metadata.
func OnlyReplicaClientUnaryInterceptor(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if replicas, ok := ring.GetAvailableReplicas(ctx); replicas == 1 && ok {
		ctx = metadata.AppendToOutgoingContext(ctx, onlyReplicaGrpcMdKey, "true")
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

// OnlyReplicaServerUnaryInterceptor propagates an only-replica marker from the incoming gRPC request metadata to the
// handling context.
func OnlyReplicaServerUnaryInterceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return handler(ctxFromMetadata(ctx), req)
}

// OnlyReplicaTapHandle propagates an only-replica marker from the incoming gRPC request metadata to the handling
// context, in preparation for use by any tap handle.
func OnlyReplicaTapHandle(ctx context.Context, _ *tap.Info) (context.Context, error) {
	return ctxFromMetadata(ctx), nil
}

// OnlyReplicaClientStreamInterceptor propagates an only-replica marker from the context to the outgoing gRPC request
// metadata.
func OnlyReplicaClientStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if replicas, ok := ring.GetAvailableReplicas(ctx); replicas == 1 && ok {
		ctx = metadata.AppendToOutgoingContext(ctx, onlyReplicaGrpcMdKey, "true")
	}
	return streamer(ctx, desc, cc, method, opts...)
}

// OnlyReplicaServerStreamInterceptor propagates an only-replica marker from the incoming gRPC request metadata to the
// handling context.
func OnlyReplicaServerStreamInterceptor(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return handler(srv, ctxStream{
		ctx:          ctxFromMetadata(ss.Context()),
		ServerStream: ss,
	})
}

// ContextWithOnlyReplica returns a new context that an ingester is the only replica handling a request. This can later
// be checked via IsOnlyReplica.
func ContextWithOnlyReplica(ctx context.Context) context.Context {
	return context.WithValue(ctx, onlyReplicaContextKey{}, true)
}

// IsOnlyReplicaContext retrurns whether the context indicates that the ingester is the only replica handling a request.
func IsOnlyReplicaContext(ctx context.Context) bool {
	result, ok := ctx.Value(onlyReplicaContextKey{}).(bool)
	return result && ok
}

func ctxFromMetadata(ctx context.Context) context.Context {
	onlyReplica := metadata.ValueFromIncomingContext(ctx, onlyReplicaGrpcMdKey)
	if len(onlyReplica) > 0 && onlyReplica[0] == "true" {
		return ContextWithOnlyReplica(ctx)
	}
	return ctx
}

type ctxStream struct {
	ctx context.Context
	grpc.ServerStream
}

func (ss ctxStream) Context() context.Context {
	return ss.ctx
}
