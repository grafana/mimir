// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortex/tracing.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimir

import (
	"context"

	"github.com/opentracing/opentracing-go"
	objstoretracing "github.com/thanos-io/objstore/tracing/opentracing"
	"google.golang.org/grpc"
)

// ThanosTracerUnaryInterceptor injects the opentracing global tracer into the context
// in order to get it picked up by Thanos components.
func ThanosTracerUnaryInterceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return handler(objstoretracing.ContextWithTracer(ctx, opentracing.GlobalTracer()), req)
}

// ThanosTracerStreamInterceptor injects the opentracing global tracer into the context
// in order to get it picked up by Thanos components.
func ThanosTracerStreamInterceptor(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return handler(srv, wrappedServerStream{
		ctx:          objstoretracing.ContextWithTracer(ss.Context(), opentracing.GlobalTracer()),
		ServerStream: ss,
	})
}

type wrappedServerStream struct {
	ctx context.Context
	grpc.ServerStream
}

func (ss wrappedServerStream) Context() context.Context {
	return ss.ctx
}
