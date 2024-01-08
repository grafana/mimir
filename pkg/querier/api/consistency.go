// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/grafana/dskit/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/util"
)

const (
	ReadConsistencyHeader = "X-Read-Consistency"

	// ReadConsistencyStrong means that a query sent by the same client will always observe the writes
	// that have completed before issuing the query.
	ReadConsistencyStrong = "strong"

	// ReadConsistencyEventual is the default consistency level for all queries.
	// This level means that a query sent by a client may not observe some of the writes that the same client has recently made.
	ReadConsistencyEventual = "eventual"
)

var ReadConsistencies = []string{ReadConsistencyStrong, ReadConsistencyEventual}

func IsValidReadConsistency(lvl string) bool {
	return util.StringsContain(ReadConsistencies, lvl)
}

type contextKey int

const consistencyContextKey contextKey = 1

// ContextWithReadConsistency returns a new context with the given consistency level.
// The consistency level can be retrieved with ReadConsistencyFromContext.
func ContextWithReadConsistency(parent context.Context, level string) context.Context {
	return context.WithValue(parent, consistencyContextKey, level)
}

// ReadConsistencyFromContext returns the consistency level from the context if set via ContextWithReadConsistency.
// The second return value is true if the consistency level was found in the context and is valid.
func ReadConsistencyFromContext(ctx context.Context) (string, bool) {
	level, _ := ctx.Value(consistencyContextKey).(string)
	return level, IsValidReadConsistency(level)
}

// ConsistencyMiddleware takes the consistency level from the X-Read-Consistency header and sets it in the context.
// It can be retrieved with ReadConsistencyFromContext.
func ConsistencyMiddleware() middleware.Interface {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if c := r.Header.Get(ReadConsistencyHeader); IsValidReadConsistency(c) {
				r = r.WithContext(ContextWithReadConsistency(r.Context(), c))
				fmt.Println("ConsistencyMiddleware", c)
			} else {
				fmt.Println("ConsistencyMiddleware", c, "invalid/not set")
			}
			next.ServeHTTP(w, r)
		})
	})
}

const consistencyLevelGrpcMdKey = "__consistency_level__"

func ReadConsistencyClientInterceptor(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if c, ok := ReadConsistencyFromContext(ctx); ok {
		fmt.Println("ReadConsistencyClientInterceptor", c)
		ctx = metadata.AppendToOutgoingContext(ctx, consistencyLevelGrpcMdKey, c)
	} else {
		fmt.Println("ReadConsistencyClientInterceptor", c, "invalid/not set")
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

func ReadConsistencyServerInterceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	consistencies := md.Get(consistencyLevelGrpcMdKey)
	if len(consistencies) > 0 && IsValidReadConsistency(consistencies[0]) {
		fmt.Println("ReadConsistencyServerInterceptor", consistencies[0])
		ctx = ContextWithReadConsistency(ctx, consistencies[0])
	} else {
		fmt.Println("ReadConsistencyServerInterceptor", consistencies, "invalid/not set")
	}
	return handler(ctx, req)
}

func ReadConsistencyClientStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if c, ok := ReadConsistencyFromContext(ctx); ok {
		ctx = metadata.AppendToOutgoingContext(ctx, consistencyLevelGrpcMdKey, c)
	}
	return streamer(ctx, desc, cc, method, opts...)
}

func ReadConsistencyServerStreamInterceptor(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	md, _ := metadata.FromIncomingContext(ss.Context())
	consistencies := md.Get(consistencyLevelGrpcMdKey)
	if len(consistencies) > 0 && IsValidReadConsistency(consistencies[0]) {
		ctx := ContextWithReadConsistency(ss.Context(), consistencies[0])
		ss = ctxStream{
			ctx:          ctx,
			ServerStream: ss,
		}
	}
	return handler(srv, ss)
}

type ctxStream struct {
	ctx context.Context
	grpc.ServerStream
}

func (ss ctxStream) Context() context.Context {
	return ss.ctx
}
