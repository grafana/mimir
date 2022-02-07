// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/fakeauth/fake_auth.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

// Package noauth provides middlewares thats injects a demo userID, so the rest of the code
// can continue to be multitenant.
package noauth

import (
	"context"
	"net/http"

	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"
)

const noAuthTenant = "demo"

// SetupAuthMiddleware for the given server config.
func SetupAuthMiddleware(config *server.Config, enabled bool, noGRPCAuthOn []string) middleware.Interface {
	if enabled {
		ignoredMethods := map[string]bool{}
		for _, m := range noGRPCAuthOn {
			ignoredMethods[m] = true
		}

		config.GRPCMiddleware = append(config.GRPCMiddleware, func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
			if ignoredMethods[info.FullMethod] {
				return handler(ctx, req)
			}
			return middleware.ServerUserHeaderInterceptor(ctx, req, info, handler)
		})

		config.GRPCStreamMiddleware = append(config.GRPCStreamMiddleware,
			func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				if ignoredMethods[info.FullMethod] {
					return handler(srv, ss)
				}
				return middleware.StreamServerUserHeaderInterceptor(srv, ss, info, handler)
			},
		)

		return middleware.AuthenticateUser
	}

	config.GRPCMiddleware = append(config.GRPCMiddleware,
		noAuthGRPCAuthUniaryMiddleware,
	)
	config.GRPCStreamMiddleware = append(config.GRPCStreamMiddleware,
		noAuthGRPCAuthStreamMiddleware,
	)
	return noAuthHTTPAuthMiddleware
}

var noAuthHTTPAuthMiddleware = middleware.Func(func(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := user.InjectOrgID(r.Context(), noAuthTenant)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
})

var noAuthGRPCAuthUniaryMiddleware = func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx = user.InjectOrgID(ctx, noAuthTenant)
	return handler(ctx, req)
}

var noAuthGRPCAuthStreamMiddleware = func(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := user.InjectOrgID(ss.Context(), noAuthTenant)
	return handler(srv, serverStream{
		ctx:          ctx,
		ServerStream: ss,
	})
}

type serverStream struct {
	ctx context.Context
	grpc.ServerStream
}

func (ss serverStream) Context() context.Context {
	return ss.ctx
}
