// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/fakeauth/fake_auth.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

// Package noauth provides middlewares that injects a tenant ID so the rest of the code
// can continue to be multitenant and validates the number of tenant IDs if supplied.
package noauth

import (
	"context"
	"fmt"
	"net/http"

	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/server"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"google.golang.org/grpc"
)

const (
	tooManyTenantsTemplate = "too many tenant IDs present. max: %d actual: %d"
	noTenantIDTemplate     = "no tenant ID present. set tenant ID using the %s header"
)

// SetupTenantMiddleware for the given server config.
func SetupTenantMiddleware(config *server.Config, multitenancyEnabled bool, noMultitenancyTenant string, federationEnabled bool, federationMaxTenants int, noGRPCAuthOn []string) middleware.Interface {
	if multitenancyEnabled {
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

		return newTenantValidationMiddleware(federationEnabled, federationMaxTenants)
	}

	config.GRPCMiddleware = append(config.GRPCMiddleware,
		func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			ctx = user.InjectOrgID(ctx, noMultitenancyTenant)
			return handler(ctx, req)
		},
	)
	config.GRPCStreamMiddleware = append(config.GRPCStreamMiddleware,
		func(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			ctx := user.InjectOrgID(ss.Context(), noMultitenancyTenant)

			return handler(srv, serverStream{
				ctx:          ctx,
				ServerStream: ss,
			})
		},
	)
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := user.InjectOrgID(r.Context(), noMultitenancyTenant)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	})

}

func newTenantValidationMiddleware(federation bool, maxTenants int) middleware.Interface {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, ctx, err := user.ExtractOrgIDFromHTTPRequest(r)
			if err != nil {
				http.Error(w, fmt.Sprintf(noTenantIDTemplate, user.OrgIDHeaderName), http.StatusUnauthorized)
				return
			}

			ids, err := tenant.TenantIDs(ctx)
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}

			numIds := len(ids)
			if !federation && numIds > 1 {
				http.Error(w, fmt.Sprintf(tooManyTenantsTemplate, 1, numIds), http.StatusUnprocessableEntity)
				return
			}

			if federation && maxTenants > 0 && numIds > maxTenants {
				http.Error(w, fmt.Sprintf(tooManyTenantsTemplate, maxTenants, numIds), http.StatusUnprocessableEntity)
				return
			}

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	})
}

type serverStream struct {
	ctx context.Context
	grpc.ServerStream
}

func (ss serverStream) Context() context.Context {
	return ss.ctx
}
