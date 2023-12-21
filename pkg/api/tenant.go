package api

import (
	"fmt"
	"net/http"

	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
)

const (
	tooManyTenantsTemplate = "too many tenant IDs present. max: %d actual: %d"
)

// NewTenantValidationMiddleware creates a new middleware that validates the number of tenants
// being accessed in a particular request is allowed given the current tenant federation configuration.
// Note that this middleware requires that tenant ID has been set on the request context by something
// like middleware.AuthenticateUser.
func NewTenantValidationMiddleware(federation bool, maxTenants int) middleware.Interface {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

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
