// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"

	"github.com/grafana/dskit/tenant"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/querier/tenantfederation"
)

const (
	tooManyTenantsTemplate = "too many tenant IDs present. max: %d actual: %d"
)

// federationMiddleware validates the number tenants in the provided context matches the
// configuration for tenant federation and that the tenant IDs are legal.
type federationMiddleware struct {
	next Handler
	cfg  tenantfederation.Config
}

func newFederationMiddleware(cfg tenantfederation.Config) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return &federationMiddleware{
			next: next,
			cfg:  cfg,
		}
	})
}

func (f *federationMiddleware) Do(ctx context.Context, r Request) (Response, error) {
	ids, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

	numIds := len(ids)
	if !f.cfg.Enabled && numIds > 1 {
		return nil, apierror.Newf(apierror.TypeExec, tooManyTenantsTemplate, 1, numIds)
	}

	if f.cfg.Enabled && f.cfg.MaxTenants > 0 && numIds > f.cfg.MaxTenants {
		return nil, apierror.Newf(apierror.TypeExec, tooManyTenantsTemplate, f.cfg.MaxTenants, numIds)
	}

	return f.next.Do(ctx, r)
}
