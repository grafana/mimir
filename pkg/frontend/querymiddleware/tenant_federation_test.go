// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"strings"
	"testing"

	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/querier/tenantfederation"
)

func TestTenantFederationMiddleware(t *testing.T) {
	for _, tc := range []struct {
		name      string
		config    tenantfederation.Config
		tenant    string
		errorType apierror.Type
	}{
		{
			name: "single tenant, federation disabled",
			config: tenantfederation.Config{
				Enabled: false,
			},
			tenant:    "123",
			errorType: apierror.TypeNone,
		},
		{
			name: "multiple tenants, federation disabled",
			config: tenantfederation.Config{
				Enabled: false,
			},
			tenant:    "123|456",
			errorType: apierror.TypeExec,
		},
		{
			name: "invalid tenant, federation disabled",
			config: tenantfederation.Config{
				Enabled: false,
			},
			tenant:    strings.Repeat("123", tenant.MaxTenantIDLength),
			errorType: apierror.TypeInternal,
		},

		{
			name: "single tenant, federation enabled",
			config: tenantfederation.Config{
				Enabled: true,
			},
			tenant:    "123",
			errorType: apierror.TypeNone,
		},
		{
			name: "multiple tenants, federation enabled",
			config: tenantfederation.Config{
				Enabled: true,
			},
			tenant:    "123|456|789",
			errorType: apierror.TypeNone,
		},
		{
			name: "too many tenants, federation enabled",
			config: tenantfederation.Config{
				Enabled:    true,
				MaxTenants: 2,
			},
			tenant:    "123|456|789",
			errorType: apierror.TypeExec,
		},
		{
			name: "invalid tenant, federation enabled",
			config: tenantfederation.Config{
				Enabled: true,
			},
			tenant:    strings.Repeat("123", tenant.MaxTenantIDLength),
			errorType: apierror.TypeInternal,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				return nil, nil
			})

			ctx := user.InjectOrgID(context.Background(), tc.tenant)
			handler := newFederationMiddleware(tc.config).Wrap(next)

			_, err := handler.Do(ctx, &PrometheusRangeQueryRequest{})

			if tc.errorType != apierror.TypeNone {
				var apiErr *apierror.APIError
				require.ErrorAs(t, err, &apiErr)
				require.Equal(t, tc.errorType, apiErr.Type)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
