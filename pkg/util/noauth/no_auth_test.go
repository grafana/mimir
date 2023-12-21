package noauth

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"
)

func TestNewTenantValidationMiddleware(t *testing.T) {
	for _, tc := range []struct {
		name               string
		federation         bool
		maxTenants         int
		header             string
		expectedHTTPStatus int
		expectedBodyText   string
	}{
		{
			name:               "federation disabled, missing tenant header",
			federation:         false,
			maxTenants:         0,
			header:             "",
			expectedHTTPStatus: 401,
			expectedBodyText:   "no tenant ID present",
		},
		{
			name:               "federation disabled, invalid tenant header",
			federation:         false,
			maxTenants:         0,
			header:             strings.Repeat("123", tenant.MaxTenantIDLength),
			expectedHTTPStatus: 401,
			expectedBodyText:   "tenant ID is too long",
		},
		{
			name:               "federation disabled, single tenant",
			federation:         false,
			maxTenants:         0,
			header:             "tenant-a",
			expectedHTTPStatus: 200,
			expectedBodyText:   "",
		},
		{
			name:               "federation disabled, multiple tenants",
			federation:         false,
			maxTenants:         0,
			header:             "tenant-a|tenant-b",
			expectedHTTPStatus: 422,
			expectedBodyText:   "too many tenant IDs present",
		},
		{
			name:               "federation enabled, missing tenant header",
			federation:         true,
			maxTenants:         0,
			header:             "",
			expectedHTTPStatus: 401,
			expectedBodyText:   "no tenant ID present",
		},
		{
			name:               "federation enabled, invalid tenant header",
			federation:         true,
			maxTenants:         0,
			header:             strings.Repeat("123", tenant.MaxTenantIDLength),
			expectedHTTPStatus: 401,
			expectedBodyText:   "tenant ID is too long",
		},
		{
			name:               "federation enabled, single tenant no limit",
			federation:         true,
			maxTenants:         0,
			header:             "tenant-a",
			expectedHTTPStatus: 200,
			expectedBodyText:   "",
		},
		{
			name:               "federation enabled, multiple tenants no limit",
			federation:         true,
			maxTenants:         0,
			header:             "tenant-a|tenant-b|tenant-c",
			expectedHTTPStatus: 200,
			expectedBodyText:   "",
		},
		{
			name:               "federation enabled, multiple tenants under limit",
			federation:         true,
			maxTenants:         2,
			header:             "tenant-a|tenant-b",
			expectedHTTPStatus: 200,
			expectedBodyText:   "",
		},
		{
			name:               "federation enabled, multiple tenants over limit",
			federation:         true,
			maxTenants:         2,
			header:             "tenant-a|tenant-b|tenant-c",
			expectedHTTPStatus: 422,
			expectedBodyText:   "too many tenant IDs present",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nop := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {})
			handler := newTenantValidationMiddleware(tc.federation, tc.maxTenants).Wrap(nop)

			req := httptest.NewRequest("GET", "/", nil)
			req.Header.Set(user.OrgIDHeaderName, tc.header)
			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)
			body, err := io.ReadAll(resp.Body)

			require.NoError(t, err)
			require.Equal(t, tc.expectedHTTPStatus, resp.Code)

			if tc.expectedBodyText != "" {
				require.Contains(t, string(body), tc.expectedBodyText)
			}
		})
	}
}
