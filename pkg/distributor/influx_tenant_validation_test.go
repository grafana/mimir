// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"unicode/utf8"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"
)

func TestInfluxHandler_InvalidTenantID(t *testing.T) {
	for _, tenantID := range []string{"tenant-\xb5", "bad tenant", "tenant:source=\xb5"} {
		t.Run(fmt.Sprintf("%q", tenantID), func(t *testing.T) {
			called := false
			push := func(context.Context, *Request) error {
				called = true
				return nil
			}
			handler := InfluxHandler(1<<20, nil, nil, RetryConfig{}, push, nil, log.NewNopLogger())
			req := httptest.NewRequest(http.MethodPost, "/write", nil).WithContext(user.InjectOrgID(t.Context(), tenantID))
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			require.False(t, called, "invalid tenants must be rejected before ingestion or usage tracking")
			require.Equal(t, http.StatusUnauthorized, recorder.Code)
			require.True(t, utf8.Valid(recorder.Body.Bytes()))
		})
	}
}
