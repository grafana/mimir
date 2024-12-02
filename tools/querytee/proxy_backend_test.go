// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/tools/querytee/proxy_backend_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querytee

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ProxyBackend_createBackendRequest_HTTPBasicAuthentication(t *testing.T) {
	tests := map[string]struct {
		clientUser   string
		clientPass   string
		clientTenant string
		backendUser  string
		backendPass  string
		expectedUser string
		expectedPass string
	}{
		"no auth": {
			expectedUser: "",
			expectedPass: "",
		},
		"if the request is authenticated and the backend has no auth it should forward the request auth": {
			clientUser: "marco",
			clientPass: "marco-secret",

			expectedUser: "marco",
			expectedPass: "marco-secret",
		},
		"if the request is authenticated and the backend has an username set it should forward the request password only": {
			clientUser:  "marco",
			clientPass:  "marco-secret",
			backendUser: "backend",

			expectedUser: "backend",
			expectedPass: "marco-secret",
		},
		"if the request is authenticated and the backend is authenticated it should use the backend auth": {
			clientUser:  "marco",
			clientPass:  "marco-secret",
			backendUser: "backend",
			backendPass: "backend-secret",

			expectedUser: "backend",
			expectedPass: "backend-secret",
		},
		"if the request is NOT authenticated and the backend is authenticated it should use the backend auth": {
			backendUser: "backend",
			backendPass: "backend-secret",

			expectedUser: "backend",
			expectedPass: "backend-secret",
		},
		"if the request is NOT authenticated and the backend has __REQUEST_HEADER_X_SCOPE_ORGID__ as the user, it should use the tenant ID from the request as the user": {
			clientUser:   "dimitar",
			clientTenant: "123",
			backendUser:  "__REQUEST_HEADER_X_SCOPE_ORGID__",
			backendPass:  "123-secret",

			expectedUser: "123",
			expectedPass: "123-secret",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			u, err := url.Parse(fmt.Sprintf("http://%s:%s@test", testData.backendUser, testData.backendPass))
			require.NoError(t, err)

			orig := httptest.NewRequest("GET", "/test", nil)
			orig.SetBasicAuth(testData.clientUser, testData.clientPass)
			if testData.clientTenant != "" {
				orig.Header.Set("X-Scope-OrgID", testData.clientTenant)
			}

			b := NewProxyBackend("test", u, time.Second, false, false, BackendConfig{})
			bp, ok := b.(*ProxyBackend)
			if !ok {
				t.Fatalf("Type assertion to *ProxyBackend failed")
			}
			r, err := bp.createBackendRequest(context.Background(), orig, nil)
			require.NoError(t, err)

			actualUser, actualPass, _ := r.BasicAuth()
			assert.Equal(t, testData.expectedUser, actualUser)
			assert.Equal(t, testData.expectedPass, actualPass)
		})
	}
}
