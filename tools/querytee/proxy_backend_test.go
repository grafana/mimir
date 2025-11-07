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

	"github.com/grafana/dskit/clusterutil"
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

			b := NewProxyBackend("test", u, time.Second, false, false, "", defaultBackendConfig())
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

func Test_ProxyBackend_RequestProportion(t *testing.T) {
	tests := []struct {
		name               string
		config             BackendConfig
		expectedProportion float64
		setProportion      *float64
		finalProportion    float64
	}{
		{
			name:               "default proportion when not configured",
			config:             BackendConfig{},
			expectedProportion: DefaultRequestProportion,
		},
		{
			name: "configured proportion from config",
			config: BackendConfig{
				RequestProportion: pointerToFloat(0.7),
			},
			expectedProportion: 0.7,
		},
		{
			name:               "set proportion via method",
			config:             BackendConfig{},
			expectedProportion: DefaultRequestProportion,
			setProportion:      pointerToFloat(0.3),
			finalProportion:    0.3,
		},
		{
			name: "explicitly configured proportion of 1.0",
			config: BackendConfig{
				RequestProportion: pointerToFloat(DefaultRequestProportion),
			},
			expectedProportion: DefaultRequestProportion,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.Parse("http://localhost:9090")
			require.NoError(t, err)

			backend := NewProxyBackend("test", u, time.Second, false, false, "", tc.config)

			assert.Equal(t, tc.expectedProportion, backend.RequestProportion())

			// Check if proportion was configured
			expectedConfigured := tc.config.RequestProportion != nil
			assert.Equal(t, expectedConfigured, backend.HasConfiguredProportion())

			if tc.setProportion != nil {
				backend.SetRequestProportion(*tc.setProportion)
				assert.Equal(t, tc.finalProportion, backend.RequestProportion())
			}
		})
	}
}

func Test_ProxyBackend_ClusterValidationLabel(t *testing.T) {
	tests := map[string]struct {
		clusterLabel           string
		expectedXClusterHeader string
		shouldHaveXCluster     bool
	}{
		"no cluster label set": {
			clusterLabel:       "",
			shouldHaveXCluster: false,
		},
		"cluster label set": {
			clusterLabel:           "test-cluster",
			expectedXClusterHeader: "test-cluster",
			shouldHaveXCluster:     true,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			backend := httptest.NewServer(nil)
			defer backend.Close()

			u, err := url.Parse(backend.URL)
			require.NoError(t, err)

			b := NewProxyBackend("test", u, time.Second, false, false, tc.clusterLabel, defaultBackendConfig())

			req := httptest.NewRequest("GET", "http://test/api/v1/query", nil)

			backendReq, err := b.(*ProxyBackend).createBackendRequest(context.Background(), req, nil)
			require.NoError(t, err)

			actualHeader := backendReq.Header.Get(clusterutil.ClusterValidationLabelHeader)
			if tc.shouldHaveXCluster {
				assert.Equal(t, tc.expectedXClusterHeader, actualHeader)
			} else {
				assert.Empty(t, actualHeader)
			}
		})
	}
}

func defaultBackendConfig() BackendConfig {
	return BackendConfig{}
}
