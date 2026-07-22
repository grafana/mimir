// SPDX-License-Identifier: AGPL-3.0-only

package readtee

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u
}

func TestNewProxy_Validation(t *testing.T) {
	logger := log.NewNopLogger()

	tests := []struct {
		name        string
		cfg         ProxyConfig
		expectedErr string
	}{
		{
			name: "no endpoint",
			cfg: ProxyConfig{
				BackendEndpoint:            "",
				AmplificationFactor:        1.0,
				AsyncMaxInFlightPerBackend: 1000,
			},
			expectedErr: "backend endpoint is required",
		},
		{
			name: "invalid backend URL",
			cfg: ProxyConfig{
				BackendEndpoint:            "://invalid-url",
				AmplificationFactor:        1.0,
				AsyncMaxInFlightPerBackend: 1000,
			},
			expectedErr: "invalid backend endpoint",
		},
		{
			name: "unsupported scheme (gRPC not supported)",
			cfg: ProxyConfig{
				BackendEndpoint:            "dns:///backend1:8080",
				AmplificationFactor:        1.0,
				AsyncMaxInFlightPerBackend: 1000,
			},
			expectedErr: "unsupported backend scheme",
		},
		{
			name: "amplification factor below 1",
			cfg: ProxyConfig{
				BackendEndpoint:            "http://backend1:8080",
				AmplificationFactor:        0.5,
				AsyncMaxInFlightPerBackend: 1000,
			},
			expectedErr: "amplification-factor must be >= 1",
		},
		{
			name: "valid single endpoint",
			cfg: ProxyConfig{
				BackendEndpoint:            "http://backend1:8080",
				AmplificationFactor:        1.0,
				AsyncMaxInFlightPerBackend: 1000,
			},
			expectedErr: "",
		},
		{
			name: "valid with amplification",
			cfg: ProxyConfig{
				BackendEndpoint:            "https://backend1:8080",
				AmplificationFactor:        3.0,
				AsyncMaxInFlightPerBackend: 1000,
			},
			expectedErr: "",
		},
		{
			name: "negative async max in-flight",
			cfg: ProxyConfig{
				BackendEndpoint:            "http://backend1:8080",
				AmplificationFactor:        1.0,
				AsyncMaxInFlightPerBackend: -1,
			},
			expectedErr: "backend.async-max-in-flight must be greater than 0",
		},
		{
			name: "zero async max in-flight",
			cfg: ProxyConfig{
				BackendEndpoint:            "http://backend1:8080",
				AmplificationFactor:        1.0,
				AsyncMaxInFlightPerBackend: 0,
			},
			expectedErr: "backend.async-max-in-flight must be greater than 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			_, err := NewProxy(tt.cfg, logger, nil, registry)
			if tt.expectedErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			}
		})
	}
}

func TestProxyEndpoint_Response(t *testing.T) {
	logger := log.NewNopLogger()

	tests := []struct {
		name               string
		backendStatus      int
		backendBody        string
		expectedStatusCode int
	}{
		{
			name:               "backend succeeds",
			backendStatus:      200,
			backendBody:        "ok",
			expectedStatusCode: 200,
		},
		{
			name:               "backend fails, status returned to client",
			backendStatus:      500,
			backendBody:        "error",
			expectedStatusCode: 500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			metrics := NewProxyMetrics(registry)

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.backendStatus)
				_, _ = w.Write([]byte(tt.backendBody))
			}))
			defer server.Close()

			backend := NewHTTPProxyBackend("backend1", mustParseURL(t, server.URL), 5*time.Second, false)

			route := Route{
				Path:      "/api/v1/query",
				RouteName: "api_v1_query",
				Methods:   []string{"GET"},
			}

			asyncDispatcher := NewAsyncBackendDispatcher(1000, metrics, logger)
			defer asyncDispatcher.Stop()

			endpoint := NewProxyEndpoint(backend, route, metrics, logger, 1.0, rewriteOptions{}, asyncDispatcher)

			req := httptest.NewRequest("GET", `/api/v1/query?query=up`, nil)
			rec := httptest.NewRecorder()

			endpoint.ServeHTTP(rec, req)

			assert.Equal(t, tt.expectedStatusCode, rec.Code)
		})
	}
}

// TestProxyEndpoint_Amplification verifies that a single incoming read fans out to exactly N
// requests to the one backend endpoint for integer amplification factor N: one unmodified original
// (sent synchronously) plus N-1 rewritten copies suffixed _amp1.._amp{N-1} (sent asynchronously).
func TestProxyEndpoint_Amplification(t *testing.T) {
	logger := log.NewNopLogger()

	const originalQuery = `up{job="api"}`

	tests := []struct {
		factor        float64
		expectedTotal int
		// expectedAmp are the rewritten queries expected in addition to the unmodified original.
		expectedAmp []string
	}{
		{
			factor:        1.0,
			expectedTotal: 1,
			expectedAmp:   nil,
		},
		{
			factor:        2.0,
			expectedTotal: 2,
			expectedAmp:   []string{`up{job="api_amp1"}`},
		},
		{
			factor:        3.0,
			expectedTotal: 3,
			expectedAmp:   []string{`up{job="api_amp1"}`, `up{job="api_amp2"}`},
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("factor_%.0f", tt.factor), func(t *testing.T) {
			registry := prometheus.NewRegistry()
			metrics := NewProxyMetrics(registry)

			var mu sync.Mutex
			var queries []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				queries = append(queries, r.URL.Query().Get("query"))
				mu.Unlock()
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("ok"))
			}))
			defer server.Close()

			backend := NewHTTPProxyBackend("backend1", mustParseURL(t, server.URL), 5*time.Second, false)

			route := Route{
				Path:      "/api/v1/query",
				RouteName: "api_v1_query",
				Methods:   []string{"GET"},
			}

			asyncDispatcher := NewAsyncBackendDispatcher(1000, metrics, logger)

			endpoint := NewProxyEndpoint(backend, route, metrics, logger, tt.factor, rewriteOptions{}, asyncDispatcher)

			req := httptest.NewRequest("GET", "/api/v1/query?query="+url.QueryEscape(originalQuery), nil)
			rec := httptest.NewRecorder()

			endpoint.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			// Drain async (fire-and-forget) amplified copies before asserting counts.
			asyncDispatcher.Stop()
			asyncDispatcher.Await()

			mu.Lock()
			got := append([]string(nil), queries...)
			mu.Unlock()

			// Total requests to the one backend must equal N.
			require.Len(t, got, tt.expectedTotal)

			// Exactly one request must be the unmodified original; the rest must be the amplified copies.
			var originals int
			amp := map[string]bool{}
			for _, q := range got {
				if q == originalQuery {
					originals++
					continue
				}
				require.True(t, strings.Contains(q, "_amp"), "unexpected non-amplified, non-original query: %q", q)
				amp[q] = true
			}
			require.Equal(t, 1, originals, "expected exactly one unmodified original request, got queries: %v", got)

			for _, want := range tt.expectedAmp {
				require.True(t, amp[want], "expected amplified copy %q among %v", want, got)
			}
			require.Len(t, amp, len(tt.expectedAmp))
		})
	}
}
