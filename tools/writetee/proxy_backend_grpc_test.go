// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/httpgrpc"
	httpgrpcserver "github.com/grafana/dskit/httpgrpc/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestGRPCProxyBackend_ForwardRequest(t *testing.T) {
	// Create a test HTTP handler that will be wrapped by HTTPgRPC server.
	var receivedBody []byte
	var receivedHeaders http.Header
	var receivedPath string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		receivedHeaders = r.Header.Clone()
		body, _ := io.ReadAll(r.Body)
		receivedBody = body
		w.Header().Set("X-Custom-Header", "test-value")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("response body"))
	})

	// Start a gRPC server with HTTPgRPC handler.
	grpcServer, addr := startTestGRPCServer(t, handler)
	defer grpcServer.Stop()

	// Create a gRPC backend pointing to the test server.
	endpoint := mustParseURL("dns://" + addr)
	backend, err := NewGRPCProxyBackend("test-backend", endpoint, 5*time.Second, false, BackendTypeMirrored, GRPCBackendConfig{
		MaxRecvMsgSize: 100 * 1024 * 1024,
		MaxSendMsgSize: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer backend.Close()

	// Create a test request.
	reqBody := []byte("test request body")
	origReq := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader(reqBody))
	origReq.Header.Set("Content-Type", "application/x-protobuf")
	origReq.Header.Set("X-Scope-OrgID", "test-tenant")

	// Forward the request.
	elapsed, status, respBody, respHeaders, err := backend.ForwardRequest(
		context.Background(),
		origReq,
		io.NopCloser(bytes.NewReader(reqBody)),
	)

	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "response body", string(respBody))
	assert.Equal(t, "test-value", respHeaders.Get("X-Custom-Header"))
	assert.True(t, elapsed > 0)

	// Verify the request was forwarded correctly.
	assert.Equal(t, "/api/v1/push", receivedPath)
	assert.Equal(t, reqBody, receivedBody)
	assert.Equal(t, "application/x-protobuf", receivedHeaders.Get("Content-Type"))
	assert.Equal(t, "test-tenant", receivedHeaders.Get("X-Scope-OrgID"))
}

func TestGRPCProxyBackend_OrgIDPropagation(t *testing.T) {
	// HTTPgRPC backends use X-Scope-OrgID header for tenant identification,
	// not Basic Auth. The org ID is propagated via gRPC metadata.
	var capturedOrgID string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedOrgID = r.Header.Get("X-Scope-OrgID")
		w.WriteHeader(http.StatusOK)
	})

	grpcServer, addr := startTestGRPCServer(t, handler)
	defer grpcServer.Stop()

	endpoint := mustParseURL("dns://" + addr)
	backend, err := NewGRPCProxyBackend("test-backend", endpoint, 5*time.Second, false, BackendTypeMirrored, GRPCBackendConfig{
		MaxRecvMsgSize: 100 * 1024 * 1024,
		MaxSendMsgSize: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer backend.Close()

	// Create test request with org ID.
	origReq := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader([]byte("test")))
	origReq.Header.Set("X-Scope-OrgID", "test-tenant")

	// Forward request.
	_, _, _, _, err = backend.ForwardRequest(
		context.Background(),
		origReq,
		io.NopCloser(bytes.NewReader([]byte("test"))),
	)
	require.NoError(t, err)

	assert.Equal(t, "test-tenant", capturedOrgID)
}

func TestGRPCProxyBackend_AuthorizationHeaderStripped(t *testing.T) {
	// HTTPgRPC backends don't use Authorization headers - they use X-Scope-OrgID.
	// Verify that Authorization headers are stripped from forwarded requests.
	var hasAuthHeader bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hasAuthHeader = r.Header.Get("Authorization") != ""
		w.WriteHeader(http.StatusOK)
	})

	grpcServer, addr := startTestGRPCServer(t, handler)
	defer grpcServer.Stop()

	endpoint := mustParseURL("dns://" + addr)
	backend, err := NewGRPCProxyBackend("test-backend", endpoint, 5*time.Second, false, BackendTypeMirrored, GRPCBackendConfig{
		MaxRecvMsgSize: 100 * 1024 * 1024,
		MaxSendMsgSize: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer backend.Close()

	// Create test request with Basic Auth.
	origReq := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader([]byte("test")))
	origReq.Header.Set("X-Scope-OrgID", "test-tenant")
	origReq.SetBasicAuth("user", "pass")

	// Forward request.
	_, _, _, _, err = backend.ForwardRequest(
		context.Background(),
		origReq,
		io.NopCloser(bytes.NewReader([]byte("test"))),
	)
	require.NoError(t, err)

	// Authorization header should be stripped.
	assert.False(t, hasAuthHeader, "Authorization header should be stripped for HTTPgRPC backends")
}

func TestGRPCProxyBackend_ErrorHandling(t *testing.T) {
	// Create a handler that returns an error status.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad request error"))
	})

	grpcServer, addr := startTestGRPCServer(t, handler)
	defer grpcServer.Stop()

	endpoint := mustParseURL("dns://" + addr)
	backend, err := NewGRPCProxyBackend("test-backend", endpoint, 5*time.Second, false, BackendTypeMirrored, GRPCBackendConfig{
		MaxRecvMsgSize: 100 * 1024 * 1024,
		MaxSendMsgSize: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer backend.Close()

	origReq := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader([]byte("test")))
	origReq.Header.Set("X-Scope-OrgID", "test-tenant")

	_, status, respBody, _, err := backend.ForwardRequest(
		context.Background(),
		origReq,
		io.NopCloser(bytes.NewReader([]byte("test"))),
	)

	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "bad request error", string(respBody))
}

func TestGRPCProxyBackend_5xxError(t *testing.T) {
	// Create a handler that returns a 5xx error - these are returned as gRPC errors.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal server error"))
	})

	grpcServer, addr := startTestGRPCServer(t, handler)
	defer grpcServer.Stop()

	endpoint := mustParseURL("dns://" + addr)
	backend, err := NewGRPCProxyBackend("test-backend", endpoint, 5*time.Second, false, BackendTypeMirrored, GRPCBackendConfig{
		MaxRecvMsgSize: 100 * 1024 * 1024,
		MaxSendMsgSize: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer backend.Close()

	origReq := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader([]byte("test")))
	origReq.Header.Set("X-Scope-OrgID", "test-tenant")

	_, status, respBody, _, err := backend.ForwardRequest(
		context.Background(),
		origReq,
		io.NopCloser(bytes.NewReader([]byte("test"))),
	)

	// The 5xx error is embedded in the gRPC error and should be extracted.
	require.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Equal(t, "internal server error", string(respBody))
}

func TestGRPCProxyBackend_PathPrepending(t *testing.T) {
	var receivedPath string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	})

	grpcServer, addr := startTestGRPCServer(t, handler)
	defer grpcServer.Stop()

	// Create an endpoint with a path prefix.
	endpoint := mustParseURL("dns://" + addr + "/prefix")
	backend, err := NewGRPCProxyBackend("test-backend", endpoint, 5*time.Second, false, BackendTypeMirrored, GRPCBackendConfig{
		MaxRecvMsgSize: 100 * 1024 * 1024,
		MaxSendMsgSize: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer backend.Close()

	origReq := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader([]byte("test")))
	origReq.Header.Set("X-Scope-OrgID", "test-tenant")

	_, _, _, _, err = backend.ForwardRequest(
		context.Background(),
		origReq,
		io.NopCloser(bytes.NewReader([]byte("test"))),
	)
	require.NoError(t, err)

	assert.Equal(t, "/prefix/api/v1/push", receivedPath)
}

func TestGRPCProxyBackend_QueryParameters(t *testing.T) {
	var receivedURL string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedURL = r.URL.String()
		w.WriteHeader(http.StatusOK)
	})

	grpcServer, addr := startTestGRPCServer(t, handler)
	defer grpcServer.Stop()

	endpoint := mustParseURL("dns://" + addr)
	backend, err := NewGRPCProxyBackend("test-backend", endpoint, 5*time.Second, false, BackendTypeMirrored, GRPCBackendConfig{
		MaxRecvMsgSize: 100 * 1024 * 1024,
		MaxSendMsgSize: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer backend.Close()

	origReq := httptest.NewRequest("POST", "/api/v1/push?foo=bar&baz=qux", bytes.NewReader([]byte("test")))
	origReq.Header.Set("X-Scope-OrgID", "test-tenant")

	_, _, _, _, err = backend.ForwardRequest(
		context.Background(),
		origReq,
		io.NopCloser(bytes.NewReader([]byte("test"))),
	)
	require.NoError(t, err)

	assert.Equal(t, "/api/v1/push?foo=bar&baz=qux", receivedURL)
}

func TestGRPCProxyBackend_Close(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	grpcServer, addr := startTestGRPCServer(t, handler)
	defer grpcServer.Stop()

	endpoint := mustParseURL("dns://" + addr)
	backend, err := NewGRPCProxyBackend("test-backend", endpoint, 5*time.Second, false, BackendTypeMirrored, GRPCBackendConfig{
		MaxRecvMsgSize: 100 * 1024 * 1024,
		MaxSendMsgSize: 100 * 1024 * 1024,
	})
	require.NoError(t, err)

	// Close should succeed.
	err = backend.Close()
	assert.NoError(t, err)
}

func TestNewProxy_GRPCBackendValidation(t *testing.T) {
	logger := log.NewNopLogger()

	tests := []struct {
		name        string
		cfg         ProxyConfig
		expectedErr string
	}{
		{
			name: "invalid scheme",
			cfg: ProxyConfig{
				BackendMirroredEndpoints:   "ftp://backend:8080",
				PreferredBackend:           "backend",
				AsyncMaxInFlightPerBackend: 1000,
			},
			expectedErr: "unsupported backend scheme",
		},
		{
			name: "mixed http and dns backends",
			cfg: ProxyConfig{
				BackendMirroredEndpoints:   "http://backend1:8080,dns://backend2:9095",
				PreferredBackend:           "backend1",
				AsyncMaxInFlightPerBackend: 1000,
				GRPCMaxRecvMsgSize:         100 * 1024 * 1024,
				GRPCMaxSendMsgSize:         100 * 1024 * 1024,
			},
			expectedErr: "", // Should succeed
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			_, err := NewProxy(tt.cfg, logger, []Route{}, registry)
			if tt.expectedErr == "" {
				// We expect no error from parsing, but gRPC connection might fail due to no server.
				// The test validates the parsing logic works correctly.
				if err != nil && !contains(err.Error(), "connection") {
					assert.NoError(t, err)
				}
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			}
		})
	}
}

func TestProxyEndpoint_MixedHTTPAndGRPCBackends(t *testing.T) {
	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	metrics := NewProxyMetrics(registry)

	// Create an HTTP backend.
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("http response"))
	}))
	defer httpServer.Close()

	// Create a gRPC backend.
	grpcHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("grpc response"))
	})
	grpcServer, grpcAddr := startTestGRPCServer(t, grpcHandler)
	defer grpcServer.Stop()

	httpBackend := NewHTTPProxyBackend("http-backend", mustParseURL(httpServer.URL), 5*time.Second, true, false, BackendTypeMirrored)

	grpcEndpoint := mustParseURL("dns://" + grpcAddr)
	grpcBackend, err := NewGRPCProxyBackend("grpc-backend", grpcEndpoint, 5*time.Second, false, BackendTypeAmplified, GRPCBackendConfig{
		MaxRecvMsgSize: 100 * 1024 * 1024,
		MaxSendMsgSize: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	defer grpcBackend.Close()

	route := Route{
		Path:      "/api/v1/push",
		RouteName: "api_v1_push",
		Methods:   []string{"POST"},
	}

	asyncDispatcher := NewAsyncBackendDispatcher(1000, metrics, logger)
	defer asyncDispatcher.Stop()

	tracker := NewAmplificationTracker()
	endpoint, err := NewProxyEndpoint([]ProxyBackend{httpBackend, grpcBackend}, route, metrics, logger, 1.0, tracker, asyncDispatcher)
	require.NoError(t, err)

	// Make a request.
	req := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader(makeTestWriteRequest(t)))
	rec := httptest.NewRecorder()

	endpoint.ServeHTTP(rec, req)

	// Wait for async requests to complete.
	asyncDispatcher.Stop()
	asyncDispatcher.Await()

	// Verify we get the response from the preferred HTTP backend.
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "http response", rec.Body.String())
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr, 0))
}

func containsAt(s, substr string, start int) bool {
	for i := start; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// startTestGRPCServer starts a gRPC server with an HTTPgRPC handler wrapping the given HTTP handler.
func startTestGRPCServer(t *testing.T, handler http.Handler) (*grpc.Server, string) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	httpgrpc.RegisterHTTPServer(grpcServer, httpgrpcserver.NewServer(handler))

	go func() {
		_ = grpcServer.Serve(lis) // Server stopped error is expected during test cleanup.
	}()

	return grpcServer, lis.Addr().String()
}
