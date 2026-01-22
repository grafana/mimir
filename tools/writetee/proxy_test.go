// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewProxy_Validation(t *testing.T) {
	logger := log.NewNopLogger()

	tests := []struct {
		name        string
		cfg         ProxyConfig
		routes      []Route
		expectedErr string
	}{
		{
			name: "no backends",
			cfg: ProxyConfig{
				BackendEndpoints: "",
			},
			routes:      []Route{},
			expectedErr: "at least 1 backend is required",
		},
		{
			name: "invalid backend URL",
			cfg: ProxyConfig{
				BackendEndpoints: "://invalid-url",
			},
			routes:      []Route{},
			expectedErr: "invalid backend endpoint",
		},
		{
			name: "preferred backend not in list",
			cfg: ProxyConfig{
				BackendEndpoints: "http://backend1:8080,http://backend2:8080",
				PreferredBackend: "backend3",
			},
			routes:      []Route{},
			expectedErr: "the preferred backend (hostname) has not been found among the list of configured backends",
		},
		{
			name: "valid single backend",
			cfg: ProxyConfig{
				BackendEndpoints: "http://backend1:8080",
			},
			routes:      []Route{},
			expectedErr: "",
		},
		{
			name: "valid multiple backends with preferred",
			cfg: ProxyConfig{
				BackendEndpoints: "http://backend1:8080,http://backend2:8080",
				PreferredBackend: "backend1",
			},
			routes:      []Route{},
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			_, err := NewProxy(tt.cfg, logger, tt.routes, registry)
			if tt.expectedErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			}
		})
	}
}

func TestProxyEndpoint_ResponseSelection(t *testing.T) {
	logger := log.NewNopLogger()

	tests := []struct {
		name               string
		backends           []mockBackend
		preferredBackend   string
		expectedBackend    string
		expectedStatusCode int
	}{
		{
			name: "preferred backend succeeds",
			backends: []mockBackend{
				{name: "backend1", response: mockResponse{statusCode: 200, body: "ok"}},
				{name: "backend2", response: mockResponse{statusCode: 200, body: "ok"}},
			},
			preferredBackend:   "backend1",
			expectedBackend:    "backend1",
			expectedStatusCode: 200,
		},
		{
			name: "preferred backend fails, others succeed",
			backends: []mockBackend{
				{name: "backend1", response: mockResponse{statusCode: 500, body: "error"}},
				{name: "backend2", response: mockResponse{statusCode: 200, body: "ok"}},
			},
			preferredBackend:   "backend1",
			expectedBackend:    "backend1",
			expectedStatusCode: 500,
		},
		{
			name: "no preferred, first succeeds",
			backends: []mockBackend{
				{name: "backend1", response: mockResponse{statusCode: 200, body: "ok"}},
				{name: "backend2", response: mockResponse{statusCode: 200, body: "ok"}},
			},
			preferredBackend:   "",
			expectedBackend:    "backend1",
			expectedStatusCode: 200,
		},
		{
			name: "no preferred, first fails, second succeeds",
			backends: []mockBackend{
				{name: "backend1", response: mockResponse{statusCode: 500, body: "error"}},
				{name: "backend2", response: mockResponse{statusCode: 200, body: "ok"}},
			},
			preferredBackend:   "",
			expectedBackend:    "backend2",
			expectedStatusCode: 200,
		},
		{
			name: "all fail, return first",
			backends: []mockBackend{
				{name: "backend1", response: mockResponse{statusCode: 500, body: "error1"}},
				{name: "backend2", response: mockResponse{statusCode: 500, body: "error2"}},
			},
			preferredBackend:   "",
			expectedBackend:    "backend1",
			expectedStatusCode: 500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new registry and metrics for each test
			registry := prometheus.NewRegistry()
			metrics := NewProxyMetrics(registry)

			// Create test HTTP servers for each backend
			servers := make([]*httptest.Server, 0, len(tt.backends))
			backendInterfaces := make([]ProxyBackend, 0, len(tt.backends))

			for _, mb := range tt.backends {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(mb.response.statusCode)
					w.Write([]byte(mb.response.body))
				}))
				defer server.Close()
				servers = append(servers, server)

				// Parse the server URL and create a backend
				backend := NewProxyBackend(mb.name, mustParseURL(server.URL), 5*time.Second, mb.name == tt.preferredBackend, false)
				backendInterfaces = append(backendInterfaces, backend)
			}

			route := Route{
				Path:      "/api/v1/push",
				RouteName: "api_v1_push",
				Methods:   []string{"POST"},
			}

			endpoint := NewProxyEndpoint(backendInterfaces, route, metrics, logger, 0)

			// Create a test request
			req := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader([]byte("test body")))
			rec := httptest.NewRecorder()

			// Execute the request
			endpoint.ServeHTTP(rec, req)

			// Verify the response
			assert.Equal(t, tt.expectedStatusCode, rec.Code)
		})
	}
}

func TestProxyEndpoint_BodySizeLimit(t *testing.T) {
	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	metrics := NewProxyMetrics(registry)

	// Create a test backend
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	backend := NewProxyBackend("backend1", mustParseURL(server.URL), 5*time.Second, false, false)
	backendInterfaces := []ProxyBackend{backend}

	route := Route{
		Path:      "/api/v1/push",
		RouteName: "api_v1_push",
		Methods:   []string{"POST"},
	}

	endpoint := NewProxyEndpoint(backendInterfaces, route, metrics, logger, 0)

	tests := []struct {
		name               string
		bodySize           int
		expectedStatusCode int
	}{
		{
			name:               "small body",
			bodySize:           1024, // 1KB
			expectedStatusCode: 200,
		},
		{
			name:               "max body size",
			bodySize:           maxBodySize,
			expectedStatusCode: 200,
		},
		{
			name:               "body too large",
			bodySize:           maxBodySize + 1,
			expectedStatusCode: http.StatusRequestEntityTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a body of the specified size
			body := bytes.Repeat([]byte("a"), tt.bodySize)
			req := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader(body))
			rec := httptest.NewRecorder()

			// Execute the request
			endpoint.ServeHTTP(rec, req)

			// Verify the response
			assert.Equal(t, tt.expectedStatusCode, rec.Code)
		})
	}
}

func TestProxyBackend_AuthHandling(t *testing.T) {
	tests := []struct {
		name         string
		endpointURL  string
		requestAuth  bool
		requestUser  string
		requestPass  string
		expectedUser string
		expectedPass string
	}{
		{
			name:         "endpoint auth takes precedence",
			endpointURL:  "http://endpointuser:endpointpass@backend:8080",
			requestAuth:  true,
			requestUser:  "requestuser",
			requestPass:  "requestpass",
			expectedUser: "endpointuser",
			expectedPass: "endpointpass",
		},
		{
			name:         "endpoint user only, use request password",
			endpointURL:  "http://endpointuser@backend:8080",
			requestAuth:  true,
			requestUser:  "requestuser",
			requestPass:  "requestpass",
			expectedUser: "endpointuser",
			expectedPass: "requestpass",
		},
		{
			name:         "no endpoint auth, use request auth",
			endpointURL:  "http://backend:8080",
			requestAuth:  true,
			requestUser:  "requestuser",
			requestPass:  "requestpass",
			expectedUser: "requestuser",
			expectedPass: "requestpass",
		},
		{
			name:         "no auth at all",
			endpointURL:  "http://backend:8080",
			requestAuth:  false,
			requestUser:  "",
			requestPass:  "",
			expectedUser: "",
			expectedPass: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test server that captures the auth header
			var capturedUser, capturedPass string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedUser, capturedPass, _ = r.BasicAuth()
				w.WriteHeader(200)
			}))
			defer server.Close()

			// Parse the endpoint URL and replace the host with the test server
			endpointURL := mustParseURL(tt.endpointURL)
			testServerURL := mustParseURL(server.URL)
			endpointURL.Host = testServerURL.Host

			backend := NewProxyBackend("backend1", endpointURL, 5*time.Second, false, false)

			// Create a request with auth if specified
			req := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader([]byte("test")))
			if tt.requestAuth {
				req.SetBasicAuth(tt.requestUser, tt.requestPass)
			}

			// Forward the request
			_, _, _, err := backend.ForwardRequest(req.Context(), req, io.NopCloser(bytes.NewReader([]byte("test"))))
			require.NoError(t, err)

			// Verify the captured auth
			assert.Equal(t, tt.expectedUser, capturedUser)
			assert.Equal(t, tt.expectedPass, capturedPass)
		})
	}
}

func TestBackendResponse_Succeeded(t *testing.T) {
	tests := []struct {
		name      string
		response  backendResponse
		succeeded bool
	}{
		{
			name: "2xx success",
			response: backendResponse{
				status: 200,
				err:    nil,
			},
			succeeded: true,
		},
		{
			name: "4xx success (except 429)",
			response: backendResponse{
				status: 400,
				err:    nil,
			},
			succeeded: true,
		},
		{
			name: "429 not success",
			response: backendResponse{
				status: 429,
				err:    nil,
			},
			succeeded: false,
		},
		{
			name: "5xx not success",
			response: backendResponse{
				status: 500,
				err:    nil,
			},
			succeeded: false,
		},
		{
			name: "error not success",
			response: backendResponse{
				status: 200,
				err:    fmt.Errorf("network error"),
			},
			succeeded: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.succeeded, tt.response.succeeded())
		})
	}
}

// Helper types and functions

type mockBackend struct {
	name     string
	response mockResponse
}

type mockResponse struct {
	statusCode int
	body       string
}

func mustParseURL(rawURL string) *url.URL {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic(fmt.Sprintf("failed to parse URL %s: %v", rawURL, err))
	}
	return u
}
