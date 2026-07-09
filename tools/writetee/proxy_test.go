// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
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
			name: "no endpoint",
			cfg: ProxyConfig{
				BackendEndpoint:            "",
				AmplificationFactor:        1.0,
				AsyncMaxInFlightPerBackend: 1000,
			},
			routes:      []Route{},
			expectedErr: "backend endpoint is required",
		},
		{
			name: "invalid backend URL",
			cfg: ProxyConfig{
				BackendEndpoint:            "://invalid-url",
				AmplificationFactor:        1.0,
				AsyncMaxInFlightPerBackend: 1000,
			},
			routes:      []Route{},
			expectedErr: "invalid backend endpoint",
		},
		{
			name: "amplification factor below 1",
			cfg: ProxyConfig{
				BackendEndpoint:            "http://backend1:8080",
				AmplificationFactor:        0.5,
				AsyncMaxInFlightPerBackend: 1000,
			},
			routes:      []Route{},
			expectedErr: "amplification-factor must be >= 1.0",
		},
		{
			name: "valid single endpoint",
			cfg: ProxyConfig{
				BackendEndpoint:            "http://backend1:8080",
				AmplificationFactor:        1.0,
				AsyncMaxInFlightPerBackend: 1000,
			},
			routes:      []Route{},
			expectedErr: "",
		},
		{
			name: "valid with amplification",
			cfg: ProxyConfig{
				BackendEndpoint:            "http://backend1:8080",
				AmplificationFactor:        10.0,
				AsyncMaxInFlightPerBackend: 1000,
			},
			routes:      []Route{},
			expectedErr: "",
		},
		{
			name: "negative async max in-flight",
			cfg: ProxyConfig{
				BackendEndpoint:            "http://backend1:8080",
				AmplificationFactor:        1.0,
				AsyncMaxInFlightPerBackend: -1,
			},
			routes:      []Route{},
			expectedErr: "backend.async-max-in-flight must be greater than 0",
		},
		{
			name: "zero async max in-flight",
			cfg: ProxyConfig{
				BackendEndpoint:            "http://backend1:8080",
				AmplificationFactor:        1.0,
				AsyncMaxInFlightPerBackend: 0,
			},
			routes:      []Route{},
			expectedErr: "backend.async-max-in-flight must be greater than 0",
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

			backend := NewHTTPProxyBackend("backend1", mustParseURL(server.URL), 5*time.Second, false)

			route := Route{
				Path:      "/api/v1/push",
				RouteName: "api_v1_push",
				Methods:   []string{"POST"},
			}

			asyncDispatcher := NewAsyncBackendDispatcher(1000, metrics, logger)
			defer asyncDispatcher.Stop()

			endpoint := NewProxyEndpoint(backend, route, metrics, logger, 1.0, nil, asyncDispatcher)

			req := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader([]byte("test body")))
			rec := httptest.NewRecorder()

			endpoint.ServeHTTP(rec, req)

			assert.Equal(t, tt.expectedStatusCode, rec.Code)
		})
	}
}

func TestProxyEndpoint_BodySizeLimit(t *testing.T) {
	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	metrics := NewProxyMetrics(registry)

	// Create a test backend
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	backend := NewHTTPProxyBackend("backend1", mustParseURL(server.URL), 5*time.Second, false)

	route := Route{
		Path:      "/api/v1/push",
		RouteName: "api_v1_push",
		Methods:   []string{"POST"},
	}

	asyncDispatcher := NewAsyncBackendDispatcher(1000, metrics, logger)
	defer asyncDispatcher.Stop()

	endpoint := NewProxyEndpoint(backend, route, metrics, logger, 1.0, nil, asyncDispatcher)

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

func TestProxyEndpoint_ServeHTTPPassthrough(t *testing.T) {
	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	metrics := NewProxyMetrics(registry)

	// Create a test backend that returns a specific response with Content-Type
	expectedBody := `{"status":"ok"}`
	expectedContentType := "application/json"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the request was forwarded correctly
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/x-protobuf", r.Header.Get("Content-Type"))

		w.Header().Set("Content-Type", expectedContentType)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(expectedBody))
	}))
	defer server.Close()

	backend := NewHTTPProxyBackend("backend1", mustParseURL(server.URL), 5*time.Second, false)

	route := Route{
		Path:      "/api/v1/push",
		RouteName: "api_v1_push",
		Methods:   []string{"POST"},
	}

	asyncDispatcher := NewAsyncBackendDispatcher(1000, metrics, logger)
	defer asyncDispatcher.Stop()

	endpoint := NewProxyEndpoint(backend, route, metrics, logger, 1.0, nil, asyncDispatcher)

	// Create a test request with Content-Type
	req := httptest.NewRequest("POST", "/some/other/path", bytes.NewReader([]byte("test body")))
	req.Header.Set("Content-Type", "application/x-protobuf")
	rec := httptest.NewRecorder()

	// Execute passthrough
	endpoint.ServeHTTPPassthrough(rec, req)

	// Verify the response
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, expectedBody, rec.Body.String())
	assert.Equal(t, expectedContentType, rec.Header().Get("Content-Type"))
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

			backend := NewHTTPProxyBackend("backend1", endpointURL, 5*time.Second, false)

			// Create a request with auth if specified
			req := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader([]byte("test")))
			if tt.requestAuth {
				req.SetBasicAuth(tt.requestUser, tt.requestPass)
			}

			// Forward the request
			_, _, _, _, err := backend.ForwardRequest(req.Context(), req, io.NopCloser(bytes.NewReader([]byte("test"))))
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

func mustParseURL(rawURL string) *url.URL {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic(fmt.Sprintf("failed to parse URL %s: %v", rawURL, err))
	}
	return u
}

// TestAmplifiedBodies verifies amplifiedBodies returns ONLY the suffixed copies (replicas 2..N),
// excluding the unsuffixed original (replica 1) which is sent synchronously.
func TestAmplifiedBodies(t *testing.T) {
	logger := log.NewNopLogger()

	tests := []struct {
		name           string
		amplifyFactor  float64
		expectedCopies int
	}{
		{
			name:           "factor 1.0 returns 0 copies",
			amplifyFactor:  1.0,
			expectedCopies: 0,
		},
		{
			name:           "factor 2.0 returns 1 copy",
			amplifyFactor:  2.0,
			expectedCopies: 1,
		},
		{
			name:           "factor 2.5 returns 2 copies (1 full + 1 fractional)",
			amplifyFactor:  2.5,
			expectedCopies: 2,
		},
		{
			name:           "factor 10.0 returns 9 copies",
			amplifyFactor:  10.0,
			expectedCopies: 9,
		},
		{
			name:           "factor 5.2 returns 5 copies (4 full + 1 fractional)",
			amplifyFactor:  5.2,
			expectedCopies: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			metrics := NewProxyMetrics(registry)
			tracker := NewAmplificationTracker()

			backend := NewHTTPProxyBackend("test", mustParseURL("http://localhost:9090"), 5*time.Second, false)
			route := Route{Path: "/api/v1/push", RouteName: "test", Methods: []string{"POST"}}

			endpoint := NewProxyEndpoint(backend, route, metrics, logger, tt.amplifyFactor, tracker, nil)

			body := makeTestWriteRequest(t)
			spLogger := spanlogger.FromContext(context.Background(), logger)
			bodies := endpoint.amplifiedBodies(body, spLogger)

			assert.Equal(t, tt.expectedCopies, len(bodies), "expected %d amplified copies, got %d", tt.expectedCopies, len(bodies))

			// All returned bodies must be suffixed - none should equal the unsuffixed original.
			for i := range bodies {
				assert.NotEqual(t, body, bodies[i], "amplified copy %d must differ from the original (suffixed)", i)
			}
		})
	}
}

// TestProxyEndpoint_Amplification verifies the TOTAL number of requests sent to the single backend
// endpoint (the synchronous original + async amplified copies) for various amplification factors.
func TestProxyEndpoint_Amplification(t *testing.T) {
	logger := log.NewNopLogger()

	tests := []struct {
		name                string
		amplificationFactor float64
		expectedRequests    int // Total requests to the backend (original + amplified copies)
	}{
		{
			name:                "factor 1.0 sends 1 request",
			amplificationFactor: 1.0,
			expectedRequests:    1,
		},
		{
			name:                "factor 2.0 sends 2 requests",
			amplificationFactor: 2.0,
			expectedRequests:    2,
		},
		{
			name:                "factor 2.5 sends 3 requests",
			amplificationFactor: 2.5,
			expectedRequests:    3, // original + _amp1 + _amp2 (sampled)
		},
		{
			name:                "factor 10.0 sends 10 requests",
			amplificationFactor: 10.0,
			expectedRequests:    10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			metrics := NewProxyMetrics(registry)

			// Track all requests hitting the single backend (sync original + async copies).
			var receivedBodies [][]byte
			var mu sync.Mutex

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				receivedBodies = append(receivedBodies, body)
				mu.Unlock()
				w.WriteHeader(200)
				_, _ = w.Write([]byte("ok"))
			}))
			defer server.Close()

			backend := NewHTTPProxyBackend("backend", mustParseURL(server.URL), 5*time.Second, false)

			route := Route{
				Path:      "/api/v1/push",
				RouteName: "api_v1_push",
				Methods:   []string{"POST"},
			}

			asyncDispatcher := NewAsyncBackendDispatcher(1000, metrics, logger)
			defer asyncDispatcher.Stop()

			tracker := NewAmplificationTracker()
			endpoint := NewProxyEndpoint(backend, route, metrics, logger, tt.amplificationFactor, tracker, asyncDispatcher)

			// Create a minimal valid write request.
			originalBody := makeTestWriteRequest(t)
			req := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader(originalBody))
			rec := httptest.NewRecorder()

			endpoint.ServeHTTP(rec, req)

			// Wait for async requests to complete.
			asyncDispatcher.Stop()
			asyncDispatcher.Await()

			// The synchronous original returns 200.
			assert.Equal(t, 200, rec.Code)

			mu.Lock()
			defer mu.Unlock()

			// Verify the total number of requests to the backend.
			require.Equal(t, tt.expectedRequests, len(receivedBodies), "expected %d total requests to the backend, got %d", tt.expectedRequests, len(receivedBodies))

			// Exactly one received request must be the unmodified original (replica 1).
			originalCount := 0
			for _, b := range receivedBodies {
				if bytes.Equal(b, originalBody) {
					originalCount++
				}
			}
			assert.Equal(t, 1, originalCount, "exactly one request must be the unmodified original")

			// Any amplified copies must differ from the original (suffixed labels).
			if tt.expectedRequests > 1 {
				differing := 0
				for _, b := range receivedBodies {
					if !bytes.Equal(b, originalBody) {
						differing++
					}
				}
				assert.Equal(t, tt.expectedRequests-1, differing, "all amplified copies must differ from the original")
			}
		})
	}
}

func makeTestWriteRequest(t *testing.T) []byte {
	req := mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: []mimirpb.LabelAdapter{
						{Name: "__name__", Value: "test_metric"},
						{Name: "job", Value: "test"},
						{Name: "instance", Value: "localhost:9090"},
					},
					Samples: []mimirpb.Sample{{Value: 1.0, TimestampMs: 1000}},
				},
			},
		},
	}
	marshaled, err := proto.Marshal(&req)
	require.NoError(t, err)
	return snappy.Encode(nil, marshaled)
}
