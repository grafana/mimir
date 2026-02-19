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
			name: "no backends",
			cfg: ProxyConfig{
				BackendMirroredEndpoints: "",
			},
			routes:      []Route{},
			expectedErr: "at least 1 backend is required",
		},
		{
			name: "invalid backend URL",
			cfg: ProxyConfig{
				BackendMirroredEndpoints: "://invalid-url",
			},
			routes:      []Route{},
			expectedErr: "invalid backend endpoint",
		},
		{
			name: "preferred backend not in list",
			cfg: ProxyConfig{
				BackendMirroredEndpoints: "http://backend1:8080,http://backend2:8080",
				PreferredBackend:         "backend3",
			},
			routes:      []Route{},
			expectedErr: "the preferred backend (hostname) has not been found among the list of configured backends",
		},
		{
			name: "no preferred backend configured",
			cfg: ProxyConfig{
				BackendMirroredEndpoints: "http://backend1:8080",
				PreferredBackend:         "",
			},
			routes:      []Route{},
			expectedErr: "preferred backend is required",
		},
		{
			name: "valid single backend with preferred",
			cfg: ProxyConfig{
				BackendMirroredEndpoints:   "http://backend1:8080",
				PreferredBackend:           "backend1",
				AsyncMaxInFlightPerBackend: 1000,
			},
			routes:      []Route{},
			expectedErr: "",
		},
		{
			name: "valid multiple backends with preferred",
			cfg: ProxyConfig{
				BackendMirroredEndpoints:   "http://backend1:8080,http://backend2:8080",
				PreferredBackend:           "backend1",
				AsyncMaxInFlightPerBackend: 1000,
			},
			routes:      []Route{},
			expectedErr: "",
		},
		{
			name: "negative async max in-flight",
			cfg: ProxyConfig{
				BackendMirroredEndpoints:   "http://backend1:8080",
				PreferredBackend:           "backend1",
				AsyncMaxInFlightPerBackend: -1,
			},
			routes:      []Route{},
			expectedErr: "backend.async-max-in-flight must be greater than 0",
		},
		{
			name: "zero async max in-flight",
			cfg: ProxyConfig{
				BackendMirroredEndpoints:   "http://backend1:8080",
				PreferredBackend:           "backend1",
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

func TestNewProxy_PreferredBackendSelection(t *testing.T) {
	logger := log.NewNopLogger()

	tests := []struct {
		name                      string
		mirroredEndpoints         string
		amplifiedEndpoints        string
		preferredBackend          string
		expectedPreferredIdx      int  // Index of backend that should be preferred
		expectedPreferredType     BackendType
		expectedNonPreferredCount int  // Number of backends that should NOT be preferred
	}{
		{
			name:                      "same hostname in mirrored and amplified - mirrored should be preferred",
			mirroredEndpoints:         "http://backend1:8080",
			amplifiedEndpoints:        "http://backend1:8080",
			preferredBackend:          "backend1",
			expectedPreferredIdx:      0, // First backend (mirrored)
			expectedPreferredType:     BackendTypeMirrored,
			expectedNonPreferredCount: 1, // The amplified backend should NOT be preferred
		},
		{
			name:                      "different hostnames - normal behavior",
			mirroredEndpoints:         "http://backend1:8080",
			amplifiedEndpoints:        "http://backend2:8080",
			preferredBackend:          "backend1",
			expectedPreferredIdx:      0,
			expectedPreferredType:     BackendTypeMirrored,
			expectedNonPreferredCount: 1,
		},
		{
			name:                      "only amplified backends with matching hostname",
			mirroredEndpoints:         "http://other:8080",
			amplifiedEndpoints:        "http://backend1:8080",
			preferredBackend:          "backend1",
			expectedPreferredIdx:      1, // Second backend (amplified)
			expectedPreferredType:     BackendTypeAmplified,
			expectedNonPreferredCount: 1,
		},
		{
			name:                      "numeric preferred backend index",
			mirroredEndpoints:         "http://backend1:8080",
			amplifiedEndpoints:        "http://backend1:8080",
			preferredBackend:          "1", // Select second backend by index
			expectedPreferredIdx:      1,
			expectedPreferredType:     BackendTypeAmplified,
			expectedNonPreferredCount: 1,
		},
		{
			name:                      "multiple mirrored backends - first match wins",
			mirroredEndpoints:         "http://backend1:8080,http://backend1:8081",
			amplifiedEndpoints:        "",
			preferredBackend:          "backend1",
			expectedPreferredIdx:      0,
			expectedPreferredType:     BackendTypeMirrored,
			expectedNonPreferredCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			cfg := ProxyConfig{
				BackendMirroredEndpoints:   tt.mirroredEndpoints,
				BackendAmplifiedEndpoints:  tt.amplifiedEndpoints,
				PreferredBackend:           tt.preferredBackend,
				AmplificationFactor:        1.0,
				AsyncMaxInFlightPerBackend: 1000,
			}

			proxy, err := NewProxy(cfg, logger, []Route{}, registry)
			require.NoError(t, err)

			// Count preferred and non-preferred backends
			var preferredCount, nonPreferredCount int
			var preferredIdx int
			var preferredType BackendType
			for idx, b := range proxy.backends {
				if b.Preferred() {
					preferredCount++
					preferredIdx = idx
					preferredType = b.BackendType()
				} else {
					nonPreferredCount++
				}
			}

			assert.Equal(t, 1, preferredCount, "exactly one backend should be preferred")
			assert.Equal(t, tt.expectedPreferredIdx, preferredIdx, "wrong backend selected as preferred")
			assert.Equal(t, tt.expectedPreferredType, preferredType, "wrong backend type selected as preferred")
			assert.Equal(t, tt.expectedNonPreferredCount, nonPreferredCount, "wrong number of non-preferred backends")
		})
	}
}

func TestProxyEndpoint_SameHostnameMirroredAndAmplified_BothReceiveTraffic(t *testing.T) {
	// This test verifies the fix for the bug where same hostname in both
	// mirrored and amplified endpoints resulted in only 1x traffic instead of 2x.
	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	metrics := NewProxyMetrics(registry)

	var mirroredRequests, amplifiedRequests int
	var mu sync.Mutex

	// Create a server that tracks requests
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		// We can't distinguish backends by URL since they're the same, but in the real
		// test we just verify total request count equals 2 (1 sync + 1 async)
		w.WriteHeader(200)
		mu.Unlock()
	}))
	defer server.Close()

	serverURL := mustParseURL(server.URL)

	// Create two backends pointing to the same server, but one is preferred (mirrored) and one is not (amplified)
	mirroredBackend := NewProxyBackend(serverURL.Hostname(), serverURL, 5*time.Second, true, false, BackendTypeMirrored)
	amplifiedBackend := NewProxyBackend(serverURL.Hostname(), serverURL, 5*time.Second, false, false, BackendTypeAmplified)

	// Track requests via custom backends that wrap the real ones
	mirroredWrapper := &trackingBackend{ProxyBackend: mirroredBackend, counter: &mirroredRequests, mu: &mu}
	amplifiedWrapper := &trackingBackend{ProxyBackend: amplifiedBackend, counter: &amplifiedRequests, mu: &mu}

	route := Route{
		Path:      "/api/v1/push",
		RouteName: "api_v1_push",
		Methods:   []string{"POST"},
	}

	asyncDispatcher := NewAsyncBackendDispatcher(1000, metrics, logger)
	defer asyncDispatcher.Stop()

	tracker := NewAmplificationTracker()
	endpoint, err := NewProxyEndpoint([]ProxyBackend{mirroredWrapper, amplifiedWrapper}, route, metrics, logger, 1.0, tracker, asyncDispatcher)
	require.NoError(t, err)

	// Make a request
	req := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader(makeTestWriteRequest(t)))
	rec := httptest.NewRecorder()

	endpoint.ServeHTTP(rec, req)

	// Wait for async requests to complete
	asyncDispatcher.Stop()
	asyncDispatcher.Await()

	// Verify both backends received requests
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, mirroredRequests, "mirrored backend should receive 1 request (synchronous)")
	assert.Equal(t, 1, amplifiedRequests, "amplified backend should receive 1 request (async)")
}

// trackingBackend wraps a ProxyBackend and counts requests
type trackingBackend struct {
	ProxyBackend
	counter *int
	mu      *sync.Mutex
}

func (t *trackingBackend) ForwardRequest(ctx context.Context, orig *http.Request, body io.ReadCloser) (time.Duration, int, []byte, http.Header, error) {
	t.mu.Lock()
	*t.counter++
	t.mu.Unlock()
	return t.ProxyBackend.ForwardRequest(ctx, orig, body)
}

func TestProxyEndpoint_ResponseSelection(t *testing.T) {
	logger := log.NewNopLogger()

	tests := []struct {
		name               string
		backends           []mockBackend
		preferredBackend   string
		expectedStatusCode int
	}{
		{
			name: "preferred backend succeeds",
			backends: []mockBackend{
				{name: "backend1", response: mockResponse{statusCode: 200, body: "ok"}},
				{name: "backend2", response: mockResponse{statusCode: 200, body: "ok"}},
			},
			preferredBackend:   "backend1",
			expectedStatusCode: 200,
		},
		{
			name: "preferred backend fails, still returns preferred response",
			backends: []mockBackend{
				{name: "backend1", response: mockResponse{statusCode: 500, body: "error"}},
				{name: "backend2", response: mockResponse{statusCode: 200, body: "ok"}},
			},
			preferredBackend:   "backend1",
			expectedStatusCode: 500,
		},
		{
			name: "single preferred backend succeeds",
			backends: []mockBackend{
				{name: "backend1", response: mockResponse{statusCode: 200, body: "ok"}},
			},
			preferredBackend:   "backend1",
			expectedStatusCode: 200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new registry and metrics for each test
			registry := prometheus.NewRegistry()
			metrics := NewProxyMetrics(registry)

			// Create test HTTP servers for each backend
			backendInterfaces := make([]ProxyBackend, 0, len(tt.backends))

			for _, mb := range tt.backends {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(mb.response.statusCode)
					_, _ = w.Write([]byte(mb.response.body))
				}))
				defer server.Close()

				// Parse the server URL and create a backend
				backend := NewProxyBackend(mb.name, mustParseURL(server.URL), 5*time.Second, mb.name == tt.preferredBackend, false, BackendTypeMirrored)
				backendInterfaces = append(backendInterfaces, backend)
			}

			route := Route{
				Path:      "/api/v1/push",
				RouteName: "api_v1_push",
				Methods:   []string{"POST"},
			}

			// Create async dispatcher for non-preferred backends
			asyncDispatcher := NewAsyncBackendDispatcher(1000, metrics, logger)
			defer asyncDispatcher.Stop()

			endpoint, err := NewProxyEndpoint(backendInterfaces, route, metrics, logger, 1.0, nil, asyncDispatcher)
			require.NoError(t, err)

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
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	backend := NewProxyBackend("backend1", mustParseURL(server.URL), 5*time.Second, true, false, BackendTypeMirrored)
	backendInterfaces := []ProxyBackend{backend}

	route := Route{
		Path:      "/api/v1/push",
		RouteName: "api_v1_push",
		Methods:   []string{"POST"},
	}

	// Create async dispatcher for non-preferred backends
	asyncDispatcher := NewAsyncBackendDispatcher(1000, metrics, logger)
	defer asyncDispatcher.Stop()

	endpoint, err := NewProxyEndpoint(backendInterfaces, route, metrics, logger, 1.0, nil, asyncDispatcher)
	require.NoError(t, err)

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

	backend := NewProxyBackend("backend1", mustParseURL(server.URL), 5*time.Second, true, false, BackendTypeMirrored)
	backendInterfaces := []ProxyBackend{backend}

	route := Route{
		Path:      "/api/v1/push",
		RouteName: "api_v1_push",
		Methods:   []string{"POST"},
	}

	asyncDispatcher := NewAsyncBackendDispatcher(1000, metrics, logger)
	defer asyncDispatcher.Stop()

	endpoint, err := NewProxyEndpoint(backendInterfaces, route, metrics, logger, 1.0, nil, asyncDispatcher)
	require.NoError(t, err)

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

			backend := NewProxyBackend("backend1", endpointURL, 5*time.Second, false, false, BackendTypeMirrored)

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

func TestPrepareAmplifiedBodies(t *testing.T) {
	logger := log.NewNopLogger()

	tests := []struct {
		name           string
		amplifyFactor  float64
		backendType    BackendType
		expectedBodies int
	}{
		{
			name:           "non-amplified backend returns 1 body",
			amplifyFactor:  10.0,
			backendType:    BackendTypeMirrored,
			expectedBodies: 1,
		},
		{
			name:           "factor 1.0 returns 1 body",
			amplifyFactor:  1.0,
			backendType:    BackendTypeAmplified,
			expectedBodies: 1,
		},
		{
			name:           "factor 0.5 sampling returns 1 body",
			amplifyFactor:  0.5,
			backendType:    BackendTypeAmplified,
			expectedBodies: 1,
		},
		{
			name:           "factor 2.0 returns 2 bodies",
			amplifyFactor:  2.0,
			backendType:    BackendTypeAmplified,
			expectedBodies: 2,
		},
		{
			name:           "factor 10.0 returns 10 bodies",
			amplifyFactor:  10.0,
			backendType:    BackendTypeAmplified,
			expectedBodies: 10,
		},
		{
			name:           "factor 2.5 returns 3 bodies (2 full + 1 fractional)",
			amplifyFactor:  2.5,
			backendType:    BackendTypeAmplified,
			expectedBodies: 3,
		},
		{
			name:           "factor 5.2 returns 6 bodies (5 full + 1 fractional)",
			amplifyFactor:  5.2,
			backendType:    BackendTypeAmplified,
			expectedBodies: 6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			metrics := NewProxyMetrics(registry)
			tracker := NewAmplificationTracker()

			backend := NewProxyBackend("test", mustParseURL("http://localhost:9090"), 5*time.Second, false, false, tt.backendType)
			route := Route{Path: "/api/v1/push", RouteName: "test", Methods: []string{"POST"}}

			endpoint := &ProxyEndpoint{
				backends:             []ProxyBackend{backend},
				route:                route,
				metrics:              metrics,
				logger:               logger,
				amplificationFactor:  tt.amplifyFactor,
				amplificationTracker: tracker,
			}

			body := makeTestWriteRequest(t)
			spLogger := spanlogger.FromContext(context.Background(), logger)
			bodies := endpoint.prepareAmplifiedBodies(body, backend, spLogger)

			assert.Equal(t, tt.expectedBodies, len(bodies), "expected %d bodies, got %d", tt.expectedBodies, len(bodies))

			// For amplification > 1, verify all bodies have suffixes (none should equal the original)
			if tt.amplifyFactor > 1.0 && tt.backendType == BackendTypeAmplified && len(bodies) > 1 {
				for i := 0; i < len(bodies); i++ {
					assert.NotEqual(t, body, bodies[i], "body %d should differ from original - all amplified copies must have a label suffix to avoid clashing with the mirrored endpoint", i)
				}
			}
		})
	}
}

func TestProxyEndpoint_Amplification(t *testing.T) {
	logger := log.NewNopLogger()

	tests := []struct {
		name                string
		amplificationFactor float64
		expectedRequests    int // Expected number of requests to the amplified backend
	}{
		{
			name:                "no amplification",
			amplificationFactor: 1.0,
			expectedRequests:    1,
		},
		{
			name:                "10x amplification",
			amplificationFactor: 10.0,
			expectedRequests:    10,
		},
		{
			name:                "2.5x amplification",
			amplificationFactor: 2.5,
			expectedRequests:    3, // 2 full + 1 fractional
		},
		{
			name:                "0.5x sampling",
			amplificationFactor: 0.5,
			expectedRequests:    1, // 1 sampled request
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			metrics := NewProxyMetrics(registry)

			// Track requests to the amplified backend
			var amplifiedBackendRequests int
			var receivedBodies [][]byte
			var mu sync.Mutex

			// Create preferred backend
			preferredServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				_, _ = w.Write([]byte("ok"))
			}))
			defer preferredServer.Close()

			// Create amplified backend that counts requests and captures bodies
			amplifiedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				amplifiedBackendRequests++
				receivedBodies = append(receivedBodies, body)
				mu.Unlock()
				w.WriteHeader(200)
				_, _ = w.Write([]byte("ok"))
			}))
			defer amplifiedServer.Close()

			preferredBackend := NewProxyBackend("preferred", mustParseURL(preferredServer.URL), 5*time.Second, true, false, BackendTypeMirrored)
			amplifiedBackend := NewProxyBackend("amplified", mustParseURL(amplifiedServer.URL), 5*time.Second, false, false, BackendTypeAmplified)

			route := Route{
				Path:      "/api/v1/push",
				RouteName: "api_v1_push",
				Methods:   []string{"POST"},
			}

			asyncDispatcher := NewAsyncBackendDispatcher(1000, metrics, logger)
			defer asyncDispatcher.Stop()

			tracker := NewAmplificationTracker()
			endpoint, err := NewProxyEndpoint([]ProxyBackend{preferredBackend, amplifiedBackend}, route, metrics, logger, tt.amplificationFactor, tracker, asyncDispatcher)
			require.NoError(t, err)

			// Create a minimal valid write request
			req := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader(makeTestWriteRequest(t)))
			rec := httptest.NewRecorder()

			endpoint.ServeHTTP(rec, req)

			// Wait for async requests to complete
			asyncDispatcher.Stop()
			asyncDispatcher.Await()

			// Verify response from preferred backend
			assert.Equal(t, 200, rec.Code)

			// Verify the number of requests to the amplified backend
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, tt.expectedRequests, amplifiedBackendRequests, "expected %d requests to amplified backend, got %d", tt.expectedRequests, amplifiedBackendRequests)

			// For amplification > 1, verify that bodies are different (unique labels)
			if tt.amplificationFactor > 1.0 && len(receivedBodies) > 1 {
				// Bodies should be different due to different label suffixes
				firstBody := receivedBodies[0]
				for i := 1; i < len(receivedBodies); i++ {
					assert.NotEqual(t, firstBody, receivedBodies[i], "request %d should have different body than request 0 (different label suffixes)", i)
				}
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
