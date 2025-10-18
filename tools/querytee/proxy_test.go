// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/tools/querytee/proxy_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querytee

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var testRoutes = []Route{
	{
		Path:               "/api/v1/query",
		RouteName:          "api_v1_query",
		Methods:            []string{"GET"},
		ResponseComparator: &testComparator{},
	},
	{
		Path:                "/api/v1/query_with_transform",
		RouteName:           "api_v1_query_with_transform",
		Methods:             []string{"POST"},
		ResponseComparator:  &testComparator{},
		RequestTransformers: []RequestTransformer{testRequestTransformer1, testRequestTransformer2},
	},
}

type testComparator struct{}

func (testComparator) Compare(_, _ []byte, _ time.Time) (ComparisonResult, error) {
	return ComparisonSuccess, nil
}

func testRequestTransformer1(r *http.Request, body []byte, _ *spanlogger.SpanLogger) (*http.Request, []byte, error) {
	r = r.Clone(r.Context())
	r.URL.Path = r.URL.Path + "/transformed_1"
	body = append(body, []byte("from 1\n")...)

	return r, body, nil
}

func testRequestTransformer2(r *http.Request, body []byte, _ *spanlogger.SpanLogger) (*http.Request, []byte, error) {
	r = r.Clone(r.Context())
	r.URL.Path = r.URL.Path + "/transformed_2"
	body = append(body, []byte("from 2\n")...)

	return r, body, nil
}

func Test_NewProxy(t *testing.T) {
	testCases := map[string]struct {
		cfg           ProxyConfig
		expectedError string
	}{
		"empty config": {
			cfg: ProxyConfig{
				SecondaryBackendsRequestProportion: 1.0,
			},
			expectedError: "at least 1 backend is required",
		},
		"single endpoint, preferred backend set and exists": {
			cfg: ProxyConfig{
				BackendEndpoints:                   "http://blah",
				PreferredBackend:                   "blah",
				SecondaryBackendsRequestProportion: 1.0,
			},
			expectedError: "",
		},
		"single endpoint with subdirectory and port, preferred backend set and exists": {
			cfg: ProxyConfig{
				BackendEndpoints:                   "http://blah.com:1234/some-sub-dir/and-another",
				PreferredBackend:                   "blah.com",
				SecondaryBackendsRequestProportion: 1.0,
			},
			expectedError: "",
		},
		"single endpoint, preferred backend set and does not exist": {
			cfg: ProxyConfig{
				BackendEndpoints:                   "http://blah",
				PreferredBackend:                   "blah-2",
				SecondaryBackendsRequestProportion: 1.0,
			},
			expectedError: "the preferred backend (hostname) has not been found among the list of configured backends",
		},
		"multiple endpoints, preferred backend set and exists": {
			cfg: ProxyConfig{
				BackendEndpoints:                   "http://blah,http://other-blah",
				PreferredBackend:                   "blah",
				SecondaryBackendsRequestProportion: 1.0,
			},
			expectedError: "",
		},
		"multiple endpoints, preferred backend set and does not exist": {
			cfg: ProxyConfig{
				BackendEndpoints:                   "http://blah,http://other-blah",
				PreferredBackend:                   "blah-2",
				SecondaryBackendsRequestProportion: 1.0,
			},
			expectedError: "the preferred backend (hostname) has not been found among the list of configured backends",
		},
		"invalid endpoint": {
			cfg: ProxyConfig{
				BackendEndpoints:                   "://blah",
				SecondaryBackendsRequestProportion: 1.0,
			},
			expectedError: `invalid backend endpoint ://blah: parse "://blah": missing protocol scheme`,
		},
		"multiple endpoints, secondary request proportion less than 0": {
			cfg: ProxyConfig{
				BackendEndpoints:                   "http://blah,http://other-blah",
				PreferredBackend:                   "blah",
				SecondaryBackendsRequestProportion: -0.1,
			},
			expectedError: "secondary request proportion must be between 0 and 1 (inclusive)",
		},
		"multiple endpoints, secondary request proportion greater than 1": {
			cfg: ProxyConfig{
				BackendEndpoints:                   "http://blah,http://other-blah",
				PreferredBackend:                   "blah",
				SecondaryBackendsRequestProportion: 1.1,
			},
			expectedError: "secondary request proportion must be between 0 and 1 (inclusive)",
		},
		"multiple endpoints, secondary request proportion 1 and preferred backend set": {
			cfg: ProxyConfig{
				BackendEndpoints:                   "http://blah,http://other-blah",
				PreferredBackend:                   "blah",
				SecondaryBackendsRequestProportion: 1.0,
			},
			expectedError: "",
		},
		"multiple endpoints, secondary request proportion 1 and preferred backend not set": {
			cfg: ProxyConfig{
				BackendEndpoints:                   "http://blah,http://other-blah",
				SecondaryBackendsRequestProportion: 1.0,
			},
			expectedError: "",
		},
		"multiple endpoints, secondary request proportion not 1 and preferred backend set": {
			cfg: ProxyConfig{
				BackendEndpoints:                   "http://blah,http://other-blah",
				PreferredBackend:                   "blah",
				SecondaryBackendsRequestProportion: 0.7,
			},
			expectedError: "",
		},
		"multiple endpoints, secondary request proportion not 1 and preferred backend not set": {
			cfg: ProxyConfig{
				BackendEndpoints:                   "http://blah,http://other-blah",
				SecondaryBackendsRequestProportion: 0.7,
			},
			expectedError: "preferred backend must be set when secondary backends request proportion is not 1",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			p, err := NewProxy(testCase.cfg, log.NewNopLogger(), testRoutes, nil)

			if testCase.expectedError == "" {
				require.NoError(t, err)
				require.NotNil(t, p)
			} else {
				require.EqualError(t, err, testCase.expectedError)
				require.Nil(t, p)
			}
		})
	}
}

func Test_Proxy_RequestsForwarding(t *testing.T) {
	const (
		querySingleMetric1 = `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"cortex_build_info"},"value":[1583320883,"1"]}]}}`
		querySingleMetric2 = `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"cortex_build_info"},"value":[1583320883,"2"]}]}}`
	)

	type mockedBackend struct {
		pathPrefix string
		handler    http.HandlerFunc
	}

	tests := map[string]struct {
		requestPath         string
		requestMethod       string
		backends            []mockedBackend
		backendConfig       map[string]*BackendConfig
		preferredBackendIdx int
		expectedStatus      int
		expectedRes         string
	}{
		"one backend returning 2xx": {
			requestPath:   "/api/v1/query",
			requestMethod: http.MethodGet,
			backends: []mockedBackend{
				{handler: mockQueryResponse("/api/v1/query", 200, querySingleMetric1)},
			},
			expectedStatus: 200,
			expectedRes:    querySingleMetric1,
		},
		"one backend returning 5xx": {
			requestPath:   "/api/v1/query",
			requestMethod: http.MethodGet,
			backends: []mockedBackend{
				{handler: mockQueryResponse("/api/v1/query", 500, "")},
			},
			expectedStatus: 500,
			expectedRes:    "",
		},
		"two backends without path prefix": {
			requestPath:   "/api/v1/query",
			requestMethod: http.MethodGet,
			backends: []mockedBackend{
				{handler: mockQueryResponse("/api/v1/query", 200, querySingleMetric1)},
				{handler: mockQueryResponse("/api/v1/query", 200, querySingleMetric2)},
			},
			preferredBackendIdx: 0,
			expectedStatus:      200,
			expectedRes:         querySingleMetric1,
		},
		"two backends with the same path prefix": {
			requestPath:   "/api/v1/query",
			requestMethod: http.MethodGet,
			backends: []mockedBackend{
				{
					pathPrefix: "/prometheus",
					handler:    mockQueryResponse("/prometheus/api/v1/query", 200, querySingleMetric1),
				},
				{
					pathPrefix: "/prometheus",
					handler:    mockQueryResponse("/prometheus/api/v1/query", 200, querySingleMetric2),
				},
			},
			preferredBackendIdx: 0,
			expectedStatus:      200,
			expectedRes:         querySingleMetric1,
		},
		"two backends with different path prefix": {
			requestPath:   "/api/v1/query",
			requestMethod: http.MethodGet,
			backends: []mockedBackend{
				{
					pathPrefix: "/prefix-1",
					handler:    mockQueryResponse("/prefix-1/api/v1/query", 200, querySingleMetric1),
				},
				{
					pathPrefix: "/prefix-2",
					handler:    mockQueryResponse("/prefix-2/api/v1/query", 200, querySingleMetric2),
				},
			},
			preferredBackendIdx: 0,
			expectedStatus:      200,
			expectedRes:         querySingleMetric1,
		},
		"preferred backend returns 4xx": {
			requestPath:   "/api/v1/query",
			requestMethod: http.MethodGet,
			backends: []mockedBackend{
				{handler: mockQueryResponse("/api/v1/query", 400, "")},
				{handler: mockQueryResponse("/api/v1/query", 200, querySingleMetric1)},
			},
			preferredBackendIdx: 0,
			expectedStatus:      400,
			expectedRes:         "",
		},
		"preferred backend returns 5xx": {
			requestPath:   "/api/v1/query",
			requestMethod: http.MethodGet,
			backends: []mockedBackend{
				{handler: mockQueryResponse("/api/v1/query", 500, querySingleMetric1)},
				{handler: mockQueryResponse("/api/v1/query", 200, querySingleMetric2)},
			},
			preferredBackendIdx: 0,
			expectedStatus:      500,
			expectedRes:         querySingleMetric1,
		},
		"non-preferred backend returns 5xx": {
			requestPath:   "/api/v1/query",
			requestMethod: http.MethodGet,
			backends: []mockedBackend{
				{handler: mockQueryResponse("/api/v1/query", 200, querySingleMetric1)},
				{handler: mockQueryResponse("/api/v1/query", 500, "")},
			},
			preferredBackendIdx: 0,
			expectedStatus:      200,
			expectedRes:         querySingleMetric1,
		},
		"all backends returns 5xx": {
			requestPath:   "/api/v1/query",
			requestMethod: http.MethodGet,
			backends: []mockedBackend{
				{handler: mockQueryResponse("/api/v1/query", 500, querySingleMetric1)},
				{handler: mockQueryResponse("/api/v1/query", 500, "")},
			},
			preferredBackendIdx: 0,
			expectedStatus:      500,
			expectedRes:         querySingleMetric1,
		},
		"request to route with outgoing request transformer": {
			requestPath:   "/api/v1/query_with_transform",
			requestMethod: http.MethodPost,
			backends: []mockedBackend{
				{handler: mockQueryResponseWithExpectedBody("/api/v1/query_with_transform/transformed_1/transformed_2", "from 1\nfrom 2\n", 200, querySingleMetric1)},
			},
			expectedStatus: 200,
			expectedRes:    querySingleMetric1,
		},
		"adds request headers to specific backend": {
			requestPath:         "/api/v1/query",
			requestMethod:       http.MethodGet,
			preferredBackendIdx: 0,
			backendConfig: map[string]*BackendConfig{
				"0": {
					RequestHeaders: map[string][]string{
						"X-Test-Header": {"test-value"},
					},
				},
			},
			backends: []mockedBackend{
				{handler: func(rw http.ResponseWriter, r *http.Request) {
					assert.Equal(t, r.Header.Get("X-Test-Header"), "test-value")
					rw.WriteHeader(http.StatusOK)
				}},
				{handler: func(rw http.ResponseWriter, r *http.Request) {
					assert.Empty(t, r.Header.Get("X-Test-Header"))
					rw.WriteHeader(http.StatusOK)
				}},
			},
			expectedStatus: http.StatusOK,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			backendURLs := []string{}

			// Start backend servers.
			for _, b := range testData.backends {
				s := httptest.NewServer(b.handler)
				defer s.Close()

				backendURLs = append(backendURLs, s.URL+b.pathPrefix)
			}

			// Start the proxy.
			cfg := ProxyConfig{
				BackendEndpoints:                   strings.Join(backendURLs, ","),
				PreferredBackend:                   strconv.Itoa(testData.preferredBackendIdx),
				parsedBackendConfig:                testData.backendConfig,
				ServerHTTPServiceAddress:           "localhost",
				ServerHTTPServicePort:              0,
				ServerGRPCServiceAddress:           "localhost",
				ServerGRPCServicePort:              0,
				BackendReadTimeout:                 time.Second,
				SecondaryBackendsRequestProportion: 1.0,
			}

			if len(backendURLs) == 2 {
				cfg.CompareResponses = true
			}

			p, err := NewProxy(cfg, log.NewNopLogger(), testRoutes, prometheus.NewRegistry())
			require.NoError(t, err)
			require.NotNil(t, p)
			defer p.Stop() //nolint:errcheck

			require.NoError(t, p.Start())

			// Send a query request to the proxy.
			endpoint := fmt.Sprintf("http://%s"+testData.requestPath, p.server.HTTPListenAddr())
			req, err := http.NewRequest(testData.requestMethod, endpoint, nil)
			require.NoError(t, err)
			res, err := http.DefaultClient.Do(req)
			require.NoError(t, err)

			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)

			assert.Equal(t, testData.expectedStatus, res.StatusCode)
			assert.Equal(t, testData.expectedRes, string(body))
		})
	}
}

func TestProxy_Passthrough(t *testing.T) {
	type route struct {
		path, response string
	}

	type mockedBackend struct {
		routes []route
	}

	type query struct {
		path               string
		expectedRes        string
		expectedStatusCode int
	}

	const (
		pathCommon = "/common" // common path implemented by both backends

		pathZero = "/zero" // only implemented by backend at index 0
		pathOne  = "/one"  // only implemented by backend at index 1

		// responses by backend at index 0
		responseCommon0 = "common-0"
		responseZero    = "zero"

		// responses by backend at index 1
		responseCommon1 = "common-1"
		responseOne     = "one"
	)

	backends := []mockedBackend{
		{
			routes: []route{
				{
					path:     pathCommon,
					response: responseCommon0,
				},
				{
					path:     pathZero,
					response: responseZero,
				},
			},
		},
		{
			routes: []route{
				{
					path:     pathCommon,
					response: responseCommon1,
				},
				{
					path:     pathOne,
					response: responseOne,
				},
			},
		},
	}

	tests := map[string]struct {
		preferredBackendIdx int
		queries             []query
	}{
		"first backend preferred": {
			preferredBackendIdx: 0,
			queries: []query{
				{
					path:               pathCommon,
					expectedRes:        responseCommon0,
					expectedStatusCode: 200,
				},
				{
					path:               pathZero,
					expectedRes:        responseZero,
					expectedStatusCode: 200,
				},
				{
					path:               pathOne,
					expectedRes:        "404 page not found\n",
					expectedStatusCode: 404,
				},
			},
		},
		"second backend preferred": {
			preferredBackendIdx: 1,
			queries: []query{
				{
					path:               pathCommon,
					expectedRes:        responseCommon1,
					expectedStatusCode: 200,
				},
				{
					path:               pathOne,
					expectedRes:        responseOne,
					expectedStatusCode: 200,
				},
				{
					path:               pathZero,
					expectedRes:        "404 page not found\n",
					expectedStatusCode: 404,
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			backendURLs := []string{}

			// Start backend servers.
			for _, b := range backends {
				router := mux.NewRouter()
				for _, route := range b.routes {
					router.Handle(route.path, mockQueryResponse(route.path, 200, route.response))
				}
				s := httptest.NewServer(router)
				defer s.Close()

				backendURLs = append(backendURLs, s.URL)
			}

			// Start the proxy.
			cfg := ProxyConfig{
				BackendEndpoints:               strings.Join(backendURLs, ","),
				PreferredBackend:               strconv.Itoa(testData.preferredBackendIdx),
				ServerHTTPServiceAddress:       "localhost",
				ServerHTTPServicePort:          0,
				ServerGRPCServiceAddress:       "localhost",
				ServerGRPCServicePort:          0,
				BackendReadTimeout:             time.Second,
				PassThroughNonRegisteredRoutes: true,
			}

			p, err := NewProxy(cfg, log.NewNopLogger(), testRoutes, prometheus.NewRegistry())
			require.NoError(t, err)
			require.NotNil(t, p)
			defer p.Stop() //nolint:errcheck

			require.NoError(t, p.Start())

			for _, query := range testData.queries {

				// Send a query request to the proxy.
				endpoint := fmt.Sprintf("http://%s%s", p.server.HTTPListenAddr(), query.path)
				res, err := http.Get(endpoint)
				require.NoError(t, err)

				defer res.Body.Close()
				body, err := io.ReadAll(res.Body)
				require.NoError(t, err)

				assert.Equal(t, query.expectedStatusCode, res.StatusCode)
				assert.Equal(t, query.expectedRes, string(body))
			}
		})
	}
}

func TestProxyHTTPGRPC(t *testing.T) {
	const (
		pathPrefix         = "/api/v1/query"
		querySingleMetric1 = `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"cortex_build_info"},"value":[1583320883,"1"]}]}}`
		querySingleMetric2 = `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"cortex_build_info"},"value":[1583320883,"2"]}]}}`
	)

	type mockedBackend struct {
		pathPrefix string
		handler    http.HandlerFunc
	}

	backends := []mockedBackend{
		{handler: mockQueryResponse(pathPrefix, 200, querySingleMetric1)},
		{handler: mockQueryResponse(pathPrefix, 200, querySingleMetric2)},
	}

	t.Run("HTTP proxy connection", func(t *testing.T) {
		var backendURLs []string
		logger := log.NewNopLogger()

		// Start backend servers.
		for _, backend := range backends {
			server, err := server.New(server.Config{
				HTTPListenAddress: "localhost",
				HTTPListenPort:    0,
				GRPCListenAddress: "localhost",
				GRPCListenPort:    0,
				Registerer:        prometheus.NewRegistry(),
				Log:               logger,
			})
			require.NoError(t, err)
			server.HTTP.Path(pathPrefix).Methods("GET").Handler(backend.handler)
			go func() {
				defer server.Shutdown()
				require.NoError(t, server.Run())
			}()

			backendURLs = append(backendURLs, getServerAddress("http", server)+backend.pathPrefix)
		}

		// Start the proxy.
		cfg := ProxyConfig{
			BackendEndpoints:         strings.Join(backendURLs, ","),
			PreferredBackend:         strconv.Itoa(0), // First backend server is preferred response
			ServerHTTPServiceAddress: "localhost",
			ServerHTTPServicePort:    0,
			ServerGRPCServiceAddress: "localhost",
			ServerGRPCServicePort:    0,
			BackendReadTimeout:       time.Second,
		}

		p, err := NewProxy(cfg, logger, testRoutes, prometheus.NewRegistry())
		require.NoError(t, err)
		require.NotNil(t, p)
		defer p.Stop() //nolint:errcheck

		require.NoError(t, p.Start())

		// Send a query request to the proxy.
		endpoint := getServerAddress("http", p.server) + "/api/v1/query"

		res, err := http.Get(endpoint)
		require.NoError(t, err)

		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)

		assert.Equal(t, 200, res.StatusCode)
		assert.Equal(t, querySingleMetric1, string(body))
	})

	t.Run("gRPC proxy connection", func(t *testing.T) {
		var backendURLs []string
		logger := log.NewNopLogger()

		// Start backend servers.
		for _, backend := range backends {
			server, err := server.New(server.Config{
				HTTPListenAddress: "localhost",
				HTTPListenPort:    0,
				GRPCListenAddress: "localhost",
				GRPCListenPort:    0,
				Registerer:        prometheus.NewRegistry(),
				Log:               logger,
			})
			require.NoError(t, err)
			server.HTTP.Path(pathPrefix).Methods("GET").Handler(backend.handler)
			go func() {
				defer server.Shutdown()
				require.NoError(t, server.Run())
			}()

			backendURLs = append(backendURLs, getServerAddress("http", server)+backend.pathPrefix)
		}

		// Start the proxy.
		cfg := ProxyConfig{
			BackendEndpoints:         strings.Join(backendURLs, ","),
			PreferredBackend:         strconv.Itoa(0), // First backend server is preferred response
			ServerHTTPServiceAddress: "localhost",
			ServerHTTPServicePort:    0,
			ServerGRPCServiceAddress: "localhost",
			ServerGRPCServicePort:    0,
			BackendReadTimeout:       time.Second,
		}

		p, err := NewProxy(cfg, logger, testRoutes, prometheus.NewRegistry())
		require.NoError(t, err)
		require.NotNil(t, p)
		defer p.Stop() //nolint:errcheck

		require.NoError(t, p.Start())

		// gRPC connection to proxy
		grpcAddress := getServerAddress("grpc", p.server)

		// nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
		conn, err := grpc.Dial(grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		grpcClient := httpgrpc.NewHTTPClient(conn)

		// HTTP request message to be send via gRPC
		req := httpgrpc.HTTPRequest{
			Method: http.MethodGet,
			Url:    "/api/v1/query",
		}

		// Make gRPC request
		res, err := grpcClient.Handle(context.Background(), &req)
		require.NoError(t, err)

		assert.Equal(t, int32(200), res.Code)
		assert.Equal(t, querySingleMetric1, string(res.Body))
	})
}

func Test_NewProxy_BackendConfigPath(t *testing.T) {
	// Helper to create a temporary file with content
	createTempFile := func(t *testing.T, content string) string {
		tmpfile, err := os.CreateTemp("", "backend-config-*.yaml")
		require.NoError(t, err)

		defer tmpfile.Close()

		_, err = tmpfile.Write([]byte(content))
		require.NoError(t, err)

		return tmpfile.Name()
	}

	tests := map[string]struct {
		configContent  string
		createFile     bool
		expectedError  string
		expectedConfig map[string]*BackendConfig
	}{
		"missing file": {
			createFile:    false,
			expectedError: "failed to read backend config file (/nonexistent/path): open /nonexistent/path: no such file or directory",
		},
		"empty file": {
			createFile:     true,
			configContent:  "",
			expectedConfig: map[string]*BackendConfig(nil),
		},
		"invalid YAML structure (not a map)": {
			createFile:    true,
			configContent: "- item1\n- item2",
			expectedError: "failed to parse backend YAML config:",
		},
		"valid configuration": {
			createFile: true,
			configContent: `
              backend1:
                request_headers:
                  X-Custom-Header: ["value1", "value2"]
                  Cache-Control: ["no-store"]
              backend2:
                request_headers:
                  Authorization: ["Bearer token123"]
            `,
			expectedConfig: map[string]*BackendConfig{
				"backend1": {
					RequestHeaders: http.Header{
						"X-Custom-Header": {"value1", "value2"},
						"Cache-Control":   {"no-store"},
					},
				},
				"backend2": {
					RequestHeaders: http.Header{
						"Authorization": {"Bearer token123"},
					},
				},
			},
		},
		"configured backend which doesn't exist": {
			createFile: true,
			configContent: `
              backend1:
                request_headers:
                  X-Custom-Header: ["value1", "value2"]
                  Cache-Control: ["no-store"]
              backend2:
                request_headers:
                  Authorization: ["Bearer token123"]
              backend3:
                request_headers:
                  Authorization: ["Bearer token123"]
            `,
			expectedError: "backend3 does not exist in the list of actual backends",
			expectedConfig: map[string]*BackendConfig{
				"backend1": {
					RequestHeaders: http.Header{
						"X-Custom-Header": {"value1", "value2"},
						"Cache-Control":   {"no-store"},
					},
				},
				"backend2": {
					RequestHeaders: http.Header{
						"Authorization": {"Bearer token123"},
					},
				},
			},
		},
		"valid config with request proportions": {
			createFile: true,
			configContent: `
                backend1:
                  request_headers:
                    X-Custom-Header: ["value1"]
                  request_proportion: 0.8
                backend2:
                  request_headers:
                    Authorization: ["Bearer token123"]
                  request_proportion: 0.5
            `,
			expectedConfig: map[string]*BackendConfig{
				"backend1": {
					RequestHeaders: http.Header{
						"X-Custom-Header": {"value1"},
					},
					RequestProportion: func() *float64 { v := 0.8; return &v }(),
				},
				"backend2": {
					RequestHeaders: http.Header{
						"Authorization": {"Bearer token123"},
					},
					RequestProportion: func() *float64 { v := 0.5; return &v }(),
				},
			},
		},
		"multiple backends with request proportions": {
			createFile: true,
			configContent: `
                backend1:
                  request_headers:
                    X-Custom-Header: ["value1"]
                  request_proportion: 0.8
                backend2:
                  request_headers:
                    Authorization: ["Bearer token123"]
                  request_proportion: 0.5
                backend3:
                  request_headers:
                    X-Tenant-ID: ["tenant3"]
                  request_proportion: 1.0
                backend4:
                  request_headers:
                    X-Debug: ["true"]
                  request_proportion: 0.0
            `,
			expectedConfig: map[string]*BackendConfig{
				"backend1": {
					RequestHeaders: http.Header{
						"X-Custom-Header": {"value1"},
					},
					RequestProportion: func() *float64 { v := 0.8; return &v }(),
				},
				"backend2": {
					RequestHeaders: http.Header{
						"Authorization": {"Bearer token123"},
					},
					RequestProportion: func() *float64 { v := 0.5; return &v }(),
				},
				"backend3": {
					RequestHeaders: http.Header{
						"X-Tenant-ID": {"tenant3"},
					},
					RequestProportion: func() *float64 { v := DefaultRequestProportion; return &v }(),
				},
				"backend4": {
					RequestHeaders: http.Header{
						"X-Debug": {"true"},
					},
					RequestProportion: func() *float64 { v := 0.0; return &v }(),
				},
			},
		},
		"invalid request proportion - negative": {
			createFile: true,
			configContent: `
                backend1:
                  request_proportion: -0.1
            `,
			expectedError: "backend backend1: request_proportion must be between 0.0 and 1.0, got -0.100000",
		},
		"invalid request proportion - greater than 1": {
			createFile: true,
			configContent: `
                backend1:
                  request_proportion: 1.5
            `,
			expectedError: "backend backend1: request_proportion must be between 0.0 and 1.0, got 1.500000",
		},
	}

	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			// Base config that's valid except for the backend config path
			backendEndpoints := "http://backend1:9090,http://backend2:9090"

			// For the multiple backends test, add more backends
			if testName == "multiple backends with request proportions" {
				backendEndpoints = "http://backend1:9090,http://backend2:9090,http://backend3:9090,http://backend4:9090"
			}

			cfg := ProxyConfig{
				BackendEndpoints:                   backendEndpoints,
				ServerHTTPServiceAddress:           "localhost",
				ServerHTTPServicePort:              0,
				ServerGRPCServiceAddress:           "localhost",
				ServerGRPCServicePort:              0,
				SecondaryBackendsRequestProportion: 1.0,
			}

			if !testCase.createFile {
				cfg.BackendConfigFile = "/nonexistent/path"
			} else {
				tmpPath := createTempFile(t, testCase.configContent)
				cfg.BackendConfigFile = tmpPath
				defer os.Remove(tmpPath)
			}

			p, err := NewProxy(cfg, log.NewNopLogger(), testRoutes, nil)

			if testCase.expectedError != "" {
				assert.ErrorContains(t, err, testCase.expectedError)
				assert.Nil(t, p)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, p)
				assert.Equal(t, testCase.expectedConfig, p.cfg.parsedBackendConfig)
			}
		})
	}
}

func mockQueryResponse(path string, status int, res string) http.HandlerFunc {
	return mockQueryResponseWithExpectedBody(path, "", status, res)
}

func mockQueryResponseWithExpectedBody(path string, expectedBody string, status int, res string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Ensure the path is the expected one.
		if r.URL.Path != path {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if expectedBody != "" {
			actualBody, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			if string(actualBody) != expectedBody {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}

		// Send back the mocked response.
		w.WriteHeader(status)
		_, _ = w.Write([]byte(res))
	}
}

// getServerAddress gets the server address of protocol proto:
//   - http: HTTPListenAddr()
//   - grpc: GRPCListenAddr()
func getServerAddress(proto string, server *server.Server) string {
	switch proto {
	case "http":
		return "http://" + server.HTTPListenAddr().String()
	case "grpc":
		return "dns:///" + server.GRPCListenAddr().String()
	}

	return ""
}

func TestProxyEndpoint_TimeBasedBackendSelection(t *testing.T) {
	testCases := map[string]struct {
		backends         []backendTestConfig
		requestPath      string
		requestParams    map[string]string
		expectedBackends []string // Names of backends expected to be selected
		description      string
	}{
		"instant query with no time parameter - all backends selected": {
			backends: []backendTestConfig{
				{name: "preferred", preferred: true, minTimeThreshold: "0s"},
				{name: "recent", preferred: false, minTimeThreshold: "1h"},
				{name: "old", preferred: false, minTimeThreshold: "24h"},
			},
			requestPath: "/api/v1/query",
			requestParams: map[string]string{
				"query": "up",
			},
			expectedBackends: []string{"preferred", "recent", "old"},
			description:      "instant query without time defaults to now, should match all thresholds",
		},
		"instant query with current time - all backends selected": {
			backends: []backendTestConfig{
				{name: "preferred", preferred: true, minTimeThreshold: "0s"},
				{name: "recent", preferred: false, minTimeThreshold: "1h"},
				{name: "old", preferred: false, minTimeThreshold: "24h"},
			},
			requestPath: "/api/v1/query",
			requestParams: map[string]string{
				"query": "up",
				"time":  strconv.FormatInt(time.Now().Unix(), 10),
			},
			expectedBackends: []string{"preferred", "recent", "old"},
			description:      "instant query with current time should match all thresholds",
		},
		"instant query with old time - only preferred backend selected": {
			backends: []backendTestConfig{
				{name: "preferred", preferred: true, minTimeThreshold: "0s"},
				{name: "recent", preferred: false, minTimeThreshold: "1h"},
				{name: "old", preferred: false, minTimeThreshold: "24h"},
			},
			requestPath: "/api/v1/query",
			requestParams: map[string]string{
				"query": "up",
				"time":  strconv.FormatInt(time.Now().Add(-48*time.Hour).Unix(), 10),
			},
			expectedBackends: []string{"preferred"},
			description:      "48-hour old instant query should only match preferred backend (has 0s threshold)",
		},
		"range query covering recent and old data - only preferred backend": {
			backends: []backendTestConfig{
				{name: "preferred", preferred: true, minTimeThreshold: "0s"},
				{name: "recent", preferred: false, minTimeThreshold: "1h"},
				{name: "old", preferred: false, minTimeThreshold: "24h"},
			},
			requestPath: "/api/v1/query_range",
			requestParams: map[string]string{
				"query": "up",
				"start": strconv.FormatInt(time.Now().Add(-48*time.Hour).Unix(), 10),
				"end":   strconv.FormatInt(time.Now().Unix(), 10),
				"step":  "1h",
			},
			expectedBackends: []string{"preferred"},
			description:      "range query with 48h-ago start should only match preferred backend (min time too old for others)",
		},
		"range query with very recent data - all backends selected": {
			backends: []backendTestConfig{
				{name: "preferred", preferred: true, minTimeThreshold: "0s"},
				{name: "recent", preferred: false, minTimeThreshold: "1h"},
				{name: "old", preferred: false, minTimeThreshold: "24h"},
			},
			requestPath: "/api/v1/query_range",
			requestParams: map[string]string{
				"query": "up",
				"start": strconv.FormatInt(time.Now().Add(-30*time.Minute).Unix(), 10),
				"end":   strconv.FormatInt(time.Now().Unix(), 10),
				"step":  "1m",
			},
			expectedBackends: []string{"preferred", "recent", "old"},
			description:      "range query with recent data should match all backends",
		},
		"backends with zero threshold serve all queries": {
			backends: []backendTestConfig{
				{name: "preferred", preferred: true, minTimeThreshold: "0s"},
				{name: "secondary", preferred: false, minTimeThreshold: "0s"},
			},
			requestPath: "/api/v1/query",
			requestParams: map[string]string{
				"query": "up",
				"time":  strconv.FormatInt(time.Now().Add(-365*24*time.Hour).Unix(), 10), // 1 year old
			},
			expectedBackends: []string{"preferred", "secondary"},
			description:      "backends with 0s threshold should serve all queries regardless of time",
		},
		"preferred backend always included regardless of threshold": {
			backends: []backendTestConfig{
				{name: "preferred", preferred: true, minTimeThreshold: "1h"},
				{name: "secondary", preferred: false, minTimeThreshold: "1h"},
			},
			requestPath: "/api/v1/query",
			requestParams: map[string]string{
				"query": "up",
				"time":  strconv.FormatInt(time.Now().Add(-48*time.Hour).Unix(), 10),
			},
			expectedBackends: []string{"preferred"},
			description:      "preferred backend should always be included even if query time is outside its threshold",
		},
		"three backends with different thresholds - 2 should be selected": {
			backends: []backendTestConfig{
				{name: "preferred", preferred: true, minTimeThreshold: "0s"},   // serves all
				{name: "short-term", preferred: false, minTimeThreshold: "1h"}, // serves last 1 hour
				{name: "long-term", preferred: false, minTimeThreshold: "72h"}, // serves last 72 hours
			},
			requestPath: "/api/v1/query",
			requestParams: map[string]string{
				"query": "up",
				"time":  strconv.FormatInt(time.Now().Add(-36*time.Hour).Unix(), 10), // 36 hours ago
			},
			expectedBackends: []string{"preferred", "long-term"},
			description:      "36-hour old query should be forwarded to preferred (0s threshold) and long-term (72h threshold) backends, but not short-term (1h threshold)",
		},
		"three backends with mixed thresholds - query within all thresholds": {
			backends: []backendTestConfig{
				{name: "preferred", preferred: true, minTimeThreshold: "0s"},
				{name: "medium", preferred: false, minTimeThreshold: "6h"},
				{name: "long", preferred: false, minTimeThreshold: "24h"},
			},
			requestPath: "/api/v1/query",
			requestParams: map[string]string{
				"query": "up",
				"time":  strconv.FormatInt(time.Now().Add(-3*time.Hour).Unix(), 10), // 3 hours ago
			},
			expectedBackends: []string{"preferred", "medium", "long"},
			description:      "3-hour old query should be forwarded to all backends as it's within all thresholds",
		},
	}

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			// Create mock backends
			var backends []ProxyBackendInterface
			for _, backendCfg := range tc.backends {
				u, err := url.Parse("http://localhost:9090")
				require.NoError(t, err)

				cfg := BackendConfig{
					MinTimeThreshold: backendCfg.minTimeThreshold,
				}

				backend := NewProxyBackend(
					backendCfg.name,
					u,
					time.Second,
					backendCfg.preferred,
					false,
					cfg,
				)
				backends = append(backends, backend)
			}

			endpoint := NewProxyEndpoint(
				backends,
				Route{RouteName: "test"},
				NewProxyMetrics(nil),
				log.NewNopLogger(),
				nil,
				0,
				1.0, // All secondary backends should be included by proportion
				false,
			)

			// Create test request
			req := httptest.NewRequest("GET", tc.requestPath, nil)
			q := req.URL.Query()
			for key, value := range tc.requestParams {
				q.Set(key, value)
			}
			req.URL.RawQuery = q.Encode()

			// Test backend selection
			selectedBackends := endpoint.selectBackends(req)

			// Extract names of selected backends
			var selectedNames []string
			for _, backend := range selectedBackends {
				selectedNames = append(selectedNames, backend.Name())
			}

			// Verify expected backends were selected
			assert.ElementsMatch(t, tc.expectedBackends, selectedNames, tc.description)
		})
	}
}

func TestTimeExtractionFunctions(t *testing.T) {
	baseTime := time.Now().Truncate(time.Second).UTC()

	testCases := map[string]struct {
		method       string
		path         string
		params       map[string]string
		expectedTime *time.Time // nil if expecting zero time
		description  string
	}{
		"instant query without time parameter": {
			method: "GET",
			path:   "/api/v1/query",
			params: map[string]string{
				"query": "up",
			},
			expectedTime: &baseTime, // Will be compared with tolerance
			description:  "should default to current time",
		},
		"instant query with Unix timestamp": {
			method: "GET",
			path:   "/api/v1/query",
			params: map[string]string{
				"query": "up",
				"time":  strconv.FormatInt(baseTime.Add(-2*time.Hour).Unix(), 10), // 2 hours ago
			},
			expectedTime: func() *time.Time { t := baseTime.Add(-2 * time.Hour); return &t }(),
			description:  "should parse Unix timestamp correctly",
		},
		"range query with start and end": {
			method: "GET",
			path:   "/api/v1/query_range",
			params: map[string]string{
				"query": "up",
				"start": strconv.FormatInt(baseTime.Add(-3*time.Hour).Unix(), 10), // 3 hours ago
				"end":   strconv.FormatInt(baseTime.Add(-2*time.Hour).Unix(), 10), // 2 hours ago
				"step":  "60s",
			},
			expectedTime: func() *time.Time { t := baseTime.Add(-3 * time.Hour); return &t }(),
			description:  "should extract start time as minimum",
		},
		"range query with end earlier than start": {
			method: "GET",
			path:   "/api/v1/query_range",
			params: map[string]string{
				"query": "up",
				"start": strconv.FormatInt(baseTime.Add(-2*time.Hour).Unix(), 10), // 2 hours ago
				"end":   strconv.FormatInt(baseTime.Add(-3*time.Hour).Unix(), 10), // 3 hours ago
				"step":  "60s",
			},
			expectedTime: func() *time.Time { t := baseTime.Add(-3 * time.Hour); return &t }(),
			description:  "should return earlier time even if it's the end parameter",
		},
		"range query with only start parameter": {
			method: "GET",
			path:   "/api/v1/query_range",
			params: map[string]string{
				"query": "up",
				"start": strconv.FormatInt(baseTime.Add(-4*time.Hour).Unix(), 10), // 4 hours ago
				"step":  "60s",
			},
			expectedTime: func() *time.Time { t := baseTime.Add(-4 * time.Hour); return &t }(),
			description:  "should use start time when end is missing",
		},
		"range query with only end parameter": {
			method: "GET",
			path:   "/api/v1/query_range",
			params: map[string]string{
				"query": "up",
				"end":   strconv.FormatInt(baseTime.Add(-5*time.Hour).Unix(), 10), // 5 hours ago
				"step":  "60s",
			},
			expectedTime: func() *time.Time { t := baseTime.Add(-5 * time.Hour); return &t }(),
			description:  "should use end time when start is missing",
		},
		"non-Prometheus API path": {
			method:       "GET",
			path:         "/api/v1/status/config",
			params:       map[string]string{},
			expectedTime: nil,
			description:  "should return zero time for non-query paths",
		},
		"invalid time format": {
			method: "GET",
			path:   "/api/v1/query",
			params: map[string]string{
				"query": "up",
				"time":  "invalid-time",
			},
			expectedTime: nil,
			description:  "should return zero time for invalid time format",
		},
	}

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			q := req.URL.Query()
			for key, value := range tc.params {
				q.Set(key, value)
			}
			req.URL.RawQuery = q.Encode()

			extractedTime := extractMinTimeFromRequest(req)

			if tc.expectedTime == nil {
				assert.True(t, extractedTime.IsZero(), tc.description)
			} else {
				// For "now" comparisons, allow some tolerance
				if tc.params["time"] == "" && tc.path == "/api/v1/query" {
					assert.WithinDuration(t, *tc.expectedTime, extractedTime, 5*time.Second, tc.description)
				} else {
					assert.Equal(t, *tc.expectedTime, extractedTime, tc.description)
				}
			}
		})
	}
}

func TestBackendConfig_TimeThresholdValidation(t *testing.T) {
	testCases := map[string]struct {
		config        map[string]*BackendConfig
		expectError   bool
		errorContains string
		description   string
	}{
		"valid time threshold": {
			config: map[string]*BackendConfig{
				"backend1": {
					MinTimeThreshold: "24h",
				},
			},
			expectError: false,
			description: "valid duration format should pass validation",
		},
		"multiple valid time thresholds": {
			config: map[string]*BackendConfig{
				"backend1": {MinTimeThreshold: "1h30m"},
				"backend2": {MinTimeThreshold: "168h"}, // 7 days = 168 hours
				"backend3": {MinTimeThreshold: "0s"},
			},
			expectError: false,
			description: "multiple valid duration formats should pass validation",
		},
		"invalid time threshold": {
			config: map[string]*BackendConfig{
				"backend1": {
					MinTimeThreshold: "invalid-duration",
				},
			},
			expectError:   true,
			errorContains: "invalid min_time_threshold format",
			description:   "invalid duration format should fail validation",
		},
		"empty time threshold": {
			config: map[string]*BackendConfig{
				"backend1": {
					MinTimeThreshold: "",
				},
			},
			expectError: false,
			description: "empty threshold should pass validation (defaults to 0)",
		},
	}

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			var backends []ProxyBackendInterface
			for backendName := range tc.config {
				u, _ := url.Parse("http://localhost:9090")
				backend := NewProxyBackend(backendName, u, time.Second, false, false, *tc.config[backendName])
				backends = append(backends, backend)
			}

			err := validateBackendConfig(backends, tc.config)

			if tc.expectError {
				assert.Error(t, err, tc.description)
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains, tc.description)
				}
			} else {
				assert.NoError(t, err, tc.description)
			}
		})
	}
}

type backendTestConfig struct {
	name             string
	preferred        bool
	minTimeThreshold string
}
