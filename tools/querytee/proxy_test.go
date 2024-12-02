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
		backendConfig       map[string]BackendConfig
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
			backendConfig: map[string]BackendConfig{
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
	if proto == "http" {
		return "http://" + server.HTTPListenAddr().String()
	} else if proto == "grpc" {
		return "dns:///" + server.GRPCListenAddr().String()
	}

	return ""
}
