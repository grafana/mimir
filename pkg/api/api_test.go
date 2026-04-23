// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/api/api_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package api

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/querier/tenantfederation"
	"github.com/grafana/mimir/pkg/util/activitytracker"
	"github.com/grafana/mimir/pkg/util/gziphandler"
)

type FakeLogger struct{}

func (fl *FakeLogger) Log(...interface{}) error {
	return nil
}

func TestNewApiWithoutSourceIPExtractor(t *testing.T) {
	cfg := Config{}
	serverCfg := getServerConfig(t)
	serverCfg.MetricsNamespace = "without_source_ip_extractor"
	federationCfg := tenantfederation.Config{}

	require.NoError(t, serverCfg.LogLevel.Set("info"))
	srv, err := server.New(serverCfg)
	require.NoError(t, err)

	api, err := New(cfg, federationCfg, serverCfg, srv, &FakeLogger{}, nil)
	require.NoError(t, err)
	require.Nil(t, api.sourceIPs)
}

func TestNewApiWithSourceIPExtractor(t *testing.T) {
	cfg := Config{}
	serverCfg := getServerConfig(t)
	serverCfg.LogSourceIPs = true
	serverCfg.MetricsNamespace = "with_source_ip_extractor"
	federationCfg := tenantfederation.Config{}

	require.NoError(t, serverCfg.LogLevel.Set("info"))
	srv, err := server.New(serverCfg)
	require.NoError(t, err)

	api, err := New(cfg, federationCfg, serverCfg, srv, &FakeLogger{}, nil)
	require.NoError(t, err)
	require.NotNil(t, api.sourceIPs)
}

func TestNewApiWithInvalidSourceIPExtractor(t *testing.T) {
	cfg := Config{}
	s := server.Server{
		HTTP: &mux.Router{},
	}
	serverCfg := getServerConfig(t)
	serverCfg.LogSourceIPs = true
	serverCfg.LogSourceIPsHeader = "SomeHeader"
	serverCfg.LogSourceIPsRegex = "[*"
	serverCfg.MetricsNamespace = "with_invalid_source_ip_extractor"
	federationCfg := tenantfederation.Config{}

	api, err := New(cfg, federationCfg, serverCfg, &s, &FakeLogger{}, nil)
	require.Error(t, err)
	require.Nil(t, api)
}

func TestApiGzip(t *testing.T) {
	cfg := Config{GzipCompressionLevel: gzip.DefaultCompression}
	serverCfg := getServerConfig(t)
	federationCfg := tenantfederation.Config{}
	srv, err := server.New(serverCfg)
	require.NoError(t, err)
	go func() { _ = srv.Run() }()
	t.Cleanup(srv.Stop)

	api, err := New(cfg, federationCfg, serverCfg, srv, log.NewNopLogger(), nil)
	require.NoError(t, err)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		size, err := strconv.Atoi(r.URL.Query().Get("respBodySize"))
		if err != nil {
			http.Error(w, fmt.Sprintf("respBodySize invalid: %s", err), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, size))
	})

	api.RegisterRoute("/gzip_enabled", handler, false, true, http.MethodGet)
	api.RegisterRoute("/gzip_disabled", handler, false, false, http.MethodGet)

	for _, tc := range []struct {
		name                 string
		endpoint             string
		respBodySize         int
		acceptEncodingHeader string
		expectedGzip         bool
	}{
		{
			name:                 "happy case gzip",
			endpoint:             "gzip_enabled",
			respBodySize:         gziphandler.DefaultMinSize + 1,
			acceptEncodingHeader: "gzip",
			expectedGzip:         true,
		},
		{
			name:                 "gzip with priority header",
			endpoint:             "gzip_enabled",
			respBodySize:         gziphandler.DefaultMinSize + 1,
			acceptEncodingHeader: "gzip;q=1",
			expectedGzip:         true,
		},
		{
			name:                 "gzip because any is accepted",
			endpoint:             "gzip_enabled",
			respBodySize:         gziphandler.DefaultMinSize + 1,
			acceptEncodingHeader: "*",
			expectedGzip:         true,
		},
		{
			name:                 "no gzip because no header",
			endpoint:             "gzip_enabled",
			respBodySize:         gziphandler.DefaultMinSize + 1,
			acceptEncodingHeader: "",
			expectedGzip:         false,
		},
		{
			name:                 "no gzip because not accepted",
			endpoint:             "gzip_enabled",
			respBodySize:         gziphandler.DefaultMinSize + 1,
			acceptEncodingHeader: "identity",
			expectedGzip:         false,
		},
		{
			name:                 "no gzip because small payload",
			endpoint:             "gzip_enabled",
			respBodySize:         1,
			acceptEncodingHeader: "gzip",
			expectedGzip:         false,
		},
		{
			name:                 "forced gzip with small payload",
			endpoint:             "gzip_enabled",
			respBodySize:         1,
			acceptEncodingHeader: "gzip;q=1, *;q=0",
			expectedGzip:         true,
		},
		{
			name:                 "gzip disabled endpoint",
			endpoint:             "gzip_disabled",
			respBodySize:         gziphandler.DefaultMinSize + 1,
			acceptEncodingHeader: "gzip",
			expectedGzip:         false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := fmt.Sprintf("http://%s:%d/%s?respBodySize=%d", serverCfg.HTTPListenAddress, serverCfg.HTTPListenPort, tc.endpoint, tc.respBodySize)
			req, err := http.NewRequest(http.MethodGet, u, nil)
			require.NoError(t, err)
			if tc.acceptEncodingHeader != "" {
				req.Header.Set("Accept-Encoding", tc.acceptEncodingHeader)
			}

			res, err := http.DefaultClient.Do(req)
			require.NoError(t, err)

			require.Equal(t, http.StatusOK, res.StatusCode)
			if tc.expectedGzip {
				require.Equal(t, "gzip", res.Header.Get("Content-Encoding"), "Invalid Content-Encoding header value")
			} else {
				require.Empty(t, res.Header.Get("Content-Encoding"), "Invalid Content-Encoding header value")
			}
		})
	}

	t.Run("compressed with gzip", func(*testing.T) {
	})
}

type MockIngester struct {
	ingester.API
}

func (mi MockIngester) ShutdownHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func TestApiIngesterShutdown(t *testing.T) {
	for _, tc := range []struct {
		name               string
		expectedStatusCode int
	}{
		{
			name:               "flag not set (default), disable GET request for ingester shutdown",
			expectedStatusCode: http.StatusMethodNotAllowed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{GzipCompressionLevel: gzip.DefaultCompression}
			serverCfg := getServerConfig(t)
			federationCfg := tenantfederation.Config{}
			srv, err := server.New(serverCfg)
			require.NoError(t, err)

			api, err := New(cfg, federationCfg, serverCfg, srv, log.NewNopLogger(), nil)
			require.NoError(t, err)
			api.RegisterIngester(&MockIngester{})

			go func() { _ = srv.Run() }()
			t.Cleanup(func() {
				srv.Stop()
				srv.Shutdown()
			})

			req := httptest.NewRequest("GET", "/ingester/shutdown", nil)
			w := httptest.NewRecorder()
			api.server.HTTP.ServeHTTP(w, req)
			require.Equal(t, tc.expectedStatusCode, w.Code)

			// for POST request, it should always return 204
			req = httptest.NewRequest("POST", "/ingester/shutdown", nil)
			w = httptest.NewRecorder()
			api.server.HTTP.ServeHTTP(w, req)
			require.Equal(t, http.StatusNoContent, w.Code)
		})
	}
}

// Generates server config, with gRPC listening on random port.
func getServerConfig(t *testing.T) server.Config {
	grpcHost, grpcPortNum := getHostnameAndRandomPort(t)
	httpHost, httpPortNum := getHostnameAndRandomPort(t)

	cfg := server.Config{
		HTTPListenAddress: httpHost,
		HTTPListenPort:    httpPortNum,

		GRPCListenAddress: grpcHost,
		GRPCListenPort:    grpcPortNum,

		GRPCServerMaxRecvMsgSize: 1024,

		// Use new registry to avoid using default one and panic due to duplicate metrics.
		Registerer: prometheus.NewPedanticRegistry(),
	}
	require.NoError(t, cfg.LogLevel.Set("info"))
	return cfg
}

func getHostnameAndRandomPort(t *testing.T) (string, int) {
	listen, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	host, port, err := net.SplitHostPort(listen.Addr().String())
	require.NoError(t, err)
	require.NoError(t, listen.Close())

	portNum, err := strconv.Atoi(port)
	require.NoError(t, err)
	return host, portNum
}

// TestNewRouteMaxBodySize verifies that the handler chain assembled by newRoute correctly enforces
// the max body size limit. The max body size handler must run before the activity tracking middleware
// so that body reads by the activity tracker are also subject to the limit.
func TestNewRouteMaxBodySize(t *testing.T) {
	for _, tc := range []struct {
		name        string
		body        string
		contentType string
		// maxBytes is the limit passed to RegisterRouteWithMaxBodySize; 0 means no limit.
		maxBytes               int64
		expectedStatus         int
		expectInnerHandlerRuns bool
		// expectHandlerBodyErr is set when the inner handler reads the body and expects a MaxBytesError.
		expectHandlerBodyErr bool
	}{
		{
			name:                   "no limit: handler always runs",
			body:                   "query=up&time=42",
			contentType:            "application/x-www-form-urlencoded",
			maxBytes:               0,
			expectedStatus:         http.StatusOK,
			expectInnerHandlerRuns: true,
		},
		{
			name:                   "form-encoded body within limit: handler runs",
			body:                   "query=up&time=42",
			contentType:            "application/x-www-form-urlencoded",
			maxBytes:               1024,
			expectedStatus:         http.StatusOK,
			expectInnerHandlerRuns: true,
		},
		{
			// The activity tracker reads the form body to capture params. When the body exceeds
			// the limit applied by the outer max body size handler, the read returns a MaxBytesError
			// and the activity tracker rejects the request before calling the inner handler.
			name:                   "form-encoded body exceeds limit: rejected before inner handler",
			body:                   "query=up&time=42",
			contentType:            "application/x-www-form-urlencoded",
			maxBytes:               5,
			expectedStatus:         http.StatusRequestEntityTooLarge,
			expectInnerHandlerRuns: false,
		},
		{
			// For non-form-encoded requests the activity tracker does not read the body, so it
			// passes r.Body (already wrapped with MaxBytesReader) to the inner handler, which
			// must handle the limit itself.
			name:                   "non-form-encoded body exceeds limit: MaxBytesReader passed to inner handler",
			body:                   "binary payload that is too large",
			contentType:            "application/octet-stream",
			maxBytes:               5,
			expectedStatus:         http.StatusOK,
			expectInnerHandlerRuns: true,
			expectHandlerBodyErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			activityFile := filepath.Join(t.TempDir(), "activity-tracker")
			reg := prometheus.NewPedanticRegistry()
			at, err := activitytracker.NewActivityTracker(activitytracker.Config{Filepath: activityFile, MaxEntries: 1024}, reg)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, at.Close()) })

			cfg := Config{}
			serverCfg := getServerConfig(t)
			federationCfg := tenantfederation.Config{}
			srv, err := server.New(serverCfg)
			require.NoError(t, err)

			api, err := New(cfg, federationCfg, serverCfg, srv, log.NewNopLogger(), at)
			require.NoError(t, err)

			var innerHandlerRan bool
			var handlerBodyErr error
			inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				innerHandlerRan = true
				// application/x-www-form-urlencoded bodies are read in the activity tracker
				if tc.contentType != "application/x-www-form-urlencoded" {
					_, handlerBodyErr = io.ReadAll(r.Body)
				}
				w.WriteHeader(http.StatusOK)
			})

			api.RegisterRouteWithMaxBodySize("/test-max-body", inner, false, false, tc.maxBytes, http.MethodPost)

			req := httptest.NewRequest(http.MethodPost, "/test-max-body", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.contentType)
			w := httptest.NewRecorder()
			api.server.HTTP.ServeHTTP(w, req)

			require.Equal(t, tc.expectedStatus, w.Code)
			require.Equal(t, tc.expectInnerHandlerRuns, innerHandlerRan)
			if tc.expectHandlerBodyErr {
				require.Error(t, handlerBodyErr)
				var maxBytesErr *http.MaxBytesError
				require.ErrorAs(t, handlerBodyErr, &maxBytesErr)
			}
		})
	}
}

// TestActivityTrackingNotAppliedToPushRoute verifies that the activity tracking middleware is not
// applied to routes registered without a max body size (like /api/v1/push). A remote write v1
// request with Content-Type: application/x-www-form-urlencoded and a protobuf body containing
// 0x3B (';') must reach the inner handler. If the middleware ran, it would call r.ParseForm() on
// the body and Go 1.17+ would reject the semicolon with "invalid semicolon separator in query",
// returning HTTP 500 before the handler is ever called.
func TestActivityTrackingNotAppliedToPushRoute(t *testing.T) {
	activityFile := filepath.Join(t.TempDir(), "activity-tracker")
	reg := prometheus.NewPedanticRegistry()
	at, err := activitytracker.NewActivityTracker(activitytracker.Config{Filepath: activityFile, MaxEntries: 1024}, reg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, at.Close()) })

	cfg := Config{}
	serverCfg := getServerConfig(t)
	federationCfg := tenantfederation.Config{}
	srv, err := server.New(serverCfg)
	require.NoError(t, err)

	api, err := New(cfg, federationCfg, serverCfg, srv, log.NewNopLogger(), at)
	require.NoError(t, err)

	var innerHandlerCalled bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		innerHandlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	// RegisterRoute passes maxBodySizeIfAny=0, matching how /api/v1/push is registered.
	api.RegisterRoute("/api/v1/push", inner, false, false, http.MethodPost)

	body := []byte{0x0a, 0x12, 0x3B, 0x08, 0x01, 0x12, 0x0e}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/push", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	api.server.HTTP.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.True(t, innerHandlerCalled, "activity tracking middleware must not run on routes registered without a max body size")
}
