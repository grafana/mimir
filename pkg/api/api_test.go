// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/api/api_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package api

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"testing"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"

	"github.com/grafana/mimir/pkg/util/gziphandler"
)

type FakeLogger struct{}

func (fl *FakeLogger) Log(keyvals ...interface{}) error {
	return nil
}

func TestNewApiWithoutSourceIPExtractor(t *testing.T) {
	cfg := Config{}
	serverCfg := server.Config{
		MetricsNamespace: "without_source_ip_extractor",
	}
	server, err := server.New(serverCfg)
	require.NoError(t, err)

	api, err := New(cfg, serverCfg, server, &FakeLogger{})
	require.NoError(t, err)
	require.Nil(t, api.sourceIPs)
}

func TestNewApiWithSourceIPExtractor(t *testing.T) {
	cfg := Config{}
	serverCfg := server.Config{
		LogSourceIPs:     true,
		MetricsNamespace: "with_source_ip_extractor",
	}
	server, err := server.New(serverCfg)
	require.NoError(t, err)

	api, err := New(cfg, serverCfg, server, &FakeLogger{})
	require.NoError(t, err)
	require.NotNil(t, api.sourceIPs)
}

func TestNewApiWithInvalidSourceIPExtractor(t *testing.T) {
	cfg := Config{}
	s := server.Server{
		HTTP: &mux.Router{},
	}
	serverCfg := server.Config{
		LogSourceIPs:       true,
		LogSourceIPsHeader: "SomeHeader",
		LogSourceIPsRegex:  "[*",
		MetricsNamespace:   "with_invalid_source_ip_extractor",
	}

	api, err := New(cfg, serverCfg, &s, &FakeLogger{})
	require.Error(t, err)
	require.Nil(t, api)
}

func TestApiGzip(t *testing.T) {
	cfg := Config{}
	serverCfg := getServerConfig(t)
	srv, err := server.New(serverCfg)
	require.NoError(t, err)
	go func() { _ = srv.Run() }()
	t.Cleanup(srv.Stop)

	api, err := New(cfg, serverCfg, srv, log.NewNopLogger())
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

	t.Run("compressed with gzip", func(t *testing.T) {
	})
}

// Generates server config, with gRPC listening on random port.
func getServerConfig(t *testing.T) server.Config {
	grpcHost, grpcPortNum := getHostnameAndRandomPort(t)
	httpHost, httpPortNum := getHostnameAndRandomPort(t)

	return server.Config{
		HTTPListenAddress: httpHost,
		HTTPListenPort:    httpPortNum,

		GRPCListenAddress: grpcHost,
		GRPCListenPort:    grpcPortNum,

		GPRCServerMaxRecvMsgSize: 1024,
	}
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
