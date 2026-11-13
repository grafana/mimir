// SPDX-License-Identifier: AGPL-3.0-only

package querytee

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type streamingTestComparator struct {
	calls atomic.Int32
}

func (c *streamingTestComparator) Compare(_, _ []byte, _ time.Time) (ComparisonResult, error) {
	c.calls.Add(1)
	return ComparisonSuccess, nil
}

func TestProxyEndpoint_StreamsPreferredOnlyResponse(t *testing.T) {
	for _, filteredSecondary := range []bool{false, true} {
		t.Run(fmt.Sprintf("filtered_secondary=%t", filteredSecondary), func(t *testing.T) {
			// Exceed HTTP buffers without requiring explicit flushing.
			initialBody := strings.Repeat("first\n", 16*1024)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/prefix/api/v1/query/transformed_1/transformed_2" || r.Header.Get("X-Test") != "configured" || r.Header.Get("Accept-Encoding") != "" {
					http.Error(w, "incorrect backend request", http.StatusBadRequest)
					return
				}
				user, pass, ok := r.BasicAuth()
				if !ok || user != "user" || pass != "password" {
					http.Error(w, "incorrect authentication", http.StatusUnauthorized)
					return
				}
				body, err := io.ReadAll(r.Body)
				if err != nil || string(body) != "query=upfrom 1\nfrom 2\n" {
					http.Error(w, "incorrect request body", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusAccepted)
				if _, err := io.WriteString(w, initialBody); err != nil {
					return // The client may have disconnected during test cleanup.
				}
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				_, err = io.WriteString(w, "last\n")
				if err != nil {
					return // The client may have disconnected during test cleanup.
				}
			}))
			t.Cleanup(backend.Close)
			t.Cleanup(unblock)
			backendURL, err := url.Parse(backend.URL + "/prefix")
			require.NoError(t, err)
			backendURL.User = url.UserPassword("user", "password")
			cfg := defaultBackendConfig()
			cfg.RequestHeaders = map[string][]string{"X-Test": {"configured"}}
			backends := []ProxyBackendInterface{NewProxyBackend("preferred", backendURL, 5*time.Second, true, false, "", cfg)}
			if filteredSecondary {
				backends = append(backends, &mockProxyBackend{name: "secondary", excludeTenants: true})
			}
			comparator := &streamingTestComparator{}
			metrics := NewProxyMetrics(nil)
			route := Route{RouteName: "test", RequestTransformers: []RequestTransformer{
				func(r *http.Request, body []byte, _ *spanlogger.SpanLogger) (*http.Request, []byte, error) {
					r = r.Clone(r.Context())
					r.URL.Path += "/transformed_1/transformed_2"
					body = append(body, []byte("from 1\nfrom 2\n")...)
					r.ContentLength = int64(len(body))
					return r, body, nil
				},
			}}
			endpoint := NewProxyEndpoint(backends, route, metrics, log.NewNopLogger(), comparator, 0, 1, false, newTestQueryDecoder())
			proxy := httptest.NewServer(endpoint)
			t.Cleanup(proxy.Close)
			client := proxy.Client()
			client.Timeout = 5 * time.Second
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, proxy.URL+"/api/v1/query", strings.NewReader("query=up"))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("Accept-Encoding", "gzip")
			req.Header.Set("X-Scope-OrgID", "tenant")
			res, err := client.Do(req)
			require.NoError(t, err)
			defer func() { require.NoError(t, res.Body.Close()) }()
			require.Equal(t, http.StatusAccepted, res.StatusCode)
			require.Equal(t, "application/json", res.Header.Get("Content-Type"))
			first := make([]byte, len("first\n"))
			_, err = io.ReadFull(res.Body, first)
			require.NoError(t, err)
			require.Equal(t, "first\n", string(first))
			// The backend cannot finish until this point: receiving the first chunk proves streaming.
			unblock()
			rest, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			require.Equal(t, initialBody[len(first):]+"last\n", string(rest))
			require.Eventually(t, func() bool {
				return testutil.ToFloat64(metrics.responsesTotal.WithLabelValues("preferred", http.MethodPost, "test")) == 1
			}, time.Second, time.Millisecond)
			require.Zero(t, comparator.calls.Load())
		})
	}
}

func TestProxyEndpoint_StreamReadTimeout(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Send enough data to reach the client despite normal HTTP buffering.
		if _, err := io.WriteString(w, strings.Repeat("partial", 16*1024)); err != nil {
			return // The client may have disconnected during test cleanup.
		}
		<-r.Context().Done()
	}))
	t.Cleanup(backend.Close)
	backendURL, err := url.Parse(backend.URL)
	require.NoError(t, err)
	backends := []ProxyBackendInterface{NewProxyBackend("preferred", backendURL, 200*time.Millisecond, true, false, "", defaultBackendConfig())}
	metrics := NewProxyMetrics(nil)
	endpoint := NewProxyEndpoint(backends, Route{RouteName: "test"}, metrics, log.NewNopLogger(), nil, 0, 1, false, newTestQueryDecoder())
	proxy := httptest.NewServer(endpoint)
	t.Cleanup(proxy.Close)
	client := proxy.Client()
	client.Timeout = 5 * time.Second
	res, err := client.Get(proxy.URL)
	require.NoError(t, err)
	defer func() { require.NoError(t, res.Body.Close()) }()
	first := make([]byte, len("partial"))
	_, err = io.ReadFull(res.Body, first)
	require.NoError(t, err)
	require.Equal(t, "partial", string(first))
	_, err = io.Copy(io.Discard, res.Body)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.responsesTotal.WithLabelValues("preferred", http.MethodGet, "test")))
}

type failingStreamWriter struct {
	http.ResponseWriter
}

func (failingStreamWriter) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func TestProxyEndpoint_StreamWriteFailure(t *testing.T) {
	cancelled := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.WriteString(w, "partial"); err != nil {
			return // The client may have disconnected during test cleanup.
		}
		if err := http.NewResponseController(w).Flush(); err != nil {
			return // The client may have disconnected during test cleanup.
		}
		<-r.Context().Done()
		close(cancelled)
	}))
	t.Cleanup(backend.Close)
	backendURL, err := url.Parse(backend.URL)
	require.NoError(t, err)
	backends := []ProxyBackendInterface{NewProxyBackend("preferred", backendURL, 5*time.Second, true, false, "", defaultBackendConfig())}
	endpoint := NewProxyEndpoint(backends, Route{RouteName: "test"}, NewProxyMetrics(nil), log.NewNopLogger(), nil, 0, 1, false, newTestQueryDecoder())
	writer := failingStreamWriter{httptest.NewRecorder()}
	require.PanicsWithValue(t, http.ErrAbortHandler, func() {
		endpoint.ServeHTTP(writer, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	})
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("backend request was not cancelled after downstream write failure")
	}
}

func TestProxyEndpoint_StreamRequestFailure(t *testing.T) {
	backendURL, err := url.Parse("http://unused")
	require.NoError(t, err)
	backend := NewProxyBackend("preferred", backendURL, time.Second, true, false, "", defaultBackendConfig())
	endpoint := NewProxyEndpoint([]ProxyBackendInterface{backend}, Route{RouteName: "test"}, NewProxyMetrics(nil), log.NewNopLogger(), nil, 0, 1, false, newTestQueryDecoder())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	writer := httptest.NewRecorder()
	endpoint.ServeHTTP(writer, httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil))
	require.Equal(t, http.StatusInternalServerError, writer.Code)
	require.Contains(t, writer.Body.String(), "context canceled")
}
