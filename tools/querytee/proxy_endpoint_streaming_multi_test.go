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
	"testing"
	"testing/iotest"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestProxyEndpoint_StreamsWithoutComparator(t *testing.T) {
	for _, secondaryFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("secondary_first=%t", secondaryFirst), func(t *testing.T) {
			// Exceed HTTP buffers without requiring explicit flushing.
			initialBody := strings.Repeat("first\n", 16*1024)
			releasePreferred := make(chan struct{})
			releaseSecondary := make(chan struct{})
			secondaryStarted := make(chan struct{})
			unblockPreferred := sync.OnceFunc(func() { close(releasePreferred) })
			unblockSecondary := sync.OnceFunc(func() { close(releaseSecondary) })
			preferred := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusAccepted)
				if _, err := io.WriteString(w, initialBody); err != nil {
					return // The client may have disconnected during test cleanup.
				}
				select {
				case <-releasePreferred:
				case <-r.Context().Done():
					return
				}
				if _, err := io.WriteString(w, "last\n"); err != nil {
					return // The client may have disconnected during test cleanup.
				}
			}))
			secondary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(secondaryStarted)
				select {
				case <-releaseSecondary:
				case <-r.Context().Done():
					return
				}
				w.WriteHeader(http.StatusServiceUnavailable)
				if _, err := io.WriteString(w, "secondary body must not reach the client"); err != nil {
					return // The client may have disconnected during test cleanup.
				}
			}))
			t.Cleanup(preferred.Close)
			t.Cleanup(secondary.Close)
			// Unblock the backends even when an assertion fails.
			t.Cleanup(unblockPreferred)
			t.Cleanup(unblockSecondary)
			preferredURL, err := url.Parse(preferred.URL)
			require.NoError(t, err)
			secondaryURL, err := url.Parse(secondary.URL)
			require.NoError(t, err)
			backends := []ProxyBackendInterface{
				NewProxyBackend("preferred", preferredURL, 5*time.Second, true, false, "", defaultBackendConfig()),
				NewProxyBackend("secondary", secondaryURL, 5*time.Second, false, false, "", defaultBackendConfig()),
			}
			registry := prometheus.NewRegistry()
			metrics := NewProxyMetrics(registry)
			route := Route{RouteName: "api_v1_cardinality_active_series"}
			endpoint := NewProxyEndpoint(backends, route, metrics, log.NewNopLogger(), nil, 0, 1, false, newTestQueryDecoder())
			proxy := httptest.NewServer(endpoint)
			t.Cleanup(proxy.Close)
			client := proxy.Client()
			client.Timeout = 5 * time.Second
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, proxy.URL+"/api/v1/cardinality/active_series", nil)
			require.NoError(t, err)
			res, err := client.Do(req)
			require.NoError(t, err)
			defer func() { require.NoError(t, res.Body.Close()) }()
			require.Equal(t, http.StatusAccepted, res.StatusCode)
			require.Equal(t, "application/json", res.Header.Get("Content-Type"))
			first := make([]byte, len("first\n"))
			_, err = io.ReadFull(res.Body, first)
			require.NoError(t, err)
			require.Equal(t, "first\n", string(first))
			select {
			case <-secondaryStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("secondary request did not start")
			}
			if secondaryFirst {
				unblockSecondary()
				require.Eventually(t, func() bool {
					return testutil.CollectAndCount(metrics.requestDuration, "cortex_querytee_backend_request_duration_seconds") == 1
				}, time.Second, time.Millisecond)
			}
			unblockPreferred()
			rest, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			require.Equal(t, initialBody[len(first):]+"last\n", string(rest))
			// The client finishes even while the secondary is blocked. Cancelling the
			// incoming request must not cancel the independent secondary request.
			cancel()
			unblockSecondary()
			require.Eventually(t, func() bool {
				return testutil.CollectAndCount(metrics.requestDuration, "cortex_querytee_backend_request_duration_seconds") == 2
			}, time.Second, time.Millisecond)
			families, err := registry.Gather()
			require.NoError(t, err)
			for _, family := range families {
				if family.GetName() != "cortex_querytee_backend_request_duration_seconds" {
					continue
				}
				for _, metric := range family.Metric {
					labels := map[string]string{}
					for _, label := range metric.Label {
						labels[label.GetName()] = label.GetValue()
					}
					expectedStatus := "202"
					if labels["backend"] == "secondary" {
						expectedStatus = "503"
					}
					require.Equal(t, expectedStatus, labels["status_code"])
					require.Equal(t, uint64(1), metric.Histogram.GetSampleCount())
				}
			}
			require.Equal(t, float64(1), testutil.ToFloat64(metrics.responsesTotal.WithLabelValues("preferred", http.MethodGet, route.RouteName)))
			require.Zero(t, testutil.CollectAndCount(metrics.responsesComparedTotal))
		})
	}
}

// limitedReadBufferBody represents a large response without allocating it. Rejecting
// oversized Read buffers catches io.ReadAll growing a response-sized allocation.
type limitedReadBufferBody struct {
	remaining int
	closed    bool
}

func (b *limitedReadBufferBody) Read(p []byte) (int, error) {
	if len(p) > 32*1024 {
		return 0, fmt.Errorf("response read buffer grew to %d bytes", len(p))
	}
	if b.remaining == 0 {
		return 0, io.EOF
	}
	n := min(len(p), b.remaining)
	clear(p[:n])
	b.remaining -= n
	return n, nil
}

func (b *limitedReadBufferBody) Close() error {
	b.closed = true
	return nil
}

func TestProxyEndpoint_SecondaryReadFailureDoesNotAffectPreferredResponse(t *testing.T) {
	backendURL, err := url.Parse("http://backend")
	require.NoError(t, err)
	newBackend := func(name string, preferred bool, body io.ReadCloser) ProxyBackendInterface {
		backend := NewProxyBackend(name, backendURL, time.Second, preferred, false, "", defaultBackendConfig()).(*ProxyBackend)
		backend.client.Transport = streamingTestRoundTripper(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: body, Header: make(http.Header)}, nil
		})
		return backend
	}
	backends := []ProxyBackendInterface{
		newBackend("preferred", true, io.NopCloser(strings.NewReader("preferred response"))),
		newBackend("secondary", false, io.NopCloser(iotest.ErrReader(io.ErrUnexpectedEOF))),
	}
	logger := newMockLogger()
	metrics := NewProxyMetrics(nil)
	endpoint := NewProxyEndpoint(backends, Route{RouteName: "test"}, metrics, logger, nil, 0, 1, false, newTestQueryDecoder())
	writer := httptest.NewRecorder()
	endpoint.ServeHTTP(writer, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	require.Equal(t, http.StatusOK, writer.Code)
	require.Equal(t, "preferred response", writer.Body.String())
	require.Eventually(t, func() bool {
		return testutil.CollectAndCount(metrics.requestDuration) == 2
	}, time.Second, time.Millisecond)
	logger.lock.Lock()
	defer logger.lock.Unlock()
	var foundError bool
	for _, message := range logger.messages {
		if message["backend"] == "secondary" && message["err"] != nil {
			require.ErrorIs(t, message["err"].(error), io.ErrUnexpectedEOF)
			foundError = true
		}
	}
	require.True(t, foundError, "secondary read failure must be logged")
}

func TestProxyEndpoint_SecondaryDrainTimeout(t *testing.T) {
	cancelled := make(chan struct{})
	secondary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.WriteString(w, "partial"); err != nil {
			return // The client may have disconnected during test cleanup.
		}
		if err := http.NewResponseController(w).Flush(); err != nil {
			return // The client may have disconnected during test cleanup.
		}
		<-r.Context().Done()
		close(cancelled)
	}))
	t.Cleanup(secondary.Close)
	secondaryURL, err := url.Parse(secondary.URL)
	require.NoError(t, err)
	preferred := &mockProxyBackend{name: "preferred", preferred: true, fakeResponseLatencies: []time.Duration{time.Millisecond}}
	backends := []ProxyBackendInterface{
		preferred,
		NewProxyBackend("secondary", secondaryURL, 200*time.Millisecond, false, false, "", defaultBackendConfig()),
	}
	endpoint := NewProxyEndpoint(backends, Route{RouteName: "test"}, NewProxyMetrics(nil), log.NewNopLogger(), nil, 0, 1, false, newTestQueryDecoder())
	writer := httptest.NewRecorder()
	endpoint.ServeHTTP(writer, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))
	require.Equal(t, http.StatusOK, writer.Code)
	require.Equal(t, "{}", writer.Body.String())
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("secondary body drain did not honour the backend timeout")
	}
}

type streamingTestRoundTripper func(*http.Request) (*http.Response, error)

func (f streamingTestRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestProxyEndpoint_DiscardsSecondaryBodyWithoutBuffering(t *testing.T) {
	backendURL, err := url.Parse("http://backend")
	require.NoError(t, err)
	preferredBody := &limitedReadBufferBody{remaining: 2 * 1024 * 1024}
	secondaryBody := &limitedReadBufferBody{remaining: 2 * 1024 * 1024}
	newBackend := func(name string, preferred bool, body *limitedReadBufferBody) ProxyBackendInterface {
		backend := NewProxyBackend(name, backendURL, time.Second, preferred, false, "", defaultBackendConfig()).(*ProxyBackend)
		backend.client.Transport = streamingTestRoundTripper(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: body, Header: make(http.Header)}, nil
		})
		return backend
	}
	backends := []ProxyBackendInterface{newBackend("preferred", true, preferredBody), newBackend("secondary", false, secondaryBody)}
	endpoint := NewProxyEndpoint(backends, Route{RouteName: "test"}, NewProxyMetrics(nil), log.NewNopLogger(), nil, 0, 1, false, newTestQueryDecoder())
	responses := make(chan *backendResponse, len(backends))
	var preferredBytes int64
	endpoint.executeBackendRequests(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil), backends, responses, func(res *http.Response) error {
		preferredBytes, err = io.Copy(io.Discard, res.Body)
		return err
	})
	for res := range responses {
		require.NoError(t, res.err)
		require.Nil(t, res.body, "response bytes must not be retained for %s", res.backend.Name())
	}
	require.Equal(t, int64(2*1024*1024), preferredBytes)
	for _, body := range []*limitedReadBufferBody{preferredBody, secondaryBody} {
		require.Zero(t, body.remaining)
		require.True(t, body.closed)
	}
}
