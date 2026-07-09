// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestProxyEndpoint_AmplifiedCopiesAreFireAndForget(t *testing.T) {
	// With amplification factor > 1, the original (unsuffixed) request is sent synchronously and
	// its response returned immediately, while the amplified (suffixed) copies are dispatched
	// fire-and-forget. A slow amplified copy must not delay the client response.
	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	metrics := NewProxyMetrics(registry)

	amplifiedReceived := make(chan struct{})
	unblock := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		decompressed, err := snappy.Decode(nil, body)
		if err == nil && bytes.Contains(decompressed, []byte("_amp")) {
			// Amplified copy: signal receipt and block until the test unblocks us,
			// simulating a slow backend without delaying the synchronous response.
			close(amplifiedReceived)
			<-unblock
			w.WriteHeader(200)
			return
		}
		// Original request: respond immediately.
		w.WriteHeader(200)
		_, _ = w.Write([]byte("original response"))
	}))
	// Unblock the slow amplified handler before closing the server (defers run LIFO).
	defer server.Close()
	defer close(unblock)

	backend := NewHTTPProxyBackend("backend", mustParseURLAsync(server.URL), 10*time.Second, false)

	asyncDispatcher := NewAsyncBackendDispatcher(1000, metrics, logger)
	defer asyncDispatcher.Stop()

	route := Route{
		Path:      "/api/v1/push",
		RouteName: "api_v1_push",
		Methods:   []string{"POST"},
	}

	endpoint := NewProxyEndpoint(backend, route, metrics, logger, 2.0, NewAmplificationTracker(), asyncDispatcher)

	req := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader(makeTestWriteRequest(t)))
	rec := httptest.NewRecorder()

	// Execute the request - should return quickly even though the amplified copy is slow.
	start := time.Now()
	endpoint.ServeHTTP(rec, req)
	elapsed := time.Since(start)

	// Response should come from the synchronous original request.
	assert.Equal(t, 200, rec.Code)
	assert.Equal(t, "original response", rec.Body.String())

	// Should complete quickly (well under 1 second, not waiting for the slow amplified copy).
	assert.Less(t, elapsed, 1*time.Second, "Request should not wait for slow amplified copy")

	// Confirm the amplified copy was dispatched fire-and-forget.
	select {
	case <-amplifiedReceived:
		// Good - amplified copy was received.
	case <-time.After(2 * time.Second):
		t.Fatal("Amplified copy should have been received")
	}
}

func TestAsyncBackendDispatcher_ShouldEnforceMaxInFlightLimit(t *testing.T) {
	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	metrics := NewProxyMetrics(registry)

	// Create a backend that blocks until we signal it
	var inFlight atomic.Int32
	unblock := make(chan struct{})
	blockingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inFlight.Add(1)
		<-unblock // Block until signaled
		inFlight.Add(-1)
		w.WriteHeader(200)
	}))
	defer blockingServer.Close()

	backend := NewHTTPProxyBackend("backend", mustParseURLAsync(blockingServer.URL), 5*time.Second, false)

	const maxInFlight = 5
	const totalRequests = 10
	asyncDispatcher := NewAsyncBackendDispatcher(maxInFlight, metrics, logger)
	defer func() {
		close(unblock) // Unblock all handlers before stopping
		asyncDispatcher.Stop()
	}()

	req := httptest.NewRequest("POST", "/test", nil)
	body := []byte("test")

	// Dispatch more requests than max-in-flight allows
	dispatched := 0
	dropped := 0
	for i := 0; i < totalRequests; i++ {
		if asyncDispatcher.Dispatch(req.Context(), req, body, backend, "test_route") {
			dispatched++
		} else {
			dropped++
		}
	}

	// Wait a bit for goroutines to start
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, maxInFlight, dispatched, "unexpected dispatched count")
	assert.Equal(t, totalRequests-maxInFlight, dropped, "unexpected dropped count")
	assert.Equal(t, int32(maxInFlight), inFlight.Load(), "unexpected in-flight count")
}

func TestAsyncBackendDispatcher_ConcurrentRequests(t *testing.T) {
	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	metrics := NewProxyMetrics(registry)

	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		time.Sleep(50 * time.Millisecond) // Short delay to allow concurrency
		w.WriteHeader(200)
	}))
	defer server.Close()

	backend := NewHTTPProxyBackend("test", mustParseURLAsync(server.URL), 5*time.Second, false)

	// Create dispatcher allowing 10 concurrent requests
	asyncDispatcher := NewAsyncBackendDispatcher(10, metrics, logger)

	req := httptest.NewRequest("POST", "/test", nil)
	body := []byte("test")

	// Dispatch 5 requests concurrently
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			asyncDispatcher.Dispatch(req.Context(), req, body, backend, "test_route")
		}()
	}

	// Wait for dispatches to complete
	wg.Wait()

	// Stop and wait for all in-flight requests to complete
	asyncDispatcher.Stop()
	asyncDispatcher.Await()

	// All 5 requests should have been processed
	assert.Equal(t, int32(5), requestCount.Load(), "All dispatched requests should be processed")
}

func TestAsyncBackendDispatcher_GracefulShutdown(t *testing.T) {
	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	metrics := NewProxyMetrics(registry)

	var completed atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond) // Simulate work
		completed.Add(1)
		w.WriteHeader(200)
	}))
	defer server.Close()

	backend := NewHTTPProxyBackend("test", mustParseURLAsync(server.URL), 5*time.Second, false)

	asyncDispatcher := NewAsyncBackendDispatcher(10, metrics, logger)

	req := httptest.NewRequest("POST", "/test", nil)
	body := []byte("test")

	// Dispatch several requests
	for i := 0; i < 3; i++ {
		asyncDispatcher.Dispatch(req.Context(), req, body, backend, "test_route")
	}

	// Stop signals shutdown, Await waits for in-flight requests to complete
	asyncDispatcher.Stop()
	asyncDispatcher.Await()

	// All requests should have completed
	assert.Equal(t, int32(3), completed.Load(), "Await should wait for all in-flight requests")

	// New dispatches after stop should be rejected
	dispatched := asyncDispatcher.Dispatch(req.Context(), req, body, backend, "test_route")
	assert.False(t, dispatched, "Dispatch after stop should be rejected")
}

func TestAsyncBackendDispatcher_MultipleBackends(t *testing.T) {
	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	metrics := NewProxyMetrics(registry)

	var backend1Count, backend2Count atomic.Int32

	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backend1Count.Add(1)
		w.WriteHeader(200)
	}))
	defer server1.Close()

	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backend2Count.Add(1)
		w.WriteHeader(200)
	}))
	defer server2.Close()

	backend1 := NewHTTPProxyBackend("backend1", mustParseURLAsync(server1.URL), 5*time.Second, false)
	backend2 := NewHTTPProxyBackend("backend2", mustParseURLAsync(server2.URL), 5*time.Second, false)

	// Each backend has its own semaphore with max 2
	asyncDispatcher := NewAsyncBackendDispatcher(2, metrics, logger)

	req := httptest.NewRequest("POST", "/test", nil)
	body := []byte("test")

	// Dispatch to both backends
	asyncDispatcher.Dispatch(req.Context(), req, body, backend1, "test_route")
	asyncDispatcher.Dispatch(req.Context(), req, body, backend1, "test_route")
	asyncDispatcher.Dispatch(req.Context(), req, body, backend2, "test_route")
	asyncDispatcher.Dispatch(req.Context(), req, body, backend2, "test_route")

	asyncDispatcher.Stop()
	asyncDispatcher.Await()

	assert.Equal(t, int32(2), backend1Count.Load(), "Backend1 should receive 2 requests")
	assert.Equal(t, int32(2), backend2Count.Load(), "Backend2 should receive 2 requests")
}

func mustParseURLAsync(rawURL string) *url.URL {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic(fmt.Sprintf("failed to parse URL %s: %v", rawURL, err))
	}
	return u
}
