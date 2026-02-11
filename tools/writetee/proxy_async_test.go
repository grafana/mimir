// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAsyncBackendDispatcher_ShouldNotBlockOnNonPreferredBackends(t *testing.T) {
	logger := log.NewNopLogger()
	registry := prometheus.NewRegistry()
	metrics := NewProxyMetrics(registry)

	// Create a slow non-preferred backend
	slowBackendReceived := make(chan struct{})
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(slowBackendReceived)
		// Simulate slow backend - sleep longer than the test timeout
		time.Sleep(5 * time.Second)
		w.WriteHeader(200)
		_, _ = w.Write([]byte("slow response"))
	}))
	defer slowServer.Close()

	// Create a fast preferred backend
	fastServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("fast response"))
	}))
	defer fastServer.Close()

	preferredBackend := NewProxyBackend("fast", mustParseURLAsync(fastServer.URL), 5*time.Second, true, false, BackendTypeMirrored)
	slowBackend := NewProxyBackend("slow", mustParseURLAsync(slowServer.URL), 5*time.Second, false, false, BackendTypeMirrored)
	backends := []ProxyBackend{preferredBackend, slowBackend}

	asyncDispatcher := NewAsyncBackendDispatcher(1000, metrics)
	defer asyncDispatcher.Stop()

	route := Route{
		Path:      "/api/v1/push",
		RouteName: "api_v1_push",
		Methods:   []string{"POST"},
	}

	endpoint, err := NewProxyEndpoint(backends, route, metrics, logger, 0, 1.0, nil, asyncDispatcher)
	require.NoError(t, err)

	// Create a test request
	req := httptest.NewRequest("POST", "/api/v1/push", bytes.NewReader([]byte("test body")))
	rec := httptest.NewRecorder()

	// Execute the request - should return quickly even though slow backend is slow
	start := time.Now()
	endpoint.ServeHTTP(rec, req)
	elapsed := time.Since(start)

	// Response should come from fast preferred backend
	assert.Equal(t, 200, rec.Code)
	assert.Equal(t, "fast response", rec.Body.String())

	// Should complete quickly (well under 1 second, not waiting for slow backend)
	assert.Less(t, elapsed, 1*time.Second, "Request should not wait for slow non-preferred backend")

	// Wait for slow backend to receive the request (confirming fire-and-forget dispatch)
	select {
	case <-slowBackendReceived:
		// Good - slow backend received the request
	case <-time.After(2 * time.Second):
		t.Fatal("Slow backend should have received the request")
	}
}

func TestAsyncBackendDispatcher_ShouldEnforceMaxInFlightLimitOnNonPreferredBackends(t *testing.T) {
	tests := []struct {
		name            string
		preferred       bool
		expectedLimited bool
	}{
		{
			name:            "non-preferred backend is limited",
			preferred:       false,
			expectedLimited: true,
		},
		{
			name:            "preferred backend is not limited",
			preferred:       true,
			expectedLimited: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			backend := NewProxyBackend("backend", mustParseURLAsync(blockingServer.URL), 5*time.Second, tt.preferred, false, BackendTypeMirrored)

			const maxInFlight = 5
			const totalRequests = 10
			asyncDispatcher := NewAsyncBackendDispatcher(maxInFlight, metrics)
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
				if asyncDispatcher.Dispatch(req.Context(), req, body, backend) {
					dispatched++
				} else {
					dropped++
				}
			}

			// Wait a bit for goroutines to start
			time.Sleep(100 * time.Millisecond)

			if tt.expectedLimited {
				assert.Equal(t, maxInFlight, dispatched, "unexpected dispatched count")
				assert.Equal(t, totalRequests-maxInFlight, dropped, "unexpected dropped count")
				assert.Equal(t, int32(maxInFlight), inFlight.Load(), "unexpected in-flight count")
			} else {
				assert.Equal(t, totalRequests, dispatched, "unexpected dispatched count")
				assert.Equal(t, 0, dropped, "unexpected dropped count")
				assert.Equal(t, int32(totalRequests), inFlight.Load(), "unexpected in-flight count")
			}
		})
	}
}

func TestAsyncBackendDispatcher_ConcurrentRequests(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewProxyMetrics(registry)

	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		time.Sleep(50 * time.Millisecond) // Short delay to allow concurrency
		w.WriteHeader(200)
	}))
	defer server.Close()

	backend := NewProxyBackend("test", mustParseURLAsync(server.URL), 5*time.Second, false, false, BackendTypeMirrored)

	// Create dispatcher allowing 10 concurrent requests
	asyncDispatcher := NewAsyncBackendDispatcher(10, metrics)

	req := httptest.NewRequest("POST", "/test", nil)
	body := []byte("test")

	// Dispatch 5 requests concurrently
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			asyncDispatcher.Dispatch(req.Context(), req, body, backend)
		}()
	}

	// Wait for dispatches to complete
	wg.Wait()

	// Stop and wait for all in-flight requests to complete
	asyncDispatcher.Stop()

	// All 5 requests should have been processed
	assert.Equal(t, int32(5), requestCount.Load(), "All dispatched requests should be processed")
}

func TestAsyncBackendDispatcher_GracefulShutdown(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewProxyMetrics(registry)

	var completed atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond) // Simulate work
		completed.Add(1)
		w.WriteHeader(200)
	}))
	defer server.Close()

	backend := NewProxyBackend("test", mustParseURLAsync(server.URL), 5*time.Second, false, false, BackendTypeMirrored)

	asyncDispatcher := NewAsyncBackendDispatcher(10, metrics)

	req := httptest.NewRequest("POST", "/test", nil)
	body := []byte("test")

	// Dispatch several requests
	for i := 0; i < 3; i++ {
		asyncDispatcher.Dispatch(req.Context(), req, body, backend)
	}

	// Stop should wait for in-flight requests to complete
	asyncDispatcher.Stop()

	// All requests should have completed
	assert.Equal(t, int32(3), completed.Load(), "Stop should wait for all in-flight requests")

	// New dispatches after stop should be rejected
	dispatched := asyncDispatcher.Dispatch(req.Context(), req, body, backend)
	assert.False(t, dispatched, "Dispatch after stop should be rejected")
}

func TestAsyncBackendDispatcher_MultipleBackends(t *testing.T) {
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

	backend1 := NewProxyBackend("backend1", mustParseURLAsync(server1.URL), 5*time.Second, false, false, BackendTypeMirrored)
	backend2 := NewProxyBackend("backend2", mustParseURLAsync(server2.URL), 5*time.Second, false, false, BackendTypeMirrored)

	// Each backend has its own semaphore with max 2
	asyncDispatcher := NewAsyncBackendDispatcher(2, metrics)

	req := httptest.NewRequest("POST", "/test", nil)
	body := []byte("test")

	// Dispatch to both backends
	asyncDispatcher.Dispatch(req.Context(), req, body, backend1)
	asyncDispatcher.Dispatch(req.Context(), req, body, backend1)
	asyncDispatcher.Dispatch(req.Context(), req, body, backend2)
	asyncDispatcher.Dispatch(req.Context(), req, body, backend2)

	asyncDispatcher.Stop()

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
