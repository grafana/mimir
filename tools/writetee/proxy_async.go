// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
)

// AsyncBackendDispatcher handles fire-and-forget requests to non-preferred backends.
// It uses a semaphore per backend to limit concurrent in-flight requests.
type AsyncBackendDispatcher struct {
	maxInFlight int
	metrics     *ProxyMetrics

	mu         sync.Mutex
	semaphores map[string]chan struct{} // semaphore per backend (buffered channel)
	wg         sync.WaitGroup
	stopped    bool
}

// NewAsyncBackendDispatcher creates a new dispatcher for non-preferred backends.
// maxInFlight controls the maximum number of concurrent in-flight requests per backend.
func NewAsyncBackendDispatcher(maxInFlight int, metrics *ProxyMetrics) *AsyncBackendDispatcher {
	return &AsyncBackendDispatcher{
		maxInFlight: maxInFlight,
		metrics:     metrics,
		semaphores:  make(map[string]chan struct{}),
	}
}

// Stop gracefully shuts down, waiting for all in-flight requests to complete.
func (d *AsyncBackendDispatcher) Stop() {
	d.mu.Lock()
	d.stopped = true
	d.mu.Unlock()

	// Wait for all in-flight requests to complete
	d.wg.Wait()
}

// Dispatch spawns a goroutine to send a request to the backend. Returns true if dispatched, false if dropped.
// Requests to non-preferred backends are dropped when the maximum number of in-flight requests is reached.
// Preferred backends are not subject to the max-in-flight limit.
func (d *AsyncBackendDispatcher) Dispatch(ctx context.Context, req *http.Request, body []byte, backend ProxyBackend) bool {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		d.metrics.droppedRequestsTotal.WithLabelValues(backend.Name(), "shutdown").Inc()
		return false
	}
	d.mu.Unlock()

	// Preferred backends are not subject to the max-in-flight limit
	if backend.Preferred() {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()

			var bodyReader io.ReadCloser
			if len(body) > 0 {
				bodyReader = io.NopCloser(bytes.NewReader(body))
			}

			// Fire and forget - we don't care about the response
			_, _, _, _ = backend.ForwardRequest(context.WithoutCancel(ctx), req, bodyReader)
		}()
		return true
	}

	d.mu.Lock()
	// Lazily create semaphore for this backend
	sema, ok := d.semaphores[backend.Name()]
	if !ok {
		sema = make(chan struct{}, d.maxInFlight)
		d.semaphores[backend.Name()] = sema
	}
	d.mu.Unlock()

	// Try to acquire permit (non-blocking)
	select {
	case sema <- struct{}{}:
		// Got permit - spawn goroutine
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			defer func() { <-sema }() // release permit

			var bodyReader io.ReadCloser
			if len(body) > 0 {
				bodyReader = io.NopCloser(bytes.NewReader(body))
			}

			// Fire and forget - we don't care about the response
			_, _, _, _ = backend.ForwardRequest(context.WithoutCancel(ctx), req, bodyReader)
		}()
		return true

	default:
		// At capacity - drop request
		d.metrics.droppedRequestsTotal.WithLabelValues(backend.Name(), "max_in_flight").Inc()
		return false
	}
}
