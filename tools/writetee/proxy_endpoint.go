// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	// maxBodySize is the maximum request body size we'll accept (100MB)
	maxBodySize = 100 * 1024 * 1024
)

type ProxyEndpoint struct {
	backends                     []ProxyBackend
	metrics                      *ProxyMetrics
	logger                       log.Logger
	amplificationFactor          float64
	amplifiedMaxSeriesPerRequest int
	amplificationTracker         *AmplificationTracker
	asyncDispatcher              *AsyncBackendDispatcher

	// The preferred backend (required).
	preferredBackend ProxyBackend

	route Route
}

func NewProxyEndpoint(backends []ProxyBackend, route Route, metrics *ProxyMetrics, logger log.Logger, amplificationFactor float64, amplifiedMaxSeriesPerRequest int, amplificationTracker *AmplificationTracker, asyncDispatcher *AsyncBackendDispatcher) (*ProxyEndpoint, error) {
	var preferredBackend ProxyBackend
	for _, backend := range backends {
		if backend.Preferred() {
			preferredBackend = backend
			break
		}
	}

	if preferredBackend == nil {
		return nil, fmt.Errorf("no preferred backend configured")
	}

	return &ProxyEndpoint{
		backends:                     backends,
		route:                        route,
		metrics:                      metrics,
		logger:                       logger,
		amplificationFactor:          amplificationFactor,
		amplifiedMaxSeriesPerRequest: amplifiedMaxSeriesPerRequest,
		amplificationTracker:         amplificationTracker,
		preferredBackend:             preferredBackend,
		asyncDispatcher:              asyncDispatcher,
	}, nil
}

func (p *ProxyEndpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger, ctx := spanlogger.New(r.Context(), p.logger, tracer, "Incoming proxied write request")
	defer logger.Finish()

	logger.SetSpanAndLogTag("path", r.URL.Path)
	logger.SetSpanAndLogTag("route_name", p.route.RouteName)
	logger.SetSpanAndLogTag("user", r.Header.Get("X-Scope-OrgID"))
	logger.SetSpanAndLogTag("method", r.Method)

	level.Debug(logger).Log("msg", "Received write request")

	// Read the entire request body into memory.
	// We need to do this because we need to send the same body to all backends,
	// and the body can only be read once.
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodySize+1))
	if err != nil {
		level.Error(logger).Log("msg", "Unable to read request body", "err", err)
		http.Error(w, fmt.Sprintf("failed to read request body: %v", err), http.StatusInternalServerError)
		return
	}
	if err := r.Body.Close(); err != nil {
		level.Warn(logger).Log("msg", "Unable to close request body", "err", err)
	}

	// Check if body exceeds max size
	if len(body) > maxBodySize {
		level.Warn(logger).Log("msg", "Request body too large", "size", len(body), "max_size", maxBodySize)
		http.Error(w, fmt.Sprintf("request body too large: %d bytes (max: %d bytes)", len(body), maxBodySize), http.StatusRequestEntityTooLarge)
		return
	}

	// Track body size
	p.metrics.bodySize.WithLabelValues(p.route.RouteName).Observe(float64(len(body)))

	// Dispatch to non-preferred backends asynchronously (fire-and-forget).
	p.dispatchToNonPreferredBackends(ctx, r, body, logger)

	// Send to preferred backend synchronously and return its response.
	res := p.executePreferredBackendRequest(ctx, r, body)

	// Return the preferred backend's response to the client
	if res.err != nil {
		level.Error(logger).Log("msg", "Preferred backend failed", "err", res.err)
		http.Error(w, res.err.Error(), res.statusCode())
	} else {
		w.Header().Set("Content-Type", res.contentType)
		w.WriteHeader(res.status)
		if _, err := w.Write(res.body); err != nil {
			level.Warn(logger).Log("msg", "Unable to write response", "err", err)
		}
	}

	p.metrics.responsesTotal.WithLabelValues(res.backend.Name(), r.Method, p.route.RouteName).Inc()
}

// ServeHTTPPassthrough forwards the request directly to the preferred backend without fan-out.
// This is used for endpoints we don't want to mirror (e.g., OTLP, Influx).
func (p *ProxyEndpoint) ServeHTTPPassthrough(w http.ResponseWriter, r *http.Request) {
	logger, ctx := spanlogger.New(r.Context(), p.logger, tracer, "Passthrough proxied request")
	defer logger.Finish()

	logger.SetSpanAndLogTag("path", r.URL.Path)
	logger.SetSpanAndLogTag("method", r.Method)
	logger.SetSpanAndLogTag("backend", p.preferredBackend.Name())

	level.Debug(logger).Log("msg", "Passing through request to preferred backend")

	// If no preferred backend, return error
	if p.preferredBackend == nil {
		level.Error(logger).Log("msg", "No preferred backend configured for passthrough")
		http.Error(w, "no preferred backend configured", http.StatusInternalServerError)
		return
	}

	// Forward request directly to preferred backend
	elapsed, status, body, contentType, err := p.preferredBackend.ForwardRequest(ctx, r, r.Body)

	// Track metrics
	p.metrics.RecordBackendResult(p.preferredBackend.Name(), r.Method, "passthrough", elapsed, status, err)

	if err != nil {
		level.Error(logger).Log("msg", "Passthrough request failed", "backend", p.preferredBackend.Name(), "err", err)

		// Determine appropriate status code
		statusCode := http.StatusBadGateway
		if status > 0 {
			statusCode = status
		}
		http.Error(w, fmt.Sprintf("failed to forward request: %v", err), statusCode)
		return
	}

	// Return the backend response to client
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.WriteHeader(status)
	if _, writeErr := w.Write(body); writeErr != nil {
		level.Warn(logger).Log("msg", "Unable to write response", "err", writeErr)
	}

	p.metrics.responsesTotal.WithLabelValues(p.preferredBackend.Name(), r.Method, "passthrough").Inc()
}

// dispatchToNonPreferredBackends sends the request to all non-preferred backends
// asynchronously via the async dispatcher. This is fire-and-forget - we don't wait
// for responses from non-preferred backends.
//
// For amplified backends with splitting enabled, multiple requests may be dispatched
// per backend (one for each split body).
func (p *ProxyEndpoint) dispatchToNonPreferredBackends(ctx context.Context, req *http.Request, body []byte, logger *spanlogger.SpanLogger) {
	if p.asyncDispatcher == nil {
		return
	}

	for _, backend := range p.backends {
		// Skip the preferred backend - it's handled synchronously
		if backend.Preferred() {
			continue
		}

		// Get amplified bodies (may be multiple if splitting occurred)
		bodiesToSend := p.amplifyWriteRequestBodies(body, backend, logger)

		// Dispatch each body to the backend
		for _, bodyToSend := range bodiesToSend {
			p.asyncDispatcher.Dispatch(ctx, req, bodyToSend, backend, p.route.RouteName)
		}
	}
}

// executePreferredBackendRequest sends the request to the preferred backend synchronously
// and returns its response.
func (p *ProxyEndpoint) executePreferredBackendRequest(ctx context.Context, req *http.Request, body []byte) *backendResponse {
	b := p.preferredBackend

	logger, ctx := spanlogger.New(ctx, p.logger, tracer, "Outgoing proxied write request")
	defer logger.Finish()

	logger.SetSpanAndLogTag("path", req.URL.Path)
	logger.SetSpanAndLogTag("route_name", p.route.RouteName)
	logger.SetSpanAndLogTag("backend", b.Name())
	logger.SetSpanAndLogTag("method", req.Method)
	logger.SetSpanAndLogTag("preferred", "true")
	logger.SetSpanAndLogTag("backend_type", fmt.Sprintf("%d", b.BackendType()))

	// The preferred backend is never amplified (validated at startup), so we always get a single body.
	bodiesToSend := p.amplifyWriteRequestBodies(body, b, logger)
	bodyToSend := bodiesToSend[0]
	elapsed, status, respBody, contentType, err := b.ForwardRequest(ctx, req, io.NopCloser(bytes.NewReader(bodyToSend)))

	// Use the actual Content-Type from the backend, with a fallback for Mimir write endpoints
	if contentType == "" {
		contentType = "application/json"
	}

	res := &backendResponse{
		backend:     b,
		status:      status,
		contentType: contentType,
		body:        respBody,
		err:         err,
		elapsedTime: elapsed,
	}

	// Log with a level based on the backend response.
	lvl := level.Debug
	if !res.succeeded() {
		lvl = level.Warn
	}

	l := lvl(logger)

	// If we got an error (rather than just a non-2xx response), log that and mark the span as failed.
	if err != nil {
		l = log.With(l, "err", err)
		logger.SetError()
	}

	l.Log("msg", "Backend response", "status", status, "elapsed", elapsed)
	p.metrics.RecordBackendResult(b.Name(), req.Method, p.route.RouteName, elapsed, status, err)
	logger.SetTag("status", status)

	return res
}

// amplifyWriteRequestBodies amplifies the request body if needed and logs stats on success.
// Returns a slice of bodies to send (may be multiple if splitting occurred).
// For non-amplified backends or errors, returns a single-element slice with the original body.
func (p *ProxyEndpoint) amplifyWriteRequestBodies(body []byte, backend ProxyBackend, logger *spanlogger.SpanLogger) [][]byte {
	if backend.BackendType() != BackendTypeAmplified || p.amplificationFactor == 1.0 || len(body) == 0 {
		return [][]byte{body}
	}

	result, err := AmplifyWriteRequest(body, p.amplificationFactor, p.amplifiedMaxSeriesPerRequest, p.amplificationTracker)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to amplify write request", "backend", backend.Name(), "err", err)
		return [][]byte{body} // Fall back to original
	}

	// Log amplification stats
	if result.WasAmplified {
		rw1, rw2, rw2Ratio := p.amplificationTracker.GetStats()
		logger.SetSpanAndLogTag("amplified", "true")
		if result.IsRW2 {
			logger.SetSpanAndLogTag("format", "rw2")
		} else {
			logger.SetSpanAndLogTag("format", "rw1")
		}
		logger.SetSpanAndLogTag("amplification_factor", fmt.Sprintf("%.1f", p.amplificationFactor))
		logger.SetSpanAndLogTag("rw2_ratio", fmt.Sprintf("%.3f", rw2Ratio))
		logger.SetSpanAndLogTag("total_rw1_series", rw1)
		logger.SetSpanAndLogTag("total_rw2_series", rw2)
		logger.SetSpanAndLogTag("original_size", len(body))
		logger.SetSpanAndLogTag("original_series_count", result.OriginalSeriesCount)
		logger.SetSpanAndLogTag("amplified_series_count", result.AmplifiedSeriesCount)
		logger.SetSpanAndLogTag("split_request_count", len(result.Bodies))

		// Log total amplified size across all bodies
		totalAmplifiedSize := 0
		for _, b := range result.Bodies {
			totalAmplifiedSize += len(b)
		}
		logger.SetSpanAndLogTag("amplified_size", totalAmplifiedSize)
	} else if result.IsRW2 {
		// This should not happen anymore - RW 2.0 is now amplified
		_, _, rw2Ratio := p.amplificationTracker.GetStats()
		logger.SetSpanAndLogTag("rw2_not_amplified", "true")
		logger.SetSpanAndLogTag("rw2_series_count", result.OriginalSeriesCount)
		logger.SetSpanAndLogTag("rw2_ratio", fmt.Sprintf("%.3f", rw2Ratio))
	}

	return result.Bodies
}

type backendResponse struct {
	backend     ProxyBackend
	status      int
	contentType string
	body        []byte
	err         error
	elapsedTime time.Duration
}

func (r *backendResponse) succeeded() bool {
	if r.err != nil {
		return false
	}

	// We consider the response successful if it's a 2xx or 4xx (but not 429).
	return (r.status >= 200 && r.status < 300) || (r.status >= 400 && r.status < 500 && r.status != 429)
}

func (r *backendResponse) statusCode() int {
	if r.err != nil || r.status <= 0 {
		return 500
	}

	return r.status
}
