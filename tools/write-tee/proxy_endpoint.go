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
	backend              ProxyBackend
	metrics              *ProxyMetrics
	logger               log.Logger
	amplificationFactor  float64
	amplificationTracker *AmplificationTracker
	asyncDispatcher      *AsyncBackendDispatcher

	route Route
}

func NewProxyEndpoint(backend ProxyBackend, route Route, metrics *ProxyMetrics, logger log.Logger, amplificationFactor float64, amplificationTracker *AmplificationTracker, asyncDispatcher *AsyncBackendDispatcher) *ProxyEndpoint {
	return &ProxyEndpoint{
		backend:              backend,
		route:                route,
		metrics:              metrics,
		logger:               logger,
		amplificationFactor:  amplificationFactor,
		amplificationTracker: amplificationTracker,
		asyncDispatcher:      asyncDispatcher,
	}
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

	// Dispatch amplified copies to the backend asynchronously (fire-and-forget).
	p.dispatchAmplifiedRequests(ctx, r, body, logger)

	// Send the original (unmodified) request to the backend synchronously and return its response.
	res := p.executeOriginalRequest(ctx, r, body)

	// Return the backend's response to the client
	if res.err != nil {
		level.Error(logger).Log("msg", "Backend request failed", "err", res.err)
		http.Error(w, res.err.Error(), res.statusCode())
	} else {
		// Copy all headers from backend response (includes Content-Type and RW 2.0 statistics headers)
		for key, values := range res.headers {
			w.Header()[key] = values
		}
		w.WriteHeader(res.status)
		if _, err := w.Write(res.body); err != nil {
			level.Warn(logger).Log("msg", "Unable to write response", "err", err)
		}
	}

	p.metrics.responsesTotal.WithLabelValues(res.backend.Name(), r.Method, p.route.RouteName).Inc()
}

// ServeHTTPPassthrough forwards the request directly to the backend without amplification.
// This is used for endpoints we don't want to amplify (e.g., OTLP, Influx).
func (p *ProxyEndpoint) ServeHTTPPassthrough(w http.ResponseWriter, r *http.Request) {
	logger, ctx := spanlogger.New(r.Context(), p.logger, tracer, "Passthrough proxied request")
	defer logger.Finish()

	logger.SetSpanAndLogTag("path", r.URL.Path)
	logger.SetSpanAndLogTag("method", r.Method)
	logger.SetSpanAndLogTag("backend", p.backend.Name())

	level.Debug(logger).Log("msg", "Passing through request to backend")

	// Forward request directly to the backend
	elapsed, status, body, headers, err := p.backend.ForwardRequest(ctx, r, r.Body)

	// Track metrics
	p.metrics.RecordBackendResult(p.backend.Name(), r.Method, "passthrough", elapsed, status, err)

	if err != nil {
		level.Error(logger).Log("msg", "Passthrough request failed", "backend", p.backend.Name(), "err", err)

		// Determine appropriate status code
		statusCode := http.StatusBadGateway
		if status > 0 {
			statusCode = status
		}
		http.Error(w, fmt.Sprintf("failed to forward request: %v", err), statusCode)
		return
	}

	// Return the backend response to client - copy all headers
	for key, values := range headers {
		w.Header()[key] = values
	}
	w.WriteHeader(status)
	if _, writeErr := w.Write(body); writeErr != nil {
		level.Warn(logger).Log("msg", "Unable to write response", "err", writeErr)
	}

	p.metrics.responsesTotal.WithLabelValues(p.backend.Name(), r.Method, "passthrough").Inc()
}

// dispatchAmplifiedRequests sends the amplified (suffixed) copies of the request to the backend
// asynchronously via the async dispatcher. This is fire-and-forget - we don't wait for their
// responses. The original (unsuffixed) request is sent synchronously elsewhere.
func (p *ProxyEndpoint) dispatchAmplifiedRequests(ctx context.Context, req *http.Request, body []byte, logger *spanlogger.SpanLogger) {
	if p.asyncDispatcher == nil {
		return
	}

	for _, bodyToSend := range p.amplifiedBodies(body, logger) {
		p.asyncDispatcher.Dispatch(ctx, req, bodyToSend, p.backend, p.route.RouteName)
	}
}

// executeOriginalRequest sends the original (unmodified) request to the backend synchronously
// and returns its response.
func (p *ProxyEndpoint) executeOriginalRequest(ctx context.Context, req *http.Request, body []byte) *backendResponse {
	b := p.backend

	logger, ctx := spanlogger.New(ctx, p.logger, tracer, "Outgoing proxied write request")
	defer logger.Finish()

	logger.SetSpanAndLogTag("path", req.URL.Path)
	logger.SetSpanAndLogTag("route_name", p.route.RouteName)
	logger.SetSpanAndLogTag("backend", b.Name())
	logger.SetSpanAndLogTag("method", req.Method)

	// Send the original body unmodified - this is replica 1, and we synchronously wait for its response.
	elapsed, status, respBody, headers, err := b.ForwardRequest(ctx, req, io.NopCloser(bytes.NewReader(body)))

	// Set a default Content-Type if the backend didn't provide one
	if headers != nil && headers.Get("Content-Type") == "" {
		headers.Set("Content-Type", "application/json")
	}

	res := &backendResponse{
		backend:     b,
		status:      status,
		headers:     headers,
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

// amplifiedBodies returns ONLY the suffixed amplified copies of the request body (replicas 2..N).
// The unsuffixed original (replica 1) is sent synchronously by executeOriginalRequest and is NOT
// included here. Returns an empty slice when there is nothing to amplify (factor == 1 or empty body).
//
// For amplification factor F:
//   - floor(F)-1 full copies are created with suffixes _amp2 .. _amp{floor(F)} (skipped when floor(F) < 2).
//   - if the fractional part frac = F - floor(F) > 0, one more copy is sampled to frac and suffixed
//     _amp{floor(F)+1}.
func (p *ProxyEndpoint) amplifiedBodies(body []byte, logger *spanlogger.SpanLogger) [][]byte {
	if p.amplificationFactor <= 1.0 || len(body) == 0 {
		return nil
	}

	floor := int(p.amplificationFactor)
	fractionalPart := p.amplificationFactor - float64(floor)

	var bodies [][]byte

	// Full amplified copies _amp1 .. _amp{floor-1} (floor-1 copies). The unsuffixed original that
	// the endpoint receives synchronously is the base, so amplified copies are 1-indexed with no gap.
	if fullCopies := floor - 1; fullCopies >= 1 {
		replicaBodies, err := AmplifyRequestBody(body, fullCopies, 1)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to create amplified replicas", "backend", p.backend.Name(), "err", err)
		} else {
			bodies = append(bodies, replicaBodies...)
		}
	}

	// Fractional part: sample the data (e.g., 50% for 0.5) and add the next amplified suffix _amp{floor}.
	if fractionalPart > 0 {
		result, err := SampleWriteRequest(body, fractionalPart, p.amplificationTracker)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to create fractional request", "backend", p.backend.Name(), "err", err)
		} else {
			fractionalBodies, err := AmplifyRequestBody(result.Body, 1, floor)
			if err != nil {
				level.Error(logger).Log("msg", "Failed to suffix fractional request", "backend", p.backend.Name(), "err", err)
			} else {
				bodies = append(bodies, fractionalBodies...)
			}
		}
	}

	logger.SetSpanAndLogTag("amplified", "true")
	logger.SetSpanAndLogTag("amplification_factor", fmt.Sprintf("%.1f", p.amplificationFactor))
	logger.SetSpanAndLogTag("num_amplified_copies", len(bodies))

	return bodies
}

type backendResponse struct {
	backend     ProxyBackend
	status      int
	headers     http.Header
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
