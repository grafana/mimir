// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
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
	backends              []ProxyBackend
	metrics               *ProxyMetrics
	logger                log.Logger
	slowResponseThreshold time.Duration

	// The preferred backend, if any.
	preferredBackend ProxyBackend

	route Route
}

func NewProxyEndpoint(backends []ProxyBackend, route Route, metrics *ProxyMetrics, logger log.Logger, slowResponseThreshold time.Duration) *ProxyEndpoint {
	var preferredBackend ProxyBackend
	for _, backend := range backends {
		if backend.Preferred() {
			preferredBackend = backend
			break
		}
	}

	return &ProxyEndpoint{
		backends:              backends,
		route:                 route,
		metrics:               metrics,
		logger:                logger,
		slowResponseThreshold: slowResponseThreshold,
		preferredBackend:      preferredBackend,
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

	// Fan out to all backends in parallel
	resCh := make(chan *backendResponse, len(p.backends))
	p.executeBackendRequests(ctx, r, body, resCh, logger)

	// Wait for all responses and select the one to return
	downstreamRes := p.selectResponseForDownstream(resCh, logger)

	// Return the selected response to the client
	if downstreamRes.err != nil {
		level.Error(logger).Log("msg", "All backends failed", "err", downstreamRes.err)
		http.Error(w, downstreamRes.err.Error(), downstreamRes.statusCode())
	} else {
		w.Header().Set("Content-Type", downstreamRes.contentType)
		w.WriteHeader(downstreamRes.status)
		if _, err := w.Write(downstreamRes.body); err != nil {
			level.Warn(logger).Log("msg", "Unable to write response", "err", err)
		}
	}

	p.metrics.responsesTotal.WithLabelValues(downstreamRes.backend.Name(), r.Method, p.route.RouteName).Inc()
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
	elapsed, status, body, err := p.preferredBackend.ForwardRequest(ctx, r, r.Body)

	// Track metrics
	p.metrics.requestDuration.WithLabelValues(p.preferredBackend.Name(), r.Method, "passthrough", strconv.Itoa(status)).Observe(elapsed.Seconds())

	if err != nil {
		level.Error(logger).Log("msg", "Passthrough request failed", "backend", p.preferredBackend.Name(), "err", err)
		p.metrics.errorsTotal.WithLabelValues(p.preferredBackend.Name(), r.Method, "passthrough", "forward_error").Inc()

		// Determine appropriate status code
		statusCode := http.StatusBadGateway
		if status > 0 {
			statusCode = status
		}
		http.Error(w, fmt.Sprintf("failed to forward request: %v", err), statusCode)
		return
	}

	// Return the backend response to client
	w.WriteHeader(status)
	if _, writeErr := w.Write(body); writeErr != nil {
		level.Warn(logger).Log("msg", "Unable to write response", "err", writeErr)
	}

	p.metrics.responsesTotal.WithLabelValues(p.preferredBackend.Name(), r.Method, "passthrough").Inc()
}

func (p *ProxyEndpoint) executeBackendRequests(ctx context.Context, req *http.Request, body []byte, resCh chan *backendResponse, logger *spanlogger.SpanLogger) {
	var (
		wg              = sync.WaitGroup{}
		timingMtx       = sync.Mutex{}
		fastestDuration time.Duration
		fastestBackend  ProxyBackend
		slowestDuration time.Duration
		slowestBackend  ProxyBackend
	)

	wg.Add(len(p.backends))
	for _, b := range p.backends {
		go func() {
			defer wg.Done()

			// Don't cancel the child request's context when the parent context is cancelled after we return a response.
			// This allows us to continue running slower requests after returning a response to the caller.
			ctx := context.WithoutCancel(ctx)
			logger, ctx := spanlogger.New(ctx, p.logger, tracer, "Outgoing proxied write request")
			defer logger.Finish()

			logger.SetSpanAndLogTag("path", req.URL.Path)
			logger.SetSpanAndLogTag("route_name", p.route.RouteName)
			logger.SetSpanAndLogTag("backend", b.Name())
			logger.SetSpanAndLogTag("method", req.Method)

			// Create a new reader for this backend
			var bodyReader io.ReadCloser
			if len(body) > 0 {
				bodyReader = io.NopCloser(bytes.NewReader(body))
			}

			elapsed, status, respBody, err := b.ForwardRequest(ctx, req, bodyReader)

			// Track timing for slow response logging
			if p.slowResponseThreshold > 0 {
				timingMtx.Lock()
				if elapsed > slowestDuration {
					slowestDuration = elapsed
					slowestBackend = b
				}
				if fastestDuration == 0 || elapsed < fastestDuration {
					fastestDuration = elapsed
					fastestBackend = b
				}
				timingMtx.Unlock()
			}

			res := &backendResponse{
				backend:     b,
				status:      status,
				contentType: "application/json", // Default for Mimir write endpoints
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
				// Track error type
				errorType := "network"
				if status == 0 {
					errorType = "timeout"
				}
				p.metrics.errorsTotal.WithLabelValues(b.Name(), req.Method, p.route.RouteName, errorType).Inc()
			}

			l.Log("msg", "Backend response", "status", status, "elapsed", elapsed)
			p.metrics.requestDuration.WithLabelValues(b.Name(), req.Method, p.route.RouteName, strconv.Itoa(res.statusCode())).Observe(elapsed.Seconds())
			logger.SetTag("status", status)

			resCh <- res
		}()
	}

	// Wait until all backend requests completed.
	wg.Wait()
	close(resCh)

	// Log queries that are slower in some backends than others
	if p.slowResponseThreshold > 0 && slowestDuration-fastestDuration >= p.slowResponseThreshold {
		level.Warn(logger).Log(
			"msg", "response time difference between backends exceeded threshold",
			"slowest_duration", slowestDuration,
			"slowest_backend", slowestBackend.Name(),
			"fastest_duration", fastestDuration,
			"fastest_backend", fastestBackend.Name(),
		)
	}
}

func (p *ProxyEndpoint) selectResponseForDownstream(resCh chan *backendResponse, logger *spanlogger.SpanLogger) *backendResponse {
	var (
		preferredResponse *backendResponse
		firstSuccessful   *backendResponse
		firstResponse     *backendResponse
		allResponses      []*backendResponse
	)

	// Collect all responses
	for res := range resCh {
		allResponses = append(allResponses, res)

		if firstResponse == nil {
			firstResponse = res
		}

		// Track the preferred backend's response
		if res.backend != nil && res.backend.Preferred() {
			preferredResponse = res
		}

		// Track first successful response
		if firstSuccessful == nil && res.succeeded() {
			firstSuccessful = res
		}
	}

	// Response selection logic:
	// 1. If we have a preferred backend, return its response (even if it failed)
	// 2. Otherwise, return the first successful response
	// 3. If all failed, return the first response
	if preferredResponse != nil {
		if !preferredResponse.succeeded() {
			level.Warn(logger).Log("msg", "Preferred backend failed, but returning its response anyway", "backend", preferredResponse.backend.Name(), "status", preferredResponse.status)
		}
		return preferredResponse
	}

	if firstSuccessful != nil {
		return firstSuccessful
	}

	level.Error(logger).Log("msg", "All backends failed, returning first response")
	return firstResponse
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
