// SPDX-License-Identifier: AGPL-3.0-only

package readtee

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	// maxBodySize is the maximum request body size we'll accept (10MB). Read requests (query
	// params, match[] selectors) are small; this is a generous cap.
	maxBodySize = 10 * 1024 * 1024

	// queryParam is the amplifiable parameter for instant/range queries.
	queryParam = "query"
	// matchParam is the amplifiable parameter for series/labels/label-values endpoints.
	matchParam = "match[]"

	formContentType = "application/x-www-form-urlencoded"
)

// amplifiedRequest is one rewritten fire-and-forget copy: a request whose query/match[] params
// have been suffixed for a replica, together with the body bytes to send (empty for GET).
type amplifiedRequest struct {
	req  *http.Request
	body []byte
}

type ProxyEndpoint struct {
	backend                   ProxyBackend
	metrics                   *ProxyMetrics
	logger                    log.Logger
	amplificationFactor       float64
	rewriteOpts               rewriteOptions
	ampAllReplicasFraction    float64
	strongConsistencyFraction float64
	asyncDispatcher           *AsyncBackendDispatcher

	route Route
}

func NewProxyEndpoint(backend ProxyBackend, route Route, metrics *ProxyMetrics, logger log.Logger, amplificationFactor float64, rewriteOpts rewriteOptions, ampAllReplicasFraction, strongConsistencyFraction float64, asyncDispatcher *AsyncBackendDispatcher) *ProxyEndpoint {
	return &ProxyEndpoint{
		backend:                   backend,
		route:                     route,
		metrics:                   metrics,
		logger:                    logger,
		amplificationFactor:       amplificationFactor,
		rewriteOpts:               rewriteOpts,
		ampAllReplicasFraction:    ampAllReplicasFraction,
		strongConsistencyFraction: strongConsistencyFraction,
		asyncDispatcher:           asyncDispatcher,
	}
}

func (p *ProxyEndpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger, ctx := spanlogger.New(r.Context(), p.logger, tracer, "Incoming proxied read request")
	defer logger.Finish()

	logger.SetSpanAndLogTag("path", r.URL.Path)
	logger.SetSpanAndLogTag("route_name", p.route.RouteName)
	logger.SetSpanAndLogTag("user", r.Header.Get("X-Scope-OrgID"))
	logger.SetSpanAndLogTag("method", r.Method)

	level.Debug(logger).Log("msg", "Received read request")

	// Read the entire request body into memory. We need the raw body both to forward the original
	// request to the preferred backend and to parse POST form params for the amplified copies. The
	// body can only be read once.
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

	// Dispatch rewritten amplified copies to the backend asynchronously (fire-and-forget).
	p.dispatchAmplifiedRequests(ctx, r, body, logger)

	// Send the ORIGINAL request to the backend synchronously and return its response.
	res := p.executeOriginalRequest(ctx, r, body)

	// Return the backend's response to the client
	if res.err != nil {
		level.Error(logger).Log("msg", "Backend request failed", "err", res.err)
		http.Error(w, res.err.Error(), res.statusCode())
	} else {
		// Copy all headers from backend response (includes Content-Type)
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

// ServeHTTPPassthrough forwards the request directly to the backend without fan-out.
// This is used for endpoints we don't want to amplify.
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

// dispatchAmplifiedRequests builds the rewritten amplified copies and sends them to the backend
// asynchronously via the async dispatcher. This is fire-and-forget - we don't wait for their
// responses. The original (unmodified) request is sent synchronously elsewhere.
func (p *ProxyEndpoint) dispatchAmplifiedRequests(ctx context.Context, req *http.Request, body []byte, logger *spanlogger.SpanLogger) {
	if p.asyncDispatcher == nil {
		return
	}

	amplified := p.prepareAmplifiedRequests(ctx, req, body, logger)
	for _, a := range amplified {
		p.asyncDispatcher.Dispatch(ctx, a.req, a.body, p.backend, p.route.RouteName)
	}
}

// amplifiedRequestSource holds the parsed original request together with its amplifiable params,
// shared across all copies built from it.
type amplifiedRequestSource struct {
	orig          *http.Request
	urlValues     url.Values
	hasURLParams  bool
	formValues    url.Values
	hasFormParams bool
}

// prepareAmplifiedRequests builds the rewritten copies of the original read request. Normally it
// builds N-1 per-replica copies (amplified replicas _amp1.._amp{N-1}, where N is the integer part
// of the amplification factor), each with its query and match[] parameters suffixed _amp{k}. For a
// sampled fraction of queries (amplify-all-replicas-fraction) it instead builds a single heavy copy
// whose matchers target the base series plus every replica at once. Copies that fail to rewrite are
// skipped and counted. Returns nil when amplification is disabled (factor <= 1).
func (p *ProxyEndpoint) prepareAmplifiedRequests(ctx context.Context, orig *http.Request, origBody []byte, logger *spanlogger.SpanLogger) []amplifiedRequest {
	// Integer factors only for v1; the fractional part is ignored.
	n := int(p.amplificationFactor)
	if n <= 1 {
		return nil
	}

	// Parse params from the URL query (GET) and from the form body (POST form). We rewrite each
	// location independently and only where the amplifiable params actually appear.
	urlValues := orig.URL.Query()
	hasURLParams := urlValues.Has(queryParam) || urlValues.Has(matchParam)

	var formValues url.Values
	hasFormParams := false
	if isFormContentType(orig.Header.Get("Content-Type")) {
		if parsed, err := url.ParseQuery(string(origBody)); err == nil {
			formValues = parsed
			hasFormParams = formValues.Has(queryParam) || formValues.Has(matchParam)
		} else {
			level.Warn(logger).Log("msg", "Unable to parse POST form body for amplification; sending copies with unmodified body", "err", err)
		}
	}

	src := amplifiedRequestSource{
		orig:          orig,
		urlValues:     urlValues,
		hasURLParams:  hasURLParams,
		formValues:    formValues,
		hasFormParams: hasFormParams,
	}

	// amp.*-mode: for a sampled fraction of queries, send a single heavy copy whose matchers target
	// the base series plus every amplified replica at once, instead of N-1 per-replica copies. This
	// raises samples-per-query to resemble heavier production queries.
	if p.ampAllReplicasFraction > 0 && rand.Float64() < p.ampAllReplicasFraction {
		opts := p.rewriteOpts
		opts.matchAllReplicas = true
		// The replica index is irrelevant when matching all replicas.
		a, ok := p.buildCopy(ctx, src, 1, opts, logger)
		logger.SetSpanAndLogTag("amplify_all_replicas", "true")
		if !ok {
			return nil
		}
		p.metrics.amplifyAllReplicasTotal.WithLabelValues(p.route.RouteName).Inc()
		return []amplifiedRequest{a}
	}

	result := make([]amplifiedRequest, 0, n-1)
	for k := 1; k <= n-1; k++ {
		a, ok := p.buildCopy(ctx, src, k, p.rewriteOpts, logger)
		if !ok {
			continue
		}
		result = append(result, a)
	}

	logger.SetSpanAndLogTag("amplified", "true")
	logger.SetSpanAndLogTag("amplification_factor", fmt.Sprintf("%d", n))
	logger.SetSpanAndLogTag("num_amplified_copies", len(result))

	return result
}

// buildCopy clones the original request and rewrites its query/match[] params for the given replica
// using opts. It returns false (and counts a rewrite error) if any param fails to rewrite.
func (p *ProxyEndpoint) buildCopy(ctx context.Context, src amplifiedRequestSource, replica int, opts rewriteOptions, logger *spanlogger.SpanLogger) (amplifiedRequest, bool) {
	clone := src.orig.Clone(ctx)
	// RequestURI is only valid on server-received requests; must be cleared before reuse.
	clone.RequestURI = ""

	// Rewrite URL query params in place (if present).
	if src.hasURLParams {
		rewritten, err := rewriteParams(src.urlValues, replica, opts)
		if err != nil {
			return p.rewriteFailed(replica, logger)
		}
		clone.URL.RawQuery = rewritten.Encode()
	}

	// Rewrite POST form body params in place (if present).
	var cloneBody []byte
	if src.hasFormParams {
		rewritten, err := rewriteParams(src.formValues, replica, opts)
		if err != nil {
			return p.rewriteFailed(replica, logger)
		}
		encoded := rewritten.Encode()
		cloneBody = []byte(encoded)
		clone.Body = io.NopCloser(bytes.NewReader(cloneBody))
		clone.ContentLength = int64(len(cloneBody))
		clone.Header.Set("Content-Length", strconv.Itoa(len(cloneBody)))
		clone.Header.Set("Content-Type", formContentType)
	}

	// Independently per copy, request strong read consistency on a sampled fraction of instant-query
	// copies. This mirrors the ruler (which runs instant queries) and only affects copies, never the
	// original client request.
	if p.route.Instant && p.strongConsistencyFraction > 0 && rand.Float64() < p.strongConsistencyFraction {
		clone.Header.Set(api.ReadConsistencyHeader, api.ReadConsistencyStrong)
		p.metrics.strongConsistencyCopiesTotal.WithLabelValues(p.route.RouteName).Inc()
	}

	return amplifiedRequest{req: clone, body: cloneBody}, true
}

func (p *ProxyEndpoint) rewriteFailed(replica int, logger *spanlogger.SpanLogger) (amplifiedRequest, bool) {
	p.metrics.rewriteErrorsTotal.WithLabelValues(p.route.RouteName).Inc()
	level.Warn(logger).Log("msg", "Failed to rewrite query for amplified copy; skipping copy", "replica", replica, "route", p.route.RouteName)
	return amplifiedRequest{}, false
}

// rewriteParams returns a copy of values with the "query" and "match[]" parameters rewritten for
// the given replica. All other parameters are copied unchanged. Returns an error if any value
// fails to rewrite (invalid PromQL), so the caller can skip that copy.
func rewriteParams(values url.Values, replica int, opts rewriteOptions) (url.Values, error) {
	out := make(url.Values, len(values))
	for key, vs := range values {
		switch key {
		case queryParam:
			rewritten := make([]string, len(vs))
			for i, v := range vs {
				rw, err := rewriteQuery(v, replica, opts)
				if err != nil {
					return nil, err
				}
				rewritten[i] = rw
			}
			out[key] = rewritten
		case matchParam:
			rewritten := make([]string, len(vs))
			for i, v := range vs {
				rw, err := rewriteSelector(v, replica, opts)
				if err != nil {
					return nil, err
				}
				rewritten[i] = rw
			}
			out[key] = rewritten
		default:
			cp := make([]string, len(vs))
			copy(cp, vs)
			out[key] = cp
		}
	}
	return out, nil
}

func isFormContentType(contentType string) bool {
	return strings.Contains(strings.ToLower(contentType), formContentType)
}

// executeOriginalRequest sends the original (unmodified) request to the backend synchronously
// and returns its response.
func (p *ProxyEndpoint) executeOriginalRequest(ctx context.Context, req *http.Request, body []byte) *backendResponse {
	b := p.backend

	logger, ctx := spanlogger.New(ctx, p.logger, tracer, "Outgoing proxied read request")
	defer logger.Finish()

	logger.SetSpanAndLogTag("path", req.URL.Path)
	logger.SetSpanAndLogTag("route_name", p.route.RouteName)
	logger.SetSpanAndLogTag("backend", b.Name())
	logger.SetSpanAndLogTag("method", req.Method)

	// Send the original query untouched (no rewrite); we synchronously wait for its response.
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
