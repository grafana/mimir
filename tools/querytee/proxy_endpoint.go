// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/tools/querytee/proxy_endpoint.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querytee

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
	"github.com/opentracing/opentracing-go/ext"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type ResponsesComparator interface {
	Compare(expected, actual []byte) (ComparisonResult, error)
}

type ProxyEndpoint struct {
	backends              []ProxyBackendInterface
	metrics               *ProxyMetrics
	logger                log.Logger
	comparator            ResponsesComparator
	slowResponseThreshold time.Duration

	// Whether for this endpoint there's a preferred backend configured.
	hasPreferredBackend bool

	route Route
}

func NewProxyEndpoint(backends []ProxyBackendInterface, route Route, metrics *ProxyMetrics, logger log.Logger, comparator ResponsesComparator, slowResponseThreshold time.Duration) *ProxyEndpoint {
	hasPreferredBackend := false
	for _, backend := range backends {
		if backend.Preferred() {
			hasPreferredBackend = true
			break
		}
	}

	return &ProxyEndpoint{
		backends:              backends,
		route:                 route,
		metrics:               metrics,
		logger:                logger,
		comparator:            comparator,
		slowResponseThreshold: slowResponseThreshold,
		hasPreferredBackend:   hasPreferredBackend,
	}
}

func (p *ProxyEndpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Send the same request to all backends.
	resCh := make(chan *backendResponse, len(p.backends))
	go p.executeBackendRequests(r, resCh)

	// Wait for the first response that's feasible to be sent back to the client.
	downstreamRes := p.waitBackendResponseForDownstream(resCh)

	if downstreamRes.err != nil {
		http.Error(w, downstreamRes.err.Error(), http.StatusInternalServerError)
	} else {
		w.Header().Set("Content-Type", downstreamRes.contentType)
		w.WriteHeader(downstreamRes.status)
		if _, err := w.Write(downstreamRes.body); err != nil {
			level.Warn(p.logger).Log("msg", "Unable to write response", "err", err)
		}
	}

	p.metrics.responsesTotal.WithLabelValues(downstreamRes.backend.Name(), r.Method, p.route.RouteName).Inc()
}

func (p *ProxyEndpoint) executeBackendRequests(req *http.Request, resCh chan *backendResponse) {
	var (
		wg           = sync.WaitGroup{}
		err          error
		body         []byte
		responses    = make([]*backendResponse, 0, len(p.backends))
		responsesMtx = sync.Mutex{}
		timingMtx    = sync.Mutex{}
		query        = req.URL.RawQuery
		logger, ctx  = spanlogger.NewWithLogger(req.Context(), p.logger, "Incoming proxied request")
	)

	defer logger.Finish()

	if req.Body != nil {
		body, err = io.ReadAll(req.Body)
		if err != nil {
			level.Warn(logger).Log("msg", "Unable to read request body", "err", err)
			resCh <- &backendResponse{err: err}
			close(resCh)
			return
		}
		if err := req.Body.Close(); err != nil {
			level.Warn(logger).Log("msg", "Unable to close request body", "err", err)
		}

		req.Body = io.NopCloser(bytes.NewReader(body))
		if err := req.ParseForm(); err != nil {
			level.Warn(logger).Log("msg", "Unable to parse form", "err", err)
		}
		query = req.Form.Encode()
	}

	setSpanAndLogTags := func(logger *spanlogger.SpanLogger) {
		logger.SetSpanAndLogTag("path", req.URL.Path)
		logger.SetSpanAndLogTag("query", query)
		logger.SetSpanAndLogTag("route_name", p.route.RouteName)
		logger.SetSpanAndLogTag("user", req.Header.Get("X-Scope-OrgID"))
		logger.SetSpanAndLogTag("user_agent", req.Header.Get("User-Agent"))
	}

	setSpanAndLogTags(logger)

	level.Debug(logger).Log("msg", "Received request")

	for _, transform := range p.route.RequestTransformers {
		req, body, err = transform(req, body, logger)

		if err != nil {
			level.Error(logger).Log("msg", "Transforming request failed", "err", err)
			resCh <- &backendResponse{err: err}
			close(resCh)
			return
		}

		// Update the query used in logging based on the updated request.
		query = req.URL.RawQuery
		if body != nil {
			query = req.Form.Encode()
		}
	}

	// Keep track of the fastest and slowest backends
	var (
		fastestDuration time.Duration
		fastestBackend  ProxyBackendInterface
		slowestDuration time.Duration
		slowestBackend  ProxyBackendInterface
	)

	wg.Add(len(p.backends))
	for _, b := range p.backends {
		b := b

		go func() {
			defer wg.Done()

			// Don't cancel the child request's context when the parent context (from the incoming HTTP request) is cancelled after we return a response.
			// This allows us to continue running slower requests after returning a response to the caller.
			ctx := context.WithoutCancel(ctx)
			logger, ctx := spanlogger.NewWithLogger(ctx, p.logger, "Outgoing proxied request")
			defer logger.Finish()
			setSpanAndLogTags(logger)
			logger.SetSpanAndLogTag("backend", b.Name())

			var bodyReader io.ReadCloser
			if len(body) > 0 {
				bodyReader = io.NopCloser(bytes.NewReader(body))
			}

			elapsed, status, body, resp, err := b.ForwardRequest(ctx, req, bodyReader)
			contentType := ""

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

			if resp != nil {
				contentType = resp.Header.Get("Content-Type")
			}

			res := &backendResponse{
				backend:     b,
				status:      status,
				contentType: contentType,
				body:        body,
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
				ext.Error.Set(logger.Span, true)
			}

			l.Log("msg", "Backend response", "status", status, "elapsed", elapsed)
			p.metrics.requestDuration.WithLabelValues(res.backend.Name(), req.Method, p.route.RouteName, strconv.Itoa(res.statusCode())).Observe(elapsed.Seconds())
			logger.SetTag("status", status)

			// Keep track of the response if required.
			if p.comparator != nil {
				responsesMtx.Lock()
				responses = append(responses, res)
				responsesMtx.Unlock()
			}

			resCh <- res
		}()
	}

	// Wait until all backend requests completed.
	wg.Wait()
	close(resCh)

	// Compare responses.
	if p.comparator != nil {
		expectedResponse := responses[0]
		actualResponse := responses[1]
		if responses[1].backend.Preferred() {
			expectedResponse, actualResponse = actualResponse, expectedResponse
		}

		result, err := p.compareResponses(expectedResponse, actualResponse)
		if result == ComparisonFailed {
			level.Error(logger).Log(
				"msg", "response comparison failed",
				"err", err,
				"expected_response_duration", expectedResponse.elapsedTime,
				"actual_response_duration", actualResponse.elapsedTime,
			)
		} else if result == ComparisonSkipped {
			level.Warn(logger).Log(
				"msg", "response comparison skipped",
				"err", err,
				"expected_response_duration", expectedResponse.elapsedTime,
				"actual_response_duration", actualResponse.elapsedTime,
			)
		}

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

		relativeDuration := actualResponse.elapsedTime - expectedResponse.elapsedTime
		proportionalDurationDifference := relativeDuration.Seconds() / expectedResponse.elapsedTime.Seconds()
		p.metrics.relativeDuration.WithLabelValues(p.route.RouteName).Observe(relativeDuration.Seconds())
		p.metrics.proportionalDuration.WithLabelValues(p.route.RouteName).Observe(proportionalDurationDifference)
		p.metrics.responsesComparedTotal.WithLabelValues(p.route.RouteName, string(result)).Inc()
	}
}

func (p *ProxyEndpoint) waitBackendResponseForDownstream(resCh chan *backendResponse) *backendResponse {
	var (
		responses                 = make([]*backendResponse, 0, len(p.backends))
		preferredResponseReceived = false
	)

	for res := range resCh {
		// If the response is successful we can immediately return it if:
		// - There's no preferred backend configured
		// - Or this response is from the preferred backend
		// - Or the preferred backend response has already been received and wasn't successful
		if res.succeeded() && (!p.hasPreferredBackend || res.backend.Preferred() || preferredResponseReceived) {
			return res
		}

		// If we received a non-successful response from the preferred backend, then we can
		// return the first successful response received so far (if any).
		if res.backend.Preferred() && !res.succeeded() {
			preferredResponseReceived = true

			for _, prevRes := range responses {
				if prevRes.succeeded() {
					return prevRes
				}
			}
		}

		// Otherwise we keep track of it for later.
		responses = append(responses, res)
	}

	// No successful response, so let's pick the first one.
	return responses[0]
}

func (p *ProxyEndpoint) compareResponses(expectedResponse, actualResponse *backendResponse) (ComparisonResult, error) {
	if expectedResponse.err != nil {
		return ComparisonFailed, fmt.Errorf("skipped comparison of response because the request to the preferred backend failed: %w", expectedResponse.err)
	}

	if actualResponse.err != nil {
		return ComparisonFailed, fmt.Errorf("skipped comparison of response because the request to the secondary backend failed: %w", actualResponse.err)
	}

	if expectedResponse.status != actualResponse.status {
		return ComparisonFailed, fmt.Errorf("expected status code %d (returned by preferred backend) but got %d from secondary backend", expectedResponse.status, actualResponse.status)
	}

	if expectedResponse.contentType != "application/json" {
		return ComparisonSkipped, fmt.Errorf("skipped comparison of response because the response from the preferred backend contained an unexpected content type '%s', expected 'application/json'", expectedResponse.contentType)
	}

	if actualResponse.contentType != "application/json" {
		return ComparisonSkipped, fmt.Errorf("skipped comparison of response because the response from the secondary backend contained an unexpected content type '%s', expected 'application/json'", actualResponse.contentType)
	}

	return p.comparator.Compare(expectedResponse.body, actualResponse.body)
}

type backendResponse struct {
	backend     ProxyBackendInterface
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
