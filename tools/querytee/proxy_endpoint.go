// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/tools/querytee/proxy_endpoint.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querytee

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type ResponsesComparator interface {
	Compare(expected, actual []byte) (ComparisonResult, error)
}

type ProxyEndpoint struct {
	backends   []*ProxyBackend
	metrics    *ProxyMetrics
	logger     log.Logger
	comparator ResponsesComparator

	// Whether for this endpoint there's a preferred backend configured.
	hasPreferredBackend bool

	// The route name used to track metrics.
	routeName string
}

func NewProxyEndpoint(backends []*ProxyBackend, routeName string, metrics *ProxyMetrics, logger log.Logger, comparator ResponsesComparator) *ProxyEndpoint {
	hasPreferredBackend := false
	for _, backend := range backends {
		if backend.preferred {
			hasPreferredBackend = true
			break
		}
	}

	return &ProxyEndpoint{
		backends:            backends,
		routeName:           routeName,
		metrics:             metrics,
		logger:              logger,
		comparator:          comparator,
		hasPreferredBackend: hasPreferredBackend,
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

	p.metrics.responsesTotal.WithLabelValues(downstreamRes.backend.name, r.Method, p.routeName).Inc()
}

func (p *ProxyEndpoint) executeBackendRequests(req *http.Request, resCh chan *backendResponse) {
	var (
		wg           = sync.WaitGroup{}
		err          error
		body         []byte
		responses    = make([]*backendResponse, 0, len(p.backends))
		responsesMtx = sync.Mutex{}
		query        = req.URL.RawQuery
	)

	if req.Body != nil {
		body, err = io.ReadAll(req.Body)
		if err != nil {
			level.Warn(p.logger).Log("msg", "Unable to read request body", "err", err)
			return
		}
		if err := req.Body.Close(); err != nil {
			level.Warn(p.logger).Log("msg", "Unable to close request body", "err", err)
		}

		req.Body = io.NopCloser(bytes.NewReader(body))
		if err := req.ParseForm(); err != nil {
			level.Warn(p.logger).Log("msg", "Unable to parse form", "err", err)
		}
		query = req.Form.Encode()
	}

	level.Debug(p.logger).Log("msg", "Received request", "path", req.URL.Path, "query", query)

	wg.Add(len(p.backends))
	for _, b := range p.backends {
		b := b

		go func() {
			defer wg.Done()
			var (
				bodyReader io.ReadCloser
				start      = time.Now()
			)
			if len(body) > 0 {
				bodyReader = io.NopCloser(bytes.NewReader(body))
			}

			status, body, resp, err := b.ForwardRequest(req, bodyReader)
			elapsed := time.Since(start)
			contentType := ""

			if resp != nil {
				contentType = resp.Header.Get("Content-Type")
			}

			res := &backendResponse{
				backend:     b,
				status:      status,
				contentType: contentType,
				body:        body,
				err:         err,
			}

			// Log with a level based on the backend response.
			lvl := level.Debug
			if !res.succeeded() {
				lvl = level.Warn
			}

			lvl(p.logger).Log("msg", "Backend response", "path", req.URL.Path, "query", query, "backend", b.name, "status", status, "elapsed", elapsed)
			p.metrics.requestDuration.WithLabelValues(res.backend.name, req.Method, p.routeName, strconv.Itoa(res.statusCode())).Observe(elapsed.Seconds())

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
		if responses[1].backend.preferred {
			expectedResponse, actualResponse = actualResponse, expectedResponse
		}

		result, err := p.compareResponses(expectedResponse, actualResponse)
		if result == ComparisonFailed {
			level.Error(p.logger).Log(
				"msg", "response comparison failed",
				"route-name", p.routeName,
				"query", query,
				"user", req.Header.Get("X-Scope-OrgID"),
				"err", err,
			)
		} else if result == ComparisonSkipped {
			level.Warn(p.logger).Log(
				"msg", "response comparison skipped",
				"route-name", p.routeName,
				"query", query,
				"user", req.Header.Get("X-Scope-OrgID"),
				"err", err,
			)
		}

		p.metrics.responsesComparedTotal.WithLabelValues(p.routeName, string(result)).Inc()
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
		if res.succeeded() && (!p.hasPreferredBackend || res.backend.preferred || preferredResponseReceived) {
			return res
		}

		// If we received a non-successful response from the preferred backend, then we can
		// return the first successful response received so far (if any).
		if res.backend.preferred && !res.succeeded() {
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
	backend     *ProxyBackend
	status      int
	contentType string
	body        []byte
	err         error
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
