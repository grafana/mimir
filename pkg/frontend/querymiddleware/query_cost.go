// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/grafana/mimir/pkg/util"
)

// emptyQueryCostResponseBody is the estimate of a query that the query-frontend skips
// without reading storage.
const emptyQueryCostResponseBody = `{"status":"success","data":{"estimate":{"seriesTouched":0,"samplesRead":0}}}`

// queryCostRoundTripper applies query middlewares to a query cost estimation request
// before it forwards the request.
type queryCostRoundTripper struct {
	next       http.RoundTripper
	codec      Codec
	middleware MetricsQueryMiddleware
}

// newQueryCostRoundTripper returns a round tripper that rejects cost estimation requests
// that the middlewares reject, and that estimates the same time range as the query.
func newQueryCostRoundTripper(next http.RoundTripper, codec Codec, middlewares ...MetricsQueryMiddleware) http.RoundTripper {
	return &queryCostRoundTripper{
		next:       next,
		codec:      codec,
		middleware: MergeMetricsQueryMiddlewares(middlewares...),
	}
}

func (rt *queryCostRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	// The codec returns a bad data error for invalid parameters.
	req, err := rt.codec.DecodeMetricsQueryRequest(r.Context(), r)
	if err != nil {
		return nil, err
	}

	// The middlewares only validate the request, but they can also clamp its time range,
	// or skip a query that is fully outside the allowed time range.
	var validated MetricsQueryRequest
	handler := rt.middleware.Wrap(HandlerFunc(func(_ context.Context, req MetricsQueryRequest) (Response, error) {
		validated = req
		return nil, nil
	}))
	if _, err := handler.Do(r.Context(), req); err != nil {
		return nil, err
	}

	if validated == nil {
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        http.Header{"Content-Type": {"application/json"}},
			Body:          io.NopCloser(strings.NewReader(emptyQueryCostResponseBody)),
			ContentLength: int64(len(emptyQueryCostResponseBody)),
		}, nil
	}

	// Only range requests have a start time that the middlewares can move.
	if IsRangeQueryCost(r.URL.Path) && validated.GetStart() != req.GetStart() {
		if r, err = withStartParam(r, validated.GetStart()); err != nil {
			return nil, err
		}
	}
	return rt.next.RoundTrip(r)
}

// withStartParam returns a copy of r with the start parameter set to startMs.
func withStartParam(r *http.Request, startMs int64) (*http.Request, error) {
	params, err := util.ParseRequestFormWithoutConsumingBody(r)
	if err != nil {
		return nil, err
	}
	params.Set("start", strconv.FormatFloat(float64(startMs)/1000, 'f', -1, 64))

	r = r.Clone(r.Context())
	encoded := params.Encode()
	if r.Method == http.MethodPost {
		// The parameters include those from the URL, so they are only sent in the body.
		r.URL.RawQuery = ""
		r.Body = io.NopCloser(bytes.NewReader([]byte(encoded)))
		r.ContentLength = int64(len(encoded))
		r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader([]byte(encoded))), nil }
	} else {
		r.URL.RawQuery = encoded
	}
	// This is what the httpgrpc code looks at.
	r.RequestURI = r.URL.RequestURI()
	return r, nil
}
