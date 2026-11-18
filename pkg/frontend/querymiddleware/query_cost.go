// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"net/http"
)

// queryCostRoundTripper applies query middlewares to a query cost estimation request
// and then forwards the original HTTP request unchanged.
type queryCostRoundTripper struct {
	next    http.RoundTripper
	codec   Codec
	handler MetricsQueryHandler
}

// newQueryCostRoundTripper returns a round tripper that rejects cost estimation requests
// that the middlewares reject. The middlewares only validate the request: a request that
// they modify, for example by clamping its time range, is still forwarded unchanged,
// because the querier applies the same time range limits to the estimate.
func newQueryCostRoundTripper(next http.RoundTripper, codec Codec, middlewares ...MetricsQueryMiddleware) http.RoundTripper {
	return &queryCostRoundTripper{
		next:  next,
		codec: codec,
		handler: MergeMetricsQueryMiddlewares(middlewares...).Wrap(HandlerFunc(func(context.Context, MetricsQueryRequest) (Response, error) {
			return nil, nil
		})),
	}
}

func (rt *queryCostRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	// The codec returns a bad data error for invalid parameters.
	req, err := rt.codec.DecodeMetricsQueryRequest(r.Context(), r)
	if err != nil {
		return nil, err
	}
	if _, err := rt.handler.Do(r.Context(), req); err != nil {
		return nil, err
	}
	return rt.next.RoundTrip(r)
}
