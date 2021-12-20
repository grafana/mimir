// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/util.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/opentracing/opentracing-go"
)

// RequestResponse contains a request response and the respective request that was used.
type RequestResponse struct {
	Request  Request
	Response Response
}

// DoRequests executes a list of requests in parallel.
func DoRequests(ctx context.Context, downstream Handler, reqs []Request, recordSpan bool) ([]RequestResponse, error) {
	g, ctx := errgroup.WithContext(ctx)
	mtx := sync.Mutex{}
	resps := make([]RequestResponse, 0, len(reqs))
	for i := 0; i < len(reqs); i++ {
		req := reqs[i]
		g.Go(func() error {
			var (
				span     opentracing.Span
				childCtx = ctx
			)

			if recordSpan {
				span, childCtx = opentracing.StartSpanFromContext(ctx, "DoRequests")
				req.LogToSpan(span)
			}

			resp, err := downstream.Do(childCtx, req)

			if span != nil {
				span.Finish()
			}
			if err != nil {
				return err
			}

			mtx.Lock()
			resps = append(resps, RequestResponse{req, resp})
			mtx.Unlock()
			return nil
		})
	}

	return resps, g.Wait()
}
