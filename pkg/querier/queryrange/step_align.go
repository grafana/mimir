// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/step_align.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"context"
)

// newStepAlignMiddleware creates a middleware that aligns the start and end of request to the step to
// improved the cacheability of the query results.
func newStepAlignMiddleware() Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, r Request) (Response, error) {
			start := (r.GetStart() / r.GetStep()) * r.GetStep()
			end := (r.GetEnd() / r.GetStep()) * r.GetStep()
			return next.Do(ctx, r.WithStartEnd(start, end))
		})
	})
}

// isRequestStepAligned returns whether the Request start and end timestamps are aligned
// with the step.
func isRequestStepAligned(req Request) bool {
	if req.GetStep() == 0 {
		return true
	}

	return req.GetEnd()%req.GetStep() == 0 && req.GetStart()%req.GetStep() == 0
}
