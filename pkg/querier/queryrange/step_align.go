// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/step_align.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"context"
)

// StepAlignMiddleware aligns the start and end of request to the step to
// improved the cacheability of the query results.
var StepAlignMiddleware = MiddlewareFunc(func(next Handler) Handler {
	return stepAlign{
		next: next,
	}
})

type stepAlign struct {
	next Handler
}

func (s stepAlign) Do(ctx context.Context, r Request) (Response, error) {
	start := (r.GetStart() / r.GetStep()) * r.GetStep()
	end := (r.GetEnd() / r.GetStep()) * r.GetStep()
	return s.next.Do(ctx, r.WithStartEnd(start, end))
}
