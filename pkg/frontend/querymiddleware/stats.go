// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type queryStatsMiddleware struct {
	nonAlignedQueries prometheus.Counter
	next              Handler
}

func newQueryStatsMiddleware(reg prometheus.Registerer) Middleware {
	nonAlignedQueries := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_query_frontend_non_step_aligned_queries_total",
		Help: "Total queries sent that are not step aligned.",
	})

	return MiddlewareFunc(func(next Handler) Handler {
		return &queryStatsMiddleware{
			nonAlignedQueries: nonAlignedQueries,
			next:              next,
		}
	})
}

func (s queryStatsMiddleware) Do(ctx context.Context, req Request) (Response, error) {
	if !isRequestStepAligned(req) {
		s.nonAlignedQueries.Inc()
	}

	return s.next.Do(ctx, req)
}
