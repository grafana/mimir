// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/step_align.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

type stepAlignMiddleware struct {
	next       MetricsQueryHandler
	limits     Limits
	logger     log.Logger
	notAligned prometheus.Counter
	adjusted   *prometheus.CounterVec
}

// newStepAlignMiddleware creates a middleware that aligns the start and end of request to the step to
// improve the cacheability of the query results based on per-tenant configuration.
func newStepAlignMiddleware(limits Limits, logger log.Logger, registerer prometheus.Registerer) MetricsQueryMiddleware {
	notAligned := promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Name: "cortex_query_frontend_non_step_aligned_queries_total",
		Help: "Total queries sent that are not step aligned.",
	})
	adjusted := promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_frontend_non_step_aligned_queries_adjusted_total",
		Help: "Number of queries whose start or end times have been adjusted to be step-aligned.",
	}, []string{"user"})

	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &stepAlignMiddleware{
			next:       next,
			limits:     limits,
			logger:     logger,
			notAligned: notAligned,
			adjusted:   adjusted,
		}
	})
}

func (s *stepAlignMiddleware) Do(ctx context.Context, r MetricsQueryRequest) (Response, error) {
	tenants, err := tenant.TenantIDs(ctx)
	if err != nil {
		return s.next.Do(ctx, r)
	}

	start := (r.GetStart() / r.GetStep()) * r.GetStep()
	end := (r.GetEnd() / r.GetStep()) * r.GetStep()
	stepAligned := start == r.GetStart() && end == r.GetEnd()

	// Request is already step aligned, nothing to do.
	if stepAligned {
		return s.next.Do(ctx, r)
	}

	s.notAligned.Inc()
	// If any tenant doesn't have the setting to force alignment enabled, just run the next handler.
	if !validation.AllTrueBooleansPerTenant(tenants, s.limits.AlignQueriesWithStep) {
		return s.next.Do(ctx, r)
	}

	for _, id := range tenants {
		s.adjusted.WithLabelValues(id).Inc()
	}

	spanlog := spanlogger.FromContext(ctx, s.logger)
	spanlog.DebugLog(
		"msg", "query start or end has been adjusted to be step-aligned",
		spanlogger.TenantIDsTagName, tenants,
		"original_start", r.GetStart(),
		"original_end", r.GetEnd(),
		"adjusted_start", start,
		"adjusted_end", end,
		"step", r.GetStep(),
	)

	updatedReq, err := r.WithStartEnd(start, end)
	if err != nil {
		return nil, err
	}

	return s.next.Do(ctx, updatedReq)
}

// isRequestStepAligned returns whether the MetricsQueryRequest start and end timestamps are aligned
// with the step.
func isRequestStepAligned(req MetricsQueryRequest) bool {
	if req.GetStep() == 0 {
		return true
	}

	return req.GetEnd()%req.GetStep() == 0 && req.GetStart()%req.GetStep() == 0
}
