// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/util/validation"
)

type queryBlockerMiddleware struct {
	next                  MetricsQueryHandler
	limits                Limits
	logger                log.Logger
	blockedQueriesCounter *prometheus.CounterVec
}

func newQueryBlockerMiddleware(
	limits Limits,
	logger log.Logger,
	blockedQueriesCounter *prometheus.CounterVec,
) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &queryBlockerMiddleware{
			next:                  next,
			limits:                limits,
			logger:                logger,
			blockedQueriesCounter: blockedQueriesCounter,
		}
	})
}

func (qb *queryBlockerMiddleware) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	tenants, err := tenant.TenantIDs(ctx)
	if err != nil {
		return qb.next.Do(ctx, req)
	}

	for _, tenant := range tenants {
		isBlocked, reason := qb.isBlocked(tenant, req)
		if isBlocked {
			qb.blockedQueriesCounter.WithLabelValues(tenant, "blocked").Inc()
			return nil, newQueryBlockedError(reason)
		}
	}
	return qb.next.Do(ctx, req)
}

func (qb *queryBlockerMiddleware) isBlocked(tenant string, req MetricsQueryRequest) (bool, string) {
	blocks := qb.limits.BlockedQueries(tenant)
	if len(blocks) == 0 {
		return false, ""
	}

	var (
		logger          = log.With(qb.logger, "user", tenant)
		queryDurationMs = req.GetEnd() - req.GetStart()
		stepMs          = req.GetStep()
	)

	queryType := validation.QueryTypeUnknown
	switch req.(type) {
	case *PrometheusInstantQueryRequest:
		queryType = validation.QueryTypeInstant
	case *PrometheusRangeQueryRequest:
		queryType = validation.QueryTypeRange
	case *remoteReadQueryRequest:
		queryType = validation.QueryTypeRemoteRead
	}

	input := validation.BlockedQueryInput{
		Query:         req.GetQuery(),
		QueryType:     queryType,
		QueryDuration: time.Duration(queryDurationMs) * time.Millisecond,
		StepAligned:   isRequestStepAligned(req),
		StepKnown:     stepMs > 0,
		Step:          time.Duration(stepMs) * time.Millisecond,
	}

	for ruleIndex, block := range blocks {
		if block.MatchesRule(input) {
			level.Info(logger).Log(
				"msg", "query blocked",
				"query", input.Query,
				"query_duration_ms", queryDurationMs,
				"step_ms", stepMs,
				"index", ruleIndex,
				"reason", block.Reason,
			)
			return true, block.Reason
		}
	}

	return false, ""
}
