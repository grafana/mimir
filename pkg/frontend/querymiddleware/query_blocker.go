// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
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
	if len(blocks) <= 0 {
		return false, ""
	}

	var (
		logger          = log.With(qb.logger, "user", tenant)
		query           = req.GetQuery()
		queryDurationMs = req.GetEnd() - req.GetStart()
		queryDuration   = time.Duration(queryDurationMs) * time.Millisecond
		isInstantQuery  = queryDurationMs == 0
		stepMs          = req.GetStep()
		stepDuration    = time.Duration(stepMs) * time.Millisecond
	)

	for ruleIndex, block := range blocks {
		if block.UnalignedRangeQueries {
			_, isRangeQuery := req.(*PrometheusRangeQueryRequest)

			if !isRangeQuery || isRequestStepAligned(req) {
				continue
			}
		}

		pattern := strings.TrimSpace(block.Pattern)

		// Check literal match regardless of regex setting (backwards compatibility).
		patternMatches := pattern == strings.TrimSpace(query)

		if block.Regex {
			r, err := labels.NewFastRegexMatcher(block.Pattern)
			if err != nil {
				level.Error(logger).Log("msg", "query blocker regex does not compile, ignoring query blocker", "pattern", block.Pattern, "err", err, "index", ruleIndex)
				continue
			}
			if r.MatchString(query) {
				patternMatches = true
			}
		}

		timeRangeViolation := !isInstantQuery &&
			block.TimeRangeLongerThan > 0 &&
			queryDuration > time.Duration(block.TimeRangeLongerThan)

		stepViolation := block.MinimumStepSize > 0 &&
			stepMs > 0 &&
			stepDuration < time.Duration(block.MinimumStepSize)

		hasCondition := pattern != "" || block.TimeRangeLongerThan > 0 || block.MinimumStepSize > 0
		shouldBlock := hasCondition &&
			(pattern == "" || patternMatches) &&
			(block.TimeRangeLongerThan == 0 || timeRangeViolation) &&
			(block.MinimumStepSize == 0 || stepViolation)

		if shouldBlock {
			level.Info(logger).Log(
				"msg", "query blocked",
				"query", query,
				"query_duration_ms", queryDurationMs,
				"step_ms", stepMs,
				"pattern_matched", patternMatches,
				"time_range_violation", timeRangeViolation,
				"step_violation", stepViolation,
				"index", ruleIndex,
				"reason", block.Reason,
			)

			return true, block.Reason
		}
	}

	return false, ""
}
