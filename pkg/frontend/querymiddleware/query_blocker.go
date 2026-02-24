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
	)

	for ruleIndex, block := range blocks {
		if block.UnalignedRangeQueries {
			_, isRangeQuery := req.(*PrometheusRangeQueryRequest)

			if !isRangeQuery || isRequestStepAligned(req) {
				continue
			}
		}

		hasPatternCheck := strings.TrimSpace(block.Pattern) != ""
		hasShorterThan := block.TimeRangeShorterThan > 0
		hasLongerThan := block.TimeRangeLongerThan > 0
		hasTimeRangeCheck := hasShorterThan || hasLongerThan

		if !hasPatternCheck && !hasTimeRangeCheck {
			continue
		}

		patternMatches := false
		if hasPatternCheck {
			if block.Regex {
				r, err := labels.NewFastRegexMatcher(block.Pattern)
				if err != nil {
					level.Error(logger).Log("msg", "query blocker regex does not compile, ignoring query blocker", "pattern", block.Pattern, "err", err, "index", ruleIndex)
					continue
				}
				if r.MatchString(query) {
					patternMatches = true
				}
			} else {
				if strings.TrimSpace(block.Pattern) == strings.TrimSpace(query) {
					patternMatches = true
				}
			}
		}

		timeRangeViolation := false
		timeRangePosition := ""
		if hasTimeRangeCheck && !isInstantQuery {
			tooShort := false
			tooLong := false

			if hasShorterThan && queryDuration < time.Duration(block.TimeRangeShorterThan) {
				tooShort = true
			}
			if hasLongerThan && queryDuration > time.Duration(block.TimeRangeLongerThan) {
				tooLong = true
			}

			// Both time range thresholds can be set for two use cases:
			// 1. longer_than >= shorter_than: block queries OUTSIDE window (too short OR too long)
			//    e.g., longer_than: 21d, shorter_than: 7d blocks queries < 7d OR > 21d
			// 2. longer_than < shorter_than: block queries INSIDE window (both conditions)
			//    e.g., longer_than: 2h, shorter_than: 3h blocks queries between 2h-3h

			if hasShorterThan && hasLongerThan && block.TimeRangeLongerThan < block.TimeRangeShorterThan {
				// Inside window: use AND logic
				timeRangeViolation = tooLong && tooShort
			} else {
				// Outside window or single threshold: use OR logic
				timeRangeViolation = tooShort || tooLong
			}

			if timeRangeViolation {
				switch {
				case tooLong && tooShort:
					timeRangePosition = "inside"
				case tooShort:
					timeRangePosition = "before"
				case tooLong:
					timeRangePosition = "after"
				}
			}
		}

		shouldBlock := false

		switch {
		case hasPatternCheck && hasTimeRangeCheck:
			shouldBlock = patternMatches && timeRangeViolation
		case hasPatternCheck:
			shouldBlock = patternMatches
		case hasTimeRangeCheck:
			shouldBlock = timeRangeViolation
		default:
		}

		if shouldBlock {
			level.Info(logger).Log(
				"msg", "query blocked",
				"query", query,
				"query_duration_ms", queryDurationMs,
				"pattern_matched", patternMatches,
				"time_range_violation", timeRangeViolation,
				"time_range_position", timeRangePosition,
				"index", ruleIndex,
				"reason", block.Reason,
			)

			return true, block.Reason
		}
	}

	return false, ""
}
