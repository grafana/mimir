// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/util/promqlext"
)

type queryBlockerMiddleware struct {
	next                  MetricsQueryHandler
	limits                Limits
	normalised            map[string]string
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
			normalised:            make(map[string]string),
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
	logger := log.With(qb.logger, "user", tenant)

	query := req.GetQuery()

	for ruleIndex, block := range blocks {
		if qb.normalizeQueryPattern(block.Pattern, ruleIndex) == strings.TrimSpace(query) {
			level.Info(logger).Log("msg", "query blocker matched with exact match policy", "query", query, "index", ruleIndex)
			return true, block.Reason
		}

		if block.Regex {
			r, err := labels.NewFastRegexMatcher(block.Pattern)
			if err != nil {
				level.Error(logger).Log("msg", "query blocker regex does not compile, ignoring query blocker", "pattern", block.Pattern, "err", err, "index", ruleIndex)
				continue
			}
			if r.MatchString(query) {
				level.Info(logger).Log("msg", "query blocker matched with regex policy", "pattern", block.Pattern, "query", query, "index", ruleIndex)
				return true, block.Reason
			}
		}
	}
	return false, ""
}

// normalizeQueryPattern will take the given non regex block query and normalise it.
// This will apply the same normalisation as the incoming user query.
// This allows for the blocked query to be defined as observed before or after normalisation, and removing the
// need for operators to determine the normalised version of a query before adding a block.
// The normalised block pattern is cached so it does not need to be processed on every block consideration.
func (qb *queryBlockerMiddleware) normalizeQueryPattern(pattern string, ruleIndex int) string {

	n, ok := qb.normalised[pattern]
	if ok {
		return n
	}

	if expr, err := promqlext.NewPromQLParser().ParseExpr(pattern); err == nil {
		n = expr.String()
	} else {
		level.Error(qb.logger).Log("msg", "blocked query is not valid PromQL", "pattern", pattern, "err", err, "index", ruleIndex)
		n = pattern
	}
	n = strings.TrimSpace(n)
	qb.normalised[pattern] = n
	return n
}
