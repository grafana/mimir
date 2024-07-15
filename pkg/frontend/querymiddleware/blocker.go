// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
	registerer prometheus.Registerer,
) MetricsQueryMiddleware {
	blockedQueriesCounter := promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_frontend_rejected_queries_total",
		Help: "Number of queries that were rejected by the cluster administrator.",
	}, []string{"user", "reason"})
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
		isBlocked := qb.isBlocked(tenant, req)
		if isBlocked {
			qb.blockedQueriesCounter.WithLabelValues(tenant, "blocked").Inc()
			return nil, newQueryBlockedError()
		}
	}
	return qb.next.Do(ctx, req)
}

func (qb *queryBlockerMiddleware) isBlocked(tenant string, req MetricsQueryRequest) bool {
	blocks := qb.limits.BlockedQueries(tenant)
	if len(blocks) <= 0 {
		return false
	}
	logger := log.With(qb.logger, "user", tenant)

	query := req.GetQuery()

	for ruleIndex, block := range blocks {
		if strings.TrimSpace(block.Pattern) == strings.TrimSpace(query) {
			level.Info(logger).Log("msg", "query blocker matched with exact match policy", "query", query, "index", ruleIndex)
			return true
		}

		if block.Regex {
			r, err := labels.NewFastRegexMatcher(block.Pattern)
			if err != nil {
				level.Error(logger).Log("msg", "query blocker regex does not compile, ignoring query blocker", "pattern", block.Pattern, "err", err, "index", ruleIndex)
				continue
			}
			if r.MatchString(query) {
				level.Info(logger).Log("msg", "query blocker matched with regex policy", "pattern", block.Pattern, "query", query, "index", ruleIndex)
				return true
			}
		}
	}
	return false
}
