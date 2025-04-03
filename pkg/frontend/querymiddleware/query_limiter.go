package querymiddleware

import (
	"context"
	"fmt"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
	"time"
)

// queryLimiterMiddleware blocks a query if it is should not be executed more frequently than a configured interval,
// and allows it if enough time has passed since the last time the query was allowed.
// When blocked, the request is rejected with a "too many requests" error.
type queryLimiterMiddleware struct {
	next                  MetricsQueryHandler
	limits                Limits
	logger                log.Logger
	lastAllowedQueryTime  map[string]time.Time
	blockedQueriesCounter *prometheus.CounterVec
}

func newQueryLimiterMiddleware(
	limits Limits,
	logger log.Logger,
	blockedQueriesCounter *prometheus.CounterVec,
) MetricsQueryMiddleware {
	lastAllowedQueryTime := make(map[string]time.Time)
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &queryLimiterMiddleware{
			next:                  next,
			limits:                limits,
			logger:                logger,
			lastAllowedQueryTime:  lastAllowedQueryTime,
			blockedQueriesCounter: blockedQueriesCounter,
		}
	})
}

func (ql *queryLimiterMiddleware) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	tenants, err := tenant.TenantIDs(ctx)
	if err != nil {
		return ql.next.Do(ctx, req)
	}

	for _, tenant := range tenants {
		if isBlocked := ql.shouldBlock(tenant, req); isBlocked {
			ql.blockedQueriesCounter.WithLabelValues(tenant, "limited").Inc()
			return nil, newQueryLimitedError()
		}
	}
	return ql.next.Do(ctx, req)
}

func (ql *queryLimiterMiddleware) shouldBlock(tenant string, req MetricsQueryRequest) bool {
	limitedQueries := ql.limits.LimitedQueries(tenant)

	if len(limitedQueries) <= 0 {
		return false
	}

	logger := log.With(ql.logger, "user", tenant)

	query := req.GetQuery()
	for i, limitedQuery := range limitedQueries {
		fmt.Printf("%+v\n", limitedQuery)
		if strings.TrimSpace(limitedQuery.Query) == strings.TrimSpace(query) && !ql.enoughTimeSinceLastQuery(limitedQuery) {
			level.Info(logger).Log("msg", "query blocker matched with exact match policy", "query", query, "index", i)
			return true
		}
	}
	return false
}

func (ql *queryLimiterMiddleware) enoughTimeSinceLastQuery(limitedQuery *validation.LimitedQuery) bool {
	if laqt, ok := ql.lastAllowedQueryTime[limitedQuery.Query]; ok {
		if time.Since(laqt) < limitedQuery.MaxFrequency {
			return false
		}
	}
	ql.lastAllowedQueryTime[limitedQuery.Query] = time.Now()
	return true
}
