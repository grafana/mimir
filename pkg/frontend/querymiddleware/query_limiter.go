// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"errors"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// queryLimiterMiddleware blocks a query if it is should not be executed more frequently than a configured interval,
// and allows it if enough time has passed since the last time the query was allowed.
// When blocked, the request is rejected with a "too many requests" error.
// The query limiter currently only matches exact queries, and does not check against query time, only execution time.
type queryLimiterMiddleware struct {
	next                  MetricsQueryHandler
	cache                 cache.Cache
	keyGen                CacheKeyGenerator
	limits                Limits
	logger                log.Logger
	blockedQueriesCounter *prometheus.CounterVec
}

func newQueryLimiterMiddleware(
	cache cache.Cache,
	cacheKeyGen CacheKeyGenerator,
	limits Limits,
	logger log.Logger,
	blockedQueriesCounter *prometheus.CounterVec,
) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &queryLimiterMiddleware{
			next:                  next,
			cache:                 cache,
			keyGen:                cacheKeyGen,
			limits:                limits,
			logger:                logger,
			blockedQueriesCounter: blockedQueriesCounter,
		}
	})
}

func (ql *queryLimiterMiddleware) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	spanLog := spanlogger.FromContext(ctx, ql.logger)
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return ql.next.Do(ctx, req)
	}

	key := ql.keyGen.QueryRequestLimiter(ctx, tenant.JoinTenantIDs(tenantIDs), req)
	hashedKey := cacheHashKey(key)

	for _, tenantID := range tenantIDs {
		if isBlocked := ql.shouldBlock(ctx, key, hashedKey, tenantID, req.GetQuery(), spanLog); isBlocked {
			ql.blockedQueriesCounter.WithLabelValues(tenantID, "limited").Inc()
			return nil, newQueryLimitedError()
		}
	}
	return ql.next.Do(ctx, req)
}

func (ql *queryLimiterMiddleware) shouldBlock(
	ctx context.Context, key, hashedKey, tenantID, query string, spanLog *spanlogger.SpanLogger,
) bool {
	limitedQueries := ql.limits.LimitedQueries(tenantID)

	if len(limitedQueries) <= 0 {
		return false
	}

	logger := log.With(ql.logger, "user", tenantID)

	for i, limitedQuery := range limitedQueries {
		if strings.TrimSpace(limitedQuery.Query) == strings.TrimSpace(query) {
			if !ql.enoughTimeSinceLastQuery(ctx, key, hashedKey, limitedQuery) {
				level.Info(logger).Log("msg", "query limiter matched exact query", "query", query, "index", i)
				return true
			}
		}
	}
	return false
}

func (ql *queryLimiterMiddleware) enoughTimeSinceLastQuery(ctx context.Context, cacheValue, hashedKey string, limitedQuery *validation.LimitedQuery) bool {
	// If the query fails to be added to the cache with ErrNotStored, that means the last time the query was called
	// is within AllowedFrequency, so we should block it. Otherwise, the query has now been added to the cache and
	// will be blocked next time if the entry still exists in the cache.
	err := ql.cache.Add(ctx, hashedKey, []byte(cacheValue), limitedQuery.AllowedFrequency)
	if errors.Is(err, cache.ErrNotStored) {
		return false
	} else {
		ql.logger.Log("msg", "error while adding to query limiter cache", "err", err)
	}
	return true
}
