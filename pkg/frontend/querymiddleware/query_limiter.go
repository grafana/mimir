package querymiddleware

import (
	"context"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
	"time"
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
			if !ql.enoughTimeSinceLastQuery(ctx, key, hashedKey, limitedQuery, spanLog) {
				level.Info(logger).Log("msg", "query limiter matched exact query", "query", query, "index", i)
				return true
			}
			// If the query matches a query that should be limited, but it shouldn't be blocked this time,
			// we should cache it so that it's blocked if it's run again within limitedQuery.MaxFrequency.
			now := time.Now().UnixMilli()
			bytes, err := proto.Marshal(&CachedLimitedQuery{
				Key:             key,
				LimitedQuery:    limitedQuery.Query,
				LastAllowedTime: now,
			})
			if err != nil {
				level.Warn(spanLog).Log(
					"msg", "unable to marshal cached error",
					"key", key,
					"limited_query", limitedQuery.Query,
					"last_allowed_query_time", now,
					"err", err,
				)
			}
			ql.cache.SetAsync(hashedKey, bytes, limitedQuery.MaxFrequency)
		}
	}
	return false
}

func (ql *queryLimiterMiddleware) enoughTimeSinceLastQuery(ctx context.Context, key, hashedKey string, limitedQuery *validation.LimitedQuery, spanLog *spanlogger.SpanLogger) bool {
	query, lastAllowedQueryTime := ql.loadLimitedQueryFromCache(ctx, key, hashedKey, spanLog)
	if query != "" && !lastAllowedQueryTime.IsZero() {
		if time.Since(lastAllowedQueryTime) < limitedQuery.MaxFrequency {
			return false
		}
	}
	return true
}

func (ql *queryLimiterMiddleware) loadLimitedQueryFromCache(
	ctx context.Context, cacheKey, hashedKey string, spanLog *spanlogger.SpanLogger,
) (query string, lastAllowedTime time.Time) {
	res := ql.cache.GetMulti(ctx, []string{hashedKey})
	cached, ok := res[hashedKey]

	if !ok {
		return "", time.Time{}
	}

	var cachedQuery CachedLimitedQuery
	if err := proto.Unmarshal(cached, &cachedQuery); err != nil {
		level.Warn(spanLog).Log("msg", "unable to unmarshall cached query", "err", err)
		return "", time.Time{}
	}

	if cachedQuery.GetKey() != cacheKey {
		spanLog.DebugLog(
			"msg", "cached limited query does not match",
			"expected_key", cacheKey,
			"actual_key", cached,
			"hashed_key", hashedKey,
		)
		return "", time.Time{}
	}

	return cachedQuery.LimitedQuery, time.UnixMilli(cachedQuery.LastAllowedTime)
}
