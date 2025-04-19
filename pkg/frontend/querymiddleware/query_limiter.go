// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/crypto/blake2b"

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
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return ql.next.Do(ctx, req)
	}

	key := ql.keyGen.QueryRequestLimiter(ctx, tenant.JoinTenantIDs(tenantIDs), req)
	hashedKey := maybeHashCacheKey(key)
	// start at max duration value
	cacheValue := validation.LimitedQuery{
		Query:            "",
		AllowedFrequency: 0, // start with min duration
	}
	query := strings.TrimSpace(req.GetQuery())
	var tenantMinAllowedFrequency string

	for _, tenantID := range tenantIDs {
		logger := log.With(ql.logger, "user", tenantID)
		for _, limitedQuery := range ql.limits.LimitedQueries(tenantID) {
			if strings.TrimSpace(limitedQuery.Query) == query {
				level.Info(logger).Log("msg", "query limiter matched exact query", "query", query, "tenant", tenantID)
				if cacheValue.Query == "" {
					cacheValue.Query = query
				}
				if limitedQuery.AllowedFrequency > cacheValue.AllowedFrequency {
					cacheValue.AllowedFrequency = limitedQuery.AllowedFrequency
					tenantMinAllowedFrequency = tenantID
				}
			}
		}
	}
	// If we found any matching limited query, we should try to cache it
	if cacheValue.Query != "" {
		if err := ql.cache.Add(ctx, hashedKey, []byte(key), cacheValue.AllowedFrequency); err != nil {
			// If we receive ErrNotStored, the entry is still in the cache and the query should be blocked
			if errors.Is(err, cache.ErrNotStored) {
				ql.blockedQueriesCounter.WithLabelValues(tenantMinAllowedFrequency, "limited").Inc()
				return nil, newQueryLimitedError(cacheValue.AllowedFrequency, tenantMinAllowedFrequency)
			}
			ql.logger.Log("msg", "error while adding to query limiter cache", "err", err)
		}
	}

	return ql.next.Do(ctx, req)
}

func maybeHashCacheKey(key string) string {
	if len(key) <= base64.RawURLEncoding.EncodedLen(blake2b.Size256) {
		return key
	}

	sum := blake2b.Sum256([]byte(key))
	return base64.RawURLEncoding.EncodeToString(sum[:blake2b.Size256])
}
