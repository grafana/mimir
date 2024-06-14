// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/prometheus/client_golang/prometheus"
)

type errorCacheMiddleware struct {
	logger log.Logger
	next   MetricsQueryHandler

	cacheHit *prometheus.CounterVec // this should be passed in from the blocker middleware cortex_query_frontend_rejected_queries_total

	cache  cache.Cache
	keyGen CacheKeyGenerator
}

func newErrorCacheMiddleware(
	logger log.Logger,
	next MetricsQueryHandler,
	cacheHit *prometheus.CounterVec,
	cache cache.Cache,
	keyGen CacheKeyGenerator) MetricsQueryMiddleware {

	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &errorCacheMiddleware{
			logger:   logger,
			next:     next,
			cacheHit: cacheHit,
			cache:    cache,
			keyGen:   keyGen,
		}
	})
}

type errorQueryTTL struct {
	limits Limits
}

func (e *errorQueryTTL) ttl(userID string) time.Duration {
	return e.limits.ResultsCacheTTL(userID)
}

func newErrorCacheRoundTripper(cache cache.Cache, generator CacheKeyGenerator, limits Limits, next http.RoundTripper, logger log.Logger, reg prometheus.Registerer) http.RoundTripper {
	ttl := &errorQueryTTL{
		limits: limits,
	}
	return newGenericQueryCacheRoundTripper(cache, generator.LabelValuesCardinality, ttl, next, logger, nil /*newResultsCacheMetrics("", reg)*/) // TODO figure out how to pass queryType through for metrics
}
func (e *errorCacheMiddleware) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {

	return nil, nil
}

// 	blockedQueriesCounter.WithLabelValues(tenant, "blocked").Inc()
