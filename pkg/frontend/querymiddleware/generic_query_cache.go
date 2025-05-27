// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

type GenericQueryCacheKey struct {
	// CacheKey is a full non-hashed representation of the request, used to uniquely identify
	// a request in the cache.
	CacheKey string

	// CacheKeyPrefix is a cache key prefix to use for this request.
	CacheKeyPrefix string
}

type tenantCacheTTL interface {
	// ttl returns the cache TTL for the input userID.
	ttl(userID string) time.Duration
}

type keyingFunc func(r *http.Request) (*GenericQueryCacheKey, error)

// genericQueryCache is a http.RoundTripped wrapping the downstream with a generic HTTP response cache.
type genericQueryCache struct {
	cache     cache.Cache
	tenantTTL tenantCacheTTL
	cacheKey  keyingFunc
	metrics   *resultsCacheMetrics
	next      http.RoundTripper
	logger    log.Logger
}

func newGenericQueryCacheRoundTripper(cache cache.Cache, cacheKey keyingFunc, tenantTTL tenantCacheTTL, next http.RoundTripper, logger log.Logger, metrics *resultsCacheMetrics) http.RoundTripper {
	return &genericQueryCache{
		cache:     cache,
		tenantTTL: tenantTTL,
		metrics:   metrics,
		cacheKey:  cacheKey,
		next:      next,
		logger:    logger,
	}
}

func (c *genericQueryCache) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()

	spanLog, ctx := spanlogger.NewWithLogger(ctx, c.logger, "genericQueryCache.RoundTrip")
	defer spanLog.Finish()

	// Skip the cache if disabled for this request.
	if decodeCacheDisabledOption(req) {
		spanLog.DebugLog("msg", "cache disabled for the request")
		return c.next.RoundTrip(req)
	}

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Skip the cache if disabled for the tenant. We look at the minimum TTL so that we skip the cache
	// if it's disabled for any of tenants.
	cacheTTL := validation.MinDurationPerTenant(tenantIDs, c.tenantTTL.ttl)
	if cacheTTL <= 0 {
		spanLog.DebugLog("msg", "cache disabled for the tenant")
		return c.next.RoundTrip(req)
	}

	queryReq, err := c.cacheKey(req)
	if err != nil {
		if !errors.Is(err, ErrUnsupportedRequest) {
			// Logging as info because it's not an actionable error here.
			// We defer it to the downstream.
			level.Info(spanLog).Log("msg", "skipped query response caching because failed to parse the request", "err", err)
		}

		// We skip the caching but let the downstream try to handle it anyway,
		// since it's not our responsibility to decide how to respond in this case.
		return c.next.RoundTrip(req)
	}

	// Lookup the cache.
	c.metrics.cacheRequests.Inc()
	cacheKey, hashedCacheKey := generateGenericQueryRequestCacheKey(tenantIDs, queryReq)
	res := c.fetchCachedResponse(ctx, cacheKey, hashedCacheKey)
	if res != nil {
		c.metrics.cacheHits.Inc()
		spanLog.DebugLog("msg", "response fetched from the cache")
		return res, nil
	}

	res, err = c.next.RoundTrip(req)
	if err != nil {
		return res, err
	}

	// Store the result in the cache.
	if isGenericQueryResponseCacheable(res) {
		cachedRes, err := EncodeCachedHTTPResponse(cacheKey, res)
		if err != nil {
			level.Warn(spanLog).Log("msg", "failed to read query response before storing it to cache", "err", err)
			return res, err
		}

		c.storeCachedResponse(ctx, cachedRes, hashedCacheKey, cacheTTL)
	}

	return res, nil
}

func (c *genericQueryCache) fetchCachedResponse(ctx context.Context, cacheKey, hashedCacheKey string) *http.Response {
	// Look up the cache.
	cacheHits := c.cache.GetMulti(ctx, []string{hashedCacheKey})
	if cacheHits[hashedCacheKey] == nil {
		// Not found in the cache.
		return nil
	}
	c.recordCacheHitQueryDetails(ctx, cacheHits)

	// Decode the cached entry.
	cachedRes := &CachedHTTPResponse{}
	if err := cachedRes.Unmarshal(cacheHits[hashedCacheKey]); err != nil {
		level.Warn(c.logger).Log("msg", "failed to decode cached query response", "cache_key", hashedCacheKey, "err", err)
		return nil
	}

	// Ensure no cache key collision.
	if cachedRes.GetCacheKey() != cacheKey {
		level.Warn(c.logger).Log("msg", "skipped cached query response because a cache key collision has been found", "cache_key", hashedCacheKey)
		return nil
	}

	return DecodeCachedHTTPResponse(cachedRes)
}

func (c *genericQueryCache) storeCachedResponse(ctx context.Context, cachedRes *CachedHTTPResponse, hashedCacheKey string, cacheTTL time.Duration) {
	// Encode the cached entry.
	encoded, err := cachedRes.Marshal()
	if err != nil {
		level.Warn(c.logger).Log("msg", "failed to encode cached query response", "err", err)
		return
	}
	toStore := map[string][]byte{hashedCacheKey: encoded}
	c.cache.SetMultiAsync(toStore, cacheTTL)
	c.recordCacheStoreQueryDetails(ctx, toStore)
}

func (c *genericQueryCache) recordCacheHitQueryDetails(ctx context.Context, hits map[string][]byte) {
	details := QueryDetailsFromContext(ctx)
	if details == nil {
		return
	}
	for _, val := range hits {
		details.ResultsCacheHitBytes += len(val)
	}
}

func (c *genericQueryCache) recordCacheStoreQueryDetails(ctx context.Context, toStore map[string][]byte) {
	details := QueryDetailsFromContext(ctx)
	if details == nil {
		return
	}
	for _, val := range toStore {
		details.ResultsCacheMissBytes += len(val)
	}
}

func generateGenericQueryRequestCacheKey(tenantIDs []string, req *GenericQueryCacheKey) (cacheKey, hashedCacheKey string) {
	cacheKey = fmt.Sprintf("%s:%s", tenant.JoinTenantIDs(tenantIDs), req.CacheKey)
	hashedCacheKey = fmt.Sprintf("%s%s", req.CacheKeyPrefix, cacheHashKey(cacheKey))
	return
}

func isGenericQueryResponseCacheable(res *http.Response) bool {
	return res.StatusCode >= 200 && res.StatusCode < 300
}
