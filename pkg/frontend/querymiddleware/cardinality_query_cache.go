// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	cardinalityLabelNamesQueryCachePrefix  = "cn:"
	cardinalityLabelValuesQueryCachePrefix = "cv:"
)

type cardinalityQueryRequest interface {
	// String returns a full representation of the request, used to uniquely identify
	// a request in the cache.
	String() string

	// RequestType returns the type of the cardinality query, used to distinguish
	// different type of requests in the cache.
	RequestType() cardinality.RequestType
}

// cardinalityQueryCache is a http.RoundTripped wrapping the downstream with an HTTP response cache.
// This RoundTripper is used to add caching support to cardinality analysis API endpoints.
type cardinalityQueryCache struct {
	cache   cache.Cache
	limits  Limits
	metrics *resultsCacheMetrics
	next    http.RoundTripper
	logger  log.Logger
}

func newCardinalityQueryCacheRoundTripper(cache cache.Cache, limits Limits, next http.RoundTripper, logger log.Logger, reg prometheus.Registerer) http.RoundTripper {
	return &cardinalityQueryCache{
		cache:   cache,
		limits:  limits,
		metrics: newResultsCacheMetrics("cardinality", reg),
		next:    next,
		logger:  logger,
	}
}

func (c *cardinalityQueryCache) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()

	spanLog, ctx := spanlogger.NewWithLogger(ctx, c.logger, "cardinalityQueryCache.RoundTrip")
	defer spanLog.Finish()

	// Skip the cache if disabled for this request.
	if decodeCacheDisabledOption(req) {
		level.Debug(spanLog).Log("msg", "cache disabled for the request")
		return c.next.RoundTrip(req)
	}

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Skip the cache if disabled for the tenant. We look at the minimum TTL so that we skip the cache
	// if it's disabled for any of tenants.
	cacheTTL := validation.MinDurationPerTenant(tenantIDs, c.limits.ResultsCacheTTLForCardinalityQuery)
	if cacheTTL <= 0 {
		level.Debug(spanLog).Log("msg", "cache disabled for the tenant")
		return c.next.RoundTrip(req)
	}

	// Decode the request.
	queryReq, err := parseCardinalityQueryRequest(req)
	if err != nil {
		// Logging as info because it's not an actionable error here.
		// We defer it to the downstream.
		level.Info(spanLog).Log("msg", "skipped cardinality query caching because failed to parse the request", "err", err)

		// We skip the caching but let the downstream try to handle it anyway,
		// since it's not our responsibility to decide how to respond in this case.
		return c.next.RoundTrip(req)
	}

	// Lookup the cache.
	c.metrics.cacheRequests.Inc()
	cacheKey, hashedCacheKey := generateCardinalityQueryRequestCacheKey(tenantIDs, queryReq)
	res := c.fetchCachedResponse(ctx, cacheKey, hashedCacheKey)
	if res != nil {
		c.metrics.cacheHits.Inc()
		level.Debug(spanLog).Log("msg", "response fetched from the cache")
		return res, nil
	}

	res, err = c.next.RoundTrip(req)
	if err != nil {
		return res, err
	}

	// Store the result in the cache.
	if isCardinalityQueryResponseCacheable(res) {
		cachedRes, err := EncodeCachedHTTPResponse(cacheKey, res)
		if err != nil {
			level.Warn(spanLog).Log("msg", "failed to read cardinality query response before storing it to cache", "err", err)
			return res, err
		}

		c.storeCachedResponse(cachedRes, hashedCacheKey, cacheTTL)
	}

	return res, nil
}

func (c *cardinalityQueryCache) fetchCachedResponse(ctx context.Context, cacheKey, hashedCacheKey string) *http.Response {
	// Look up the cache.
	cacheHits := c.cache.Fetch(ctx, []string{hashedCacheKey})
	if cacheHits[hashedCacheKey] == nil {
		// Not found in the cache.
		return nil
	}

	// Decode the cached entry.
	cachedRes := &CachedHTTPResponse{}
	if err := cachedRes.Unmarshal(cacheHits[hashedCacheKey]); err != nil {
		level.Warn(c.logger).Log("msg", "failed to decode cached cardinality query response", "cache_key", hashedCacheKey, "err", err)
		return nil
	}

	// Ensure no cache key collision.
	if cachedRes.GetCacheKey() != cacheKey {
		level.Warn(c.logger).Log("msg", "skipped cached cardinality query response because a cache key collision has been found", "cache_key", hashedCacheKey)
		return nil
	}

	return DecodeCachedHTTPResponse(cachedRes)
}

func (c *cardinalityQueryCache) storeCachedResponse(cachedRes *CachedHTTPResponse, hashedCacheKey string, cacheTTL time.Duration) {
	// Encode the cached entry.
	encoded, err := cachedRes.Marshal()
	if err != nil {
		level.Warn(c.logger).Log("msg", "failed to encode cached cardinality query response", "err", err)
		return
	}

	c.cache.StoreAsync(map[string][]byte{hashedCacheKey: encoded}, cacheTTL)
}

func parseCardinalityQueryRequest(req *http.Request) (cardinalityQueryRequest, error) {
	switch {
	case strings.HasSuffix(req.URL.Path, cardinalityLabelNamesPathSuffix):
		return cardinality.DecodeLabelNamesRequest(req)
	case strings.HasSuffix(req.URL.Path, cardinalityLabelValuesPathSuffix):
		return cardinality.DecodeLabelValuesRequest(req)
	default:
		return nil, errors.New("unknown cardinality API endpoint")
	}
}

func generateCardinalityQueryRequestCacheKey(tenantIDs []string, req cardinalityQueryRequest) (cacheKey, hashedCacheKey string) {
	var prefix string

	// Get the cache key prefix.
	switch req.RequestType() {
	case cardinality.RequestTypeLabelNames:
		prefix = cardinalityLabelNamesQueryCachePrefix
	case cardinality.RequestTypeLabelValues:
		prefix = cardinalityLabelValuesQueryCachePrefix
	}

	cacheKey = fmt.Sprintf("%s:%s", tenant.JoinTenantIDs(tenantIDs), req.String())
	hashedCacheKey = fmt.Sprintf("%s%s", prefix, cacheHashKey(cacheKey))
	return
}

func isCardinalityQueryResponseCacheable(res *http.Response) bool {
	return res.StatusCode >= 200 && res.StatusCode < 300
}
