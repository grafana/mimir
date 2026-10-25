// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/splitandcache"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	cardinalityEstimatePathSuffix       = "/api/v1/cardinality/estimate"
	cardinalityEstimateQueryCachePrefix = "ce:"

	// maxCardinalityEstimateCachedBytes is the largest response body the
	// cache stores. A tenant-wide metric-name table can be hundreds of
	// megabytes; such a response is passed through without being buffered.
	maxCardinalityEstimateCachedBytes = 1 << 20

	// queryTypeCardinalityEstimate labels the cache metrics.
	queryTypeCardinalityEstimate = "cardinality_estimate"
)

// cardinalityEstimateCacheKeyParams are the request parameters that change a
// cardinality estimate's answer, in the order they go into the cache key.
// compare is left out on purpose: such a request is never cached.
var cardinalityEstimateCacheKeyParams = []string{"start", "end", "match[]", "group_by", "step", "window", "limit", "min_count", "max_index_bytes", "sketch_above", "snap", "shards"}

// IsCardinalityEstimateQuery reports whether path is the experimental
// cardinality estimate route.
func IsCardinalityEstimateQuery(path string) bool {
	return strings.HasSuffix(path, cardinalityEstimatePathSuffix)
}

// CardinalityEstimate returns the cache key for a cardinality estimate
// request: every parameter that changes the answer, with match[] sorted. A
// request with compare=true is not cacheable.
func (DefaultCacheKeyGenerator) CardinalityEstimate(r *http.Request) (*GenericQueryCacheKey, error) {
	values, err := util.ParseRequestFormWithoutConsumingBody(r)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}
	if values.Get("compare") == "true" {
		return nil, fmt.Errorf("compare=true: %w", ErrUnsupportedRequest)
	}
	return &GenericQueryCacheKey{CacheKey: cardinalityEstimateCacheKey(values), CacheKeyPrefix: cardinalityEstimateQueryCachePrefix}, nil
}

func cardinalityEstimateCacheKey(values url.Values) string {
	var b strings.Builder
	for _, name := range cardinalityEstimateCacheKeyParams {
		vs := slices.Clone(values[name])
		slices.Sort(vs)
		b.WriteString(name)
		b.WriteByte('=')
		b.WriteString(strings.Join(vs, "\x00"))
		b.WriteByte('\x01')
	}
	return b.String()
}

// cardinalityEstimateCache caches the final cardinality estimate response in
// the query-frontend, so a repeated request reads neither the ingesters nor
// the store-gateways. How long a response is kept follows the window's end,
// the same rule as the query results cache: a window that ends within
// max-cache-freshness is not cached, one that ends inside the out-of-order
// time window is kept for results-cache-ttl-for-out-of-order-time-window, and
// an older one for results-cache-ttl. A response the querier marks with
// Cache-Control: no-store, such as a lower bound, is never kept.
type cardinalityEstimateCache struct {
	generic *genericQueryCache
	keyFn   keyingFunc
	limits  Limits
	now     func() time.Time
}

func newCardinalityEstimateCacheRoundTripper(cache cache.Cache, generator CacheKeyGenerator, limits Limits, next http.RoundTripper, logger log.Logger, reg prometheus.Registerer) http.RoundTripper {
	g := &genericQueryCache{
		cache:    cache,
		cacheKey: generator.CardinalityEstimate,
		next:     next,
		logger:   logger,
		metrics:  splitandcache.NewResultsCacheMetrics(queryTypeCardinalityEstimate, reg),
	}
	return &cardinalityEstimateCache{generic: g, keyFn: generator.CardinalityEstimate, limits: limits, now: time.Now}
}

func (c *cardinalityEstimateCache) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	spanLog, ctx := spanlogger.New(ctx, c.generic.logger, tracer, "cardinalityEstimateCache.RoundTrip")
	defer spanLog.Finish()

	if requestoptions.DecodeCacheDisabledOption(req) {
		return c.generic.next.RoundTrip(req)
	}
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}
	key, err := c.keyFn(req)
	if err != nil {
		if !errors.Is(err, ErrUnsupportedRequest) {
			level.Info(spanLog).Log("msg", "skipped cardinality estimate caching because the request could not be parsed", "err", err)
		}
		return c.generic.next.RoundTrip(req)
	}
	ttl, err := c.ttl(req, tenantIDs)
	if err != nil || ttl <= 0 {
		return c.generic.next.RoundTrip(req)
	}

	c.generic.metrics.CacheRequests.Inc()
	cacheKey, hashedCacheKey := generateGenericQueryRequestCacheKey(tenantIDs, key)
	if res := c.generic.fetchCachedResponse(ctx, cacheKey, hashedCacheKey); res != nil {
		c.generic.metrics.CacheHits.Inc()
		return res, nil
	}

	res, err := c.generic.next.RoundTrip(req)
	if err != nil || !isGenericQueryResponseCacheable(res) || strings.Contains(res.Header.Get("Cache-Control"), "no-store") {
		return res, err
	}
	// Read at most one byte past the limit, so a large response is passed
	// on without being held in memory.
	head, err := io.ReadAll(io.LimitReader(res.Body, maxCardinalityEstimateCachedBytes+1))
	if err != nil {
		return nil, err
	}
	if len(head) > maxCardinalityEstimateCachedBytes {
		res.Body = struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(head), res.Body), res.Body}
		return res, nil
	}
	_ = res.Body.Close()
	res.Body = io.NopCloser(bytes.NewReader(head))
	cachedRes, err := EncodeCachedHTTPResponse(cacheKey, res)
	if err != nil {
		return nil, err
	}
	c.generic.storeCachedResponse(ctx, cachedRes, hashedCacheKey, ttl)
	return res, nil
}

// ttl is how long to keep the response to req: 0 when its window ends too
// recently to cache.
func (c *cardinalityEstimateCache) ttl(req *http.Request, tenantIDs []string) (time.Duration, error) {
	values, err := util.ParseRequestFormWithoutConsumingBody(req)
	if err != nil {
		return 0, err
	}
	// The route takes the same timestamps as the query API: seconds or RFC 3339.
	endMs, err := util.ParseTime(values.Get("end"))
	if err != nil {
		return 0, err
	}
	now := c.now()
	if fresh := validation.MaxDurationPerTenant(tenantIDs, c.limits.MaxCacheFreshness); endMs > now.Add(-fresh).UnixMilli() {
		return 0, nil
	}
	if ooo := validation.MaxDurationPerTenant(tenantIDs, c.limits.OutOfOrderTimeWindow); ooo > 0 && endMs > now.Add(-ooo).UnixMilli() {
		return validation.MinDurationPerTenant(tenantIDs, c.limits.ResultsCacheTTLForOutOfOrderTimeWindow), nil
	}
	return validation.MinDurationPerTenant(tenantIDs, c.limits.ResultsCacheTTL), nil
}
