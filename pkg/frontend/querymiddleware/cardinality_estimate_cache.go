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
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/splitandcache"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	cardinalityEstimateQueryCachePrefix = "ce:"

	// maxCardinalityEstimateCachedBytes is the largest response body cached, larger ones pass through.
	maxCardinalityEstimateCachedBytes = 1 << 20

	queryTypeCardinalityEstimate = "cardinality_estimate"
)

// cardinalityEstimateCacheKeyParams are the request parameters that go into the cache key, in order.
var cardinalityEstimateCacheKeyParams = querier.CardinalityEstimateRequestParams

// CardinalityEstimate returns the cache key for a cardinality estimate request.
// match[] is the only list parameter, so its values are sorted; the key takes the first value of the others.
func (DefaultCacheKeyGenerator) CardinalityEstimate(r *http.Request) (*GenericQueryCacheKey, error) {
	values, err := util.ParseRequestFormWithoutConsumingBody(r)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}
	return &GenericQueryCacheKey{CacheKey: cardinalityEstimateCacheKey(values), CacheKeyPrefix: cardinalityEstimateQueryCachePrefix}, nil
}

func cardinalityEstimateCacheKey(values url.Values) string {
	var b strings.Builder
	for _, name := range cardinalityEstimateCacheKeyParams {
		b.WriteString(name)
		b.WriteByte('=')
		if name == "match[]" {
			vs := slices.Clone(values[name])
			slices.Sort(vs)
			b.WriteString(strings.Join(vs, "\x00"))
		} else {
			b.WriteString(values.Get(name))
		}
		b.WriteByte('\x01')
	}
	return b.String()
}

// cardinalityEstimateCache caches the final cardinality estimate response.
// The TTL follows the window's end as in the query results cache. A response
// marked Cache-Control: no-store is never kept.
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

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}
	// The querier refuses a federated request before anything else.
	if len(tenantIDs) != 1 {
		return c.generic.next.RoundTrip(req)
	}
	// Checked before the cache, so a disabled tenant gets no cached answer.
	if !c.limits.CardinalityEstimateEnabled(tenantIDs[0]) {
		return cardinalityEstimateDisabledResponse(req, tenantIDs[0]), nil
	}
	if requestoptions.DecodeCacheDisabledOption(req) {
		return c.generic.next.RoundTrip(req)
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
	// Read one byte past the limit to tell whether the response is too large.
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

// ttl is how long to keep the response to req, 0 when its window ends too recently.
func (c *cardinalityEstimateCache) ttl(req *http.Request, tenantIDs []string) (time.Duration, error) {
	values, err := util.ParseRequestFormWithoutConsumingBody(req)
	if err != nil {
		return 0, err
	}
	// Seconds or RFC 3339, as in the query API.
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

// cardinalityEstimateDisabledResponse mirrors the querier's answer for a disabled tenant.
func cardinalityEstimateDisabledResponse(req *http.Request, tenantID string) *http.Response {
	body := fmt.Sprintf("cardinality estimate is disabled for the tenant: %v\n", tenantID)
	return &http.Response{
		StatusCode:    http.StatusBadRequest,
		Status:        fmt.Sprintf("%d %s", http.StatusBadRequest, http.StatusText(http.StatusBadRequest)),
		Header:        http.Header{"Content-Type": []string{"text/plain; charset=utf-8"}, "X-Content-Type-Options": []string{"nosniff"}},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}
