// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	// cardinalityEstimateTTL is how long a key must see no write to expire and be removed from the cache.
	cardinalityEstimateTTL = 7 * 24 * time.Hour

	// cardinalityEstimateBucketSize is the size of buckets that queries are bucketed into.
	cardinalityEstimateBucketSize = 2 * time.Hour

	// cacheErrorToleranceFraction is how much the estimate must deviate
	// from the actually observed cardinality to update the cache.
	cacheErrorToleranceFraction = 0.1
)

// cardinalityEstimation is a MetricsQueryHandler that caches estimates for a query's
// cardinality based on similar queries seen previously.
type cardinalityEstimation struct {
	cache  cache.Cache
	next   MetricsQueryHandler
	logger log.Logger

	estimationError prometheus.Histogram
}

func newCardinalityEstimationMiddleware(cache cache.Cache, logger log.Logger, registerer prometheus.Registerer) MetricsQueryMiddleware {
	estimationError := promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
		Name:    "cortex_query_frontend_cardinality_estimation_difference",
		Help:    "Difference between estimated and actual query cardinality",
		Buckets: prometheus.ExponentialBuckets(100, 2, 10),
	})
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &cardinalityEstimation{
			cache:  cache,
			next:   next,
			logger: logger,

			estimationError: estimationError,
		}
	})
}

// Do injects a cardinality estimate into the query hints (if available) and
// caches the actual cardinality observed for this query.
func (c *cardinalityEstimation) Do(ctx context.Context, request MetricsQueryRequest) (Response, error) {
	spanLog := spanlogger.FromContext(ctx, c.logger)

	tenants, err := tenant.TenantIDs(ctx)
	if err != nil {
		return c.next.Do(ctx, request)
	}

	k := generateCardinalityEstimationCacheKey(tenant.JoinTenantIDs(tenants), request, cardinalityEstimateBucketSize)
	spanLog.LogFields(otlog.String("cache key", k))

	estimatedCardinality, estimateAvailable := c.lookupCardinalityForKey(ctx, k)
	if estimateAvailable {
		newRequest, err := request.WithEstimatedSeriesCountHint(estimatedCardinality)
		if err != nil {
			return c.next.Do(ctx, request)
		}
		request = newRequest
		spanLog.LogFields(
			otlog.Bool("estimate available", true),
			otlog.Uint64("estimated cardinality", estimatedCardinality),
		)
	} else {
		spanLog.LogFields(otlog.Bool("estimate available", false))
	}

	res, err := c.next.Do(ctx, request)
	if err != nil {
		return nil, err
	}

	statistics := stats.FromContext(ctx)
	actualCardinality := statistics.GetFetchedSeriesCount()
	spanLog.LogFields(otlog.Uint64("actual cardinality", actualCardinality))

	if !estimateAvailable || !isCardinalitySimilar(actualCardinality, estimatedCardinality) {
		c.storeCardinalityForKey(k, actualCardinality)
		spanLog.LogFields(otlog.Bool("cache updated", true))
	}

	if estimateAvailable {
		estimationError := math.Abs(float64(actualCardinality) - float64(estimatedCardinality))
		c.estimationError.Observe(estimationError)
		statistics.AddEstimatedSeriesCount(estimatedCardinality)
		spanLog.LogFields(otlog.Float64("estimation error", estimationError))
	}

	return res, nil
}

// lookupCardinalityForKey fetches a cardinality estimate for the given key from
// the results cache.
func (c *cardinalityEstimation) lookupCardinalityForKey(ctx context.Context, key string) (uint64, bool) {
	if c.cache == nil {
		return 0, false
	}
	res := c.cache.GetMulti(ctx, []string{key})
	if val, ok := res[key]; ok {
		qs := &QueryStatistics{}
		err := proto.Unmarshal(val, qs)
		if err != nil {
			level.Warn(c.logger).Log("msg", "failed to unmarshal cardinality estimate")
			return 0, false
		}
		return qs.EstimatedSeriesCount, true
	}
	return 0, false
}

// storeCardinalityForKey stores a cardinality estimate for the given key in the
// results cache.
func (c *cardinalityEstimation) storeCardinalityForKey(key string, count uint64) {
	if c.cache == nil {
		return
	}
	m := &QueryStatistics{EstimatedSeriesCount: count}
	marshaled, err := proto.Marshal(m)
	if err != nil {
		level.Warn(c.logger).Log("msg", "failed to marshal cardinality estimate")
		return
	}
	// The store is executed asynchronously, potential errors are logged and not
	// propagated back up the stack.
	c.cache.SetMultiAsync(map[string][]byte{key: marshaled}, cardinalityEstimateTTL)
}

func isCardinalitySimilar(actualCardinality, estimatedCardinality uint64) bool {
	if actualCardinality == 0 {
		// We can't do a ratio-based comparison in case the actual cardinality is 0,
		// therefore we fall back to an exactly-equal comparison here.
		return estimatedCardinality == actualCardinality
	}

	estimate := float64(estimatedCardinality)
	actual := float64(actualCardinality)
	return math.Abs(estimate/actual-1) < cacheErrorToleranceFraction
}

// generateCardinalityEstimationCacheKey generates a key to cache a request's
// cardinality estimate under. Queries are assigned to buckets of fixed width
// with respect to both start time and range size. To avoid expiry of all
// estimates at the bucket boundary, an offset is added based on the hash of the
// query string.
func generateCardinalityEstimationCacheKey(userID string, r MetricsQueryRequest, bucketSize time.Duration) string {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(r.GetQuery()))

	// This assumes that `bucketSize` is positive.
	offset := hasher.Sum64() % uint64(bucketSize.Milliseconds())
	startBucket := (r.GetStart() + int64(offset)) / bucketSize.Milliseconds()
	rangeBucket := (r.GetEnd() - r.GetStart()) / bucketSize.Milliseconds()

	// Prefix key with `QS` (short for "query statistics").
	return fmt.Sprintf("QS:%s:%s:%d:%d", userID, cacheHashKey(r.GetQuery()), startBucket, rangeBucket)
}
