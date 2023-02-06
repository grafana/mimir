// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	// cardinalityEstimateTTL is how long a key must see no write to expire and be removed from the cache.
	cardinalityEstimateTTL = 7 * 24 * time.Hour

	// cardinalityEstimateBucketSize is the size of buckets that queries are bucketed into.
	cardinalityEstimateBucketSize = 24 * time.Hour

	// cacheErrorToleranceFraction is how much the estimate must deviate
	// from the actually observed cardinality to update the cache.
	cacheErrorToleranceFraction = 0.1
)

type cardinalityEstimationMetrics struct {
	estimationError prometheus.Histogram
}

// cardinalityEstimation is a Handler that caches estimates for a query's
// cardinality based on similar queries seen previously.
type cardinalityEstimation struct {
	cache  cache.Cache
	next   Handler
	logger log.Logger

	cardinalityEstimationMetrics
}

func newCardinalityEstimationMiddleware(cache cache.Cache, logger log.Logger, registerer prometheus.Registerer) Middleware {
	metrics := cardinalityEstimationMetrics{estimationError: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
		Name:    "cortex_frontend_cardinality_estimation_error",
		Help:    "Difference between estimated and actual query cardinality",
		Buckets: prometheus.ExponentialBuckets(10, 10, 6),
	})}
	return MiddlewareFunc(func(next Handler) Handler {
		return &cardinalityEstimation{
			cache:  cache,
			next:   next,
			logger: logger,

			cardinalityEstimationMetrics: metrics,
		}
	})
}

// Do injects a cardinality estimate into the query hints (if available) and
// caches the actual cardinality observed for this query.
func (c *cardinalityEstimation) Do(ctx context.Context, request Request) (Response, error) {
	spanLog, ctx := spanlogger.NewWithLogger(ctx, c.logger, "cardinalityEstimation.Do")
	defer spanLog.Finish()

	tenants, err := tenant.TenantIDs(ctx)
	if err != nil {
		return c.next.Do(ctx, request)
	}

	k := generateCardinalityEstimationCacheKey(tenant.JoinTenantIDs(tenants), request, cardinalityEstimateBucketSize)

	var estimatedCardinality uint64
	if estimate, ok := c.lookupCardinalityForKey(ctx, k); ok {
		estimatedCardinality = estimate
		request = request.WithEstimatedCardinalityHint(estimate)
	}

	res, err := c.next.Do(ctx, request)
	if err != nil {
		return nil, err
	}

	statistics := stats.FromContext(ctx)
	if statistics == nil {
		return res, nil
	}

	actualCardinality := statistics.GetFetchedSeriesCount()
	if !isCardinalitySimilar(actualCardinality, estimatedCardinality) {
		c.storeCardinalityForKey(ctx, k, actualCardinality)
	}

	if estimatedCardinality > 0 {
		c.estimationError.Observe(math.Abs(float64(actualCardinality) - float64(estimatedCardinality)))
		statistics.FetchedSeriesEstimate = estimatedCardinality
		spanLog.LogKV("estimated cardinality", estimatedCardinality, "actual cardinality", actualCardinality)
	}

	return res, nil
}

// lookupCardinalityForKey fetches a cardinality estimate for the given key from
// the results cache.
func (c *cardinalityEstimation) lookupCardinalityForKey(ctx context.Context, key string) (uint64, bool) {
	if c.cache == nil {
		return 0, false
	}
	res := c.cache.Fetch(ctx, []string{key})
	if val, ok := res[key]; ok {
		cardinality := &QueryCardinality{}
		err := proto.Unmarshal(val, cardinality)
		if err != nil {
			level.Warn(c.logger).Log("msg", "failed to unmarshal cardinality estimate")
			return 0, false
		}
		return cardinality.Estimated, ok
	}
	return 0, false
}

// storeCardinalityForKey stores a cardinality estimate for the given key in the
// results cache.
func (c *cardinalityEstimation) storeCardinalityForKey(ctx context.Context, key string, count uint64) {
	if c.cache == nil {
		return
	}
	m := &QueryCardinality{Estimated: count}
	marshaled, err := proto.Marshal(m)
	if err != nil {
		level.Warn(c.logger).Log("msg", "failed to marshal cardinality estimate")
	}
	// The store is executed asynchronously, potential errors are logged and not
	// propagated back up the stack.
	c.cache.Store(ctx, map[string][]byte{key: marshaled}, cardinalityEstimateTTL)
}

func isCardinalitySimilar(actualCardinality, estimatedCardinality uint64) bool {
	estimate := float64(estimatedCardinality)
	actual := float64(actualCardinality)
	return estimate > (1-cacheErrorToleranceFraction)*actual && estimate < (1+cacheErrorToleranceFraction)*actual
}

// generateCardinalityEstimationCacheKey generates a key to cache a request's cardinality estimate under.
func generateCardinalityEstimationCacheKey(userID string, r Request, bucketSize time.Duration) string {
	startBucket := r.GetStart() / bucketSize.Milliseconds()
	rangeBucket := (r.GetEnd() - r.GetStart()) / bucketSize.Milliseconds()

	// Prefix key with `QS` (short for "query statistics").
	return fmt.Sprintf("QS:%s:%s:%d:%d", userID, cacheHashKey(r.GetQuery()), startBucket, rangeBucket)
}
