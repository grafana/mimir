// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/querier/stats"
)

const (
	// TODO think about a reasonable value for cardinalityEstimateTTL.
	cardinalityEstimateTTL = 7 * 24 * time.Hour

	// cardinalityEstimateBucketSize is the size of the buckets that two requests
	// must fall into to have the same estimated cardinality.
	cardinalityEstimateBucketSize = 24 * time.Hour
)

// cardinalityEstimation is a Middleware that caches estimates for a query's
// cardinality based on similar queries seen previously.
type cardinalityEstimation struct {
	cache cache.Cache
	next  Handler
}

func newCardinalityEstimationMiddleware(cache cache.Cache) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return &cardinalityEstimation{
			cache: cache,
			next:  next,
		}
	})
}

// Do injects a cardinality estimate into the query hints (if available) and
// caches the actual cardinality observed for this query.
func (c *cardinalityEstimation) Do(ctx context.Context, request Request) (Response, error) {
	tenants, err := tenant.TenantIDs(ctx)
	if err != nil {
		return c.next.Do(ctx, request)
	}

	k := cardinalityEstimateBucket(cardinalityEstimateBucketSize).generateCacheKey(tenant.JoinTenantIDs(tenants), request)

	var estimatedCardinality uint64
	if estimatedCardinality, ok := c.lookupCardinalityForKey(ctx, k); ok {
		request = injectCardinalityEstimate(request, estimatedCardinality)
	}

	res, err := c.next.Do(ctx, request)
	if err != nil {
		return nil, err
	}

	s := stats.FromContext(ctx)
	if s == nil {
		return res, err
	}

	if actualCardinality := s.LoadFetchedSeries(); actualCardinality != estimatedCardinality {
		c.storeCardinalityForKey(ctx, k, actualCardinality)
	}
	return res, err
}

// injectCardinalityEstimate injects a given estimate into the request's hints.
func injectCardinalityEstimate(request Request, estimate uint64) Request {
	if hints := request.GetHints(); hints != nil {
		hints.EstimatedCardinality = estimate
	} else {
		request = request.WithHints(&Hints{EstimatedCardinality: estimate})
	}
	return request
}

// lookupCardinalityForKey fetches a cardinality estimate for the given key from
// the results cache.
func (c *cardinalityEstimation) lookupCardinalityForKey(ctx context.Context, key string) (uint64, bool) {
	if c.cache == nil {
		return 0, false
	}
	cacheKey := cacheHashKey(key)
	res := c.cache.Fetch(ctx, []string{cacheKey})
	if val, ok := res[cacheKey]; ok {
		return unmarshalBinary(val), ok
	}
	return 0, false
}

// storeCardinalityForKey stores a cardinality estimate for the given key in the
// results cache.
func (c *cardinalityEstimation) storeCardinalityForKey(ctx context.Context, key string, count uint64) {
	if c.cache == nil {
		return
	}
	c.cache.Store(ctx, map[string][]byte{cacheHashKey(key): marshalBinary(count)}, cardinalityEstimateTTL)
}

// marshalBinary marshals a cardinality estimate value for storage in the cache.
func marshalBinary(in uint64) []byte {
	marshaled := make([]byte, 8)
	binary.LittleEndian.PutUint64(marshaled, in)
	return marshaled
}

// unmarshalBinary unmarshals the cached representation of a cardinality estimate.
func unmarshalBinary(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

// cardinalityEstimateBucket is a utility to allow splitting cardinality estimate
// keys into buckets of fixed width (time).
type cardinalityEstimateBucket time.Duration

// generateCacheKey generates a key for a request's cardinality estimate.
func (s cardinalityEstimateBucket) generateCacheKey(userID string, r Request) string {
	startInterval := r.GetStart() / time.Duration(s).Milliseconds()
	rangeSizeSeconds := (r.GetEnd() - r.GetStart()) / 1000
	if rangeSizeSeconds == 0 {
		return fmt.Sprintf("%s:%s:%d", userID, r.GetQuery(), startInterval)
	}
	return fmt.Sprintf("%s:%s:%d:%d", userID, r.GetQuery(), startInterval, rangeSizeSeconds)
}
