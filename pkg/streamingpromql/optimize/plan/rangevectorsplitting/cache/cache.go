// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/streamingpromql/caching"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type TTLProvider interface {
	GetMinResultsCacheTTL(ctx context.Context) (time.Duration, error)
}

type CacheFactory struct {
	backend      caching.Backend
	ttlProvider  TTLProvider
	keyGenerator *caching.CacheKeyGenerator
	metrics      *cacheMetrics
	logger       log.Logger
}

type cacheMetrics struct {
	cacheRequests prometheus.Counter
	cacheHits     prometheus.Counter
}

func newCacheMetrics(reg prometheus.Registerer) *cacheMetrics {
	return &cacheMetrics{
		cacheRequests: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mimir_query_engine_intermediate_result_cache_requests_total",
			Help: "Total number of requests (or partial requests) looked up in the results cache.",
		}),
		cacheHits: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mimir_query_engine_intermediate_result_cache_hits_total",
			Help: "Total number of requests (or partial requests) fetched from the results cache.",
		}),
	}
}

func NewCacheFactory(cfg Config, ttlProvider TTLProvider, prefixGenerator caching.PrefixGenerator, logger log.Logger, reg prometheus.Registerer) (*CacheFactory, error) {
	client, err := cache.CreateClient("intermediate-result-cache", cfg.BackendConfig, logger, prometheus.WrapRegistererWithPrefix("mimir_", reg))
	if err != nil {
		return nil, err
	} else if client == nil {
		return nil, errUnsupportedResultsCacheBackend(cfg.Backend)
	}

	var backend cache.Cache = cache.NewSpanlessTracingCache(client, logger, tenant.NewMultiResolver())
	backend = cache.NewCompression(cfg.Compression, backend, logger)
	keyGenerator := caching.NewCacheKeyGenerator(caching.VersioningAndItemTypePrefixGenerator(cacheItemTypePrefix, resultsCacheVersion), prefixGenerator)
	return NewCacheFactoryWithBackend(caching.NewAdaptor(backend), ttlProvider, keyGenerator, reg, logger), nil
}

func NewCacheFactoryWithBackend(backend caching.Backend, ttlProvider TTLProvider, keyGenerator *caching.CacheKeyGenerator, reg prometheus.Registerer, logger log.Logger) *CacheFactory {
	return &CacheFactory{
		backend:      backend,
		ttlProvider:  ttlProvider,
		keyGenerator: keyGenerator,
		metrics:      newCacheMetrics(reg),
		logger:       logger,
	}
}

func generateKeySuffix(function functions.Function, selector []byte, start, end int64) []byte {
	// We don't include the tenant ID here: the cache prefix (which carries the tenant IDs) is prepended in the CacheKeyGenerator before hashing.
	return fmt.Appendf(nil, "%d:%s:%d:%d", function, selector, start, end)
}

// TestGenerateHashedCacheKey generates a hashed cache key using the same logic as the cache internals.
// This should only be used in tests.
func TestGenerateHashedCacheKey(ctx context.Context, cacheKeyGenerator *caching.CacheKeyGenerator, function functions.Function, selector []byte, start, end int64) (string, error) {
	_, hashed, err := cacheKeyGenerator.ComputeCacheKey(ctx, generateKeySuffix(function, selector, start, end))
	if err != nil {
		return "", err
	}
	return hashed, nil
}

// SplitCodec handles serialization of intermediate results for query splitting.
type SplitCodec[T any] interface {
	// Marshal serializes a slice of intermediate results to bytes.
	Marshal(results []T) ([]byte, error)

	// Unmarshal deserializes bytes back to a slice of intermediate results.
	Unmarshal(data []byte) ([]T, error)
}

type Cache[T any] struct {
	backend      caching.Backend
	ttlProvider  TTLProvider
	keyGenerator *caching.CacheKeyGenerator
	metrics      *cacheMetrics
	logger       log.Logger
	codec        SplitCodec[T]
}

func NewCache[T any](factory *CacheFactory, codec SplitCodec[T]) *Cache[T] {
	return &Cache[T]{
		backend:      factory.backend,
		ttlProvider:  factory.ttlProvider,
		keyGenerator: factory.keyGenerator,
		metrics:      factory.metrics,
		logger:       factory.logger,
		codec:        codec,
	}
}

// cacheKeys generates the cache keys for the given inputs.
// The full key includes the prefixGenerator prefix (the tenant IDs).
// Both the full key and the hashed (backend) key are returned:
//   - the full key should be kept and stored in the cache entry for collision verification;
//   - the hashed key should be used for backend interactions, as caching.HashCacheKey() ensures
//     it stays within the cache's key-size limit.
func (c *Cache[T]) cacheKeys(ctx context.Context, function functions.Function, innerKey []byte, start, end int64) (fullKey []byte, hashedKey string, err error) {
	key := generateKeySuffix(function, innerKey, start, end)

	fullKey, hashed, err := c.keyGenerator.ComputeCacheKey(ctx, key)
	if err != nil {
		return nil, "", err
	}

	return fullKey, hashed, nil
}

// GetRange identifies the time range of a single cache entry to look up.
type GetRange struct {
	Start int64
	End   int64
}

// GetResult holds the outcome of the lookup of a single range. When Found is false, data fields are zero.
type GetResult[T any] struct {
	SeriesMetadata []querierpb.SeriesMetadata
	Annotations    querierpb.Annotations
	Results        []T
	Stats          types.EncodedOperatorEvaluationStats
	Start          int64
	End            int64
	Found          bool
}

// GetMulti looks up the cache entries for all the given ranges using a single backend request.
// The returned slice is aligned with ranges. An entry that is missing, fails to decode or
// collides on its hashed key is reported as not found. Only backend or codec errors fail the call.
func (c *Cache[T]) GetMulti(ctx context.Context, function functions.Function, innerKey []byte, ranges []GetRange, stats *CacheStats) ([]GetResult[T], error) {
	if len(ranges) == 0 {
		return nil, nil
	}

	tenantID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	c.metrics.cacheRequests.Add(float64(len(ranges)))

	cacheKeys := make([][]byte, len(ranges))
	hashedKeys := make([]string, len(ranges))
	for i, r := range ranges {
		cacheKeys[i], hashedKeys[i], err = c.cacheKeys(ctx, function, innerKey, r.Start, r.End)
		if err != nil {
			return nil, err
		}
	}

	foundData, err := c.backend.GetMulti(ctx, hashedKeys)
	if err != nil {
		return nil, fmt.Errorf("getting cached results: %w", err)
	}

	results := make([]GetResult[T], len(ranges))

	for i, r := range ranges {
		results[i] = GetResult[T]{Start: r.Start, End: r.End}

		hashedKey := hashedKeys[i]

		data, ok := foundData[hashedKey]
		if !ok || len(data) == 0 {
			continue
		}

		var cached CachedSeries
		if err := cached.Unmarshal(data); err != nil {
			level.Warn(c.logger).Log("msg", "failed to decode cached result", "hashed_cache_key", hashedKey, "err", err)
			continue
		}

		if !bytes.Equal(cached.CacheKey, cacheKeys[i]) {
			level.Warn(c.logger).Log("msg", "skipped cached result because a cache key collision has been found", "hashed_cache_key", hashedKey)
			continue
		}

		decoded, err := c.codec.Unmarshal(cached.Results)
		if err != nil {
			return nil, fmt.Errorf("unmarshaling cached results: %w", err)
		}

		c.metrics.cacheHits.Inc()
		level.Debug(c.logger).Log("msg", "cache hit", "tenant", tenantID, "function", function, "hashed_cache_key", hashedKey, "start", r.Start, "end", r.End)

		stats.AddReadEntryStat(len(cached.SeriesMetadata), len(data))

		results[i] = GetResult[T]{
			SeriesMetadata: cached.SeriesMetadata,
			Annotations:    cached.Annotations,
			Results:        decoded,
			Stats:          cached.Stats,
			Start:          cached.Start,
			End:            cached.End,
			Found:          true,
		}
	}

	return results, nil
}

func (c *Cache[T]) Set(
	ctx context.Context,
	function functions.Function,
	innerKey []byte,
	start, end int64,
	seriesMetadata []querierpb.SeriesMetadata,
	annotations querierpb.Annotations,
	results []T,
	operatorStats types.EncodedOperatorEvaluationStats,
	totalSeriesCount int,
	stats *CacheStats,
) error {
	ttl, err := c.ttlProvider.GetMinResultsCacheTTL(ctx)
	if err != nil {
		return fmt.Errorf("getting results cache TTL: %w", err)
	}

	cacheKey, hashedKey, err := c.cacheKeys(ctx, function, innerKey, start, end)
	if err != nil {
		return err
	}

	resultBytes, err := c.codec.Marshal(results)
	if err != nil {
		return fmt.Errorf("marshaling results: %w", err)
	}

	cached := &CachedSeries{
		CacheKey:       cacheKey,
		Start:          start,
		End:            end,
		SeriesMetadata: seriesMetadata,
		Annotations:    annotations,
		Results:        resultBytes,
		Stats:          operatorStats,
	}

	data, err := cached.Marshal()
	if err != nil {
		return fmt.Errorf("marshalling cached series: %w", err)
	}

	if err := c.backend.SetAsync(ctx, hashedKey, data, ttl); err != nil {
		return fmt.Errorf("storing cached results: %w", err)
	}

	seriesCount := len(seriesMetadata)
	level.Debug(c.logger).Log("msg", "cache entry written", "hashed_cache_key", hashedKey, "series_count", seriesCount, "entry_size", len(data))

	stats.AddWriteEntryStat(seriesCount, totalSeriesCount, len(data))

	return nil
}
