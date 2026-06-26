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
	backend         caching.Backend
	ttlProvider     TTLProvider
	prefixGenerator caching.PrefixGenerator
	metrics         *cacheMetrics
	logger          log.Logger
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

	var backend cache.Cache = cache.NewVersioned(
		cache.NewSpanlessTracingCache(client, logger, tenant.NewMultiResolver()),
		resultsCacheVersion,
		logger,
	)
	backend = cache.NewCompression(cfg.Compression, backend, logger)

	return NewCacheFactoryWithBackend(caching.NewAdaptor(backend), ttlProvider, prefixGenerator, reg, logger), nil
}

func NewCacheFactoryWithBackend(backend caching.Backend, ttlProvider TTLProvider, prefixGenerator caching.PrefixGenerator, reg prometheus.Registerer, logger log.Logger) *CacheFactory {
	return &CacheFactory{
		backend:         backend,
		ttlProvider:     ttlProvider,
		prefixGenerator: prefixGenerator,
		metrics:         newCacheMetrics(reg),
		logger:          logger,
	}
}

func generateCacheKey(function functions.Function, selector []byte, start, end int64) []byte {
	// We don't include the tenant ID here: the cache prefix (which carries the tenant IDs) is prepended in cacheKeys before hashing.
	return fmt.Appendf(nil, "%d:%s:%d:%d", function, selector, start, end)
}

// TestGenerateHashedCacheKey generates a hashed cache key using the same logic as the cache internals
// for a cache configured with no prefix generator. This should only be used in tests.
func TestGenerateHashedCacheKey(function functions.Function, selector []byte, start, end int64) string {
	return caching.HashCacheKey(generateCacheKey(function, selector, start, end))
}

// SplitCodec handles serialization of intermediate results for query splitting.
type SplitCodec[T any] interface {
	// Marshal serializes a slice of intermediate results to bytes.
	Marshal(results []T) ([]byte, error)

	// Unmarshal deserializes bytes back to a slice of intermediate results.
	Unmarshal(data []byte) ([]T, error)
}

type Cache[T any] struct {
	backend         caching.Backend
	ttlProvider     TTLProvider
	prefixGenerator caching.PrefixGenerator
	metrics         *cacheMetrics
	logger          log.Logger
	codec           SplitCodec[T]
}

func NewCache[T any](factory *CacheFactory, codec SplitCodec[T]) *Cache[T] {
	return &Cache[T]{
		backend:         factory.backend,
		ttlProvider:     factory.ttlProvider,
		prefixGenerator: factory.prefixGenerator,
		metrics:         factory.metrics,
		logger:          factory.logger,
		codec:           codec,
	}
}

// cacheKeys generates the cache keys for the given inputs.
// When a prefixGenerator is configured, the full key includes its prefix (the tenant IDs).
// Both the full key and the hashed (backend) key are returned:
//   - the full key should be kept and stored in the cache entry for collision verification;
//   - the hashed key should be used for backend interactions, as caching.HashCacheKey() ensures
//     it stays within the cache's key-size limit.
func (c *Cache[T]) cacheKeys(ctx context.Context, function functions.Function, innerKey []byte, start, end int64) (fullKey []byte, hashedKey string, err error) {
	fullKey = generateCacheKey(function, innerKey, start, end)

	if c.prefixGenerator != nil {
		prefix, err := c.prefixGenerator(ctx)
		if err != nil {
			return nil, "", fmt.Errorf("generating cache key prefix: %w", err)
		}

		fullKey = append([]byte(prefix), fullKey...)
	}

	return fullKey, caching.HashCacheKey(fullKey), nil
}

func (c *Cache[T]) Get(
	ctx context.Context,
	function functions.Function,
	innerKey []byte,
	start, end int64,
	stats *CacheStats,
) (seriesMetadata []querierpb.SeriesMetadata, annotations querierpb.Annotations, results []T, operatorStats types.EncodedOperatorEvaluationStats, found bool, err error) {
	tenant, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, querierpb.Annotations{}, nil, types.EncodedOperatorEvaluationStats{}, false, err
	}

	c.metrics.cacheRequests.Inc()
	cacheKey, hashedKey, err := c.cacheKeys(ctx, function, innerKey, start, end)
	if err != nil {
		return nil, querierpb.Annotations{}, nil, types.EncodedOperatorEvaluationStats{}, false, err
	}

	foundData, err := c.backend.GetMulti(ctx, []string{hashedKey})
	if err != nil {
		return nil, querierpb.Annotations{}, nil, types.EncodedOperatorEvaluationStats{}, false, fmt.Errorf("getting cached results: %w", err)
	}

	data, ok := foundData[hashedKey]
	if !ok || len(data) == 0 {
		return nil, querierpb.Annotations{}, nil, types.EncodedOperatorEvaluationStats{}, false, nil
	}

	var cached CachedSeries
	if err := cached.Unmarshal(data); err != nil {
		level.Warn(c.logger).Log("msg", "failed to decode cached result", "hashed_cache_key", hashedKey, "err", err)
		return nil, querierpb.Annotations{}, nil, types.EncodedOperatorEvaluationStats{}, false, nil
	}

	if !bytes.Equal(cached.CacheKey, cacheKey) {
		level.Warn(c.logger).Log("msg", "skipped cached result because a cache key collision has been found", "hashed_cache_key", hashedKey)
		return nil, querierpb.Annotations{}, nil, types.EncodedOperatorEvaluationStats{}, false, nil
	}

	c.metrics.cacheHits.Inc()
	level.Debug(c.logger).Log("msg", "cache hit", "tenant", tenant, "function", function, "hashed_cache_key", hashedKey, "start", start, "end", end)

	stats.AddReadEntryStat(len(cached.SeriesMetadata), len(data))

	results, err = c.codec.Unmarshal(cached.Results)
	if err != nil {
		return nil, querierpb.Annotations{}, nil, types.EncodedOperatorEvaluationStats{}, false, fmt.Errorf("unmarshaling cached results: %w", err)
	}

	return cached.SeriesMetadata, cached.Annotations, results, cached.Stats, true, nil
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
