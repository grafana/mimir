// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"context"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/user"
	"github.com/grafana/mimir/pkg/mimirpb"
)

type Backend interface {
	GetMulti(ctx context.Context, keys []string, opts ...cache.Option) map[string][]byte
	SetMultiAsync(data map[string][]byte, ttl time.Duration)
}

type CacheFactory struct {
	backend Backend
	metrics *cacheMetrics
	logger  log.Logger
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

func NewCacheFactory(cfg Config, logger log.Logger, reg prometheus.Registerer) (*CacheFactory, error) {
	client, err := cache.CreateClient("intermediate-result-cache", cfg.BackendConfig, logger, prometheus.WrapRegistererWithPrefix("mimir_", reg))
	if err != nil {
		return nil, err
	} else if client == nil {
		return nil, errUnsupportedResultsCacheBackend(cfg.Backend)
	}

	backend := cache.NewVersioned(
		cache.NewSpanlessTracingCache(client, logger, tenant.NewMultiResolver()),
		resultsCacheVersion,
		logger,
	)

	logger.Log("msg", "intermediate results cache initialized", "backend", cfg.Backend)
	return NewCacheFactoryWithBackend(backend, reg, logger), nil
}

func NewCacheFactoryWithBackend(backend Backend, reg prometheus.Registerer, logger log.Logger) *CacheFactory {
	return &CacheFactory{
		backend: backend,
		metrics: newCacheMetrics(reg),
		logger:  logger,
	}
}

func generateCacheKey(tenant string, function int32, selector string, start, end int64) string {
	return fmt.Sprintf("%s:%d:%s:%d:%d", tenant, function, selector, start, end)
}

// cacheHashKey is needed due to memcached key limit
func cacheHashKey(key string) string {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(key))
	return hex.EncodeToString(hasher.Sum(nil))
}

// SplitCodec handles serialization of intermediate results for query splitting.
type SplitCodec[T any] interface {
	// Marshal serializes a slice of intermediate results to bytes.
	Marshal(results []T) ([]byte, error)

	// Unmarshal deserializes bytes back to a slice of intermediate results.
	Unmarshal(data []byte) ([]T, error)
}

type Cache[T any] struct {
	backend Backend
	metrics *cacheMetrics
	logger  log.Logger
	codec   SplitCodec[T]
}

func NewCache[T any](factory *CacheFactory, codec SplitCodec[T]) *Cache[T] {
	return &Cache[T]{
		backend: factory.backend,
		metrics: factory.metrics,
		logger:  factory.logger,
		codec:   codec,
	}
}

func (c *Cache[T]) Get(
	ctx context.Context,
	function int32,
	innerKey string,
	start, end int64,
	stats *CacheStats,
) (seriesProtos []mimirpb.Metric, annotations []Annotation, results []T, found bool, err error) {
	tenant, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, nil, nil, false, err
	}

	c.metrics.cacheRequests.Inc()
	cacheKey := generateCacheKey(tenant, function, innerKey, start, end)
	hashedKey := cacheHashKey(cacheKey)

	foundData := c.backend.GetMulti(ctx, []string{hashedKey})
	data, ok := foundData[hashedKey]
	if !ok || len(data) == 0 {
		return nil, nil, nil, false, nil
	}

	var cached CachedSeries
	if err := cached.Unmarshal(data); err != nil {
		level.Warn(c.logger).Log("msg", "failed to decode cached result", "hashed_cache_key", hashedKey, "cache_key", cacheKey, "err", err)
		return nil, nil, nil, false, nil
	}

	if cached.CacheKey != cacheKey {
		level.Warn(c.logger).Log("msg", "skipped cached result because a cache key collision has been found", "hashed_cache_key", hashedKey, "cache_key", cacheKey)
		return nil, nil, nil, false, nil
	}

	var cachedSeriesMetadata CachedSeriesMetadata
	if err := cachedSeriesMetadata.Unmarshal(cached.SeriesMetadata); err != nil {
		level.Warn(c.logger).Log("msg", "failed to decode cached series metadata", "hashed_cache_key", hashedKey, "cache_key", cacheKey, "err", err)
		return nil, nil, nil, false, nil
	}

	c.metrics.cacheHits.Inc()
	level.Debug(c.logger).Log("msg", "cache hit", "tenant", tenant, "function", function, "innerKey", innerKey, "start", start, "end", end)

	stats.AddReadEntryStat(len(cachedSeriesMetadata.Series), len(data))

	results, err = c.codec.Unmarshal(cached.Results)
	if err != nil {
		return nil, nil, nil, false, fmt.Errorf("unmarshaling cached results: %w", err)
	}

	return cachedSeriesMetadata.Series, cached.Annotations, results, true, nil
}

func (c *Cache[T]) Set(
	ctx context.Context,
	function int32,
	innerKey string,
	start, end int64,
	serializedSeries []byte,
	seriesCount int,
	annotations []Annotation,
	results []T,
	stats *CacheStats,
) error {
	tenant, err := user.ExtractOrgID(ctx)
	if err != nil {
		return err
	}

	cacheKey := generateCacheKey(tenant, function, innerKey, start, end)

	resultBytes, err := c.codec.Marshal(results)
	if err != nil {
		return fmt.Errorf("marshaling results: %w", err)
	}

	cached := &CachedSeries{
		CacheKey:         cacheKey,
		Start:            start,
		End:              end,
		SeriesMetadata: serializedSeries,
		Annotations:      annotations,
		Results:          resultBytes,
	}

	data, err := cached.Marshal()
	if err != nil {
		return fmt.Errorf("marshalling cached series: %w", err)
	}

	hashedKey := cacheHashKey(cacheKey)
	c.backend.SetMultiAsync(map[string][]byte{hashedKey: data}, defaultTTL)

	level.Debug(c.logger).Log("msg", "cache entry written", "cache_key", cacheKey, "series_count", seriesCount, "entry_size", len(data))

	stats.AddWriteEntryStat(seriesCount, len(data))

	return nil
}
