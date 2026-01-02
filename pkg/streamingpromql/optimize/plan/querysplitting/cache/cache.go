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
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// Backend is the underlying storage backend (memcached, etc.)
type Backend interface {
	GetMulti(ctx context.Context, keys []string, opts ...cache.Option) map[string][]byte
	SetMultiAsync(data map[string][]byte, ttl time.Duration)
}

type Cache struct {
	backend Backend
	metrics *resultsCacheMetrics
	logger  log.Logger
}

type resultsCacheMetrics struct {
	cacheRequests prometheus.Counter
	cacheHits     prometheus.Counter
}

func newResultsCacheMetrics(reg prometheus.Registerer) *resultsCacheMetrics {
	return &resultsCacheMetrics{
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

func NewResultsCache(cfg Config, logger log.Logger, reg prometheus.Registerer) (*Cache, error) {
	client, err := cache.CreateClient("intermediate-result-cache", cfg.BackendConfig, logger, prometheus.WrapRegistererWithPrefix("mimir_", reg))
	if err != nil {
		return nil, err
	} else if client == nil {
		return nil, errUnsupportedResultsCacheBackend(cfg.Backend)
	}

	backend := cache.NewVersioned(
		cache.NewSpanlessTracingCache(client, logger, tenant.NewMultiResolver()),
		resultsCacheVersion,
	)

	logger.Log("msg", "intermediate results cache initialized", "backend", cfg.Backend)
	return NewResultsCacheWithBackend(backend, reg, logger), nil
}

func NewResultsCacheWithBackend(backend Backend, reg prometheus.Registerer, logger log.Logger) *Cache {
	return &Cache{
		backend: backend,
		metrics: newResultsCacheMetrics(reg),
		logger:  logger,
	}
}

type SplitWriter[T any] interface {
	WriteNextResult(T) error
	Finalize() error
}

type SplitReader[T any] interface {
	ReadResultAt(idx int) (T, error)
}

type SplitCodec[T any] interface {
	// TODO: if results are streamed instead, we should return in a writer that can be updated incrementally.
	NewWriter(setResultBytes func([]byte)) (SplitWriter[T], error)
	// TODO: if results are streamed instead, we should pass in a reader that can be read incrementally
	NewReader(bytes []byte) (SplitReader[T], error)
}

type ReadEntry[T any] interface {
	ReadSeriesMetadata(*limiter.MemoryConsumptionTracker) ([]types.SeriesMetadata, error)
	ReadResultAt(idx int) (T, error)
}

type WriteEntry[T any] interface {
	WriteSeriesMetadata([]types.SeriesMetadata) error
	WriteNextResult(T) error
	Finalize() error
}

func NewReadEntry[T any](
	c *Cache,
	codec SplitCodec[T],
	ctx context.Context,
	function int32,
	innerKey string,
	start, end int64,
	stats *CacheStats,
) (ReadEntry[T], bool, error) {
	tenant, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, false, err
	}

	c.metrics.cacheRequests.Inc()
	cacheKey := generateCacheKey(tenant, function, innerKey, start, end)
	hashedKey := cacheHashKey(cacheKey)

	found := c.backend.GetMulti(ctx, []string{hashedKey})
	data, ok := found[hashedKey]
	if !ok || len(data) == 0 {
		return nil, false, nil
	}

	var cached CachedSeries
	if err := cached.Unmarshal(data); err != nil {
		level.Warn(c.logger).Log("msg", "failed to decode cached result", "hashed_cache_key", hashedKey, "cache_key", cacheKey, "err", err)
		return nil, false, nil
	}

	if cached.CacheKey != cacheKey {
		level.Warn(c.logger).Log("msg", "skipped cached result because a cache key collision has been found", "hashed_cache_key", hashedKey, "cache_key", cacheKey)

		return nil, false, nil
	}

	c.metrics.cacheHits.Inc()
	level.Debug(c.logger).Log("msg", "cache hit", "tenant", tenant, "function", function, "innerKey", innerKey, "start", start, "end", end)

	reader, err := codec.NewReader(cached.Results)
	if err != nil {
		return nil, false, err
	}

	stats.AddReadEntryStat(len(cached.Series), len(data))

	return &bufferedReadEntry[T]{
		cached: cached,
		reader: reader,
	}, true, nil
}

func NewWriteEntry[T any](
	c *Cache,
	codec SplitCodec[T],
	ctx context.Context,
	function int32,
	innerKey string,
	start, end int64,
	stats *CacheStats,
) (WriteEntry[T], error) {
	tenant, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	cacheKey := generateCacheKey(tenant, function, innerKey, start, end)

	entry := &bufferedWriteEntry[T]{
		cache: c.backend,
		cached: CachedSeries{
			CacheKey: cacheKey,
			Start:    start,
			End:      end,
		},
		finalized: false,
		logger:    c.logger,
		stats:     stats,
	}

	writer, err := codec.NewWriter(func(data []byte) {
		entry.cached.Results = data
	})
	if err != nil {
		return nil, err
	}

	entry.writer = writer
	return entry, nil
}

type bufferedReadEntry[T any] struct {
	cached CachedSeries
	reader SplitReader[T]
}

func (e *bufferedReadEntry[T]) ReadResultAt(idx int) (T, error) {
	return e.reader.ReadResultAt(idx)
}

func (e *bufferedReadEntry[T]) ReadSeriesMetadata(memoryTracker *limiter.MemoryConsumptionTracker) ([]types.SeriesMetadata, error) {
	series, err := types.SeriesMetadataSlicePool.Get(len(e.cached.Series), memoryTracker)
	if err != nil {
		return nil, err
	}

	for _, m := range e.cached.Series {
		lbls := mimirpb.FromLabelAdaptersToLabels(m.Labels)
		if err = memoryTracker.IncreaseMemoryConsumptionForLabels(lbls); err != nil {
			return nil, err
		}
		series = append(series, types.SeriesMetadata{Labels: lbls})
	}

	return series, nil
}

type bufferedWriteEntry[T any] struct {
	cache     Backend
	cached    CachedSeries
	writer    SplitWriter[T]
	finalized bool
	stats     *CacheStats
	logger    log.Logger
}

func (e *bufferedWriteEntry[T]) WriteSeriesMetadata(metadata []types.SeriesMetadata) error {
	e.cached.Series = make([]mimirpb.Metric, len(metadata))
	for i, sm := range metadata {
		e.cached.Series[i] = mimirpb.Metric{
			Labels: mimirpb.FromLabelsToLabelAdapters(sm.Labels),
		}
	}
	return nil
}

func (e *bufferedWriteEntry[T]) WriteNextResult(result T) error {
	return e.writer.WriteNextResult(result)
}

func (e *bufferedWriteEntry[T]) Finalize() error {
	if e.finalized {
		return nil
	}

	if err := e.writer.Finalize(); err != nil {
		return err
	}

	data, err := e.cached.Marshal()
	if err != nil {
		return fmt.Errorf("marshalling cached series: %w", err)
	}

	hashedKey := cacheHashKey(e.cached.CacheKey)
	e.cache.SetMultiAsync(map[string][]byte{hashedKey: data}, defaultTTL)

	level.Debug(e.logger).Log("msg", "cache entry written", "cache_key", e.cached.CacheKey, "series_count", len(e.cached.Series), "entry_size", len(data))

	e.stats.AddWriteEntryStat(len(e.cached.Series), len(data))

	e.finalized = true
	return nil
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
