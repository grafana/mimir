// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type IntermediateResultsCache interface {
	NewReadEntry(ctx context.Context, function, selector string, start int64, end int64) (ReadEntry, bool, error)
	NewWriteEntry(ctx context.Context, function, selector string, start int64, end int64) (WriteEntry, error)
}

type CacheBackend interface {
	GetMulti(ctx context.Context, keys []string, opts ...cache.Option) map[string][]byte
	SetMultiAsync(data map[string][]byte, ttl time.Duration)
}

type ReadEntry interface {
	ReadSeriesMetadata(memoryTracker *limiter.MemoryConsumptionTracker) ([]types.SeriesMetadata, error)
	// ResultsReader returns a reader for all intermediate results.
	// The function is responsible for reading and decoding all results from this reader.
	ResultsReader() io.Reader
}

type WriteEntry interface {
	WriteSeriesMetadata([]types.SeriesMetadata) error
	// ResultsWriter returns a writer for all intermediate results.
	// The function is responsible for encoding and writing all results to this writer.
	// Must call Finalize() after writing all results.
	ResultsWriter() io.Writer
	Finalize() error
}

func NewResultsCache(cfg Config, logger log.Logger, reg prometheus.Registerer) (IntermediateResultsCache, error) {
	client, err := cache.CreateClient("intermediate-result-cache", cfg.BackendConfig, logger, prometheus.WrapRegistererWithPrefix("mimir_", reg))
	if err != nil {
		return nil, err
	} else if client == nil {
		return nil, errUnsupportedResultsCacheBackend(cfg.Backend)
	}

	c := cache.NewVersioned(
		cache.NewSpanlessTracingCache(client, logger, tenant.NewMultiResolver()),
		resultsCacheVersion,
	)

	logger.Log("msg", "intermediate results cache initialized", "backend", cfg.Backend)
	return NewIntermediateResultsCacheWithBackend(c, logger), nil
}

// NewIntermediateResultsCacheWithBackend creates an IntermediateResultsCache with a specific cache backend.
// This is useful for testing with mock cache backends.
func NewIntermediateResultsCacheWithBackend(cacheBackend CacheBackend, logger log.Logger) IntermediateResultsCache {
	return &intermediateResultsCacheImpl{cache: cacheBackend, logger: logger}
}

type intermediateResultsCacheImpl struct {
	cache  CacheBackend
	logger log.Logger
}

type resultsCacheMetrics struct {
	cacheRequests prometheus.Counter
	cacheHits     prometheus.Counter
}

// TODO: use this
func newResultsCacheMetrics(requestType string, reg prometheus.Registerer) *resultsCacheMetrics {
	return &resultsCacheMetrics{
		cacheRequests: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "mimir_query_engine_intermediate_result_cache_requests_total",
			Help:        "Total number of requests (or partial requests) looked up in the results cache.",
			ConstLabels: map[string]string{"request_type": requestType},
		}),
		cacheHits: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "mimir_query_engine_intermediate_result_cache_hits_total",
			Help:        "Total number of requests (or partial requests) fetched from the results cache.",
			ConstLabels: map[string]string{"request_type": requestType},
		}),
	}
}

func (ic *intermediateResultsCacheImpl) NewReadEntry(ctx context.Context, function, selector string, start int64, end int64) (ReadEntry, bool, error) {
	tenant, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, false, err
	}

	cacheKey := generateCacheKey(tenant, function, selector, start, end)
	hashedKey := cacheHashKey(cacheKey)

	found := ic.cache.GetMulti(ctx, []string{hashedKey})
	data, ok := found[hashedKey]
	if !ok || len(data) == 0 {
		return nil, false, nil
	}

	var cached CachedSeries
	if err := cached.Unmarshal(data); err != nil {
		return nil, false, nil
	}

	if cached.CacheKey != cacheKey {
		return nil, false, nil
	}

	ic.logger.Log("msg", "split function cache hit", "tenant", tenant, "function", function, "selector", selector, "start", start, "end", end)

	return newBufferedCacheReadEntry(cached), true, nil
}

func (ic *intermediateResultsCacheImpl) NewWriteEntry(ctx context.Context, function, selector string, start int64, end int64) (WriteEntry, error) {
	tenant, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}
	cacheKey := generateCacheKey(tenant, function, selector, start, end)
	return newBufferedWriteEntry(ic.cache, cacheKey, start, end, ic.logger), nil
}

// bufferedReadEntry buffers the entire cache entry in memory.
type bufferedReadEntry struct {
	cached       CachedSeries
	metadataRead bool
}

func newBufferedCacheReadEntry(cached CachedSeries) ReadEntry {
	return &bufferedReadEntry{cached: cached}
}

func (e *bufferedReadEntry) ReadSeriesMetadata(memoryTracker *limiter.MemoryConsumptionTracker) ([]types.SeriesMetadata, error) {
	if e.metadataRead {
		return nil, errors.New("metadata already read")
	}
	e.metadataRead = true

	series, err := types.SeriesMetadataSlicePool.Get(len(e.cached.Series), memoryTracker)
	if err != nil {
		return nil, err
	}

	for _, m := range e.cached.Series {
		lbls := mimirpb.FromLabelAdaptersToLabels(m.Labels)

		if err = memoryTracker.IncreaseMemoryConsumptionForLabels(lbls); err != nil {
			return nil, err
		}

		series = append(series, types.SeriesMetadata{
			Labels: lbls,
		})
	}

	return series, nil
}

// TODO: only allow to read once
func (e *bufferedReadEntry) ResultsReader() io.Reader {
	return bytes.NewReader(e.cached.Results)
}

// bufferedWriteEntry buffers all writes in memory before flushing to cache.
type bufferedWriteEntry struct {
	cache         CacheBackend
	cached        CachedSeries
	resultsBuffer *bytes.Buffer
	finalized     bool

	logger log.Logger
}

func newBufferedWriteEntry(cache CacheBackend, cacheKey string, start, end int64, logger log.Logger) WriteEntry {
	return &bufferedWriteEntry{
		cache: cache,
		cached: CachedSeries{
			CacheKey: cacheKey,
			Version:  int64(resultsCacheVersion),
			Start:    start,
			End:      end,
		},
		resultsBuffer: &bytes.Buffer{},
		logger:        logger,
	}
}

func (e *bufferedWriteEntry) WriteSeriesMetadata(metadata []types.SeriesMetadata) error {
	e.cached.Series = make([]mimirpb.Metric, len(metadata))
	for i, sm := range metadata {
		e.cached.Series[i] = mimirpb.Metric{
			Labels: mimirpb.FromLabelsToLabelAdapters(sm.Labels),
		}
	}
	return nil
}

func (e *bufferedWriteEntry) ResultsWriter() io.Writer {
	return e.resultsBuffer
}

func (e *bufferedWriteEntry) Finalize() error {
	if e.finalized {
		return nil
	}

	e.cached.Results = e.resultsBuffer.Bytes()

	data, err := e.cached.Marshal()
	if err != nil {
		return fmt.Errorf("marshalling split function cached series, err: %w", err)
	}

	hashedKey := cacheHashKey(e.cached.CacheKey)
	e.cache.SetMultiAsync(map[string][]byte{hashedKey: data}, defaultTTL)

	level.Debug(e.logger).Log("msg", "split function cache set", "cache_key", e.cached.CacheKey, "series_count", len(e.cached.Series))

	e.finalized = true
	return nil
}

// generateCacheKey generates a cache key from the given parameters.
func generateCacheKey(tenant, function, selector string, start, end int64) string {
	return fmt.Sprintf("%s:%s:%s:%d:%d", tenant, function, selector, start, end)
}

// cacheHashKey is needed due to memcached key limit
func cacheHashKey(key string) string {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(key))
	return hex.EncodeToString(hasher.Sum(nil))
}
