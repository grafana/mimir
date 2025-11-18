// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"hash/fnv"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/histogram"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

const (
	// resultsCacheVersion should be increased every time the cache format changes.
	resultsCacheVersion = 1

	// defaultIntermediateResultsCacheTTL is the default TTL for intermediate results cache entries.
	defaultIntermediateResultsCacheTTL = 24 * time.Hour
)

// IntermediateResult is dualed with IntermediateResultProto.
type IntermediateResult struct {
	SumOverTime SumOverTimeIntermediate
}

type SumOverTimeIntermediate struct {
	SumF     float64
	SumC     float64 // FIXME figure out how to use this in summation.
	SumH     *histogram.FloatHistogram
	HasFloat bool
}

var (
	supportedResultsCacheBackends = []string{cache.BackendMemcached}

	errUnsupportedBackend = errors.New("unsupported cache backend")
)

// ResultsCacheConfig is the config for the results cache.
type ResultsCacheConfig struct {
	cache.BackendConfig `yaml:",inline"`
	Compression         cache.CompressionConfig `yaml:",inline"`
}

// RegisterFlags registers flags.
func (cfg *ResultsCacheConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix(f, "querier.mimir-query-engine.intermediate-results-cache.")
}

// RegisterFlagsWithPrefix registers flags with the given prefix.
func (cfg *ResultsCacheConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Backend, prefix+"backend", "", fmt.Sprintf("Backend for intermediate results cache, if not empty. Supported values: %s.", strings.Join(supportedResultsCacheBackends, ", ")))
	cfg.Memcached.RegisterFlagsWithPrefix(prefix+"memcached.", f)
	cfg.Compression.RegisterFlagsWithPrefix(f, prefix)
}

func (cfg *ResultsCacheConfig) Validate() error {
	if cfg.Backend != "" && !slices.Contains(supportedResultsCacheBackends, cfg.Backend) {
		return errUnsupportedResultsCacheBackend(cfg.Backend)
	}

	switch cfg.Backend {
	case cache.BackendMemcached:
		if err := cfg.Memcached.Validate(); err != nil {
			return errors.Wrap(err, "querier intermediate results cache")
		}
	}

	if err := cfg.Compression.Validate(); err != nil {
		return errors.Wrap(err, "querier intermediate results cache")
	}
	return nil
}

func errUnsupportedResultsCacheBackend(backend string) error {
	return fmt.Errorf("%w: %q, supported values: %v", errUnsupportedBackend, backend, supportedResultsCacheBackends)
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

func NewResultsCache(cfg ResultsCacheConfig, logger log.Logger, reg prometheus.Registerer) (IntermediateResultsCache, error) {
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
	return &intermediateResultsCache{c: c, logger: logger}, nil
}

type intermediateResultsCache struct {
	c      cache.Cache
	logger log.Logger
}

type IntermediateResultsCache interface {
	Get(ctx context.Context, function, selector string, start int64, end int64) (CacheReadEntry, bool, error) // start is exclusive, end is inclusive
	NewWriteEntry(ctx context.Context, function, selector string, start int64, end int64) (CacheWriteEntry, error)
}

func (ic *intermediateResultsCache) Get(ctx context.Context, function, selector string, start int64, end int64) (CacheReadEntry, bool, error) {
	tenant, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, false, err
	}

	cacheKey := generateCacheKey(tenant, function, selector, start, end)
	hashedKey := cacheHashKey(cacheKey)

	found := ic.c.GetMulti(ctx, []string{hashedKey})
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

	ic.logger.Log("msg", "intermediate results cache hit", "tenant", tenant, "function", function, "selector", selector, "start", start, "end", end)
	return newBufferedCacheReadEntry(cached), true, nil
}

func (ic *intermediateResultsCache) NewWriteEntry(ctx context.Context, function, selector string, start int64, end int64) (CacheWriteEntry, error) {
	tenant, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}
	cacheKey := generateCacheKey(tenant, function, selector, start, end)
	return newBufferedCacheWriteEntry(ic, cacheKey, start, end), nil
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

type CacheReadEntry interface {
	// ReadSeriesMetadata will not reference series metadata after the function returns
	// Should only be called once
	ReadSeriesMetadata(memoryTracker *limiter.MemoryConsumptionTracker) ([]types.SeriesMetadata, error)
	// ReadResultAtIdx will return the result at the supplied idx. Results are ordered in the same way as the series metadata.
	// Each idx should only be read once (to allow for streaming cache implementations).
	ReadResultAtIdx(idx int) (IntermediateResult, error)
}

type CacheWriteEntry interface {
	// WriteSeriesMetadata will not reference series metadata after the function returns
	WriteSeriesMetadata([]types.SeriesMetadata) error
	WriteNextResult(IntermediateResult) error
	Finalize() error // Commits results to cache - only call if successful
}

// bufferedCacheReadEntry buffers the entire cache entry in memory after reading from the cache.
type bufferedCacheReadEntry struct {
	cached CachedSeries

	metadataRead bool
	resultIdx    int
}

func newBufferedCacheReadEntry(cached CachedSeries) CacheReadEntry {
	return &bufferedCacheReadEntry{cached: cached}
}

func (e *bufferedCacheReadEntry) ReadSeriesMetadata(memoryTracker *limiter.MemoryConsumptionTracker) ([]types.SeriesMetadata, error) {
	if e.metadataRead {
		return nil, errors.New("metadata already read")
	}
	e.metadataRead = true

	var series []types.SeriesMetadata
	var err error

	series, err = types.SeriesMetadataSlicePool.Get(len(e.cached.Series), memoryTracker)

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

func (e *bufferedCacheReadEntry) ReadResultAtIdx(idx int) (IntermediateResult, error) {
	if idx >= len(e.cached.Results) {
		return IntermediateResult{}, fmt.Errorf("series index %d out of range (have %d series)", idx, len(e.cached.Results))
	}

	proto := e.cached.Results[idx]
	result := IntermediateResult{
		SumOverTime: SumOverTimeIntermediate{
			SumF:     proto.SumF,
			HasFloat: proto.HasFloat,
			SumC:     proto.SumC,
		},
	}

	return result, nil
}

var _ CacheReadEntry = &bufferedCacheReadEntry{}

// bufferedCacheWriteEntry buffers all writes in memory before flushing to cache on Finalize().
type bufferedCacheWriteEntry struct {
	cache     *intermediateResultsCache
	cached    CachedSeries
	finalized bool
}

func newBufferedCacheWriteEntry(cache *intermediateResultsCache, cacheKey string, start, end int64) CacheWriteEntry {
	return &bufferedCacheWriteEntry{
		cache: cache,
		cached: CachedSeries{
			CacheKey: cacheKey,
			Version:  int64(resultsCacheVersion),
			Start:    start,
			End:      end,
		},
	}
}

func (e *bufferedCacheWriteEntry) WriteSeriesMetadata(metadata []types.SeriesMetadata) error {
	e.cached.Series = make([]mimirpb.Metric, len(metadata))
	for i, sm := range metadata {
		e.cached.Series[i] = mimirpb.Metric{
			Labels: mimirpb.FromLabelsToLabelAdapters(sm.Labels),
		}
	}
	return nil
}

func (e *bufferedCacheWriteEntry) WriteNextResult(result IntermediateResult) error {
	proto := IntermediateResultProto{
		SumF:     result.SumOverTime.SumF,
		HasFloat: result.SumOverTime.HasFloat,
		SumC:     result.SumOverTime.SumC,
	}
	if result.SumOverTime.SumH != nil {
		histProto := mimirpb.FromFloatHistogramToHistogramProto(0, result.SumOverTime.SumH)
		proto.SumH = &histProto
	}
	e.cached.Results = append(e.cached.Results, proto)
	return nil
}

func (e *bufferedCacheWriteEntry) Finalize() error {
	if e.finalized {
		return nil
	}

	data, err := e.cached.Marshal()
	if err != nil {
		return errors.Wrap(err, "marshalling cached series")
	}

	hashedKey := cacheHashKey(e.cached.CacheKey)
	e.cache.c.SetMultiAsync(map[string][]byte{hashedKey: data}, defaultIntermediateResultsCacheTTL)
	e.cache.logger.Log("msg", "intermediate results cache set", "cache_key", e.cached.CacheKey, "series_count", len(e.cached.Series))

	e.finalized = true
	return nil
}

var _ CacheWriteEntry = &bufferedCacheWriteEntry{}
