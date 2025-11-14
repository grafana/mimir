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

type IntermediateResultBlock struct {
	Version          int
	StartTimestampMs int // Exclusive start
	EndTimestampMs   int // Inclusive end
	Series           []types.SeriesMetadata
	Results          []IntermediateResult // Per-series, matching indexes of Series.
}

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
	Get(ctx context.Context, function, selector string, start int64, end int64, memoryTracker *limiter.MemoryConsumptionTracker) (IntermediateResultBlock, bool, error) // start is exclusive, end is inclusive
	// Set writes the block into the cache.
	// The cache MUST NOT reference the block after Set() returned, as the caller may release slices within the block
	// back to pools to be reused.
	Set(ctx context.Context, function, selector string, start int64, end int64, block IntermediateResultBlock) error // start is exclusive, end is inclusive
}

func (ic *intermediateResultsCache) Get(ctx context.Context, function, selector string, start int64, end int64, memoryTracker *limiter.MemoryConsumptionTracker) (IntermediateResultBlock, bool, error) {
	tenant, err := user.ExtractOrgID(ctx)
	if err != nil {
		return IntermediateResultBlock{}, false, err
	}

	cacheKey := generateCacheKey(tenant, function, selector, start, end)
	hashedKey := cacheHashKey(cacheKey)

	found := ic.c.GetMulti(ctx, []string{hashedKey})
	data, ok := found[hashedKey]
	if !ok || len(data) == 0 {
		return IntermediateResultBlock{}, false, nil
	}

	var cached CachedSeries
	if err := cached.Unmarshal(data); err != nil {
		return IntermediateResultBlock{}, false, nil
	}

	if cached.CacheKey != cacheKey {
		return IntermediateResultBlock{}, false, nil
	}

	block, err := CachedSeriesToBlock(cached, memoryTracker)
	if err != nil {
		return IntermediateResultBlock{}, false, nil
	}

	ic.logger.Log("msg", "intermediate results cache hit", "tenant", tenant, "function", function, "selector", selector, "start", start, "end", end)
	return block, true, nil
}

func (ic *intermediateResultsCache) Set(ctx context.Context, function, selector string, start int64, end int64, block IntermediateResultBlock) error {
	block.Version = resultsCacheVersion

	tenant, err := user.ExtractOrgID(ctx)
	if err != nil {
		return err
	}

	cacheKey := generateCacheKey(tenant, function, selector, start, end)
	// TODO: track proto memory usage?
	cached, err := BlockToCachedSeries(cacheKey, block)
	if err != nil {
		return errors.Wrap(err, "converting block to cached series")
	}

	data, err := cached.Marshal()
	if err != nil {
		return errors.Wrap(err, "marshalling cached series")
	}

	hashedKey := cacheHashKey(cacheKey)
	ic.c.SetMultiAsync(map[string][]byte{hashedKey: data}, defaultIntermediateResultsCacheTTL)
	ic.logger.Log("msg", "intermediate results cache set", "tenant", tenant, "function", function, "selector", selector, "start", start, "end", end, "series_count", len(block.Series))
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

func BlockToCachedSeries(cacheKey string, block IntermediateResultBlock) (CachedSeries, error) {
	series := make([]mimirpb.Metric, len(block.Series))
	for i, sm := range block.Series {
		series[i] = mimirpb.Metric{
			Labels: mimirpb.FromLabelsToLabelAdapters(sm.Labels),
		}
	}

	results := make([]IntermediateResultProto, len(block.Results))
	for i, result := range block.Results {
		proto := IntermediateResultProto{
			SumF:     result.SumOverTime.SumF,
			HasFloat: result.SumOverTime.HasFloat,
			SumC:     result.SumOverTime.SumC,
		}
		if result.SumOverTime.SumH != nil {
			// Convert histogram.FloatHistogram to mimirpb.Histogram
			histProto := mimirpb.FromFloatHistogramToHistogramProto(0, result.SumOverTime.SumH)
			proto.SumH = &histProto
		}
		results[i] = proto
	}

	return CachedSeries{
		CacheKey: cacheKey,
		Version:  int64(block.Version),
		Start:    int64(block.StartTimestampMs),
		End:      int64(block.EndTimestampMs),
		Series:   series,
		Results:  results,
	}, nil
}

func CachedSeriesToBlock(cached CachedSeries, memoryTracker *limiter.MemoryConsumptionTracker) (IntermediateResultBlock, error) {
	var series []types.SeriesMetadata
	var err error

	series, err = types.SeriesMetadataSlicePool.Get(len(cached.Series), memoryTracker)
	if err != nil {
		return IntermediateResultBlock{}, err
	}

	for _, m := range cached.Series {
		lbls := mimirpb.FromLabelAdaptersToLabels(m.Labels)

		if err := memoryTracker.IncreaseMemoryConsumptionForLabels(lbls); err != nil {
			return IntermediateResultBlock{}, err
		}

		series = append(series, types.SeriesMetadata{
			Labels: lbls,
		})
	}

	results := make([]IntermediateResult, len(cached.Results))
	for i, proto := range cached.Results {
		result := IntermediateResult{
			SumOverTime: SumOverTimeIntermediate{
				SumF:     proto.SumF,
				HasFloat: proto.HasFloat,
				SumC:     proto.SumC,
			},
		}
		if proto.SumH != nil {
			result.SumOverTime.SumH = mimirpb.FromFloatHistogramProtoToFloatHistogram(proto.SumH)
		}
		results[i] = result
	}

	return IntermediateResultBlock{
		Version:          int(cached.Version),
		StartTimestampMs: int(cached.Start),
		EndTimestampMs:   int(cached.End),
		Series:           series,
		Results:          results,
	}, nil
}
