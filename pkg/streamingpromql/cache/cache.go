// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"flag"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/histogram"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

const (
	// resultsCacheVersion should be increased every time the cache format changes.
	resultsCacheVersion = 1
)

type IntermediateResultBlock struct {
	Version          int
	StartTimestampMs int
	DurationMs       int
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
	f.StringVar(&cfg.Backend, "querier.mimir-query-engine.intermediate-results-cache.backend", "", fmt.Sprintf("Backend for query-frontend results cache, if not empty. Supported values: %s.", strings.Join(supportedResultsCacheBackends, ", ")))
	cfg.Memcached.RegisterFlagsWithPrefix("querier.mimir-query-engine.intermediate-results-cache.memcached.", f)
	cfg.Compression.RegisterFlagsWithPrefix(f, "querier.mimir-query-engine.intermediate-results-cache.")
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

// newResultsCache creates a new results cache based on the input configuration.
func newResultsCache(cfg ResultsCacheConfig, logger log.Logger, reg prometheus.Registerer) (IntermediateResultsCache, error) {
	// Add the "component" label similarly to other components, so that metrics don't clash and have the same labels set
	// when running in monolithic mode.
	reg = prometheus.WrapRegistererWith(prometheus.Labels{"component": "querier"}, reg)

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
	return intermediateResultsCache{c: c}, nil
}

type intermediateResultsCache struct {
	c cache.Cache
}

type IntermediateResultsCache interface {
	Get(user, function, selector string, start int64, duration time.Duration) []IntermediateResultBlock
}

func (ic intermediateResultsCache) Get(user, function, selector string, start int64, duration time.Duration) []IntermediateResultBlock {
	return nil
}
