// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/web/api/v1/api.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package querymiddleware

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/regexp"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"
	"go.opentelemetry.io/otel"

	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/util"
)

const (
	day                                               = 24 * time.Hour
	queryRangePathSuffix                              = "/api/v1/query_range"
	instantQueryPathSuffix                            = "/api/v1/query"
	cardinalityLabelNamesPathSuffix                   = "/api/v1/cardinality/label_names"
	cardinalityLabelValuesPathSuffix                  = "/api/v1/cardinality/label_values"
	cardinalityActiveSeriesPathSuffix                 = "/api/v1/cardinality/active_series"
	cardinalityActiveNativeHistogramMetricsPathSuffix = "/api/v1/cardinality/active_native_histogram_metrics"
	labelNamesPathSuffix                              = "/api/v1/labels"
	remoteReadPathSuffix                              = "/api/v1/read"
	seriesPathSuffix                                  = "/api/v1/series"

	queryTypeInstant                      = "query"
	queryTypeRange                        = "query_range"
	queryTypeRemoteRead                   = "remote_read"
	queryTypeCardinality                  = "cardinality"
	queryTypeLabels                       = "label_names_and_values"
	queryTypeActiveSeries                 = "active_series"
	queryTypeActiveNativeHistogramMetrics = "active_native_histogram_metrics"
	queryTypeOther                        = "other"
)

var (
	tracer                = otel.Tracer("pkg/querymiddleware")
	labelValuesPathSuffix = regexp.MustCompile(`\/api\/v1\/label\/([^\/]+)\/values$`)
)

// Config for query_range middleware chain.
type Config struct {
	SplitQueriesByInterval                    time.Duration      `yaml:"split_queries_by_interval" category:"advanced"`
	ResultsCache                              ResultsCacheConfig `yaml:"results_cache"`
	CacheResults                              bool               `yaml:"cache_results"`
	CacheErrors                               bool               `yaml:"cache_errors"`
	MaxRetries                                int                `yaml:"max_retries" category:"advanced"`
	NotRunningTimeout                         time.Duration      `yaml:"not_running_timeout" category:"advanced"`
	ShardedQueries                            bool               `yaml:"parallelize_shardable_queries"`
	EnableRemoteExecution                     bool               `yaml:"enable_remote_execution" category:"experimental"`
	EnableMultipleNodeRemoteExecutionRequests bool               `yaml:"enable_multiple_node_remote_execution_requests" category:"experimental"`
	UseMQEForSharding                         bool               `yaml:"use_mimir_query_engine_for_sharding" category:"experimental"`
	RewriteQueriesHistogram                   bool               `yaml:"rewrite_histogram_queries" category:"experimental"`
	RewriteQueriesPropagateMatchers           bool               `yaml:"rewrite_propagate_matchers" category:"experimental"`
	TargetSeriesPerShard                      uint64             `yaml:"query_sharding_target_series_per_shard" category:"advanced"`
	ShardActiveSeriesQueries                  bool               `yaml:"shard_active_series_queries" category:"experimental"`
	UseActiveSeriesDecoder                    bool               `yaml:"use_active_series_decoder" category:"experimental"`

	// CacheKeyGenerator allows to inject a CacheKeyGenerator to use for generating cache keys.
	// If nil, the querymiddleware package uses a DefaultCacheKeyGenerator with SplitQueriesByInterval.
	CacheKeyGenerator CacheKeyGenerator `yaml:"-"`

	// ExtraInstantQueryMiddlewares and ExtraRangeQueryMiddlewares allows to
	// inject custom middlewares into the middleware chain of instant and
	// range queries. These middlewares will be placed right after default
	// middlewares and before the query sharding middleware, in order to avoid
	// interfering with core functionality.
	ExtraInstantQueryMiddlewares []MetricsQueryMiddleware `yaml:"-"`
	ExtraRangeQueryMiddlewares   []MetricsQueryMiddleware `yaml:"-"`

	InternalFunctionNames FunctionNamesSet `yaml:"-"`

	ExtraPropagateHeaders flagext.StringSliceCSV `yaml:"extra_propagated_headers" category:"advanced"`

	QueryResultResponseFormat string `yaml:"query_result_response_format"`

	// Deprecated in Mimir 3.1, remove in Mimir 3.3.
	CacheSamplesProcessedStats bool `yaml:"cache_samples_processed_stats" category:"deprecated"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxRetries, "query-frontend.max-retries-per-request", 5, "Maximum number of retries for a single request; beyond this, the downstream error is returned.")
	f.DurationVar(&cfg.NotRunningTimeout, "query-frontend.not-running-timeout", 2*time.Second, "Maximum time to wait for the query-frontend to become ready before rejecting requests received before the frontend was ready. 0 to disable (i.e. fail immediately if a request is received while the frontend is still starting up)")
	f.DurationVar(&cfg.SplitQueriesByInterval, "query-frontend.split-queries-by-interval", 24*time.Hour, "Split range queries by an interval and execute in parallel. You should use a multiple of 24 hours to optimize querying blocks. 0 to disable it.")
	f.BoolVar(&cfg.CacheResults, "query-frontend.cache-results", false, "Cache query results.")
	f.BoolVar(&cfg.CacheErrors, "query-frontend.cache-errors", false, "Cache non-transient errors from queries.")
	f.BoolVar(&cfg.ShardedQueries, "query-frontend.parallelize-shardable-queries", false, "True to enable query sharding.")
	f.BoolVar(&cfg.EnableRemoteExecution, "query-frontend.enable-remote-execution", false, "If set to true and the Mimir query engine is in use, use remote execution to evaluate queries in queriers.")
	f.BoolVar(&cfg.EnableMultipleNodeRemoteExecutionRequests, "query-frontend.enable-multiple-node-remote-execution-requests", false, "Set to true to allow evaluating multiple query plan nodes within a single remote execution request to queriers.")
	f.BoolVar(&cfg.UseMQEForSharding, "query-frontend.use-mimir-query-engine-for-sharding", false, "Set to true to enable performing query sharding inside the Mimir query engine (MQE). This setting has no effect if sharding is disabled. Requires remote execution and MQE to be enabled.")
	f.BoolVar(&cfg.RewriteQueriesHistogram, "query-frontend.rewrite-histogram-queries", false, "Set to true to enable rewriting histogram queries for a more efficient order of execution.")
	f.BoolVar(&cfg.RewriteQueriesPropagateMatchers, "query-frontend.rewrite-propagate-matchers", false, "Set to true to enable rewriting queries to propagate label matchers across binary expressions.")
	f.Uint64Var(&cfg.TargetSeriesPerShard, "query-frontend.query-sharding-target-series-per-shard", 0, "How many series a single sharded partial query should load at most. This is not a strict requirement guaranteed to be honoured by query sharding, but a hint given to the query sharding when the query execution is initially planned. 0 to disable cardinality-based hints.")
	f.Var(&cfg.ExtraPropagateHeaders, "query-frontend.extra-propagated-headers", "Comma-separated list of request header names to allow to pass through to the rest of the query path. This is in addition to a list of required headers that the read path needs.")
	f.StringVar(&cfg.QueryResultResponseFormat, "query-frontend.query-result-response-format", formatProtobuf, fmt.Sprintf("Format to use when retrieving query results from queriers. Supported values: %s", strings.Join(allFormats, ", ")))
	f.BoolVar(&cfg.ShardActiveSeriesQueries, "query-frontend.shard-active-series-queries", false, "True to enable sharding of active series queries.")
	f.BoolVar(&cfg.UseActiveSeriesDecoder, "query-frontend.use-active-series-decoder", false, "Set to true to use the zero-allocation response decoder for active series queries.")
	f.BoolVar(&cfg.CacheSamplesProcessedStats, "query-frontend.cache-samples-processed-stats", false, "Cache statistics of processed samples on results cache. Deprecated: has no effect.")
	cfg.ResultsCache.RegisterFlags(f)

	// This field isn't user-configurable, but we still need to set a default value so that subsequent Add() calls don't panic due to a nil map.
	cfg.InternalFunctionNames = FunctionNamesSet{}
}

// Validate validates the config.
func (cfg *Config) Validate() error {
	if cfg.CacheResults {
		if cfg.SplitQueriesByInterval <= 0 {
			return errors.New("-query-frontend.cache-results may only be enabled in conjunction with -query-frontend.split-queries-by-interval. Please set the latter")
		}
	}

	if cfg.CacheResults || cfg.CacheErrors || cfg.cardinalityBasedShardingEnabled() {
		if err := cfg.ResultsCache.Validate(); err != nil {
			return errors.Wrap(err, "invalid query-frontend results cache config")
		}
	}

	if !slices.Contains(allFormats, cfg.QueryResultResponseFormat) {
		return fmt.Errorf("unknown query result response format '%s'. Supported values: %s", cfg.QueryResultResponseFormat, strings.Join(allFormats, ", "))
	}
	return nil
}

func (cfg *Config) cardinalityBasedShardingEnabled() bool {
	return cfg.TargetSeriesPerShard > 0
}

func (cfg *Config) isPruningQueriesEnabled() bool {
	return cfg.RewriteQueriesHistogram || cfg.RewriteQueriesPropagateMatchers
}

// HandlerFunc is like http.HandlerFunc, but for MetricsQueryHandler.
type HandlerFunc func(context.Context, MetricsQueryRequest) (Response, error)

// Do implements MetricsQueryHandler.
func (q HandlerFunc) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	return q(ctx, req)
}

// MetricsQueryHandler is like http.Handler, but specifically for Prometheus query and query_range calls.
type MetricsQueryHandler interface {
	Do(context.Context, MetricsQueryRequest) (Response, error)
}

// LabelsHandlerFunc is like http.HandlerFunc, but for LabelsQueryHandler.
type LabelsHandlerFunc func(context.Context, LabelsSeriesQueryRequest) (Response, error)

// Do implements LabelsQueryHandler.
func (q LabelsHandlerFunc) Do(ctx context.Context, req LabelsSeriesQueryRequest) (Response, error) {
	return q(ctx, req)
}

// LabelsQueryHandler is like http.Handler, but specifically for Prometheus label names and values calls.
type LabelsQueryHandler interface {
	Do(context.Context, LabelsSeriesQueryRequest) (Response, error)
}

// MetricsQueryMiddlewareFunc is like http.HandlerFunc, but for MetricsQueryMiddleware.
type MetricsQueryMiddlewareFunc func(MetricsQueryHandler) MetricsQueryHandler

// Wrap implements MetricsQueryMiddleware.
func (q MetricsQueryMiddlewareFunc) Wrap(h MetricsQueryHandler) MetricsQueryHandler {
	return q(h)
}

// MetricsQueryMiddleware is a higher order MetricsQueryHandler.
type MetricsQueryMiddleware interface {
	Wrap(MetricsQueryHandler) MetricsQueryHandler
}

// MergeMetricsQueryMiddlewares produces a middleware that applies multiple middleware in turn;
// ie Merge(f,g,h).Wrap(handler) == f.Wrap(g.Wrap(h.Wrap(handler)))
func MergeMetricsQueryMiddlewares(middleware ...MetricsQueryMiddleware) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		for i := len(middleware) - 1; i >= 0; i-- {
			next = middleware[i].Wrap(next)
		}
		return next
	})
}

// Tripperware is a signature for all http client-side middleware.
type Tripperware func(http.RoundTripper) http.RoundTripper

// MergeTripperwares produces a tripperware that applies multiple tripperware in turn;
// ie Merge(f,g,h).Wrap(tripper) == f(g(h(tripper)))
func MergeTripperwares(tripperware ...Tripperware) Tripperware {
	return func(next http.RoundTripper) http.RoundTripper {
		for i := len(tripperware) - 1; i >= 0; i-- {
			next = tripperware[i](next)
		}
		return next
	}
}

// RoundTripFunc is to http.RoundTripper what http.HandlerFunc is to http.Handler.
type RoundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip implements http.RoundTripper.
func (f RoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

// NewTripperware returns a Tripperware configured with middlewares to limit, align, split, retry and cache requests.
func NewTripperware(
	cfg Config,
	log log.Logger,
	limits Limits,
	codec Codec,
	cacheExtractor Extractor,
	engine promql.QueryEngine,
	engineOpts promql.EngineOpts,
	ingestStorageTopicOffsetsReaders map[string]*ingest.TopicOffsetsReader,
	useRemoteExecution bool,
	streamingEngine *streamingpromql.Engine,
	registerer prometheus.Registerer,
) (Tripperware, error) {
	queryRangeTripperware, err := newQueryTripperware(cfg, log, limits, codec, cacheExtractor, engine, engineOpts, ingestStorageTopicOffsetsReaders, useRemoteExecution, streamingEngine, registerer)
	if err != nil {
		return nil, err
	}
	return MergeTripperwares(
		newQueryCountTripperware(registerer),
		queryRangeTripperware,
	), err
}

func newQueryTripperware(
	cfg Config,
	log log.Logger,
	limits Limits,
	codec Codec,
	cacheExtractor Extractor,
	engine promql.QueryEngine,
	engineOpts promql.EngineOpts,
	ingestStorageTopicOffsetsReaders map[string]*ingest.TopicOffsetsReader,
	useRemoteExecution bool,
	streamingEngine *streamingpromql.Engine,
	registerer prometheus.Registerer,
) (Tripperware, error) {
	var c cache.Cache
	if cfg.CacheResults || cfg.cardinalityBasedShardingEnabled() {
		var err error

		c, err = newResultsCache(cfg.ResultsCache, log, registerer)
		if err != nil {
			return nil, err
		}
		c = cache.NewCompression(cfg.ResultsCache.Compression, c, log)
	}

	cacheKeyGenerator := cfg.CacheKeyGenerator
	if cacheKeyGenerator == nil {
		cacheKeyGenerator = NewDefaultCacheKeyGenerator(codec, cfg.SplitQueriesByInterval)
	}

	retryMetrics := newRetryMetrics(registerer)

	queryRangeMiddleware, queryInstantMiddleware, remoteReadMiddleware := newQueryMiddlewares(
		cfg,
		log,
		limits,
		codec,
		c,
		cacheKeyGenerator,
		cacheExtractor,
		engine,
		engineOpts,
		registerer,
		retryMetrics,
	)
	requestBlocker := newRequestBlocker(limits, log, registerer)

	return func(next http.RoundTripper) http.RoundTripper {
		// IMPORTANT: roundtrippers are executed in *reverse* order because they are wrappers.
		// It means that the first roundtrippers defined in this function will be the last to be
		// executed.

		var queryHandler MetricsQueryHandler

		if useRemoteExecution {
			queryHandler = NewEngineQueryRequestRoundTripperHandler(streamingEngine, codec, log)
		} else {
			queryHandler = NewHTTPQueryRequestRoundTripperHandler(next, codec, log)
		}
		queryrange := NewLimitedParallelismRoundTripper(queryHandler, codec, limits, cfg.EnableRemoteExecution, queryRangeMiddleware...)
		instant := NewLimitedParallelismRoundTripper(queryHandler, codec, limits, cfg.EnableRemoteExecution, queryInstantMiddleware...)
		remoteRead := NewRemoteReadRoundTripper(next, remoteReadMiddleware...)

		// Wrap next for cardinality, labels queries and all other queries.
		// That attempts to parse "start" and "end" from the HTTP request and set them in the request's QueryDetails.
		// range and instant queries have more accurate logic for query details.
		next = NewQueryDetailsStartEndRoundTripper(next)
		cardinality := next
		activeSeries := next
		activeNativeHistogramMetrics := next
		labels := next
		series := next

		if cfg.MaxRetries > 0 {
			cardinality = newRetryRoundTripper(cardinality, log, cfg.MaxRetries, retryMetrics)
			series = newRetryRoundTripper(series, log, cfg.MaxRetries, retryMetrics)
			labels = newRetryRoundTripper(labels, log, cfg.MaxRetries, retryMetrics)
			activeSeries = newRetryRoundTripper(series, log, cfg.MaxRetries, retryMetrics)
		}

		if cfg.ShardActiveSeriesQueries {
			activeSeries = newShardActiveSeriesMiddleware(activeSeries, cfg.UseActiveSeriesDecoder, limits, log)
			activeNativeHistogramMetrics = newShardActiveNativeHistogramMetricsMiddleware(activeNativeHistogramMetrics, limits, log)
		}

		// Enforce read consistency after caching.
		if len(ingestStorageTopicOffsetsReaders) > 0 {
			metrics := newReadConsistencyMetrics(registerer, ingestStorageTopicOffsetsReaders)

			queryrange = newReadConsistencyRoundTripper(queryrange, ingestStorageTopicOffsetsReaders, limits, log, metrics)
			instant = newReadConsistencyRoundTripper(instant, ingestStorageTopicOffsetsReaders, limits, log, metrics)
			cardinality = newReadConsistencyRoundTripper(cardinality, ingestStorageTopicOffsetsReaders, limits, log, metrics)
			activeSeries = newReadConsistencyRoundTripper(activeSeries, ingestStorageTopicOffsetsReaders, limits, log, metrics)
			activeNativeHistogramMetrics = newReadConsistencyRoundTripper(activeNativeHistogramMetrics, ingestStorageTopicOffsetsReaders, limits, log, metrics)
			labels = newReadConsistencyRoundTripper(labels, ingestStorageTopicOffsetsReaders, limits, log, metrics)
			series = newReadConsistencyRoundTripper(series, ingestStorageTopicOffsetsReaders, limits, log, metrics)
			remoteRead = newReadConsistencyRoundTripper(remoteRead, ingestStorageTopicOffsetsReaders, limits, log, metrics)
			next = newReadConsistencyRoundTripper(next, ingestStorageTopicOffsetsReaders, limits, log, metrics)
		}

		// Look up cache as first thing after validation.
		if cfg.CacheResults {
			cardinality = newCardinalityQueryCacheRoundTripper(c, cacheKeyGenerator, limits, cardinality, log, registerer)
			labels = newLabelsQueryCacheRoundTripper(c, cacheKeyGenerator, limits, labels, log, registerer)
		}

		// Optimize labels queries after validation.
		labels = newLabelsQueryOptimizer(codec, limits, labels, log, registerer)

		// Validate the request before any processing.
		queryrange = NewMetricsQueryRequestValidationRoundTripper(codec, queryrange)
		instant = NewMetricsQueryRequestValidationRoundTripper(codec, instant)
		labels = NewLabelsQueryRequestValidationRoundTripper(codec, labels)
		series = NewLabelsQueryRequestValidationRoundTripper(codec, series)
		cardinality = NewCardinalityQueryRequestValidationRoundTripper(cardinality)

		return RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			if err := requestBlocker.isBlocked(r); err != nil {
				return nil, err
			}

			switch {
			case IsRangeQuery(r.URL.Path):
				return queryrange.RoundTrip(r)
			case IsInstantQuery(r.URL.Path):
				return instant.RoundTrip(r)
			case IsCardinalityQuery(r.URL.Path):
				return cardinality.RoundTrip(r)
			case IsActiveSeriesQuery(r.URL.Path):
				return activeSeries.RoundTrip(r)
			case IsActiveNativeHistogramMetricsQuery(r.URL.Path):
				return activeNativeHistogramMetrics.RoundTrip(r)
			case IsLabelsQuery(r.URL.Path):
				return labels.RoundTrip(r)
			case IsSeriesQuery(r.URL.Path):
				return series.RoundTrip(r)
			case IsRemoteReadQuery(r.URL.Path):
				return remoteRead.RoundTrip(r)
			default:
				return next.RoundTrip(r)
			}
		})
	}, nil
}

// newQueryMiddlewares creates and returns the middlewares that should injected for each type of request
// handled by the query-frontend.
func newQueryMiddlewares(
	cfg Config,
	log log.Logger,
	limits Limits,
	codec Codec,
	cacheClient cache.Cache,
	cacheKeyGenerator CacheKeyGenerator,
	cacheExtractor Extractor,
	engine promql.QueryEngine,
	engineOpts promql.EngineOpts,
	registerer prometheus.Registerer,
	retryMetrics prometheus.Observer,
) (queryRangeMiddleware, queryInstantMiddleware, remoteReadMiddleware []MetricsQueryMiddleware) {
	// Metric used to keep track of each middleware execution duration.
	metrics := newInstrumentMiddlewareMetrics(registerer)
	blockedQueriesCounter := promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_frontend_rejected_queries_total",
		Help: "Number of queries that were rejected by the cluster administrator.",
	}, []string{"user", "reason"})

	queryBlockerMiddleware := newQueryBlockerMiddleware(limits, log, blockedQueriesCounter)
	queryLimiterMiddleware := newQueryLimiterMiddleware(cacheClient, cacheKeyGenerator, limits, log, blockedQueriesCounter)
	queryStatsMiddleware := newQueryStatsMiddleware(registerer, engineOpts)
	prom2CompatMiddleware := newProm2RangeCompatMiddleware(limits, log, registerer)
	blockInternalFunctionsMiddleware := newBlockInternalFunctionsMiddleware(cfg.InternalFunctionNames, log)

	remoteReadMiddleware = append(remoteReadMiddleware,
		// Track query range statistics. Added first before any subsequent middleware modifies the request.
		queryStatsMiddleware,
		newLimitsMiddleware(limits, log),
		queryBlockerMiddleware,
	)

	queryRangeMiddleware = append(queryRangeMiddleware,
		// Track query range statistics. Added first before any subsequent middleware modifies the request.
		queryStatsMiddleware,
		newLimitsMiddleware(limits, log),
		queryBlockerMiddleware,
		queryLimiterMiddleware,
		blockInternalFunctionsMiddleware,
		newInstrumentMiddleware("prom2_compat", metrics),
		newDurationsMiddleware(log),
		prom2CompatMiddleware,
		newInstrumentMiddleware("step_align", metrics),
		newStepAlignMiddleware(limits, log, registerer),
	)

	queryInstantMiddleware = append(queryInstantMiddleware,
		queryStatsMiddleware,
		newLimitsMiddleware(limits, log),
		queryBlockerMiddleware,
		queryLimiterMiddleware,
		blockInternalFunctionsMiddleware,
		newInstrumentMiddleware("prom2_compat", metrics),
		newDurationsMiddleware(log),
		prom2CompatMiddleware,
	)

	// Inject the extra middlewares provided by the user before the caching, query pruning, and
	// query sharding middlewares.
	// This is important because these extra middlewares can potentially mutate the incoming
	// query.
	if len(cfg.ExtraInstantQueryMiddlewares) > 0 {
		queryInstantMiddleware = append(queryInstantMiddleware, cfg.ExtraInstantQueryMiddlewares...)
	}
	if len(cfg.ExtraRangeQueryMiddlewares) > 0 {
		queryRangeMiddleware = append(queryRangeMiddleware, cfg.ExtraRangeQueryMiddlewares...)
	}

	if cfg.CacheResults && cfg.CacheErrors {
		errorCachingMiddleware := newErrorCachingMiddleware(cacheClient, limits, resultsCacheEnabledByOption, cacheKeyGenerator, log, registerer)

		queryRangeMiddleware = append(
			queryRangeMiddleware,
			newInstrumentMiddleware("error_caching", metrics),
			errorCachingMiddleware,
		)
		queryInstantMiddleware = append(
			queryInstantMiddleware,
			newInstrumentMiddleware("error_caching", metrics),
			errorCachingMiddleware,
		)
	}

	// Does not apply to remote read as those are executed remotely and the enabling of PromQL experimental
	// functions for those are not controlled here.
	experimentalFunctionsMiddleware := newExperimentalFeaturesMiddleware(limits, log)
	queryRangeMiddleware = append(
		queryRangeMiddleware,
		newInstrumentMiddleware("experimental_functions", metrics),
		experimentalFunctionsMiddleware,
	)
	queryInstantMiddleware = append(
		queryInstantMiddleware,
		newInstrumentMiddleware("experimental_functions", metrics),
		experimentalFunctionsMiddleware,
	)

	// This is here for now as we need to run it before query sharding, but we plan to make it an AST optimization pass later.
	if cfg.isPruningQueriesEnabled() {
		rewriteMiddleware := newRewriteMiddleware(log, cfg, registerer)
		queryRangeMiddleware = append(
			queryRangeMiddleware,
			newInstrumentMiddleware("rewriting", metrics),
			rewriteMiddleware,
		)
		queryInstantMiddleware = append(
			queryInstantMiddleware,
			newInstrumentMiddleware("rewriting", metrics),
			rewriteMiddleware,
		)
	}

	// Create split and cache middleware if either splitting or caching is enabled
	var splitAndCacheMiddleware MetricsQueryMiddleware
	if cfg.SplitQueriesByInterval > 0 || cfg.CacheResults {
		splitAndCacheMiddleware = newSplitAndCacheMiddleware(
			cfg.SplitQueriesByInterval > 0,
			cfg.CacheResults,
			cfg.SplitQueriesByInterval,
			limits,
			codec,
			cacheClient,
			cacheKeyGenerator,
			cacheExtractor,
			resultsCacheEnabledByOption,
			log,
			registerer,
		)

		queryRangeMiddleware = append(queryRangeMiddleware, newInstrumentMiddleware("split_by_interval_and_results_cache", metrics), splitAndCacheMiddleware)
	}

	queryInstantMiddleware = append(
		queryInstantMiddleware,
		newInstrumentMiddleware("spin_off_subqueries", metrics),
		newSpinOffSubqueriesMiddleware(limits, log, engine, registerer, splitAndCacheMiddleware, engineOpts.NoStepSubqueryIntervalFn),
	)

	if cfg.ShardedQueries {
		// Inject the cardinality estimation middleware after time-based splitting and
		// before query-sharding so that it can operate on the partial queries that are
		// considered for sharding.
		if cfg.cardinalityBasedShardingEnabled() {
			cardinalityEstimationMiddleware := newCardinalityEstimationMiddleware(cacheClient, log, registerer)
			queryRangeMiddleware = append(
				queryRangeMiddleware,
				newInstrumentMiddleware("cardinality_estimation", metrics),
				cardinalityEstimationMiddleware,
			)
			queryInstantMiddleware = append(
				queryInstantMiddleware,
				newInstrumentMiddleware("cardinality_estimation", metrics),
				cardinalityEstimationMiddleware,
			)
		}

		if !cfg.UseMQEForSharding {
			queryShardingMiddleware := newQueryShardingMiddleware(
				log,
				engine,
				limits,
				cfg.TargetSeriesPerShard,
				registerer,
			)

			queryRangeMiddleware = append(
				queryRangeMiddleware,
				newInstrumentMiddleware("querysharding", metrics),
				queryShardingMiddleware,
			)
			queryInstantMiddleware = append(
				queryInstantMiddleware,
				newInstrumentMiddleware("querysharding", metrics),
				queryShardingMiddleware,
			)
		}
	}

	if cfg.MaxRetries > 0 {
		queryRangeMiddleware = append(queryRangeMiddleware, newInstrumentMiddleware("retry", metrics), newRetryMiddleware(log, cfg.MaxRetries, retryMetrics))
		queryInstantMiddleware = append(queryInstantMiddleware, newInstrumentMiddleware("retry", metrics), newRetryMiddleware(log, cfg.MaxRetries, retryMetrics))
		remoteReadMiddleware = append(remoteReadMiddleware, newInstrumentMiddleware("retry", metrics), newRetryMiddleware(log, cfg.MaxRetries, retryMetrics))
	}

	return
}

// NewQueryDetailsStartEndRoundTripper parses "start" and "end" parameters from the query and sets same fields in the QueryDetails in the context.
func NewQueryDetailsStartEndRoundTripper(next http.RoundTripper) http.RoundTripper {
	return RoundTripFunc(func(req *http.Request) (*http.Response, error) {
		params, _ := util.ParseRequestFormWithoutConsumingBody(req)
		if details := QueryDetailsFromContext(req.Context()); details != nil {
			if startMs, _ := util.ParseTime(params.Get("start")); startMs != 0 {
				details.Start = time.UnixMilli(startMs)
				details.MinT = details.Start
			}

			if endMs, _ := util.ParseTime(params.Get("end")); endMs != 0 {
				details.End = time.UnixMilli(endMs)
				details.MaxT = details.End
			}
		}
		return next.RoundTrip(req)
	})
}

func newQueryCountTripperware(registerer prometheus.Registerer) Tripperware {
	// Per tenant query metrics.
	queriesPerTenant := promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_frontend_queries_total",
		Help: "Total queries sent per tenant.",
	}, []string{"op", "user"})

	activeUsers := util.NewActiveUsersCleanupWithDefaultValues(func(user string) {
		queriesPerTenant.DeletePartialMatch(prometheus.Labels{"user": user})
	})

	// Start cleanup. If cleaner stops or fail, we will simply not clean the metrics for inactive users.
	_ = activeUsers.StartAsync(context.Background())
	return func(next http.RoundTripper) http.RoundTripper {
		return RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			op := queryTypeOther
			switch {
			case IsRangeQuery(r.URL.Path):
				op = queryTypeRange
			case IsInstantQuery(r.URL.Path):
				op = queryTypeInstant
			case IsRemoteReadQuery(r.URL.Path):
				op = queryTypeRemoteRead
			case IsCardinalityQuery(r.URL.Path):
				op = queryTypeCardinality
			case IsActiveSeriesQuery(r.URL.Path):
				op = queryTypeActiveSeries
			case IsActiveNativeHistogramMetricsQuery(r.URL.Path):
				op = queryTypeActiveNativeHistogramMetrics
			case IsLabelsQuery(r.URL.Path):
				op = queryTypeLabels
			}

			tenantIDs, err := tenant.TenantIDs(r.Context())
			// This should never happen anyways because we have auth middleware before this.
			if err != nil {
				return nil, err
			}
			userStr := tenant.JoinTenantIDs(tenantIDs)
			activeUsers.UpdateUserTimestamp(userStr, time.Now())
			queriesPerTenant.WithLabelValues(op, userStr).Inc()

			return next.RoundTrip(r)
		})
	}
}

func IsRangeQuery(path string) bool {
	return strings.HasSuffix(path, queryRangePathSuffix)
}

func IsInstantQuery(path string) bool {
	return strings.HasSuffix(path, instantQueryPathSuffix)
}

func IsCardinalityQuery(path string) bool {
	return strings.HasSuffix(path, cardinalityLabelNamesPathSuffix) ||
		strings.HasSuffix(path, cardinalityLabelValuesPathSuffix)
}

func IsLabelNamesQuery(path string) bool {
	return strings.HasSuffix(path, labelNamesPathSuffix)
}

func IsLabelValuesQuery(path string) bool {
	return labelValuesPathSuffix.MatchString(path)
}

func IsLabelsQuery(path string) bool {
	return IsLabelNamesQuery(path) || IsLabelValuesQuery(path)
}

func IsSeriesQuery(path string) bool {
	return strings.HasSuffix(path, seriesPathSuffix)
}

func IsActiveSeriesQuery(path string) bool {
	return strings.HasSuffix(path, cardinalityActiveSeriesPathSuffix)
}

func IsActiveNativeHistogramMetricsQuery(path string) bool {
	return strings.HasSuffix(path, cardinalityActiveNativeHistogramMetricsPathSuffix)
}

func IsRemoteReadQuery(path string) bool {
	return strings.HasSuffix(path, remoteReadPathSuffix)
}
