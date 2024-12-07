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
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/regexp"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/exp/slices"

	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storage/ingest"
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
	labelValuesPathSuffix = regexp.MustCompile(`\/api\/v1\/label\/([^\/]+)\/values$`)
)

// Config for query_range middleware chain.
type Config struct {
	SplitQueriesByInterval           time.Duration `yaml:"split_queries_by_interval" category:"advanced"`
	ResultsCacheConfig               `yaml:"results_cache"`
	CacheResults                     bool          `yaml:"cache_results"`
	CacheErrors                      bool          `yaml:"cache_errors" category:"experimental"`
	MaxRetries                       int           `yaml:"max_retries" category:"advanced"`
	NotRunningTimeout                time.Duration `yaml:"not_running_timeout" category:"advanced"`
	ShardedQueries                   bool          `yaml:"parallelize_shardable_queries"`
	PrunedQueries                    bool          `yaml:"prune_queries" category:"experimental"`
	BlockPromQLExperimentalFunctions bool          `yaml:"block_promql_experimental_functions" category:"experimental"`
	TargetSeriesPerShard             uint64        `yaml:"query_sharding_target_series_per_shard" category:"advanced"`
	ShardActiveSeriesQueries         bool          `yaml:"shard_active_series_queries" category:"experimental"`
	UseActiveSeriesDecoder           bool          `yaml:"use_active_series_decoder" category:"experimental"`

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

	QueryResultResponseFormat string `yaml:"query_result_response_format"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxRetries, "query-frontend.max-retries-per-request", 5, "Maximum number of retries for a single request; beyond this, the downstream error is returned.")
	f.DurationVar(&cfg.NotRunningTimeout, "query-frontend.not-running-timeout", 2*time.Second, "Maximum time to wait for the query-frontend to become ready before rejecting requests received before the frontend was ready. 0 to disable (i.e. fail immediately if a request is received while the frontend is still starting up)")
	f.DurationVar(&cfg.SplitQueriesByInterval, "query-frontend.split-queries-by-interval", 24*time.Hour, "Split range queries by an interval and execute in parallel. You should use a multiple of 24 hours to optimize querying blocks. 0 to disable it.")
	f.BoolVar(&cfg.CacheResults, "query-frontend.cache-results", false, "Cache query results.")
	f.BoolVar(&cfg.CacheErrors, "query-frontend.cache-errors", false, "Cache non-transient errors from queries.")
	f.BoolVar(&cfg.ShardedQueries, "query-frontend.parallelize-shardable-queries", false, "True to enable query sharding.")
	f.BoolVar(&cfg.PrunedQueries, "query-frontend.prune-queries", false, "True to enable pruning dead code (eg. expressions that cannot produce any results) and simplifying expressions (eg. expressions that can be evaluated immediately) in queries.")
	f.BoolVar(&cfg.BlockPromQLExperimentalFunctions, "query-frontend.block-promql-experimental-functions", false, "True to control access to specific PromQL experimental functions per tenant.")
	f.Uint64Var(&cfg.TargetSeriesPerShard, "query-frontend.query-sharding-target-series-per-shard", 0, "How many series a single sharded partial query should load at most. This is not a strict requirement guaranteed to be honoured by query sharding, but a hint given to the query sharding when the query execution is initially planned. 0 to disable cardinality-based hints.")
	f.StringVar(&cfg.QueryResultResponseFormat, "query-frontend.query-result-response-format", formatProtobuf, fmt.Sprintf("Format to use when retrieving query results from queriers. Supported values: %s", strings.Join(allFormats, ", ")))
	f.BoolVar(&cfg.ShardActiveSeriesQueries, "query-frontend.shard-active-series-queries", false, "True to enable sharding of active series queries.")
	f.BoolVar(&cfg.UseActiveSeriesDecoder, "query-frontend.use-active-series-decoder", false, "Set to true to use the zero-allocation response decoder for active series queries.")
	cfg.ResultsCacheConfig.RegisterFlags(f)
}

// Validate validates the config.
func (cfg *Config) Validate() error {
	if cfg.CacheResults {
		if cfg.SplitQueriesByInterval <= 0 {
			return errors.New("-query-frontend.cache-results may only be enabled in conjunction with -query-frontend.split-queries-by-interval. Please set the latter")
		}
	}

	if cfg.CacheResults || cfg.CacheErrors || cfg.cardinalityBasedShardingEnabled() {
		if err := cfg.ResultsCacheConfig.Validate(); err != nil {
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
	engineOpts promql.EngineOpts,
	engineExperimentalFunctionsEnabled bool,
	ingestStorageTopicOffsetsReader *ingest.TopicOffsetsReader,
	registerer prometheus.Registerer,
) (Tripperware, error) {
	queryRangeTripperware, err := newQueryTripperware(cfg, log, limits, codec, cacheExtractor, engineOpts, engineExperimentalFunctionsEnabled, ingestStorageTopicOffsetsReader, registerer)
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
	engineOpts promql.EngineOpts,
	engineExperimentalFunctionsEnabled bool,
	ingestStorageTopicOffsetsReader *ingest.TopicOffsetsReader,
	registerer prometheus.Registerer,
) (Tripperware, error) {
	// Disable concurrency limits for sharded queries.
	engineOpts.ActiveQueryTracker = nil
	engine := promql.NewEngine(engineOpts)

	// Experimental functions can only be enabled globally, and not on a per-engine basis.
	parser.EnableExperimentalFunctions = engineExperimentalFunctionsEnabled

	var c cache.Cache
	if cfg.CacheResults || cfg.cardinalityBasedShardingEnabled() {
		var err error

		c, err = newResultsCache(cfg.ResultsCacheConfig, log, registerer)
		if err != nil {
			return nil, err
		}
		c = cache.NewCompression(cfg.ResultsCacheConfig.Compression, c, log)
	}

	cacheKeyGenerator := cfg.CacheKeyGenerator
	if cacheKeyGenerator == nil {
		cacheKeyGenerator = NewDefaultCacheKeyGenerator(codec, cfg.SplitQueriesByInterval)
	}

	queryRangeMiddleware, queryInstantMiddleware, remoteReadMiddleware := newQueryMiddlewares(cfg, log, limits, codec, c, cacheKeyGenerator, cacheExtractor, engine, registerer)

	return func(next http.RoundTripper) http.RoundTripper {
		// IMPORTANT: roundtrippers are executed in *reverse* order because they are wrappers.
		// It means that the first roundtrippers defined in this function will be the last to be
		// executed.

		queryrange := NewLimitedParallelismRoundTripper(next, codec, limits, queryRangeMiddleware...)
		instant := NewLimitedParallelismRoundTripper(next, codec, limits, queryInstantMiddleware...)
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

		if cfg.ShardActiveSeriesQueries {
			activeSeries = newShardActiveSeriesMiddleware(activeSeries, cfg.UseActiveSeriesDecoder, limits, log)
			activeNativeHistogramMetrics = newShardActiveNativeHistogramMetricsMiddleware(activeNativeHistogramMetrics, limits, log)
		}

		// Enforce read consistency after caching.
		if ingestStorageTopicOffsetsReader != nil {
			metrics := newReadConsistencyMetrics(registerer)

			queryrange = NewReadConsistencyRoundTripper(queryrange, querierapi.ReadConsistencyOffsetsHeader, ingestStorageTopicOffsetsReader, limits, log, metrics)
			instant = NewReadConsistencyRoundTripper(instant, querierapi.ReadConsistencyOffsetsHeader, ingestStorageTopicOffsetsReader, limits, log, metrics)
			cardinality = NewReadConsistencyRoundTripper(cardinality, querierapi.ReadConsistencyOffsetsHeader, ingestStorageTopicOffsetsReader, limits, log, metrics)
			activeSeries = NewReadConsistencyRoundTripper(activeSeries, querierapi.ReadConsistencyOffsetsHeader, ingestStorageTopicOffsetsReader, limits, log, metrics)
			activeNativeHistogramMetrics = NewReadConsistencyRoundTripper(activeNativeHistogramMetrics, querierapi.ReadConsistencyOffsetsHeader, ingestStorageTopicOffsetsReader, limits, log, metrics)
			labels = NewReadConsistencyRoundTripper(labels, querierapi.ReadConsistencyOffsetsHeader, ingestStorageTopicOffsetsReader, limits, log, metrics)
			series = NewReadConsistencyRoundTripper(series, querierapi.ReadConsistencyOffsetsHeader, ingestStorageTopicOffsetsReader, limits, log, metrics)
			remoteRead = NewReadConsistencyRoundTripper(remoteRead, querierapi.ReadConsistencyOffsetsHeader, ingestStorageTopicOffsetsReader, limits, log, metrics)
			next = NewReadConsistencyRoundTripper(next, querierapi.ReadConsistencyOffsetsHeader, ingestStorageTopicOffsetsReader, limits, log, metrics)
		}

		// Look up cache as first thing after validation.
		if cfg.CacheResults {
			cardinality = newCardinalityQueryCacheRoundTripper(c, cacheKeyGenerator, limits, cardinality, log, registerer)
			labels = newLabelsQueryCacheRoundTripper(c, cacheKeyGenerator, limits, labels, log, registerer)
		}

		// Validate the request before any processing.
		queryrange = NewMetricsQueryRequestValidationRoundTripper(codec, queryrange)
		instant = NewMetricsQueryRequestValidationRoundTripper(codec, instant)
		labels = NewLabelsQueryRequestValidationRoundTripper(codec, labels)
		series = NewLabelsQueryRequestValidationRoundTripper(codec, series)
		cardinality = NewCardinalityQueryRequestValidationRoundTripper(cardinality)

		return RoundTripFunc(func(r *http.Request) (*http.Response, error) {
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
	engine *promql.Engine,
	registerer prometheus.Registerer,
) (queryRangeMiddleware, queryInstantMiddleware, remoteReadMiddleware []MetricsQueryMiddleware) {
	// Metric used to keep track of each middleware execution duration.
	metrics := newInstrumentMiddlewareMetrics(registerer)
	queryBlockerMiddleware := newQueryBlockerMiddleware(limits, log, registerer)
	queryStatsMiddleware := newQueryStatsMiddleware(registerer, engine)

	remoteReadMiddleware = append(remoteReadMiddleware,
		// Track query range statistics. Added first before any subsequent middleware modifies the request.
		queryStatsMiddleware,
		newLimitsMiddleware(limits, log),
		queryBlockerMiddleware)

	queryRangeMiddleware = append(queryRangeMiddleware,
		// Track query range statistics. Added first before any subsequent middleware modifies the request.
		queryStatsMiddleware,
		newLimitsMiddleware(limits, log),
		queryBlockerMiddleware,
		newInstrumentMiddleware("step_align", metrics),
		newStepAlignMiddleware(limits, log, registerer),
	)

	if cfg.CacheResults && cfg.CacheErrors {
		queryRangeMiddleware = append(
			queryRangeMiddleware,
			newInstrumentMiddleware("error_caching", metrics),
			newErrorCachingMiddleware(cacheClient, limits, resultsCacheEnabledByOption, cacheKeyGenerator, log, registerer),
		)
	}

	// Inject the middleware to split requests by interval + results cache (if at least one of the two is enabled).
	if cfg.SplitQueriesByInterval > 0 || cfg.CacheResults {
		queryRangeMiddleware = append(queryRangeMiddleware, newInstrumentMiddleware("split_by_interval_and_results_cache", metrics), newSplitAndCacheMiddleware(
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
		))
	}

	queryInstantMiddleware = append(queryInstantMiddleware,
		// Track query range statistics. Added first before any subsequent middleware modifies the request.
		queryStatsMiddleware,
		newLimitsMiddleware(limits, log),
		newSplitInstantQueryByIntervalMiddleware(limits, log, engine, registerer),
		queryBlockerMiddleware,
	)

	// Inject the extra middlewares provided by the user before the query pruning and query sharding middleware.
	if len(cfg.ExtraInstantQueryMiddlewares) > 0 {
		queryInstantMiddleware = append(queryInstantMiddleware, cfg.ExtraInstantQueryMiddlewares...)
	}
	// Inject the extra middlewares provided by the user before the query pruning and query sharding middleware.
	if len(cfg.ExtraRangeQueryMiddlewares) > 0 {
		queryRangeMiddleware = append(queryRangeMiddleware, cfg.ExtraRangeQueryMiddlewares...)
	}

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

		queryshardingMiddleware := newQueryShardingMiddleware(
			log,
			engine,
			limits,
			cfg.TargetSeriesPerShard,
			registerer,
		)

		queryRangeMiddleware = append(
			queryRangeMiddleware,
			newInstrumentMiddleware("querysharding", metrics),
			queryshardingMiddleware,
		)
		queryInstantMiddleware = append(
			queryInstantMiddleware,
			newInstrumentMiddleware("querysharding", metrics),
			queryshardingMiddleware,
		)
	}

	if cfg.PrunedQueries {
		pruneMiddleware := newPruneMiddleware(log)
		queryRangeMiddleware = append(
			queryRangeMiddleware,
			newInstrumentMiddleware("pruning", metrics),
			pruneMiddleware,
		)
		queryInstantMiddleware = append(
			queryInstantMiddleware,
			newInstrumentMiddleware("pruning", metrics),
			pruneMiddleware,
		)
	}

	if cfg.MaxRetries > 0 {
		retryMiddlewareMetrics := newRetryMiddlewareMetrics(registerer)
		queryRangeMiddleware = append(queryRangeMiddleware, newInstrumentMiddleware("retry", metrics), newRetryMiddleware(log, cfg.MaxRetries, retryMiddlewareMetrics))
		queryInstantMiddleware = append(queryInstantMiddleware, newInstrumentMiddleware("retry", metrics), newRetryMiddleware(log, cfg.MaxRetries, retryMiddlewareMetrics))
	}

	if parser.EnableExperimentalFunctions && cfg.BlockPromQLExperimentalFunctions {
		// We only need to check for tenant-specific settings if experimental functions are enabled globally
		// and if we want to control access to them per tenant.
		// Does not apply to remote read as those are executed remotely and the enabling of PromQL experimental
		// functions for those are not controlled here.
		experimentalFunctionsMiddleware := newExperimentalFunctionsMiddleware(limits, log)
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
