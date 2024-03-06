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

	"github.com/grafana/mimir/pkg/util"
)

const (
	day                               = 24 * time.Hour
	queryRangePathSuffix              = "/api/v1/query_range"
	instantQueryPathSuffix            = "/api/v1/query"
	cardinalityLabelNamesPathSuffix   = "/api/v1/cardinality/label_names"
	cardinalityLabelValuesPathSuffix  = "/api/v1/cardinality/label_values"
	cardinalityActiveSeriesPathSuffix = "/api/v1/cardinality/active_series"
	labelNamesPathSuffix              = "/api/v1/labels"

	// DefaultDeprecatedAlignQueriesWithStep is the default value for the deprecated querier frontend config DeprecatedAlignQueriesWithStep
	// which has been moved to a per-tenant limit; TODO remove in Mimir 2.14
	DefaultDeprecatedAlignQueriesWithStep = false

	queryTypeInstant      = "query"
	queryTypeRange        = "query_range"
	queryTypeCardinality  = "cardinality"
	queryTypeLabels       = "label_names_and_values"
	queryTypeActiveSeries = "active_series"
	queryTypeOther        = "other"
)

var (
	labelValuesPathSuffix = regexp.MustCompile(`\/api\/v1\/label\/([^\/]+)\/values$`)
)

// Config for query_range middleware chain.
type Config struct {
	SplitQueriesByInterval         time.Duration `yaml:"split_queries_by_interval" category:"advanced"`
	DeprecatedAlignQueriesWithStep bool          `yaml:"align_queries_with_step" doc:"hidden"` // Deprecated: Deprecated in Mimir 2.12, remove in Mimir 2.14 (https://github.com/grafana/mimir/issues/6712)
	ResultsCacheConfig             `yaml:"results_cache"`
	CacheResults                   bool          `yaml:"cache_results"`
	MaxRetries                     int           `yaml:"max_retries" category:"advanced"`
	NotRunningTimeout              time.Duration `yaml:"not_running_timeout" category:"advanced"`
	ShardedQueries                 bool          `yaml:"parallelize_shardable_queries"`
	TargetSeriesPerShard           uint64        `yaml:"query_sharding_target_series_per_shard" category:"advanced"`
	ShardActiveSeriesQueries       bool          `yaml:"shard_active_series_queries" category:"experimental"`

	// CacheKeyGenerator allows to inject a CacheKeyGenerator to use for generating cache keys.
	// If nil, the querymiddleware package uses a DefaultCacheKeyGenerator with SplitQueriesByInterval.
	CacheKeyGenerator CacheKeyGenerator `yaml:"-"`

	QueryResultResponseFormat string `yaml:"query_result_response_format"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxRetries, "query-frontend.max-retries-per-request", 5, "Maximum number of retries for a single request; beyond this, the downstream error is returned.")
	f.DurationVar(&cfg.NotRunningTimeout, "query-frontend.not-running-timeout", 2*time.Second, "Maximum time to wait for the query-frontend to become ready before rejecting requests received before the frontend was ready. 0 to disable (i.e. fail immediately if a request is received while the frontend is still starting up)")
	f.DurationVar(&cfg.SplitQueriesByInterval, "query-frontend.split-queries-by-interval", 24*time.Hour, "Split range queries by an interval and execute in parallel. You should use a multiple of 24 hours to optimize querying blocks. 0 to disable it.")
	f.BoolVar(&cfg.CacheResults, "query-frontend.cache-results", false, "Cache query results.")
	f.BoolVar(&cfg.ShardedQueries, "query-frontend.parallelize-shardable-queries", false, "True to enable query sharding.")
	f.Uint64Var(&cfg.TargetSeriesPerShard, "query-frontend.query-sharding-target-series-per-shard", 0, "How many series a single sharded partial query should load at most. This is not a strict requirement guaranteed to be honoured by query sharding, but a hint given to the query sharding when the query execution is initially planned. 0 to disable cardinality-based hints.")
	f.StringVar(&cfg.QueryResultResponseFormat, "query-frontend.query-result-response-format", formatProtobuf, fmt.Sprintf("Format to use when retrieving query results from queriers. Supported values: %s", strings.Join(allFormats, ", ")))
	f.BoolVar(&cfg.ShardActiveSeriesQueries, "query-frontend.shard-active-series-queries", false, "True to enable sharding of active series queries.")
	cfg.ResultsCacheConfig.RegisterFlags(f)

	// The query-frontend.align-queries-with-step flag has been moved to the limits.go file
	// cfg.DeprecatedAlignQueriesWithStep is set to the default here for clarity
	// and consistency with the process for migrating limits to per-tenant config
	// TODO: Remove in Mimir 2.14
	cfg.DeprecatedAlignQueriesWithStep = DefaultDeprecatedAlignQueriesWithStep
}

// Validate validates the config.
func (cfg *Config) Validate() error {
	if cfg.CacheResults {
		if cfg.SplitQueriesByInterval <= 0 {
			return errors.New("-query-frontend.cache-results may only be enabled in conjunction with -query-frontend.split-queries-by-interval. Please set the latter")
		}
	}

	if cfg.CacheResults || cfg.cardinalityBasedShardingEnabled() {
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

// HandlerFunc is like http.HandlerFunc, but for Handler.
type HandlerFunc func(context.Context, Request) (Response, error)

// Do implements Handler.
func (q HandlerFunc) Do(ctx context.Context, req Request) (Response, error) {
	return q(ctx, req)
}

// Handler is like http.Handle, but specifically for Prometheus query_range calls.
type Handler interface {
	Do(context.Context, Request) (Response, error)
}

// MiddlewareFunc is like http.HandlerFunc, but for Middleware.
type MiddlewareFunc func(Handler) Handler

// Wrap implements Middleware.
func (q MiddlewareFunc) Wrap(h Handler) Handler {
	return q(h)
}

// Middleware is a higher order Handler.
type Middleware interface {
	Wrap(Handler) Handler
}

// MergeMiddlewares produces a middleware that applies multiple middleware in turn;
// ie Merge(f,g,h).Wrap(handler) == f.Wrap(g.Wrap(h.Wrap(handler)))
func MergeMiddlewares(middleware ...Middleware) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
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
	registerer prometheus.Registerer,
) (Tripperware, error) {
	queryRangeTripperware, err := newQueryTripperware(cfg, log, limits, codec, cacheExtractor, engineOpts, engineExperimentalFunctionsEnabled, registerer)
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
	registerer prometheus.Registerer,
) (Tripperware, error) {
	// Disable concurrency limits for sharded queries.
	engineOpts.ActiveQueryTracker = nil
	engine := promql.NewEngine(engineOpts)

	// Experimental functions can only be enabled globally, and not on a per-engine basis.
	parser.EnableExperimentalFunctions = engineExperimentalFunctionsEnabled

	// Metric used to keep track of each middleware execution duration.
	metrics := newInstrumentMiddlewareMetrics(registerer)
	queryBlockerMiddleware := newQueryBlockerMiddleware(limits, log, registerer)
	queryStatsMiddleware := newQueryStatsMiddleware(registerer, engine)

	queryRangeMiddleware := []Middleware{
		// Track query range statistics. Added first before any subsequent middleware modifies the request.
		queryStatsMiddleware,
		newLimitsMiddleware(limits, log),
		queryBlockerMiddleware,
		newInstrumentMiddleware("step_align", metrics),
		newStepAlignMiddleware(limits, log, registerer),
	}

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
		cacheKeyGenerator = DefaultCacheKeyGenerator{Interval: cfg.SplitQueriesByInterval}
	}

	// Inject the middleware to split requests by interval + results cache (if at least one of the two is enabled).
	if cfg.SplitQueriesByInterval > 0 || cfg.CacheResults {
		shouldCache := func(r Request) bool {
			return !r.GetOptions().CacheDisabled
		}

		queryRangeMiddleware = append(queryRangeMiddleware, newInstrumentMiddleware("split_by_interval_and_results_cache", metrics), newSplitAndCacheMiddleware(
			cfg.SplitQueriesByInterval > 0,
			cfg.CacheResults,
			cfg.SplitQueriesByInterval,
			limits,
			codec,
			c,
			cacheKeyGenerator,
			cacheExtractor,
			shouldCache,
			log,
			registerer,
		))
	}

	queryInstantMiddleware := []Middleware{
		// Track query range statistics. Added first before any subsequent middleware modifies the request.
		queryStatsMiddleware,
		newLimitsMiddleware(limits, log),
		newSplitInstantQueryByIntervalMiddleware(limits, log, engine, registerer),
		queryBlockerMiddleware,
	}

	if cfg.ShardedQueries {
		// Inject the cardinality estimation middleware after time-based splitting and
		// before query-sharding so that it can operate on the partial queries that are
		// considered for sharding.
		if cfg.cardinalityBasedShardingEnabled() {
			cardinalityEstimationMiddleware := newCardinalityEstimationMiddleware(c, log, registerer)
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

	if cfg.MaxRetries > 0 {
		retryMiddlewareMetrics := newRetryMiddlewareMetrics(registerer)
		queryRangeMiddleware = append(queryRangeMiddleware, newInstrumentMiddleware("retry", metrics), newRetryMiddleware(log, cfg.MaxRetries, retryMiddlewareMetrics))
		queryInstantMiddleware = append(queryInstantMiddleware, newInstrumentMiddleware("retry", metrics), newRetryMiddleware(log, cfg.MaxRetries, retryMiddlewareMetrics))
	}

	return func(next http.RoundTripper) http.RoundTripper {
		queryrange := newLimitedParallelismRoundTripper(next, codec, limits, queryRangeMiddleware...)
		instant := newLimitedParallelismRoundTripper(next, codec, limits, queryInstantMiddleware...)

		// Wrap next for cardinality, labels queries and all other queries.
		// That attempts to parse "start" and "end" from the HTTP request and set them in the request's QueryDetails.
		// range and instant queries have more accurate logic for query details.
		next = newQueryDetailsStartEndRoundTripper(next)
		cardinality := next
		activeSeries := next
		labels := next

		// Inject the cardinality and labels query cache roundtripper only if the query results cache is enabled.
		if cfg.CacheResults {
			cardinality = newCardinalityQueryCacheRoundTripper(c, cacheKeyGenerator, limits, cardinality, log, registerer)
			labels = newLabelsQueryCacheRoundTripper(c, cacheKeyGenerator, limits, labels, log, registerer)
		}

		if cfg.ShardActiveSeriesQueries {
			activeSeries = newShardActiveSeriesMiddleware(activeSeries, limits, log)
		}

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
			case IsLabelsQuery(r.URL.Path):
				return labels.RoundTrip(r)
			default:
				return next.RoundTrip(r)
			}
		})
	}, nil
}

// newQueryDetailsStartEndRoundTripper parses "start" and "end" parameters from the query and sets same fields in the QueryDetails in the context.
func newQueryDetailsStartEndRoundTripper(next http.RoundTripper) http.RoundTripper {
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
			case IsCardinalityQuery(r.URL.Path):
				op = queryTypeCardinality
			case IsActiveSeriesQuery(r.URL.Path):
				op = queryTypeActiveSeries
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

func IsLabelsQuery(path string) bool {
	return strings.HasSuffix(path, labelNamesPathSuffix) || labelValuesPathSuffix.MatchString(path)
}

func IsActiveSeriesQuery(path string) bool {
	return strings.HasSuffix(path, cardinalityActiveSeriesPathSuffix)
}
