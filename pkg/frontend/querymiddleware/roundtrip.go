// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/web/api/v1/api.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package querymiddleware

import (
	"context"
	"flag"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/cache"
	"github.com/grafana/mimir/pkg/tenant"
	"github.com/grafana/mimir/pkg/util"
)

const (
	day                    = 24 * time.Hour
	queryRangePathSuffix   = "/query_range"
	instantQueryPathSuffix = "/query"
)

// Config for query_range middleware chain.
type Config struct {
	SplitQueriesByInterval time.Duration `yaml:"split_queries_by_interval" category:"advanced"`
	AlignQueriesWithStep   bool          `yaml:"align_queries_with_step"`
	ResultsCacheConfig     `yaml:"results_cache"`
	CacheResults           bool `yaml:"cache_results"`
	MaxRetries             int  `yaml:"max_retries"`
	ShardedQueries         bool `yaml:"parallelize_shardable_queries"`
	CacheUnalignedRequests bool `yaml:"cache_unaligned_requests"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxRetries, "query-frontend.max-retries-per-request", 5, "Maximum number of retries for a single request; beyond this, the downstream error is returned.")
	f.DurationVar(&cfg.SplitQueriesByInterval, "query-frontend.split-queries-by-interval", 24*time.Hour, "Split queries by an interval and execute in parallel. You should use a multiple of 24 hours to optimize querying blocks. 0 to disable it.")
	f.BoolVar(&cfg.AlignQueriesWithStep, "query-frontend.align-querier-with-step", false, "Mutate incoming queries to align their start and end with their step.")
	f.BoolVar(&cfg.CacheResults, "query-frontend.cache-results", false, "Cache query results.")
	f.BoolVar(&cfg.ShardedQueries, "query-frontend.parallelize-shardable-queries", false, "True to enable query sharding.")
	f.BoolVar(&cfg.CacheUnalignedRequests, "query-frontend.cache-unaligned-requests", false, "Cache requests that are not step-aligned.")
	cfg.ResultsCacheConfig.RegisterFlags(f)
}

// Validate validates the config.
func (cfg *Config) Validate() error {
	if cfg.CacheResults {
		if cfg.SplitQueriesByInterval <= 0 {
			return errors.New("-query-frontend.cache-results may only be enabled in conjunction with -query-frontend.split-queries-by-interval. Please set the latter")
		}
		if err := cfg.ResultsCacheConfig.Validate(); err != nil {
			return errors.Wrap(err, "invalid ResultsCache config")
		}
	}
	return nil
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
	registerer prometheus.Registerer,
) (Tripperware, error) {
	queryRangeTripperware, err := newQueryTripperware(cfg, log, limits, codec, cacheExtractor, engineOpts, registerer)
	if err != nil {
		return nil, err
	}
	return MergeTripperwares(
		newActiveUsersTripperware(log, registerer),
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
	registerer prometheus.Registerer,
) (Tripperware, error) {
	// Metric used to keep track of each middleware execution duration.
	metrics := newInstrumentMiddlewareMetrics(registerer)

	queryRangeMiddleware := []Middleware{
		// Track query range statistics. Added first before any subsequent middleware modifies the request.
		newQueryStatsMiddleware(registerer),
		newLimitsMiddleware(limits, log),
	}
	if cfg.AlignQueriesWithStep {
		queryRangeMiddleware = append(queryRangeMiddleware, newInstrumentMiddleware("step_align", metrics, log), newStepAlignMiddleware())
	}

	// Inject the middleware to split requests by interval + results cache (if at least one of the two is enabled).
	if cfg.SplitQueriesByInterval > 0 || cfg.CacheResults {
		var c cache.Cache

		// Init the cache client.
		if cfg.CacheResults {
			var err error

			c, err = newResultsCache(cfg.ResultsCacheConfig, log, registerer)
			if err != nil {
				return nil, err
			}
			c = cache.NewCompression(cfg.ResultsCacheConfig.Compression, c, log)
		}

		shouldCache := func(r Request) bool {
			return !r.GetOptions().CacheDisabled
		}

		queryRangeMiddleware = append(queryRangeMiddleware, newInstrumentMiddleware("split_by_interval_and_results_cache", metrics, log), newSplitAndCacheMiddleware(
			cfg.SplitQueriesByInterval > 0,
			cfg.CacheResults,
			cfg.SplitQueriesByInterval,
			cfg.CacheUnalignedRequests,
			limits,
			codec,
			c,
			constSplitter(cfg.SplitQueriesByInterval),
			cacheExtractor,
			shouldCache,
			log,
			registerer,
		))
	}
	queryInstantMiddleware := []Middleware{newLimitsMiddleware(limits, log)}

	if cfg.ShardedQueries {
		// Disable concurrency limits for sharded queries.
		engineOpts.ActiveQueryTracker = nil
		queryshardingMiddleware := newQueryShardingMiddleware(
			log,
			promql.NewEngine(engineOpts),
			limits,
			registerer,
		)
		queryRangeMiddleware = append(
			queryRangeMiddleware,
			newInstrumentMiddleware("querysharding", metrics, log),
			queryshardingMiddleware,
		)
		queryInstantMiddleware = append(
			queryInstantMiddleware,
			newInstrumentMiddleware("querysharding", metrics, log),
			queryshardingMiddleware,
		)
	}

	if cfg.MaxRetries > 0 {
		retryMiddlewareMetrics := newRetryMiddlewareMetrics(registerer)
		queryRangeMiddleware = append(queryRangeMiddleware, newInstrumentMiddleware("retry", metrics, log), newRetryMiddleware(log, cfg.MaxRetries, retryMiddlewareMetrics))
		queryInstantMiddleware = append(queryInstantMiddleware, newInstrumentMiddleware("retry", metrics, log), newRetryMiddleware(log, cfg.MaxRetries, retryMiddlewareMetrics))
	}

	return func(next http.RoundTripper) http.RoundTripper {
		queryrange := newLimitedParallelismRoundTripper(next, codec, limits, queryRangeMiddleware...)
		instant := defaultInstantQueryParamsRoundTripper(
			newLimitedParallelismRoundTripper(next, codec, limits, queryInstantMiddleware...),
			time.Now,
		)
		return RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch {
			case isRangeQuery(r.URL.Path):
				return queryrange.RoundTrip(r)
			case isInstantQuery(r.URL.Path):
				return instant.RoundTrip(r)
			default:
				return next.RoundTrip(r)
			}
		})
	}, nil
}

func newActiveUsersTripperware(logger log.Logger, registerer prometheus.Registerer) Tripperware {
	// Per tenant query metrics.
	queriesPerTenant := promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_frontend_queries_total",
		Help: "Total queries sent per tenant.",
	}, []string{"op", "user"})

	activeUsers := util.NewActiveUsersCleanupWithDefaultValues(func(user string) {
		err := util.DeleteMatchingLabels(queriesPerTenant, map[string]string{"user": user})
		if err != nil {
			level.Warn(logger).Log("msg", "failed to remove cortex_query_frontend_queries_total metric for user", "user", user)
		}
	})

	// Start cleanup. If cleaner stops or fail, we will simply not clean the metrics for inactive users.
	_ = activeUsers.StartAsync(context.Background())
	return func(next http.RoundTripper) http.RoundTripper {
		return RoundTripFunc(func(r *http.Request) (*http.Response, error) {
			op := "query"
			if isRangeQuery(r.URL.Path) {
				op = "query_range"
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

func isRangeQuery(path string) bool {
	return strings.HasSuffix(path, queryRangePathSuffix)
}

func isInstantQuery(path string) bool {
	return strings.HasSuffix(path, instantQueryPathSuffix)
}

func defaultInstantQueryParamsRoundTripper(next http.RoundTripper, now func() time.Time) http.RoundTripper {
	return RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if isInstantQuery(r.URL.Path) && !r.URL.Query().Has("time") {
			q := r.URL.Query()
			q.Add("time", strconv.FormatInt(time.Now().Unix(), 10))
			r.URL.RawQuery = q.Encode()
		}
		return next.RoundTrip(r)
	})
}
