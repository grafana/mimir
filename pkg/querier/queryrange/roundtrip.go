// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/web/api/v1/api.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package queryrange

import (
	"context"
	"flag"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/chunk/cache"
	"github.com/grafana/mimir/pkg/chunk/storage"
	"github.com/grafana/mimir/pkg/tenant"
	"github.com/grafana/mimir/pkg/util"
)

const day = 24 * time.Hour

var (
	// PassthroughMiddleware is a noop middleware
	PassthroughMiddleware = MiddlewareFunc(func(next Handler) Handler {
		return next
	})

	errInvalidShardingStorage = errors.New("query sharding support is only available for blocks storage")
)

// Config for query_range middleware chain.
type Config struct {
	SplitQueriesByInterval time.Duration `yaml:"split_queries_by_interval"`
	AlignQueriesWithStep   bool          `yaml:"align_queries_with_step"`
	ResultsCacheConfig     `yaml:"results_cache"`
	CacheResults           bool `yaml:"cache_results"`
	MaxRetries             int  `yaml:"max_retries"`
	ShardedQueries         bool `yaml:"parallelise_shardable_queries"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxRetries, "querier.max-retries-per-request", 5, "Maximum number of retries for a single request; beyond this, the downstream error is returned.")
	f.DurationVar(&cfg.SplitQueriesByInterval, "querier.split-queries-by-interval", 0, "Split queries by an interval and execute in parallel, 0 disables it. You should use an a multiple of 24 hours (same as the storage bucketing scheme), to avoid queriers downloading and processing the same chunks. This also determines how cache keys are chosen when result caching is enabled")
	f.BoolVar(&cfg.AlignQueriesWithStep, "querier.align-querier-with-step", false, "Mutate incoming queries to align their start and end with their step. Can be disabled on a per-request basis setting 'Cache-Control: no-store' header (will disable results cache too).")
	f.BoolVar(&cfg.CacheResults, "querier.cache-results", false, "Cache query results.")
	f.BoolVar(&cfg.ShardedQueries, "query-frontend.parallelise-shardable-queries", false, "Perform query parallelisations based on storage sharding configuration and query ASTs. This feature is supported only by the blocks storage engine.")
	cfg.ResultsCacheConfig.RegisterFlags(f)
}

// Validate validates the config.
func (cfg *Config) Validate() error {
	if cfg.CacheResults {
		if cfg.SplitQueriesByInterval <= 0 {
			return errors.New("querier.cache-results may only be enabled in conjunction with querier.split-queries-by-interval. Please set the latter")
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
	storageEngine string,
	engineOpts promql.EngineOpts,
	registerer prometheus.Registerer,
	cacheGenNumberLoader CacheGenNumberLoader,
) (Tripperware, cache.Cache, error) {
	// Per tenant query metrics.
	queriesPerTenant := promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_frontend_queries_total",
		Help: "Total queries sent per tenant.",
	}, []string{"op", "user"})

	activeUsers := util.NewActiveUsersCleanupWithDefaultValues(func(user string) {
		err := util.DeleteMatchingLabels(queriesPerTenant, map[string]string{"user": user})
		if err != nil {
			level.Warn(log).Log("msg", "failed to remove cortex_query_frontend_queries_total metric for user", "user", user)
		}
	})

	// Metric used to keep track of each middleware execution duration.
	metrics := NewInstrumentMiddlewareMetrics(registerer)

	queryRangeMiddleware := []Middleware{NewLimitsMiddleware(limits, log)}
	if cfg.AlignQueriesWithStep {
		queryRangeMiddleware = append(queryRangeMiddleware, InstrumentMiddleware("step_align", metrics), StepAlignMiddleware)
	}
	if cfg.SplitQueriesByInterval != 0 {
		staticIntervalFn := func(_ Request) time.Duration { return cfg.SplitQueriesByInterval }
		queryRangeMiddleware = append(queryRangeMiddleware, InstrumentMiddleware("split_by_interval", metrics), SplitByIntervalMiddleware(staticIntervalFn, limits, codec, registerer))
	}

	var c cache.Cache
	if cfg.CacheResults {
		shouldCache := func(r Request) bool {
			return !r.GetOptions().CacheDisabled
		}
		queryCacheMiddleware, cache, err := NewResultsCacheMiddleware(log, cfg.ResultsCacheConfig, constSplitter(cfg.SplitQueriesByInterval), limits, codec, cacheExtractor, cacheGenNumberLoader, shouldCache, registerer)
		if err != nil {
			return nil, nil, err
		}
		c = cache
		queryRangeMiddleware = append(queryRangeMiddleware, InstrumentMiddleware("results_cache", metrics), queryCacheMiddleware)
	}

	if cfg.ShardedQueries {
		if storageEngine != storage.StorageEngineBlocks {
			if c != nil {
				c.Stop()
			}
			return nil, nil, errInvalidShardingStorage
		}

		// Disable concurrency limits for sharded queries.
		engineOpts.ActiveQueryTracker = nil

		queryRangeMiddleware = append(
			queryRangeMiddleware,
			InstrumentMiddleware("querysharding", metrics),
			NewQueryShardingMiddleware(
				log,
				promql.NewEngine(engineOpts),
				limits,
				registerer,
			),
		)
	}

	if cfg.MaxRetries > 0 {
		queryRangeMiddleware = append(queryRangeMiddleware, InstrumentMiddleware("retry", metrics), NewRetryMiddleware(log, cfg.MaxRetries, NewRetryMiddlewareMetrics(registerer)))
	}

	// Track query range statistics.
	queryRangeMiddleware = append(queryRangeMiddleware, newQueryStatsMiddleware(registerer))

	// Start cleanup. If cleaner stops or fail, we will simply not clean the metrics for inactive users.
	_ = activeUsers.StartAsync(context.Background())
	return func(next http.RoundTripper) http.RoundTripper {
		// Finally, if the user selected any query range middleware, stitch it in.
		if len(queryRangeMiddleware) > 0 {
			queryrange := NewLimitedRoundTripper(next, codec, limits, queryRangeMiddleware...)
			return RoundTripFunc(func(r *http.Request) (*http.Response, error) {
				isQueryRange := strings.HasSuffix(r.URL.Path, "/query_range")
				op := "query"
				if isQueryRange {
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

				if !isQueryRange {
					return next.RoundTrip(r)
				}
				return queryrange.RoundTrip(r)
			})
		}
		return next
	}, c, nil
}

type roundTripper struct {
	handler Handler
	codec   Codec
}

// NewRoundTripper merges a set of middlewares into an handler, then inject it into the `next` roundtripper
// using the codec to translate requests and responses.
func NewRoundTripper(next http.RoundTripper, codec Codec, logger log.Logger, middlewares ...Middleware) http.RoundTripper {
	return roundTripper{
		handler: MergeMiddlewares(middlewares...).Wrap(roundTripperHandler{
			logger: logger,
			next:   next,
			codec:  codec,
		}),
		codec: codec,
	}
}

func (q roundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	request, err := q.codec.DecodeRequest(r.Context(), r)
	if err != nil {
		return nil, err
	}

	if span := opentracing.SpanFromContext(r.Context()); span != nil {
		request.LogToSpan(span)
	}

	response, err := q.handler.Do(r.Context(), request)
	if err != nil {
		return nil, err
	}

	return q.codec.EncodeResponse(r.Context(), response)
}

// roundTripperHandler is a handler that roundtrips requests to next roundtripper.
// It basically encodes a Request from Handler.Do and decode response from next roundtripper.
type roundTripperHandler struct {
	logger log.Logger
	next   http.RoundTripper
	codec  Codec
}

// Do implements Handler.
func (q roundTripperHandler) Do(ctx context.Context, r Request) (Response, error) {
	request, err := q.codec.EncodeRequest(ctx, r)
	if err != nil {
		return nil, err
	}

	if err := user.InjectOrgIDIntoHTTPRequest(ctx, request); err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, "%s", err)
	}

	response, err := q.next.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()

	return q.codec.DecodeResponse(ctx, response, r, q.logger)
}
