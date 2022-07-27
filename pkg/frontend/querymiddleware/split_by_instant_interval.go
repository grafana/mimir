// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/storage/lazyquery"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// splitInstantQueryByIntervalMiddleware is a Middleware that can (optionally) split the instant query by splitInterval
type splitInstantQueryByIntervalMiddleware struct {
	next   Handler
	limits Limits
	logger log.Logger

	engine *promql.Engine

	splitEnabled  bool
	splitInterval time.Duration

	instantQuerySplittingMetrics
}

type instantQuerySplittingMetrics struct {
	splittingAttempts    prometheus.Counter
	mappedSplitQueries   *prometheus.CounterVec
	splitQueries         prometheus.Counter
	splitQueriesPerQuery prometheus.Histogram
}

// Parsing evaluation result used in instantQuerySplittingMetrics
const (
	SuccessKey = "success"
	FailureKey = "failure"
	NoopKey    = "noop"
)

func newInstantQuerySplittingMetrics(registerer prometheus.Registerer) instantQuerySplittingMetrics {
	return instantQuerySplittingMetrics{
		splittingAttempts: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "frontend_instant_query_splitting_rewrites_attempted_total",
			Help:      "Total number of instant queries the query-frontend attempted to split.",
		}),
		mappedSplitQueries: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "frontend_instant_query_sharding_rewrites_succeeded_total",
			Help:      "Number of instant queries the query-frontend attempted to split by evaluation type.",
		}, []string{"evaluation"}),
		splitQueries: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "frontend_instant_query_split_queries_total",
			Help:      "Total number of split partial queries.",
		}),
		splitQueriesPerQuery: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "frontend_instant_query_split_queries_per_query",
			Help:      "Number of split partial queries a single instant query has been rewritten to.",
			Buckets:   prometheus.ExponentialBuckets(2, 2, 10),
		}),
	}
}

// newSplitInstantQueryByIntervalMiddleware makes a new splitInstantQueryByIntervalMiddleware.
func newSplitInstantQueryByIntervalMiddleware(
	splitEnabled bool,
	splitInterval time.Duration,
	limits Limits,
	logger log.Logger,
	engine *promql.Engine,
	registerer prometheus.Registerer) Middleware {

	return MiddlewareFunc(func(next Handler) Handler {
		return &splitInstantQueryByIntervalMiddleware{
			splitEnabled:                 splitEnabled,
			next:                         next,
			limits:                       limits,
			splitInterval:                splitInterval,
			logger:                       logger,
			engine:                       engine,
			instantQuerySplittingMetrics: newInstantQuerySplittingMetrics(registerer),
		}
	})
}

func (s *splitInstantQueryByIntervalMiddleware) Do(ctx context.Context, req Request) (Response, error) {
	log, ctx := spanlogger.NewWithLogger(ctx, s.logger, "splitInstantQueryByIntervalMiddleware.Do")
	defer log.Span.Finish()

	_, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	if !s.splitEnabled || s.splitInterval <= 0 {
		return s.next.Do(ctx, req)
	}

	// Increment total number of instant queries attempted to split metrics
	s.splittingAttempts.Inc()

	mapper := astmapper.NewInstantQuerySplitter(s.splitInterval, s.logger)

	expr, err := parser.ParseExpr(req.GetQuery())
	if err != nil {
		level.Warn(log).Log("msg", "failed to parse query", "query", req.GetQuery(), "err", err)
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	stats := astmapper.NewMapperStats()
	instantSplitQuery, err := mapper.Map(expr, stats)
	if err != nil {
		level.Error(log).Log("msg", "failed to map the input query, falling back to try executing without splitting", "query", req.GetQuery(), "err", err)
		s.mappedSplitQueries.WithLabelValues(FailureKey).Inc()
		return s.next.Do(ctx, req)
	}

	noop := instantSplitQuery.String() == expr.String()
	if noop {
		// the query cannot be split, so continue
		level.Debug(log).Log("msg", "input query resulted in a no operation, falling back to try executing without splitting", "query", req.GetQuery())
		s.mappedSplitQueries.WithLabelValues(NoopKey).Inc()
		return s.next.Do(ctx, req)
	}

	level.Debug(log).Log("msg", "instant query has been split by interval", "original", req.GetQuery(), "rewritten", instantSplitQuery, "split_queries", stats.GetShardedQueries())

	// Send hint with number of embedded queries to the sharding middleware
	hints := &Hints{TotalQueries: int32(stats.GetShardedQueries())}

	// Update metrics
	s.mappedSplitQueries.WithLabelValues(SuccessKey).Inc()
	s.splitQueries.Add(float64(stats.GetShardedQueries()))
	s.splitQueriesPerQuery.Observe(float64(stats.GetShardedQueries()))

	req = req.WithQuery(instantSplitQuery.String()).WithHints(hints)
	shardedQueryable := newShardedQueryable(req, s.next)

	qry, err := newQuery(req, s.engine, lazyquery.NewLazyQueryable(shardedQueryable))
	if err != nil {
		level.Warn(log).Log("msg", "failed to create new query from splittable request", "req", req.GetQuery(), "err", err)
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	res := qry.Exec(ctx)
	extracted, err := promqlResultToSamples(res)
	if err != nil {
		level.Warn(log).Log("msg", "failed to extract promql results from splittable request", "res", res, "err", err)
		return nil, mapEngineError(err)
	}
	return &PrometheusResponse{
		Status: statusSuccess,
		Data: &PrometheusData{
			ResultType: string(res.Value.Type()),
			Result:     extracted,
		},
		Headers: shardedQueryable.getResponseHeaders(),
	}, nil
}
