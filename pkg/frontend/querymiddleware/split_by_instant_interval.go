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
	successKey = "success"
	failureKey = "failure"
	noopKey    = "noop"
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
			Name:      "frontend_instant_query_splitting_rewrites_succeeded_total",
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
	splitInterval time.Duration,
	limits Limits,
	logger log.Logger,
	engine *promql.Engine,
	registerer prometheus.Registerer) Middleware {
	metrics := newInstantQuerySplittingMetrics(registerer)

	return MiddlewareFunc(func(next Handler) Handler {
		return &splitInstantQueryByIntervalMiddleware{
			next:                         next,
			limits:                       limits,
			splitInterval:                splitInterval,
			logger:                       logger,
			engine:                       engine,
			instantQuerySplittingMetrics: metrics,
		}
	})
}

func (s *splitInstantQueryByIntervalMiddleware) Do(ctx context.Context, req Request) (Response, error) {
	// Log the instant query and its timestamp in every error log, so that we have more information for debugging failures.
	logger := log.With(s.logger, "query", req.GetQuery(), "query_timestamp", req.GetStart())

	spanLog, ctx := spanlogger.NewWithLogger(ctx, logger, "splitInstantQueryByIntervalMiddleware.Do")
	defer spanLog.Span.Finish()

	_, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	if s.splitInterval <= 0 {
		return s.next.Do(ctx, req)
	}

	// Increment total number of instant queries attempted to split metrics
	s.splittingAttempts.Inc()

	mapper := astmapper.NewInstantQuerySplitter(s.splitInterval, s.logger)

	expr, err := parser.ParseExpr(req.GetQuery())
	if err != nil {
		level.Warn(spanLog).Log("msg", "failed to parse query", "err", err)
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	stats := astmapper.NewMapperStats()
	instantSplitQuery, err := mapper.Map(expr, stats)
	if err != nil {
		level.Error(spanLog).Log("msg", "failed to map the input query, falling back to try executing without splitting", "err", err)
		s.mappedSplitQueries.WithLabelValues(failureKey).Inc()
		return s.next.Do(ctx, req)
	}

	if stats.GetShardedQueries() == 0 {
		// the query cannot be split, so continue
		level.Debug(spanLog).Log("msg", "input query resulted in a no operation, falling back to try executing without splitting")
		s.mappedSplitQueries.WithLabelValues(noopKey).Inc()
		return s.next.Do(ctx, req)
	}

	level.Debug(spanLog).Log("msg", "instant query has been split by interval", "rewritten", instantSplitQuery, "split_queries", stats.GetShardedQueries())

	// Send hint with number of embedded queries to the sharding middleware
	hints := &Hints{TotalQueries: int32(stats.GetShardedQueries())}

	// Update metrics
	s.mappedSplitQueries.WithLabelValues(successKey).Inc()
	s.splitQueries.Add(float64(stats.GetShardedQueries()))
	s.splitQueriesPerQuery.Observe(float64(stats.GetShardedQueries()))

	req = req.WithQuery(instantSplitQuery.String()).WithHints(hints)
	shardedQueryable := newShardedQueryable(req, s.next)

	qry, err := newQuery(req, s.engine, lazyquery.NewLazyQueryable(shardedQueryable))
	if err != nil {
		level.Warn(spanLog).Log("msg", "failed to create new query from splittable request", "err", err)
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	res := qry.Exec(ctx)
	extracted, err := promqlResultToSamples(res)
	if err != nil {
		level.Warn(spanLog).Log("msg", "failed to execute split instant query", "err", err)
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
