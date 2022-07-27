// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/storage/lazyquery"
)

// splitByInstantIntervalMiddleware is a Middleware that can (optionally) split the instant query by splitInterval
type splitByInstantIntervalMiddleware struct {
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

// newSplitByInstantIntervalMiddleware makes a new splitByInstantIntervalMiddleware.
func newSplitByInstantIntervalMiddleware(
	splitEnabled bool,
	splitInterval time.Duration,
	limits Limits,
	logger log.Logger,
	engine *promql.Engine,
	registerer prometheus.Registerer) Middleware {

	return MiddlewareFunc(func(next Handler) Handler {
		return &splitByInstantIntervalMiddleware{
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

func (s *splitByInstantIntervalMiddleware) Do(ctx context.Context, req Request) (Response, error) {
	_, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	if !s.splitEnabled || s.splitInterval <= 0 {
		return s.next.Do(ctx, req)
	}

	// Increment total number of instant queries attempted to split metrics
	s.splittingAttempts.Inc()

	mapper, err := astmapper.NewInstantSplitter(s.splitInterval, s.logger)
	if err != nil {
		return s.next.Do(ctx, req)
	}

	expr, err := parser.ParseExpr(req.GetQuery())
	if err != nil {
		return s.next.Do(ctx, req)
	}

	stats := astmapper.NewMapperStats()
	instantSplitQuery, err := mapper.Map(expr, stats)
	if err != nil {
		s.mappedSplitQueries.WithLabelValues(FailureKey).Inc()
		return s.next.Do(ctx, req)
	}

	noop := instantSplitQuery.String() == expr.String()
	if noop {
		s.mappedSplitQueries.WithLabelValues(NoopKey).Inc()
		// the query cannot be split, so continue
		return s.next.Do(ctx, req)
	}

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
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	res := qry.Exec(ctx)
	extracted, err := promqlResultToSamples(res)
	if err != nil {
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
