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

const (
	skippedReasonParsingFailed = "parsing-failed"
	skippedReasonMappingFailed = "mapping-failed"
	skippedReasonNoop          = "noop"
)

// splitInstantQueryByIntervalMiddleware is a Middleware that can (optionally) split the instant query by splitInterval
type splitInstantQueryByIntervalMiddleware struct {
	next   Handler
	limits Limits
	logger log.Logger

	engine *promql.Engine

	splitInterval time.Duration

	metrics instantQuerySplittingMetrics
}

type instantQuerySplittingMetrics struct {
	splittingAttempts    prometheus.Counter
	splittingSuccesses   prometheus.Counter
	splittingSkipped     *prometheus.CounterVec
	splitQueries         prometheus.Counter
	splitQueriesPerQuery prometheus.Histogram
}

func newInstantQuerySplittingMetrics(registerer prometheus.Registerer) instantQuerySplittingMetrics {
	m := instantQuerySplittingMetrics{
		splittingAttempts: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_instant_query_splitting_rewrites_attempted_total",
			Help: "Total number of instant queries the query-frontend attempted to split by interval.",
		}),
		splittingSuccesses: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_instant_query_splitting_rewrites_succeeded_total",
			Help: "Total number of instant queries the query-frontend successfully split by interval.",
		}),
		splittingSkipped: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_frontend_instant_query_splitting_rewrites_skipped_total",
			Help: "Total number of instant queries the query-frontend skipped or failed to split by interval.",
		}, []string{"reason"}),
		splitQueries: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_instant_query_split_queries_total",
			Help: "Total number of split partial queries.",
		}),
		splitQueriesPerQuery: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_frontend_instant_query_split_queries_per_query",
			Help:    "Number of split partial queries a single instant query has been rewritten to.",
			Buckets: prometheus.ExponentialBuckets(2, 2, 10),
		}),
	}

	// Initialize known label values.
	for _, reason := range []string{skippedReasonParsingFailed, skippedReasonMappingFailed, skippedReasonNoop} {
		m.splittingSkipped.WithLabelValues(reason)
	}

	return m
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
			next:          next,
			limits:        limits,
			splitInterval: splitInterval,
			logger:        logger,
			engine:        engine,
			metrics:       metrics,
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
	s.metrics.splittingAttempts.Inc()

	mapper := astmapper.NewInstantQuerySplitter(s.splitInterval, s.logger)

	expr, err := parser.ParseExpr(req.GetQuery())
	if err != nil {
		level.Warn(spanLog).Log("msg", "failed to parse query", "err", err)
		s.metrics.splittingSkipped.WithLabelValues(skippedReasonParsingFailed).Inc()
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	stats := astmapper.NewMapperStats()
	instantSplitQuery, err := mapper.Map(expr, stats)
	if err != nil {
		level.Error(spanLog).Log("msg", "failed to map the input query, falling back to try executing without splitting", "err", err)
		s.metrics.splittingSkipped.WithLabelValues(skippedReasonMappingFailed).Inc()
		return s.next.Do(ctx, req)
	}

	if stats.GetShardedQueries() == 0 {
		// the query cannot be split, so continue
		level.Debug(spanLog).Log("msg", "input query resulted in a no operation, falling back to try executing without splitting")
		s.metrics.splittingSkipped.WithLabelValues(skippedReasonNoop).Inc()
		return s.next.Do(ctx, req)
	}

	level.Debug(spanLog).Log("msg", "instant query has been split by interval", "rewritten", instantSplitQuery, "split_queries", stats.GetShardedQueries())

	// Send hint with number of embedded queries to the sharding middleware
	hints := &Hints{TotalQueries: int32(stats.GetShardedQueries())}

	// Update metrics
	s.metrics.splittingSuccesses.Inc()
	s.metrics.splitQueries.Add(float64(stats.GetShardedQueries()))
	s.metrics.splitQueriesPerQuery.Observe(float64(stats.GetShardedQueries()))

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
