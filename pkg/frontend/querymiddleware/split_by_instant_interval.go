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
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/lazyquery"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	skippedReasonParsingFailed = "parsing-failed"
	skippedReasonMappingFailed = "mapping-failed"
)

// splitInstantQueryByIntervalMiddleware is a Middleware that can (optionally) split the instant query by splitInterval
type splitInstantQueryByIntervalMiddleware struct {
	next   Handler
	limits Limits
	logger log.Logger

	engine *promql.Engine

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
	for _, reason := range []string{skippedReasonParsingFailed, skippedReasonMappingFailed,
		string(astmapper.SkippedReasonSmallInterval), string(astmapper.SkippedReasonSubquery), string(astmapper.SkippedReasonNonSplittable)} {
		m.splittingSkipped.WithLabelValues(reason)
	}

	return m
}

// newSplitInstantQueryByIntervalMiddleware makes a new splitInstantQueryByIntervalMiddleware.
func newSplitInstantQueryByIntervalMiddleware(
	limits Limits,
	logger log.Logger,
	engine *promql.Engine,
	registerer prometheus.Registerer) Middleware {
	metrics := newInstantQuerySplittingMetrics(registerer)

	return MiddlewareFunc(func(next Handler) Handler {
		return &splitInstantQueryByIntervalMiddleware{
			next:    next,
			limits:  limits,
			logger:  logger,
			engine:  engine,
			metrics: metrics,
		}
	})
}

func (s *splitInstantQueryByIntervalMiddleware) Do(ctx context.Context, req Request) (Response, error) {
	// Log the instant query and its timestamp in every error log, so that we have more information for debugging failures.
	logger := log.With(s.logger, "query", req.GetQuery(), "query_timestamp", req.GetStart())

	spanLog, ctx := spanlogger.NewWithLogger(ctx, logger, "splitInstantQueryByIntervalMiddleware.Do")
	defer spanLog.Span.Finish()

	tenantsIds, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	splitInterval := s.getSplitIntervalForQuery(tenantsIds, req, logger)
	if splitInterval <= 0 {
		level.Debug(logger).Log("msg", "query splitting is disabled for this query or tenant")
		return s.next.Do(ctx, req)
	}

	// Increment total number of instant queries attempted to split metrics
	s.metrics.splittingAttempts.Inc()

	mapperStats := astmapper.NewInstantSplitterStats()
	mapper := astmapper.NewInstantQuerySplitter(splitInterval, s.logger, mapperStats)

	expr, err := parser.ParseExpr(req.GetQuery())
	if err != nil {
		level.Warn(spanLog).Log("msg", "failed to parse query", "err", err)
		s.metrics.splittingSkipped.WithLabelValues(skippedReasonParsingFailed).Inc()
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	instantSplitQuery, err := mapper.Map(expr)
	if err != nil {
		level.Error(spanLog).Log("msg", "failed to map the input query, falling back to try executing without splitting", "err", err)
		s.metrics.splittingSkipped.WithLabelValues(skippedReasonMappingFailed).Inc()
		return s.next.Do(ctx, req)
	}

	if mapperStats.GetSplitQueries() == 0 {
		// the query cannot be split, so continue
		level.Debug(spanLog).Log("msg", "input query resulted in a no operation, falling back to try executing without splitting")
		switch mapperStats.GetSkippedReason() {
		case astmapper.SkippedReasonSmallInterval:
			s.metrics.splittingSkipped.WithLabelValues(string(astmapper.SkippedReasonSmallInterval)).Inc()
		case astmapper.SkippedReasonSubquery:
			s.metrics.splittingSkipped.WithLabelValues(string(astmapper.SkippedReasonSubquery)).Inc()
		default:
			// If there are no split queries, the default skipped reason case is a non-splittable query
			s.metrics.splittingSkipped.WithLabelValues(string(astmapper.SkippedReasonNonSplittable)).Inc()
		}
		return s.next.Do(ctx, req)
	}

	level.Debug(spanLog).Log("msg", "instant query has been split by interval", "rewritten", instantSplitQuery, "split_queries", mapperStats.GetSplitQueries())

	// Send hint with number of embedded queries to the sharding middleware
	hints := &Hints{TotalQueries: int32(mapperStats.GetSplitQueries())}

	// Update query stats.
	queryStats := stats.FromContext(ctx)
	queryStats.AddSplitQueries(uint32(mapperStats.GetSplitQueries()))

	// Update metrics.
	s.metrics.splittingSuccesses.Inc()
	s.metrics.splitQueries.Add(float64(mapperStats.GetSplitQueries()))
	s.metrics.splitQueriesPerQuery.Observe(float64(mapperStats.GetSplitQueries()))

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

// getSplitIntervalForQuery calculates and return the split interval that should be used to run the instant query.
func (s *splitInstantQueryByIntervalMiddleware) getSplitIntervalForQuery(tenantsIds []string, r Request, spanLog log.Logger) time.Duration {
	// Check if splitting is disabled for the given request.
	if r.GetOptions().InstantSplitDisabled {
		return 0
	}

	splitInterval := validation.SmallestPositiveNonZeroDurationPerTenant(tenantsIds, s.limits.SplitInstantQueriesByInterval)
	if splitInterval <= 0 {
		return 0
	}

	// Honor the split interval specified in the request (if any).
	if r.GetOptions().InstantSplitInterval > 0 {
		splitInterval = time.Duration(r.GetOptions().InstantSplitInterval)
	}

	level.Debug(spanLog).Log("msg", "getting split instant query interval", "tenantsIds", tenantsIds, "split interval", splitInterval)

	return splitInterval
}
