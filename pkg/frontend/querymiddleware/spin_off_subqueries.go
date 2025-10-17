// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"errors"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/lazyquery"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	subquerySpinoffSkippedReasonParsingFailed     = "parsing-failed"
	subquerySpinoffSkippedReasonMappingFailed     = "mapping-failed"
	subquerySpinoffSkippedReasonNoSubqueries      = "no-subqueries"
	subquerySpinoffSkippedReasonDownstreamQueries = "too-many-downstream-queries"
)

type spinOffSubqueriesMiddleware struct {
	next      MetricsQueryHandler
	rangeNext MetricsQueryHandler
	limits    Limits
	logger    log.Logger

	engine          promql.QueryEngine
	defaultStepFunc func(int64) int64

	metrics spinOffSubqueriesMetrics
}

type spinOffSubqueriesMetrics struct {
	spinOffAttempts           prometheus.Counter
	spinOffSuccesses          prometheus.Counter
	spinOffSkipped            *prometheus.CounterVec
	spunOffSubqueries         prometheus.Counter
	spunOffSubqueriesPerQuery prometheus.Histogram
}

func newSpinOffSubqueriesMetrics(registerer prometheus.Registerer) spinOffSubqueriesMetrics {
	m := spinOffSubqueriesMetrics{
		spinOffAttempts: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_subquery_spinoff_attempts_total",
			Help: "Total number of queries the query-frontend attempted to spin-off subqueries from.",
		}),
		spinOffSuccesses: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_subquery_spinoff_successes_total",
			Help: "Total number of queries the query-frontend successfully spun off subqueries from.",
		}),
		spinOffSkipped: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_frontend_subquery_spinoff_skipped_total",
			Help: "Total number of queries the query-frontend skipped or failed to spin-off subqueries from.",
		}, []string{"reason"}),
		spunOffSubqueries: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_spun_off_subqueries_total",
			Help: "Total number of subqueries that were spun off.",
		}),
		spunOffSubqueriesPerQuery: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_frontend_spun_off_subqueries_per_query",
			Help:    "Number of subqueries spun off from a single query.",
			Buckets: prometheus.ExponentialBuckets(2, 2, 10),
		}),
	}

	// Initialize known label values.
	for _, reason := range []string{
		subquerySpinoffSkippedReasonParsingFailed,
		subquerySpinoffSkippedReasonMappingFailed,
		subquerySpinoffSkippedReasonNoSubqueries,
		subquerySpinoffSkippedReasonDownstreamQueries,
	} {
		m.spinOffSkipped.WithLabelValues(reason)
	}

	return m
}

func newSpinOffSubqueriesMiddleware(
	limits Limits,
	logger log.Logger,
	engine promql.QueryEngine,
	registerer prometheus.Registerer,
	rangeMiddleware MetricsQueryMiddleware,
	defaultStepFunc func(int64) int64,
) MetricsQueryMiddleware {
	metrics := newSpinOffSubqueriesMetrics(registerer)
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		rangeNext := next
		if rangeMiddleware != nil {
			rangeNext = rangeMiddleware.Wrap(next)
		}

		return &spinOffSubqueriesMiddleware{
			next:            next,
			rangeNext:       rangeNext,
			limits:          limits,
			logger:          logger,
			engine:          engine,
			metrics:         metrics,
			defaultStepFunc: defaultStepFunc,
		}
	})
}

func (s *spinOffSubqueriesMiddleware) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	// Log the instant query and its timestamp in every error log, so that we have more information for debugging failures.
	logger := log.With(s.logger, "query", req.GetQuery(), "query_timestamp", req.GetStart())

	spanLog, ctx := spanlogger.New(ctx, logger, tracer, "spinOffSubqueriesMiddleware.Do")
	defer spanLog.Finish()

	// For now, the feature is completely opt-in
	// So we check that the given query is allowed to be spun off
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	if !validation.AllTrueBooleansPerTenant(tenantIDs, s.limits.SubquerySpinOffEnabled) {
		spanLog.DebugLog("msg", "subquery spin-off is disabled for a tenant", "tenant_ids", tenantIDs)
		return s.next.Do(ctx, req)
	}

	// Increment total number of instant queries attempted to spin-off subqueries from.
	s.metrics.spinOffAttempts.Inc()

	mapperStats := astmapper.NewSubquerySpinOffMapperStats()
	mapperCtx, cancel := context.WithTimeout(ctx, shardingTimeout)
	defer cancel()
	mapper := astmapper.NewSubquerySpinOffMapper(s.defaultStepFunc, spanLog, mapperStats)

	expr, err := astmapper.CloneExpr(req.GetParsedQuery())
	if err != nil {
		level.Warn(spanLog).Log("msg", "failed to parse query", "err", err)
		s.metrics.spinOffSkipped.WithLabelValues(subquerySpinoffSkippedReasonParsingFailed).Inc()
		return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}

	spinOffQuery, err := mapper.Map(mapperCtx, expr)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			level.Error(spanLog).Log("msg", "timeout while spinning off subqueries, please fill in a bug report with this query, falling back to try executing without spin-off", "err", err)
		} else {
			level.Error(spanLog).Log("msg", "failed to map the input query, falling back to try executing without spin-off", "err", err)
		}
		s.metrics.spinOffSkipped.WithLabelValues(subquerySpinoffSkippedReasonMappingFailed).Inc()
		return s.next.Do(ctx, req)
	}

	if mapperStats.SpunOffSubqueries() == 0 {
		// the query has no subqueries, so continue downstream
		spanLog.DebugLog("msg", "input query resulted in a no operation, falling back to try executing without spinning off subqueries")
		s.metrics.spinOffSkipped.WithLabelValues(subquerySpinoffSkippedReasonNoSubqueries).Inc()
		return s.next.Do(ctx, req)
	}

	if mapperStats.DownstreamQueries() > mapperStats.SpunOffSubqueries() {
		// the query has more downstream queries than subqueries, so continue downstream
		// It's probably more efficient to just execute the query as is
		spanLog.DebugLog("msg", "input query resulted in more downstream queries than subqueries, falling back to try executing without spinning off subqueries")
		s.metrics.spinOffSkipped.WithLabelValues(subquerySpinoffSkippedReasonDownstreamQueries).Inc()
		return s.next.Do(ctx, req)
	}

	spanLog.DebugLog("msg", "instant query has been rewritten to spin-off subqueries", "rewritten", spinOffQuery, "regular_downstream_queries", mapperStats.DownstreamQueries(), "subqueries_spun_off", mapperStats.SpunOffSubqueries())

	// Update query stats.
	queryStats := stats.FromContext(ctx)
	queryStats.AddSpunOffSubqueries(uint32(mapperStats.SpunOffSubqueries()))

	// Update metrics.
	s.metrics.spinOffSuccesses.Inc()
	s.metrics.spunOffSubqueries.Add(float64(mapperStats.SpunOffSubqueries()))
	s.metrics.spunOffSubqueriesPerQuery.Observe(float64(mapperStats.SpunOffSubqueries()))

	// Send hint with number of embedded queries to the sharding middleware
	req, err = req.WithExpr(spinOffQuery)
	if err != nil {
		return nil, err
	}

	annotationAccumulator := NewAnnotationAccumulator()

	queryable := newSpinOffSubqueriesQueryable(req, annotationAccumulator, s.next, s.rangeNext)

	qry, err := newQuery(ctx, req, s.engine, lazyquery.NewLazyQueryable(queryable))
	if err != nil {
		level.Warn(spanLog).Log("msg", "failed to create new query from subquery spin request", "err", err)
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	res := qry.Exec(ctx)
	extracted, err := promqlResultToSamples(res)
	if err != nil {
		level.Warn(spanLog).Log("msg", "failed to execute spun off subquery", "err", err)
		return nil, mapEngineError(err)
	}

	// Note that the positions based on the original query may be wrong as the rewritten
	// query which is actually used is different, but the user does not see the rewritten
	// query, so we pass in an empty string as the query so the positions will be hidden.
	warn, info := res.Warnings.AsStrings("", 0, 0)

	// Add any annotations returned by the sharded queries, and remove any duplicates.
	// We remove any position information for the same reason as above: the position information
	// relates to the rewritten expression sent to queriers, not the original expression provided by the user.
	accumulatedWarnings, accumulatedInfos := annotationAccumulator.getAll()
	warn = append(warn, removeAllAnnotationPositionInformation(accumulatedWarnings)...)
	info = append(info, removeAllAnnotationPositionInformation(accumulatedInfos)...)
	warn = removeDuplicates(warn)
	info = removeDuplicates(info)

	return &PrometheusResponseWithFinalizer{
		PrometheusResponse: &PrometheusResponse{
			Status: statusSuccess,
			Data: &PrometheusData{
				ResultType: string(res.Value.Type()),
				Result:     extracted,
			},
			Headers:  queryable.getResponseHeaders(),
			Warnings: warn,
			Infos:    info,
		},
		finalizer: qry.Close,
	}, nil
}
