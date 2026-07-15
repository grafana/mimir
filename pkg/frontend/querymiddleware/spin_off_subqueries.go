// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/subqueryspinoff"
	"github.com/grafana/mimir/pkg/storage/lazyquery"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

type spinOffSubqueriesMiddleware struct {
	next      MetricsQueryHandler
	rangeNext MetricsQueryHandler
	limits    Limits
	logger    log.Logger

	engine          promql.QueryEngine
	defaultStepFunc func(int64) int64
	wrapper         astmapper.SubquerySpinOffWrapper

	metrics subqueryspinoff.Metrics
}

func newSpinOffSubqueriesMiddleware(
	limits Limits,
	logger log.Logger,
	engine promql.QueryEngine,
	registerer prometheus.Registerer,
	rangeMiddleware MetricsQueryMiddleware,
	defaultStepFunc func(int64) int64,
) MetricsQueryMiddleware {
	metrics := subqueryspinoff.NewMetrics(registerer)
	wrapper := astmapper.NewSelectorSubquerySpinOffWrapper()
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
			wrapper:         wrapper,
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
	s.metrics.SpinOffAttempts.Inc()

	expr, err := req.GetClonedParsedQuery()
	if err != nil {
		level.Warn(spanLog).Log("msg", "failed to parse query", "err", err)
		s.metrics.SpinOffSkipped.WithLabelValues(subqueryspinoff.SkippedReasonParsingFailed).Inc()
		return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}

	spinOffQuery, ok := subqueryspinoff.Map(ctx, expr, s.wrapper, s.metrics, s.defaultStepFunc, shardingTimeout, spanLog)
	if !ok {
		// Spinning off subqueries was not possible or not worthwhile, so fall back to executing the query downstream.
		return s.next.Do(ctx, req)
	}

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

	// Ownership of qry is transferred to the response finalizer on success. On
	// any failure path before that hand-off we must Close qry ourselves, otherwise
	// the query's resources (memory consumption tracker, pooled buffers, evaluator
	// context) leak.
	shouldCloseQuery := true
	defer func() {
		if shouldCloseQuery {
			qry.Close()
		}
	}()

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

	resp := &PrometheusResponseWithFinalizer{
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
	}

	shouldCloseQuery = false
	return resp, nil
}
