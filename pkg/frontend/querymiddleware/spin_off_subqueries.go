// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/lazyquery"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	subquerySpinoffSkippedReasonParsingFailed     = "parsing-failed"
	subquerySpinoffSkippedReasonMappingFailed     = "mapping-failed"
	subquerySpinoffSkippedReasonNoSubqueries      = "no-subqueries"
	subquerySpinoffSkippedReasonDownstreamQueries = "too-many-downstream-queries"
)

type spinOffSubqueriesMiddleware struct {
	next   MetricsQueryHandler
	limits Limits
	logger log.Logger

	rangeHandler    MetricsQueryHandler
	engine          *promql.Engine
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
	engine *promql.Engine,
	rangeHandler MetricsQueryHandler,
	registerer prometheus.Registerer,
	defaultStepFunc func(int64) int64,
) MetricsQueryMiddleware {
	metrics := newSpinOffSubqueriesMetrics(registerer)

	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &spinOffSubqueriesMiddleware{
			next:            next,
			limits:          limits,
			logger:          logger,
			engine:          engine,
			rangeHandler:    rangeHandler,
			metrics:         metrics,
			defaultStepFunc: defaultStepFunc,
		}
	})
}

func (s *spinOffSubqueriesMiddleware) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	// Log the instant query and its timestamp in every error log, so that we have more information for debugging failures.
	logger := log.With(s.logger, "query", req.GetQuery(), "query_timestamp", req.GetStart())

	spanLog, ctx := spanlogger.NewWithLogger(ctx, logger, "spinOffSubqueriesMiddleware.Do")
	defer spanLog.Span.Finish()

	// For now, the feature is completely opt-in
	// So we check that the given query is allowed to be spun off
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	matched := false
	for _, tenantID := range tenantIDs {
		patterns := s.limits.InstantQueriesWithSubquerySpinOff(tenantID)

		for _, pattern := range patterns {
			matcher, err := labels.NewFastRegexMatcher(pattern)
			if err != nil {
				return nil, apierror.New(apierror.TypeBadData, err.Error())
			}

			if matcher.MatchString(req.GetQuery()) {
				matched = true
				break
			}
		}

		if matched {
			break
		}
	}

	if !matched {
		spanLog.DebugLog("msg", "expression did not match any configured subquery spin-off patterns, so subquery spin-off is disabled for this query")
		return s.next.Do(ctx, req)
	}

	// Increment total number of instant queries attempted to spin-off subqueries from.
	s.metrics.spinOffAttempts.Inc()

	mapperStats := astmapper.NewSubquerySpinOffMapperStats()
	mapperCtx, cancel := context.WithTimeout(ctx, shardingTimeout)
	defer cancel()
	mapper := astmapper.NewSubquerySpinOffMapper(mapperCtx, s.defaultStepFunc, spanLog, mapperStats)

	expr, err := parser.ParseExpr(req.GetQuery())
	if err != nil {
		level.Warn(spanLog).Log("msg", "failed to parse query", "err", err)
		s.metrics.spinOffSkipped.WithLabelValues(subquerySpinoffSkippedReasonParsingFailed).Inc()
		return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}

	spinOffQuery, err := mapper.Map(expr)
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

	queryable := newSpinOffSubqueriesQueryable(req, annotationAccumulator, s.next, s.rangeHandler)

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

	return &PrometheusResponse{
		Status: statusSuccess,
		Data: &PrometheusData{
			ResultType: string(res.Value.Type()),
			Result:     extracted,
		},
		Headers:  queryable.getResponseHeaders(),
		Warnings: warn,
		Infos:    info,
	}, nil
}

func newSpinOffQueryHandler(codec Codec, logger log.Logger, sendURL string) (MetricsQueryHandler, error) {
	rangeQueryURL, err := url.Parse(sendURL)
	if err != nil {
		return nil, fmt.Errorf("invalid spin-off URL: %w", err)
	}

	return &spinOffQueryHandler{
		codec:         codec,
		logger:        logger,
		rangeQueryURL: rangeQueryURL,
	}, nil
}

// spinOffQueryHandler is a query handler that takes a request and sends it to a remote endpoint.
type spinOffQueryHandler struct {
	codec         Codec
	logger        log.Logger
	rangeQueryURL *url.URL
}

func (s *spinOffQueryHandler) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	httpReq, err := s.codec.EncodeMetricsQueryRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("error encoding request: %w", err)
	}
	httpReq.RequestURI = "" // Reset RequestURI to force URL to be used in the request.
	// Override the URL with the configured range query URL.
	httpReq.URL.Scheme = s.rangeQueryURL.Scheme
	httpReq.URL.Host = s.rangeQueryURL.Host
	httpReq.URL.Path = s.rangeQueryURL.Path

	if err := user.InjectOrgIDIntoHTTPRequest(ctx, httpReq); err != nil {
		return nil, fmt.Errorf("error injecting org ID into request: %v", err)
	}

	client := http.DefaultClient
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()
	decoded, err := s.codec.DecodeMetricsQueryResponse(ctx, resp, req, s.logger)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}
	promRes, ok := decoded.(*PrometheusResponse)
	if !ok {
		return nil, fmt.Errorf("expected PrometheusResponse, got %T", decoded)
	}
	return promRes, nil
}
