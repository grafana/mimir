// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

type queryStatsMiddleware struct {
	lookbackDelta                     time.Duration
	regexpMatcherCount                prometheus.Counter
	regexpMatcherOptimizedCount       prometheus.Counter
	consistencyCounter                *prometheus.CounterVec
	queryExpressionSizeBytesHistogram *prometheus.HistogramVec
	next                              MetricsQueryHandler
}

func newQueryStatsMiddleware(reg prometheus.Registerer, engineOpts promql.EngineOpts) MetricsQueryMiddleware {
	regexpMatcherCount := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_query_frontend_regexp_matchers_total",
		Help: "Total number of regexp matchers",
	})
	regexpMatcherOptimizedCount := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_query_frontend_regexp_matchers_optimized_total",
		Help: "Total number of optimized regexp matchers",
	})
	consistencyCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_frontend_queries_consistency_total",
		Help: "Total number of queries that explicitly request a level of consistency.",
	}, []string{"user", "consistency"})
	queryExpressionSizeBytesHistogram := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "cortex_query_frontend_queries_expression_bytes",
		Help:                            "Histogram of the length of query expressions requested.",
		NativeHistogramBucketFactor:     1.4,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: 1 * time.Hour,
	}, []string{"user"})

	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &queryStatsMiddleware{
			lookbackDelta:                     streamingpromql.DetermineLookbackDelta(engineOpts),
			regexpMatcherCount:                regexpMatcherCount,
			regexpMatcherOptimizedCount:       regexpMatcherOptimizedCount,
			consistencyCounter:                consistencyCounter,
			queryExpressionSizeBytesHistogram: queryExpressionSizeBytesHistogram,
			next:                              next,
		}
	})
}

func (s queryStatsMiddleware) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	s.trackRegexpMatchers(req)
	s.trackReadConsistency(ctx)
	s.trackQueryExpressionSize(ctx, req)
	s.populateQueryDetails(ctx, req)

	return s.next.Do(ctx, req)
}

func (s queryStatsMiddleware) trackRegexpMatchers(req MetricsQueryRequest) {
	expr, err := req.GetClonedParsedQuery()
	if err != nil {
		return
	}
	for _, selectors := range parser.ExtractSelectors(expr) {
		for _, matcher := range selectors {
			if matcher.Type != labels.MatchRegexp && matcher.Type != labels.MatchNotRegexp {
				continue
			}

			s.regexpMatcherCount.Inc()
			if matcher.IsRegexOptimized() {
				s.regexpMatcherOptimizedCount.Inc()
			}
		}
	}
}

func (s queryStatsMiddleware) trackQueryExpressionSize(ctx context.Context, req MetricsQueryRequest) {
	queryExpressionLength := len(req.GetQuery())

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return
	}

	for _, tenantID := range tenantIDs {
		s.queryExpressionSizeBytesHistogram.WithLabelValues(tenantID).Observe(float64(queryExpressionLength))
	}
}

func (s queryStatsMiddleware) populateQueryDetails(ctx context.Context, req MetricsQueryRequest) {
	details := QueryDetailsFromContext(ctx)
	if details == nil {
		return
	}
	// This middleware may run multiple times for the same request in case of a remote read request
	// (once for each query in the request). In such case, we compute the start/end time as the min/max
	// timestamp we see across all queries in the request.
	if details.Start.IsZero() || details.Start.After(time.UnixMilli(req.GetStart())) {
		details.Start = time.UnixMilli(req.GetStart())
	}
	if details.End.IsZero() || details.End.Before(time.UnixMilli(req.GetEnd())) {
		details.End = time.UnixMilli(req.GetEnd())
	}
	details.Step = time.Duration(req.GetStep()) * time.Millisecond

	minT, maxT, ok := ExtractMinMaxTime(ctx, req, s.lookbackDelta)
	if !ok {
		return
	}

	// This middleware may run multiple times for the same request in case of a remote read request
	// (once for each query in the request). In such case, we compute the minT/maxT time as the min/max
	// timestamp we see across all queries in the request.
	if minT != 0 && (details.MinT.IsZero() || details.MinT.After(time.UnixMilli(minT))) {
		details.MinT = time.UnixMilli(minT)
	}
	if maxT != 0 && (details.MaxT.IsZero() || details.MaxT.Before(time.UnixMilli(maxT))) {
		details.MaxT = time.UnixMilli(maxT)
	}
}

func (s queryStatsMiddleware) trackReadConsistency(ctx context.Context) {
	consistency, ok := api.ReadConsistencyLevelFromContext(ctx)
	if !ok {
		return
	}
	tenants, _ := tenant.TenantIDs(ctx)
	for _, tenantID := range tenants {
		s.consistencyCounter.WithLabelValues(tenantID, consistency).Inc()
	}
}

type QueryDetails struct {
	QuerierStats *stats.SafeStats

	// Start and End are the parsed start and end times of the unmodified user request.
	Start, End time.Time
	// MinT and MaxT are the earliest and latest points in time which the query might try to use.
	// For example, they account for range selectors and @ modifiers.
	// MinT and MaxT may be zero-valued if the query doesn't process samples.
	MinT, MaxT time.Time
	Step       time.Duration

	ResultsCacheMissBytes int
	ResultsCacheHitBytes  int
}

type contextKey int

var ctxKey = contextKey(0)

// ContextWithEmptyDetails returns a context with empty QueryDetails.
// The returned context also has querier stats.Stats injected. The stats pointer in the context
// and the stats pointer in the QueryDetails are the same.
func ContextWithEmptyDetails(ctx context.Context) (*QueryDetails, context.Context) {
	s, ctx := stats.ContextWithEmptyStats(ctx)
	details := &QueryDetails{QuerierStats: s}
	ctx = context.WithValue(ctx, ctxKey, details)
	return details, ctx
}

// QueryDetailsFromContext gets the QueryDetails out of the Context. Returns nil if stats have not
// been initialised in the context.
func QueryDetailsFromContext(ctx context.Context) *QueryDetails {
	o := ctx.Value(ctxKey)
	if o == nil {
		return nil
	}
	return o.(*QueryDetails)
}

// ExtractMinMaxTime extracts the min and max timestamps that may be accessed by the query.
// TODO: do we need the lookbackDelta as an argument? Can't we use req.GetLookbackDelta()?
func ExtractMinMaxTime(ctx context.Context, req MetricsQueryRequest, lookbackDelta time.Duration) (int64, int64, bool) {
	switch r := req.(type) {
	case *PrometheusRangeQueryRequest, *PrometheusInstantQueryRequest:
		expr, err := promqlext.NewPromQLParser().ParseExpr(r.GetQuery())
		if err != nil {
			return 0, 0, false
		}

		evalStmt := &parser.EvalStmt{
			Expr:          expr,
			Start:         util.TimeFromMillis(req.GetStart()),
			End:           util.TimeFromMillis(req.GetEnd()),
			Interval:      time.Duration(req.GetStep()) * time.Millisecond,
			LookbackDelta: lookbackDelta,
		}

		minT, maxT := promql.FindMinMaxTime(evalStmt)
		return minT, maxT, true
	case *remoteReadQueryRequest:
		minT := r.GetStart() + 1 // The query time range is left-open, but minT is expected to be inclusive.
		return minT, r.GetEnd(), true
	default:
		return 0, 0, false
	}
}
