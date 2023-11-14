// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/querier/stats"
)

type queryStatsMiddleware struct {
	engine                      *promql.Engine
	nonAlignedQueries           prometheus.Counter
	regexpMatcherCount          prometheus.Counter
	regexpMatcherOptimizedCount prometheus.Counter
	next                        Handler
}

func newQueryStatsMiddleware(reg prometheus.Registerer, engine *promql.Engine) Middleware {
	nonAlignedQueries := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_query_frontend_non_step_aligned_queries_total",
		Help: "Total queries sent that are not step aligned.",
	})
	regexpMatcherCount := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_query_frontend_regexp_matcher_count",
		Help: "Total number of regexp matchers",
	})
	regexpMatcherOptimizedCount := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_query_frontend_regexp_matcher_optimized_count",
		Help: "Total number of optimized regexp matchers",
	})

	return MiddlewareFunc(func(next Handler) Handler {
		return &queryStatsMiddleware{
			engine:                      engine,
			nonAlignedQueries:           nonAlignedQueries,
			regexpMatcherCount:          regexpMatcherCount,
			regexpMatcherOptimizedCount: regexpMatcherOptimizedCount,
			next:                        next,
		}
	})
}

func (s queryStatsMiddleware) Do(ctx context.Context, req Request) (Response, error) {
	if !isRequestStepAligned(req) {
		s.nonAlignedQueries.Inc()
	}

	if expr, err := parser.ParseExpr(req.GetQuery()); err == nil {
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

	s.populateQueryDetails(ctx, req)

	return s.next.Do(ctx, req)
}

var queryStatsErrQueryable = &storage.MockQueryable{MockQuerier: &storage.MockQuerier{SelectMockFunction: func(bool, *storage.SelectHints, ...*labels.Matcher) storage.SeriesSet {
	return storage.ErrSeriesSet(errors.New("cannot use query stats queryable for running queries"))
}}}

func (s queryStatsMiddleware) populateQueryDetails(ctx context.Context, req Request) {
	details := QueryDetailsFromContext(ctx)
	if details == nil {
		return
	}
	details.Start = time.UnixMilli(req.GetStart())
	details.End = time.UnixMilli(req.GetEnd())
	details.Step = time.Duration(req.GetStep()) * time.Millisecond

	query, err := newQuery(ctx, req, s.engine, queryStatsErrQueryable)
	if err != nil {
		return
	}
	defer query.Close()

	evalStmt, ok := query.Statement().(*parser.EvalStmt)
	if !ok {
		return
	}
	minT, maxT := promql.FindMinMaxTime(evalStmt)
	if minT != 0 {
		details.MinT = time.UnixMilli(minT)
	}
	if maxT != 0 {
		details.MaxT = time.UnixMilli(maxT)
	}
}

type QueryDetails struct {
	QuerierStats *stats.Stats

	// Start and End are the parsed start and end times of the unmodified user request.
	Start, End time.Time
	// MinT and MaxT are the earlest and latest points in time which the query might try to use.
	// For example, they account for range selectors and @ modifiers.
	// MinT and MaxT may be zero-valued if the query doesn't process samples.
	MinT, MaxT time.Time
	Step       time.Duration
}

type contextKey int

var ctxKey = contextKey(0)

// ContextWithEmptyDetails returns a context with empty QueryDetails.
// The returned context also has querier stats.Stats injected. The stats pointer in the context
// and the stats pointer in the QueryDetails are the same.
func ContextWithEmptyDetails(ctx context.Context) (*QueryDetails, context.Context) {
	stats, ctx := stats.ContextWithEmptyStats(ctx)
	details := &QueryDetails{QuerierStats: stats}
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
