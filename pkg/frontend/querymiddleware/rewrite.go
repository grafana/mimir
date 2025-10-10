// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type rewriteMiddleware struct {
	next   MetricsQueryHandler
	logger log.Logger
	cfg    Config

	rewriteMetrics
}

type rewriteMetrics struct {
	reorderHistogramAggregationAttempts prometheus.Counter
	reorderHistogramAggregationRewrites prometheus.Counter
	propagateMatchersAttempts           prometheus.Counter
	propagateMatchersRewrites           prometheus.Counter
}

// newRewriteMiddleware creates a middleware that optimises queries by rewriting them to avoid
// unnecessary computations.
func newRewriteMiddleware(
	logger log.Logger,
	cfg Config,
	registerer prometheus.Registerer,
) MetricsQueryMiddleware {
	metrics := rewriteMetrics{
		reorderHistogramAggregationAttempts: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_rewrites_reorder_histogram_aggregation_attempted_total",
			Help: "Total number of queries the query-frontend attempted to rewrite by reordering histogram aggregations.",
		}),
		reorderHistogramAggregationRewrites: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_rewrites_reorder_histogram_aggregation_succeeded_total",
			Help: "Total number of queries where the query-frontend has rewritten the query by reordering histogram aggregations.",
		}),
		propagateMatchersAttempts: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_rewrites_propagate_matchers_attempted_total",
			Help: "Total number of queries the query-frontend attempted to rewrite by propagating matchers across binary operations.",
		}),
		propagateMatchersRewrites: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_rewrites_propagate_matchers_succeeded_total",
			Help: "Total number of queries where the query-frontend has rewritten the query by propagating matchers across binary operations.",
		}),
	}
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &rewriteMiddleware{
			next:           next,
			logger:         logger,
			cfg:            cfg,
			rewriteMetrics: metrics,
		}
	})
}

func (m *rewriteMiddleware) Do(ctx context.Context, r MetricsQueryRequest) (Response, error) {
	log := spanlogger.FromContext(ctx, m.logger)

	rewrittenQuery, success, err := m.rewriteQuery(ctx, r.GetParsedQuery())
	if err != nil {
		return nil, err
	}

	if !success {
		level.Debug(log).Log("msg", "query rewriting had no effect", "query", r.GetQuery())
		return m.next.Do(ctx, r)
	}

	level.Debug(log).Log("msg", "query has been rewritten", "original", r.GetQuery(), "rewritten", rewrittenQuery)

	updatedReq, err := r.WithExpr(rewrittenQuery)
	if err != nil {
		return nil, err
	}

	return m.next.Do(ctx, updatedReq)
}

func (m *rewriteMiddleware) rewriteQuery(ctx context.Context, expr parser.Expr) (parser.Expr, bool, error) {
	rewrittenQuery, err := astmapper.CloneExpr(expr)
	if err != nil {
		return nil, false, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}
	changed := false

	if m.cfg.RewriteQueriesHistogram {
		mapperHistogram := ast.NewReorderHistogramAggregationMapper()
		m.reorderHistogramAggregationAttempts.Inc()
		var err error
		rewrittenQuery, err = mapperHistogram.Map(ctx, rewrittenQuery)
		if err != nil {
			return nil, false, err
		}
		if mapperHistogram.HasChanged() {
			changed = true
			m.reorderHistogramAggregationRewrites.Inc()
		}
	}

	if m.cfg.RewriteQueriesPropagateMatchers {
		mapperPropagateMatchers := ast.NewPropagateMatchersMapper()
		m.propagateMatchersAttempts.Inc()
		var err error
		rewrittenQuery, err = mapperPropagateMatchers.Map(ctx, rewrittenQuery)
		if err != nil {
			return nil, false, err
		}
		if mapperPropagateMatchers.HasChanged() {
			changed = true
			m.propagateMatchersRewrites.Inc()
		}
	}

	if changed {
		return rewrittenQuery, true, nil
	}
	return nil, false, nil
}
