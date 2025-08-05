// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type rewriteMiddleware struct {
	next                    MetricsQueryHandler
	logger                  log.Logger
	cfg                     Config
	mapperHistogram         astmapper.ASTMapper
	mapperPropagateMatchers astmapper.ASTMapper
}

// newRewriteMiddleware creates a middleware that optimises queries by rewriting them to avoid
// unnecessary computations.
func newRewriteMiddleware(logger log.Logger, cfg Config) MetricsQueryMiddleware {
	mapperHistogram := ast.NewMapperReorderHistogramAggregation()
	mapperPropagateMatchers := ast.NewMapperPropagateMatchers()
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &rewriteMiddleware{
			next:                    next,
			logger:                  logger,
			cfg:                     cfg,
			mapperHistogram:         mapperHistogram,
			mapperPropagateMatchers: mapperPropagateMatchers,
		}
	})
}

func (m *rewriteMiddleware) Do(ctx context.Context, r MetricsQueryRequest) (Response, error) {
	log := spanlogger.FromContext(ctx, m.logger)

	rewrittenQuery, success, err := m.rewriteQuery(ctx, r.GetQuery())
	if err != nil {
		level.Warn(log).Log("msg", "failed to rewrite the input query, falling back to the original query", "query", r.GetQuery(), "err", err)

		return m.next.Do(ctx, r)
	}

	if !success {
		level.Debug(log).Log("msg", "query rewriting had no effect", "query", r.GetQuery())
		return m.next.Do(ctx, r)
	}

	level.Debug(log).Log("msg", "query has been rewritten", "original", r.GetQuery(), "rewritten", rewrittenQuery)

	updatedReq, err := r.WithQuery(rewrittenQuery)
	if err != nil {
		return nil, err
	}

	return m.next.Do(ctx, updatedReq)
}

func (m *rewriteMiddleware) rewriteQuery(ctx context.Context, query string) (string, bool, error) {
	// Parse the query.
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", false, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}
	origQueryString := expr.String()
	rewrittenQuery := expr

	if m.cfg.RewriteQueriesHistogram {
		rewrittenQuery, err = m.mapperHistogram.Map(ctx, rewrittenQuery)
		if err != nil {
			return "", false, err
		}
	}

	if m.cfg.RewriteQueriesPropagateMatchers {
		rewrittenQuery, err = m.mapperPropagateMatchers.Map(ctx, rewrittenQuery)
		if err != nil {
			return "", false, err
		}
	}

	rewrittenQueryString := rewrittenQuery.String()

	return rewrittenQueryString, origQueryString != rewrittenQueryString, nil
}
