// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type pruneMiddleware struct {
	next   MetricsQueryHandler
	logger log.Logger
}

// newPruneMiddleware creates a middleware that optimises queries by rewriting them to prune away
// unnecessary computations.
func newPruneMiddleware(logger log.Logger) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &pruneMiddleware{
			next:   next,
			logger: logger,
		}
	})
}

func (p *pruneMiddleware) Do(ctx context.Context, r MetricsQueryRequest) (Response, error) {
	spanLogger := spanlogger.FromContext(ctx, p.logger)

	originalExpr := r.GetQueryExpr()
	originalQuery := originalExpr.String()

	prunedExpr, success, err := p.pruneQuery(ctx, originalExpr, originalQuery)
	if err != nil {
		level.Warn(spanLogger).Log("msg", "failed to prune the input query, falling back to the original query", "query", originalQuery, "err", err)

		return p.next.Do(ctx, r)
	}

	if !success {
		spanLogger.DebugLog("msg", "query pruning had no effect", "query", originalQuery)
		return p.next.Do(ctx, r)
	}

	spanLogger.DebugLog("msg", "query has been rewritten by pruning", "original", originalQuery, "rewritten", prunedExpr)
	updatedReq, err := r.WithExpr(prunedExpr)
	if err != nil {
		return nil, err
	}

	return p.next.Do(ctx, updatedReq)
}

func (p *pruneMiddleware) pruneQuery(ctx context.Context, queryExpr parser.Expr, queryString string) (parser.Expr, bool, error) {
	mapper := astmapper.NewQueryPruner(ctx, p.logger)
	prunedQuery, err := mapper.Map(queryExpr)
	if err != nil {
		return nil, false, err
	}
	prunedQueryString := prunedQuery.String()

	return prunedQuery, queryString != prunedQueryString, nil
}
