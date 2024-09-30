// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
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
	log := spanlogger.FromContext(ctx, p.logger)

	prunedQuery, success, err := p.pruneQuery(ctx, r.GetQuery())
	if err != nil {
		level.Warn(log).Log("msg", "failed to prune the input query, falling back to the original query", "query", r.GetQuery(), "err", err)

		return p.next.Do(ctx, r)
	}

	if !success {
		level.Debug(log).Log("msg", "query pruning had no effect", "query", r.GetQuery())
		return p.next.Do(ctx, r)
	}

	level.Debug(log).Log("msg", "query has been rewritten by pruning", "original", r.GetQuery(), "rewritten", prunedQuery)

	updatedReq, err := r.WithQuery(prunedQuery)
	if err != nil {
		return nil, err
	}

	return p.next.Do(ctx, updatedReq)
}

func (p *pruneMiddleware) pruneQuery(ctx context.Context, query string) (string, bool, error) {
	// Parse the query.
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", false, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}
	origQueryString := expr.String()

	mapper := astmapper.NewQueryPruner(ctx, p.logger)
	prunedQuery, err := mapper.Map(expr)
	if err != nil {
		return "", false, err
	}
	prunedQueryString := prunedQuery.String()

	return prunedQueryString, origQueryString != prunedQueryString, nil
}
