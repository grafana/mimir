// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/step_align.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

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

	prunedQuery, err := p.pruneQuery(ctx, r.GetQuery())
	if err != nil {
		level.Warn(log).Log("msg", "failed to prune the input query, falling back to the original query", "query", r.GetQuery(), "err", err)

		return p.next.Do(ctx, r)
	}

	level.Debug(log).Log("msg", "query has been purged", "original", r.GetQuery(), "rewritten", prunedQuery)

	updatedReq, err := r.WithQuery(prunedQuery)
	if err != nil {
		return nil, err
	}

	return p.next.Do(ctx, updatedReq)
}

func (p *pruneMiddleware) pruneQuery(ctx context.Context, query string) (string, error) {
	mapper := astmapper.NewQueryPruner(ctx, p.logger)

	// The mapper can modify the input expression in-place, so we must re-parse the original query
	// each time before passing it to the mapper.
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", apierror.New(apierror.TypeBadData, decorateWithParamName(err, "query").Error())
	}

	prunedQuery, err := mapper.Map(expr)
	if err != nil {
		return "", err
	}

	return prunedQuery.String(), nil
}
