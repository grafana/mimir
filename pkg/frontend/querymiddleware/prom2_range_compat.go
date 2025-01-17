// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// newProm2RangeCompatMiddleware creates a new query middleware that adjusts range and resolution
// selectors in subqueries in some cases for compatibility with Prometheus 3.
func newProm2RangeCompatMiddleware(limits Limits, logger log.Logger) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &prom2RangeCompatHandler{
			next:   next,
			limits: limits,
			logger: logger,
		}
	})
}

type prom2RangeCompatHandler struct {
	next   MetricsQueryHandler
	limits Limits
	logger log.Logger
}

func (c *prom2RangeCompatHandler) Do(ctx context.Context, r MetricsQueryRequest) (Response, error) {
	spanLog := spanlogger.FromContext(ctx, c.logger)
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return c.next.Do(ctx, r)
	}

	if !validation.AllTrueBooleansPerTenant(tenantIDs, c.limits.Prom2RangeCompat) {
		return c.next.Do(ctx, r)
	}

	origQuery := r.GetQuery()
	rewritten, err := c.rewrite(ctx, origQuery)
	if err != nil {
		return c.next.Do(ctx, r)
	}

	rewrittenQuery := rewritten.String()
	if origQuery != rewrittenQuery {
		spanLog.DebugLog(
			"msg", "modified subquery for compatibility with Prometheus 3 range selectors",
			"original", origQuery,
			"rewritten", rewrittenQuery,
		)
		r, _ = r.WithExpr(rewritten)
	}

	return c.next.Do(ctx, r)
}

func (c *prom2RangeCompatHandler) rewrite(ctx context.Context, query string) (parser.Expr, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}

	mapper := astmapper.NewProm2RangeCompat(ctx)
	rewritten, err := mapper.Map(expr)
	if err != nil {
		return nil, err
	}

	return rewritten, nil
}
