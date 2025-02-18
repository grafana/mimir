// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// newProm2RangeCompatMiddleware creates a new query middleware that adjusts range and resolution
// selectors in subqueries in some cases for compatibility with Prometheus 3.
func newProm2RangeCompatMiddleware(limits Limits, logger log.Logger, reg prometheus.Registerer) MetricsQueryMiddleware {
	rewritten := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_frontend_prom2_range_compat_rewritten_total",
		Help: "The number of times a query was rewritten for Prometheus 2/3 range selector compatibility",
	}, []string{"user"})

	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &prom2RangeCompatHandler{
			next:      next,
			limits:    limits,
			logger:    logger,
			rewritten: rewritten,
		}
	})
}

type prom2RangeCompatHandler struct {
	next      MetricsQueryHandler
	limits    Limits
	logger    log.Logger
	rewritten *prometheus.CounterVec
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

	origExpr := r.GetQueryExpr()
	origQuery := origExpr.String()

	rewritten, err := c.rewrite(ctx, origExpr)
	if err != nil {
		return c.next.Do(ctx, r)
	}

	rewrittenQuery := rewritten.String()
	if origQuery != rewrittenQuery {
		tenantIDString := tenant.JoinTenantIDs(tenantIDs)
		c.rewritten.WithLabelValues(tenantIDString).Inc()
		spanLog.DebugLog(
			"msg", "modified subquery for compatibility with Prometheus 3 range selectors",
			"tenants", tenantIDString,
			"original", origQuery,
			"rewritten", rewrittenQuery,
		)
		r, _ = r.WithExpr(rewritten)
	}

	return c.next.Do(ctx, r)
}

func (c *prom2RangeCompatHandler) rewrite(ctx context.Context, expr parser.Expr) (parser.Expr, error) {
	mapper := astmapper.NewProm2RangeCompat(ctx)
	rewritten, err := mapper.Map(expr)
	if err != nil {
		return nil, err
	}

	return rewritten, nil
}
