// SPDX-License-Identifier: AGPL-3.0-only

package subqueryspinoff

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	frontendspinoff "github.com/grafana/mimir/pkg/frontend/querymiddleware/subqueryspinoff"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// mapTimeout bounds how long the subquery spin-off mapping is allowed to run, matching the timeout
// used by the query-frontend subquery spin-off middleware.
const mapTimeout = 10 * time.Second

// Limits exposes the per-tenant configuration required by the subquery spin-off optimization pass.
type Limits interface {
	// SubquerySpinOffEnabled returns whether spinning off subqueries from instant queries is enabled for
	// the given tenant.
	SubquerySpinOffEnabled(userID string) bool
}

// OptimizationPass spins off subqueries from instant queries as separate range queries, mirroring the
// query-frontend subquery spin-off middleware but implemented as an AST optimization pass so that the
// spun-off subqueries can be sharded, split, cached and executed remotely inside the Mimir query
// engine.
//
// It runs before the sharding optimization pass.
type OptimizationPass struct {
	limits          Limits
	defaultStepFunc func(rangeMillis int64) int64
	metrics         frontendspinoff.Metrics
	wrapper         astmapper.SubquerySpinOffWrapper
	opts            frontendspinoff.Options
	logger          log.Logger
}

var _ optimize.ASTOptimizationPass = (*OptimizationPass)(nil)

func NewOptimizationPass(limits Limits, defaultStepFunc func(rangeMillis int64) int64, opts frontendspinoff.Options, reg prometheus.Registerer, logger log.Logger) *OptimizationPass {
	return &OptimizationPass{
		limits:          limits,
		defaultStepFunc: defaultStepFunc,
		metrics:         frontendspinoff.NewMetrics(reg),
		wrapper:         NewEvaluationRootWrapper(),
		opts:            opts,
		logger:          logger,
	}
}

func (o *OptimizationPass) Name() string {
	return "Subquery spin-off"
}

func (o *OptimizationPass) Apply(ctx context.Context, expr parser.Expr, timeRange types.QueryTimeRange) (parser.Expr, error) {
	// Subquery spin-off only applies to instant queries: range queries are left unchanged.
	if !timeRange.IsInstant {
		return expr, nil
	}

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Spinning off subqueries is opt-in, and is only applied when it is enabled for all tenants in the
	// query.
	if !validation.AllTrueBooleansPerTenant(tenantIDs, o.limits.SubquerySpinOffEnabled) {
		return expr, nil
	}

	spanLog := spanlogger.FromContext(ctx, o.logger)

	o.metrics.SpinOffAttempts.Inc()

	spinOffQuery, ok := frontendspinoff.Map(ctx, expr, o.wrapper, o.metrics, o.defaultStepFunc, mapTimeout, spanLog, o.opts)
	if !ok {
		// Spinning off subqueries was not possible or not worthwhile, so leave the query unchanged.
		return expr, nil
	}

	return spinOffQuery, nil
}
