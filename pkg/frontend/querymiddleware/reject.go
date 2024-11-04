// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util/validation"
)

type rejectMiddleware struct {
	next   MetricsQueryHandler
	limits Limits
	logger log.Logger
}

// newRejectMiddleware creates a middleware that blocks queries that contain PromQL experimental functions
// that are not enabled for the active tenant, allowing us to enable them only for selected tenants.
func newRejectMiddleware(limits Limits, logger log.Logger) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &rejectMiddleware{
			next:   next,
			limits: limits,
			logger: logger,
		}
	})
}

func (rm *rejectMiddleware) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	// log := spanlogger.FromContext(ctx, rm.logger)

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	experimentalFunctionsEnabled := validation.AllTrueBooleansPerTenant(tenantIDs, rm.limits.PromQLExperimentalFunctionsEnabled)

	if experimentalFunctionsEnabled {
		return rm.next.Do(ctx, req)
	}

	expr, err := parser.ParseExpr(req.GetQuery())
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}

	if res, name := containsExperimentalFunction(expr); res {
		err := fmt.Errorf("function %q is not enabled for the active tenant", name)
		return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}

	return rm.next.Do(ctx, req)
}

// containsExperimentalFunction checks if the query contains PromQL experimental functions.
func containsExperimentalFunction(expr parser.Expr) (bool, string) {
	switch e := expr.(type) {
	case *parser.MatrixSelector:
		return containsExperimentalFunction(e.VectorSelector)
	case *parser.Call:
		if parser.Functions[e.Func.Name].Experimental {
			return true, e.Func.Name
		}
		for _, arg := range e.Args {
			if res, name := containsExperimentalFunction(arg); res {
				return true, name
			}
		}
	case *parser.BinaryExpr:
		if res, name := containsExperimentalFunction(e.LHS); res {
			return true, name
		}
		if res, name := containsExperimentalFunction(e.RHS); res {
			return true, name
		}
		return false, ""
	case *parser.AggregateExpr:
		switch e.Op {
		case parser.LIMITK:
			return true, "limitk"
		case parser.LIMIT_RATIO:
			return true, "limit_ratio"
		}
		if res, name := containsExperimentalFunction(e.Param); res {
			return true, name
		}
		return containsExperimentalFunction(e.Expr)
	case *parser.SubqueryExpr:
		return containsExperimentalFunction(e.Expr)
	case *parser.ParenExpr:
		return containsExperimentalFunction(e.Expr)
	case *parser.UnaryExpr:
		return containsExperimentalFunction(e.Expr)
	case *parser.StepInvariantExpr:
		return containsExperimentalFunction(e.Expr)
	case *parser.VectorSelector, *parser.NumberLiteral, *parser.StringLiteral:
		return false, ""
	default:
		return false, ""
	}
	return false, ""
}
