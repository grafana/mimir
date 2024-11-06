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

type experimentalFunctionsMiddleware struct {
	next   MetricsQueryHandler
	limits Limits
	logger log.Logger
}

// newExperimentalFunctionsMiddleware creates a middleware that blocks queries that contain PromQL experimental functions
// that are not enabled for the active tenant, allowing us to enable them only for selected tenants.
func newExperimentalFunctionsMiddleware(limits Limits, logger log.Logger) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &experimentalFunctionsMiddleware{
			next:   next,
			limits: limits,
			logger: logger,
		}
	})
}

func (m *experimentalFunctionsMiddleware) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	experimentalFunctionsEnabled := validation.AllTrueBooleansPerTenant(tenantIDs, m.limits.PromQLExperimentalFunctionsEnabled)

	if experimentalFunctionsEnabled {
		// If experimental functions are enabled for this tenant, we don't need to check the query
		// for those functions and can skip this middleware.
		return m.next.Do(ctx, req)
	}

	expr := req.GetQueryExpr()
	if res, name := containsExperimentalFunction(expr); res {
		err := fmt.Errorf("function %q is not enabled for the active tenant", name)
		return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}

	// If the query does not contain any experimental functions, we can continue.
	return m.next.Do(ctx, req)
}

// containsExperimentalFunction checks if the query contains PromQL experimental functions.
func containsExperimentalFunction(expr parser.Expr) (bool, string) {
	expFuncNames := make([]string, 0)
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		call, ok := node.(*parser.Call)
		if ok {
			if parser.Functions[call.Func.Name].Experimental {
				expFuncNames = append(expFuncNames, call.Func.Name)
			}
			return nil
		}
		agg, ok := node.(*parser.AggregateExpr)
		if ok {
			// Note that unlike most PromQL functions, the experimental nature of the aggregation functions are manually
			// defined and enforced, so they have to be hardcoded here and updated along with changes in Prometheus.
			switch agg.Op {
			case parser.LIMITK, parser.LIMIT_RATIO:
				expFuncNames = append(expFuncNames, agg.Op.String())
			}
		}
		return nil
	})
	if len(expFuncNames) > 0 {
		return true, expFuncNames[0]
	}
	return false, ""
}
