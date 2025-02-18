// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"slices"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
)

const (
	allExperimentalFunctions = "all"
)

type experimentalFunctionsMiddleware struct {
	next   MetricsQueryHandler
	limits Limits
	logger log.Logger
}

// newExperimentalFunctionsMiddleware creates a middleware that blocks queries that contain PromQL experimental functions
// that are not enabled for the active tenant(s), allowing us to enable specific functions only for selected tenants.
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

	enabledExperimentalFunctions := make(map[string][]string, len(tenantIDs))
	allExperimentalFunctionsEnabled := true
	for _, tenantID := range tenantIDs {
		enabled := m.limits.EnabledPromQLExperimentalFunctions(tenantID)
		enabledExperimentalFunctions[tenantID] = enabled
		if len(enabled) == 0 || enabled[0] != allExperimentalFunctions {
			allExperimentalFunctionsEnabled = false
		}
	}

	if allExperimentalFunctionsEnabled {
		// If all experimental functions are enabled for all tenants here, we don't need to check the query
		// for those functions and can skip this middleware.
		return m.next.Do(ctx, req)
	}

	expr := req.GetQueryExpr()
	funcs := containedExperimentalFunctions(expr)
	if len(funcs) == 0 {
		// This query does not contain any experimental functions, so we can continue to the next middleware.
		return m.next.Do(ctx, req)
	}

	// Make sure that every used experimental function is enabled for all the tenants here.
	for name := range funcs {
		for tenantID, enabled := range enabledExperimentalFunctions {
			if len(enabled) > 0 && enabled[0] == allExperimentalFunctions {
				// If the first item matches the const value of allExperimentalFunctions, then all experimental
				// functions are enabled for this tenant.
				continue
			}
			if !slices.Contains(enabled, name) {
				err := fmt.Errorf("function %q is not enabled for tenant %s", name, tenantID)
				return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
			}
		}
	}

	// Every used experimental function is enabled for the tenant(s).
	return m.next.Do(ctx, req)
}

// containedExperimentalFunctions returns any PromQL experimental functions used in the query.
func containedExperimentalFunctions(expr parser.Expr) map[string]struct{} {
	expFuncNames := map[string]struct{}{}
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		call, ok := node.(*parser.Call)
		if ok {
			if parser.Functions[call.Func.Name].Experimental {
				expFuncNames[call.Func.Name] = struct{}{}
			}
			return nil
		}
		agg, ok := node.(*parser.AggregateExpr)
		if ok {
			if agg.Op.IsExperimentalAggregator() {
				expFuncNames[agg.Op.String()] = struct{}{}
			}
		}
		return nil
	})
	return expFuncNames
}
