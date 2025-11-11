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
	isFunction               = 1
	isAggregator             = 2
	isExtendedRangeSelector  = 3
)

type experimentalFunctionsMiddleware struct {
	next   MetricsQueryHandler
	limits Limits
	logger log.Logger
}

// newExperimentalFunctionsMiddleware creates a middleware that blocks queries that contain PromQL experimental functions
// that are not enabled for the active tenant(s), allowing us to enable specific functions only for selected tenants.
// This middleware also manages blocking queries that contain PromQL experimental aggregates and extended range selector modifiers.
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

	enabledExtendedRangeSelectors := make(map[string][]string, len(tenantIDs))
	allExtendedRangeSelectorsEnabled := true
	for _, tenantID := range tenantIDs {
		enabled := m.limits.EnabledPromQLExtendedRangeSelectors(tenantID)
		enabledExtendedRangeSelectors[tenantID] = enabled
		if len(enabled) == 0 || enabled[0] != allExperimentalFunctions {
			allExtendedRangeSelectorsEnabled = false
		}
	}

	if allExperimentalFunctionsEnabled && allExtendedRangeSelectorsEnabled {
		// If all experimental functions are enabled for all tenants here, we don't need to check the query
		// for those functions and can skip this middleware.
		return m.next.Do(ctx, req)
	}

	expr, err := parser.ParseExpr(req.GetQuery())
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}
	funcs := containedExperimentalFunctions(expr)
	if len(funcs) == 0 {
		// This query does not contain any experimental functions, so we can continue to the next middleware.
		return m.next.Do(ctx, req)
	}

	// Make sure that every used experimental function/modifier is enabled for all the tenants here.
	var tenantMap map[string][]string
	for name, t := range funcs {
		switch t {
		case isFunction, isAggregator:
			tenantMap = enabledExperimentalFunctions
		case isExtendedRangeSelector:
			tenantMap = enabledExtendedRangeSelectors
		}

		for tenantID, enabled := range tenantMap {
			if len(enabled) > 0 && enabled[0] == allExperimentalFunctions {
				// If the first item matches the const value of allExperimentalFunctions, then all experimental
				// functions are enabled for this tenant.
				continue
			}
			if !slices.Contains(enabled, name) {
				return nil, raiseError(tenantID, name, t)
			}
		}
	}

	// Every used experimental function is enabled for the tenant(s).
	return m.next.Do(ctx, req)
}

func raiseError(tenantID string, name string, t int) error {
	switch t {
	case isFunction:
		err := fmt.Errorf("function %q is not enabled for tenant %s", name, tenantID)
		return apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	case isAggregator:
		err := fmt.Errorf("aggregate %q is not enabled for tenant %s", name, tenantID)
		return apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	case isExtendedRangeSelector:
		err := fmt.Errorf("extended range selector modifier %q is not enabled for tenant %s", name, tenantID)
		return apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}
	return nil
}

// containedExperimentalFunctions returns any PromQL experimental functions used in the query.
func containedExperimentalFunctions(expr parser.Expr) map[string]int {
	expFuncNames := map[string]int{}
	_ = inspect(expr, func(node parser.Node) error {
		switch n := node.(type) {
		case *parser.Call:
			if parser.Functions[n.Func.Name].Experimental {
				expFuncNames[n.Func.Name] = isFunction
			}
		case *parser.AggregateExpr:
			if n.Op.IsExperimentalAggregator() {
				expFuncNames[n.Op.String()] = isAggregator
			}
		case *parser.MatrixSelector:
			// technically anchored & smoothed are range selectors not functions
			vs, ok := n.VectorSelector.(*parser.VectorSelector)
			if ok && vs.Anchored {
				expFuncNames["anchored"] = isExtendedRangeSelector
			} else if ok && vs.Smoothed {
				expFuncNames["smoothed"] = isExtendedRangeSelector
			}
		case *parser.VectorSelector:
			if n.Smoothed {
				expFuncNames["smoothed"] = isExtendedRangeSelector
			}
		}
		return nil
	})
	return expFuncNames
}
