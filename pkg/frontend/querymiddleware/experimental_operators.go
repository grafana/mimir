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
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
)

const (
	allExperimentalOperators = "all"
)

type experimentalOperationType struct {
	label string
}

var functionType = experimentalOperationType{label: "function"}
var aggregationType = experimentalOperationType{label: "aggregation"}
var extendedRangeSelectorModifierType = experimentalOperationType{label: "extended range selector modifier"}

// experimentalOperatorsMiddleware manages the per-tenant access to experimental functions, aggregations and extended range selector modifiers.
type experimentalOperatorsMiddleware struct {
	next   MetricsQueryHandler
	limits Limits
	logger log.Logger
}

// newExperimentalOperatorsMiddleware creates a middleware that blocks queries that contain PromQL experimental functions, aggregates or range selector modifiers
// that are not enabled for the active tenant(s), allowing us to enable specific operations only for selected tenants.
func newExperimentalOperatorsMiddleware(limits Limits, logger log.Logger) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &experimentalOperatorsMiddleware{
			next:   next,
			limits: limits,
			logger: logger,
		}
	})
}

func (m *experimentalOperatorsMiddleware) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	enabledExperimentalFunctions := make(map[string][]string, len(tenantIDs))
	allExperimentalFunctionsEnabled := true
	for _, tenantID := range tenantIDs {
		// note that this includes both functions and aggregations (ie limitk, limit_ratio)
		enabled := m.limits.EnabledPromQLExperimentalFunctions(tenantID)
		enabledExperimentalFunctions[tenantID] = enabled
		if len(enabled) == 0 || enabled[0] != allExperimentalOperators {
			allExperimentalFunctionsEnabled = false
		}
	}

	enabledExtendedRangeSelectors := make(map[string][]string, len(tenantIDs))
	allExtendedRangeSelectorsEnabled := true
	for _, tenantID := range tenantIDs {
		enabled := m.limits.EnabledPromQLExtendedRangeSelectors(tenantID)
		enabledExtendedRangeSelectors[tenantID] = enabled
		if len(enabled) == 0 || enabled[0] != allExperimentalOperators {
			allExtendedRangeSelectorsEnabled = false
		}
	}

	if allExperimentalFunctionsEnabled && allExtendedRangeSelectorsEnabled {
		// If all experimental functions are enabled for all tenants here, we don't need to check the query
		// for those functions and can skip this middleware.
		return m.next.Do(ctx, req)
	}

	expr, err := astmapper.CloneExpr(req.GetParsedQuery())
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
	}
	operations := containedExperimentalOperations(expr)
	if len(operations) == 0 {
		// This query does not contain any experimental functions, so we can continue to the next middleware.
		return m.next.Do(ctx, req)
	}

	// Make sure that every used experimental operation is enabled for all the tenants here.
	var tenantMap map[string][]string
	for op, operationType := range operations {
		switch operationType {
		case functionType, aggregationType:
			tenantMap = enabledExperimentalFunctions
		case extendedRangeSelectorModifierType:
			tenantMap = enabledExtendedRangeSelectors
		}

		for _, enabled := range tenantMap {
			if len(enabled) > 0 && enabled[0] == allExperimentalOperators {
				// If the first item matches the const value of allExperimentalOperations, then all experimental
				// operations are enabled for this tenant.
				continue
			}
			if !slices.Contains(enabled, op) {
				return nil, createExperimentalOperationError(operationType, op)
			}
		}
	}

	// Every used experimental operation is enabled for the tenant(s).
	return m.next.Do(ctx, req)
}

func createExperimentalOperationError(operationType experimentalOperationType, operation string) error {
	err := fmt.Errorf("%s \"%s\" is not enabled for tenant", operationType.label, operation)
	return apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
}

// containedExperimentalOperations returns any PromQL experimental functions, aggregations or range selector modifiers used in the query.
func containedExperimentalOperations(expr parser.Expr) map[string]experimentalOperationType {
	expFuncNames := map[string]experimentalOperationType{}
	_ = inspect(expr, func(node parser.Node) error {
		switch n := node.(type) {
		case *parser.Call:
			if parser.Functions[n.Func.Name].Experimental {
				expFuncNames[n.Func.Name] = functionType
			}
		case *parser.AggregateExpr:
			if n.Op.IsExperimentalAggregator() {
				expFuncNames[n.Op.String()] = aggregationType
			}
		case *parser.MatrixSelector:
			vs, ok := n.VectorSelector.(*parser.VectorSelector)
			if ok && vs.Anchored {
				expFuncNames["anchored"] = extendedRangeSelectorModifierType
			} else if ok && vs.Smoothed {
				expFuncNames["smoothed"] = extendedRangeSelectorModifierType
			}
		case *parser.VectorSelector:
			if n.Smoothed {
				expFuncNames["smoothed"] = extendedRangeSelectorModifierType
			}
		}
		return nil
	})
	return expFuncNames
}
