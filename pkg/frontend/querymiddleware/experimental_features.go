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
	allExperimentalFeatures = "all"
)

type experimentalFeatureType string

const (
	functionType                      = experimentalFeatureType("function")
	aggregationType                   = experimentalFeatureType("aggregation")
	extendedRangeSelectorModifierType = experimentalFeatureType("extended range selector modifier")
)

// experimentalFeaturesMiddleware manages the per-tenant access to experimental functions, aggregations and extended range selector modifiers.
type experimentalFeaturesMiddleware struct {
	next   MetricsQueryHandler
	limits Limits
	logger log.Logger
}

// newExperimentalFeaturesMiddleware creates a middleware that blocks queries that contain PromQL experimental functions, aggregates or range selector modifiers
// that are not enabled for the active tenant(s), allowing us to enable specific features only for selected tenants.
func newExperimentalFeaturesMiddleware(limits Limits, logger log.Logger) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &experimentalFeaturesMiddleware{
			next:   next,
			limits: limits,
			logger: logger,
		}
	})
}

func (m *experimentalFeaturesMiddleware) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
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
		if len(enabled) == 0 || enabled[0] != allExperimentalFeatures {
			allExperimentalFunctionsEnabled = false
		}
	}

	enabledExtendedRangeSelectors := make(map[string][]string, len(tenantIDs))
	allExtendedRangeSelectorsEnabled := true
	for _, tenantID := range tenantIDs {
		enabled := m.limits.EnabledPromQLExtendedRangeSelectors(tenantID)
		enabledExtendedRangeSelectors[tenantID] = enabled
		if len(enabled) == 0 || enabled[0] != allExperimentalFeatures {
			allExtendedRangeSelectorsEnabled = false
		}
	}

	if allExperimentalFunctionsEnabled && allExtendedRangeSelectorsEnabled {
		// If all experimental functions are enabled for all tenants here, we don't need to check the query
		// for those functions and can skip this middleware.
		return m.next.Do(ctx, req)
	}

	expr := req.GetParsedQuery()
	features := containedExperimentalFeatures(expr)
	if len(features) == 0 {
		// This query does not contain any experimental functions, so we can continue to the next middleware.
		return m.next.Do(ctx, req)
	}

	// Make sure that every used experimental feature is enabled for all the tenants here.
	var tenantMap map[string][]string
	for feature, featureType := range features {
		switch featureType {
		case functionType, aggregationType:
			tenantMap = enabledExperimentalFunctions
		case extendedRangeSelectorModifierType:
			tenantMap = enabledExtendedRangeSelectors
		}

		for _, enabled := range tenantMap {
			if len(enabled) > 0 && enabled[0] == allExperimentalFeatures {
				// If the first item matches the const value of allExperimentalFeatures, then all experimental
				// features are enabled for this tenant.
				continue
			}
			if !slices.Contains(enabled, feature) {
				return nil, createExperimentalFeatureError(featureType, feature)
			}
		}
	}

	// Every used experimental feature is enabled for the tenant(s).
	return m.next.Do(ctx, req)
}

func createExperimentalFeatureError(featureType experimentalFeatureType, feature string) error {
	err := fmt.Errorf("experimental %s %q is not enabled for tenant", featureType, feature)
	return apierror.New(apierror.TypeBadData, DecorateWithParamName(err, "query").Error())
}

// containedExperimentalFeatures returns any PromQL experimental functions, aggregations or range selector modifiers used in the query.
func containedExperimentalFeatures(expr parser.Expr) map[string]experimentalFeatureType {
	expFuncNames := map[string]experimentalFeatureType{}
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
