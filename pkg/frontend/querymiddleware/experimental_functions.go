// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/exp/slices"

	apierror "github.com/grafana/mimir/pkg/api/error"
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

	enabledExperimentalFunctions := make(map[string][]string, len(tenantIDs))
	for _, id := range tenantIDs {
		enabled := m.limits.EnabledPromQLExperimentalFunctions(id)
		if len(enabled) == 0 {
			// The default is to allow all experimental functions.
			continue
		}
		enabledExperimentalFunctions[id] = enabled
	}

	if len(enabledExperimentalFunctions) == 0 {
		// All functions are enabled.
		return m.next.Do(ctx, req)
	}

	expr := req.GetQueryExpr()
	funcs := containedExperimentalFunctions(expr)
	if len(funcs) == 0 {
		return m.next.Do(ctx, req)
	}

	// Make sure that every used experimental function is enabled.
	for name := range funcs {
		for id, enabled := range enabledExperimentalFunctions {
			if !slices.Contains(enabled, name) {
				err := fmt.Errorf("function %s is not enabled for tenant %s", name, id)
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
			// Note that unlike most PromQL functions, the experimental nature of the aggregation functions are manually
			// defined and enforced, so they have to be hardcoded here and updated along with changes in Prometheus.
			switch agg.Op {
			case parser.LIMITK, parser.LIMIT_RATIO:
				expFuncNames[agg.Op.String()] = struct{}{}
			}
		}
		return nil
	})
	return expFuncNames
}
