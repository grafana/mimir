// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type SkipReason int

const (
	SkipReasonOk              = SkipReason(0)
	SkipReasonNoAggregations  = SkipReason(1)
	SkipReasonBinaryOperation = SkipReason(2)
	SkipReasonDeduplicate     = SkipReason(3)
	SkipReasonNotSupported    = SkipReason(4)
)

func (r SkipReason) String() string {
	switch r {
	case SkipReasonOk:
		return ""
	case SkipReasonNoAggregations:
		return "no-aggregations"
	case SkipReasonBinaryOperation:
		return "binary-operation"
	case SkipReasonDeduplicate:
		return "deduplicate-and-merge"
	case SkipReasonNotSupported:
		return "not-supported"
	default:
		panic(fmt.Sprintf("unexpected projection skip reason %d, this is a bug", r))
	}
}

var _ optimize.QueryPlanOptimizationPass = &ProjectionPushdownOptimizationPass{}

type ProjectionPushdownOptimizationPass struct {
	examined prometheus.Counter
	modified prometheus.Counter
	skipped  *prometheus.CounterVec
	logger   log.Logger
}

func NewProjectionPushdownOptimizationPass(reg prometheus.Registerer, logger log.Logger) *ProjectionPushdownOptimizationPass {
	return &ProjectionPushdownOptimizationPass{
		examined: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_projection_pushdown_examined_total",
			Help: "Total number of selectors that the optimization pass examined to see if projections could be used.",
		}),
		modified: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_projection_pushdown_modified_total",
			Help: "Total number of selectors where projections could be used.",
		}),
		skipped: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_projection_pushdown_skipped_total",
			Help: "Total number of selectors where projections could not be used.",
		}, []string{"reason"}),
		logger: logger,
	}
}

func (p *ProjectionPushdownOptimizationPass) Name() string {
	return "Push down label projections"
}

func (p *ProjectionPushdownOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, v planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	spanlog := spanlogger.FromContext(ctx, p.logger)
	var (
		selectors = 0
		modified  = 0
	)

	_ = optimize.Walk(plan.Root, optimize.VisitorFunc(func(node planning.Node, path []planning.Node) error {
		switch e := node.(type) {
		case *core.MatrixSelector:
			selectors++

			m, skipped := examinePath(path)
			if skipped != SkipReasonOk {
				p.skipped.WithLabelValues(skipped.String()).Inc()
				return nil
			}

			required := flattenLabels(m)
			e.ProjectionLabels = required
			e.ProjectionInclude = true

			modified++
			spanlog.DebugLog(
				"msg", "applying projection to selector",
				"selector_matchers", core.LabelMatchersStringer(e.Matchers),
				"required_labels", required,
			)
		case *core.VectorSelector:
			selectors++

			m, skipped := examinePath(path)
			if skipped != SkipReasonOk {
				p.skipped.WithLabelValues(skipped.String()).Inc()
				return nil
			}

			required := flattenLabels(m)
			e.ProjectionLabels = required
			e.ProjectionInclude = true

			modified++
			spanlog.DebugLog(
				"msg", "applying projection to selector",
				"selector_matchers", core.LabelMatchersStringer(e.Matchers),
				"required_labels", required,
			)
		}
		return nil
	}))

	p.examined.Add(float64(selectors))
	p.modified.Add(float64(modified))

	return plan, nil
}

// flattenLabels converts a set of labels to a slice and sorts them.
func flattenLabels(m map[string]struct{}) []string {
	ret := make([]string, 0, len(m))
	for k := range m {
		ret = append(ret, k)
	}

	slices.Sort(ret)
	return ret
}

// examinePath looks at the nodes that lead to a leaf node, starting from the closest, to the
// root and returns a set of any labels that are specifically required the expressions. An enum
// is returned to indicate if the expressions are eligible for projection and why.
func examinePath(path []planning.Node) (map[string]struct{}, SkipReason) {
	requiredLabels := make(map[string]struct{})

	for i := len(path) - 1; i >= 0; i-- {
		switch e := path[i].(type) {
		case *core.AggregateExpression:
			m, skipped := examineAggregate(e)
			if skipped != SkipReasonOk {
				return nil, skipped
			}

			maps.Copy(requiredLabels, m)
			// We stop examining the path to the root as soon as we find the first
			// aggregation: if this aggregation requires specific labels those are
			// the labels we need to fetch since any expressions that occur after
			// this aggregation could not use any others and still work.
			return requiredLabels, SkipReasonOk
		case *core.BinaryExpression:
			return nil, SkipReasonBinaryOperation
		case *core.DeduplicateAndMerge:
			return nil, SkipReasonDeduplicate
		case *core.FunctionCall:
			m, skipped := examineFunction(e)
			if skipped != SkipReasonOk {
				return nil, skipped
			}

			maps.Copy(requiredLabels, m)
		}
	}

	// If we've made it this far, there's no aggregations applied to the selector
	// so we can't use projections for this leg of the query.
	return nil, SkipReasonNoAggregations
}

func examineAggregate(a *core.AggregateExpression) (map[string]struct{}, SkipReason) {
	// In order to keep this implementation simple, we only support including specific
	// fields as part of a projection - not excluding all fields except specific ones.
	if a.Without {
		return nil, SkipReasonNotSupported
	}

	requiredLabels := make(map[string]struct{})
	for _, l := range a.Grouping {
		requiredLabels[l] = struct{}{}
	}

	if a.Op == core.AGGREGATION_COUNT_VALUES {
		if l, ok := a.Param.(*core.StringLiteral); ok {
			requiredLabels[l.Value] = struct{}{}
		}
	}

	return requiredLabels, SkipReasonOk
}

func examineFunction(f *core.FunctionCall) (map[string]struct{}, SkipReason) {
	switch f.Function {
	case functions.FUNCTION_INFO:
		// Keep this implementation simple and skip any projection when the
		// query uses the info function. Note that at time of writing MQE does
		// not support the info function at all so this code path isn't exercised.
		return nil, SkipReasonNotSupported
	case functions.FUNCTION_LABEL_JOIN:
		return functionLabelArgs(f.Args[3:]...), SkipReasonOk
	case functions.FUNCTION_LABEL_REPLACE:
		return functionLabelArgs(f.Args[3]), SkipReasonOk
	case functions.FUNCTION_SORT_BY_LABEL:
		return functionLabelArgs(f.Args[1:]...), SkipReasonOk
	case functions.FUNCTION_SORT_BY_LABEL_DESC:
		return functionLabelArgs(f.Args[1:]...), SkipReasonOk
	default:
		// Not a function that requires any particular label.
		return nil, SkipReasonOk
	}
}

func functionLabelArgs(args ...planning.Node) map[string]struct{} {
	required := make(map[string]struct{})

	for _, arg := range args {
		if a, ok := arg.(*core.StringLiteral); ok {
			required[a.Value] = struct{}{}
		}
	}

	return required
}
