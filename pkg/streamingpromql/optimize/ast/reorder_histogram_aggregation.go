// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
)

// NewReorderHistogramAggregationMapper optimizes queries by reordering histogram functions and aggregations
// for more efficient execution.
func NewReorderHistogramAggregationMapper() *astmapper.ASTExprMapperWithState {
	mapper := &reorderHistogramAggregation{}
	return astmapper.NewASTExprMapperWithState(mapper)
}

type reorderHistogramAggregation struct {
	changed bool
}

func (mapper *reorderHistogramAggregation) HasChanged() bool {
	return mapper.changed
}

func (mapper *reorderHistogramAggregation) Stats() (int, int, int) {
	return 0, 0, 0
}

func (mapper *reorderHistogramAggregation) MapExpr(ctx context.Context, expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	call, ok := expr.(*parser.Call)
	if !ok || !mapper.isSwitchableCall(call.Func) {
		return expr, false, nil
	}

	if len(call.Args) != 1 {
		return expr, false, nil
	}

	agg, ok := call.Args[0].(*parser.AggregateExpr)
	if !ok || !mapper.isSwitchableAgg(agg.Op) {
		return expr, false, nil
	}

	for _, label := range agg.Grouping {
		if label == model.MetricNameLabel {
			// Do not reorder if __name__ is used in grouping, as it can lead to incorrect aggregations.
			return expr, false, nil
		}
	}

	if vectorSelectorContainsNonExactMetricNameMatcher(agg.Expr) {
		// Do not reorder if any vector selector matcher specifies __name__, as it can lead to errors due to duplicate label sets.
		return expr, false, nil
	}

	newExpr := &parser.AggregateExpr{
		Op: agg.Op,
		Expr: &parser.Call{
			Func:     call.Func,
			Args:     []parser.Expr{agg.Expr},
			PosRange: call.PosRange,
		},
		Param:    agg.Param,
		Grouping: agg.Grouping,
		Without:  agg.Without,
		PosRange: agg.PosRange,
	}

	mapper.changed = true

	return newExpr, false, nil
}

func (*reorderHistogramAggregation) isSwitchableCall(callFunc *parser.Function) bool {
	return callFunc.Name == "histogram_sum" || callFunc.Name == "histogram_count"
}

func (*reorderHistogramAggregation) isSwitchableAgg(op parser.ItemType) bool {
	return op == parser.SUM || op == parser.AVG
}

func vectorSelectorContainsNonExactMetricNameMatcher(expr parser.Expr) bool {
	switch e := expr.(type) {
	case *parser.VectorSelector:
		for _, matcher := range e.LabelMatchers {
			if matcher.Name == model.MetricNameLabel && matcher.Type != labels.MatchEqual {
				return true
			}
		}
		return false
	case *parser.MatrixSelector:
		return vectorSelectorContainsNonExactMetricNameMatcher(e.VectorSelector)
	case *parser.StepInvariantExpr:
		return vectorSelectorContainsNonExactMetricNameMatcher(e.Expr)
	case *parser.ParenExpr:
		return vectorSelectorContainsNonExactMetricNameMatcher(e.Expr)
	case *parser.UnaryExpr:
		return vectorSelectorContainsNonExactMetricNameMatcher(e.Expr)
	case *parser.Call:
		// TODO: Handle more cases in this function and check if we need a separate one for this pass.
		if i, _ := VectorSelectorArgumentIndex(e.Func.Name); i >= 0 {
			if len(e.Args) <= i {
				return false
			}
			return vectorSelectorContainsNonExactMetricNameMatcher(e.Args[i])
		}
		// If unsure, don't reorder.
		return true
	case *parser.AggregateExpr:
		return vectorSelectorContainsNonExactMetricNameMatcher(e.Expr)
	case *parser.BinaryExpr:
		if vectorSelectorContainsNonExactMetricNameMatcher(e.LHS) {
			return true
		}
		return vectorSelectorContainsNonExactMetricNameMatcher(e.RHS)
	default:
		// If unsure, don't reorder.
		return true
	}
}
