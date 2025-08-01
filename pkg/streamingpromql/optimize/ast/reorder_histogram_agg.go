// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
)

// ReorderHistogramAggregation optimizes queries by reordering histogram functions and aggregations
// for more efficient execution.
type ReorderHistogramAggregation struct {
	mapper astmapper.ASTMapper
}

func NewReorderHistogramAggregation() *ReorderHistogramAggregation {
	mapper := NewMapperReorderHistogramAggregation()
	return &ReorderHistogramAggregation{
		mapper: mapper,
	}
}

func (r *ReorderHistogramAggregation) Name() string {
	return "Histogram aggregation reordering"
}

func (r *ReorderHistogramAggregation) Apply(ctx context.Context, expr parser.Expr) (parser.Expr, error) {
	return r.mapper.Map(ctx, expr)
}

func NewMapperReorderHistogramAggregation() astmapper.ASTMapper {
	mapper := &reorderHistogramAgg{}
	return astmapper.NewASTExprMapper(mapper)
}

type reorderHistogramAgg struct{}

func (mapper *reorderHistogramAgg) MapExpr(ctx context.Context, expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
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

	return newExpr, false, nil
}

func (*reorderHistogramAgg) isSwitchableCall(callFunc *parser.Function) bool {
	return callFunc.Name == "histogram_sum" || callFunc.Name == "histogram_count"
}

func (*reorderHistogramAgg) isSwitchableAgg(op parser.ItemType) bool {
	return op == parser.SUM || op == parser.AVG
}
