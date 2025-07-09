// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
)

// ReorderHistogramAgg optimizes queries by reordering histogram functions and aggregations
// for more efficient execution.
type ReorderHistogramAgg struct{}

func (r *ReorderHistogramAgg) Name() string {
	return "Histogram aggregation reordering"
}

func (r *ReorderHistogramAgg) Apply(ctx context.Context, expr parser.Expr) (parser.Expr, error) {
	mapper := &reorderHistogramAgg{
		ctx: ctx,
	}
	ASTExprMapper := astmapper.NewASTExprMapper(mapper)
	return ASTExprMapper.Map(expr)
}

type reorderHistogramAgg struct {
	ctx context.Context
}

func (mapper *reorderHistogramAgg) MapExpr(expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	if err := mapper.ctx.Err(); err != nil {
		return nil, false, err
	}

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
