// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"
)

func NewQueryPrunerHistogram(ctx context.Context, logger log.Logger) ASTMapper {
	pruner := newQueryPrunerHistogram(ctx, logger)
	return NewASTExprMapper(pruner)
}

type queryPrunerHistogram struct {
	ctx    context.Context
	logger log.Logger
}

func newQueryPrunerHistogram(ctx context.Context, logger log.Logger) *queryPrunerHistogram {
	return &queryPrunerHistogram{
		ctx:    ctx,
		logger: logger,
	}
}

func (pruner *queryPrunerHistogram) MapExpr(expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	if err := pruner.ctx.Err(); err != nil {
		return nil, false, err
	}

	call, ok := expr.(*parser.Call)
	if !ok || !pruner.isSwitchableCall(call.Func) {
		return expr, false, nil
	}

	if len(call.Args) != 1 {
		return expr, false, nil
	}

	agg, ok := call.Args[0].(*parser.AggregateExpr)
	if !ok || !pruner.isSwitchableAgg(agg.Op) {
		return expr, false, nil
	}

	newExpr := &parser.AggregateExpr{
		Op: agg.Op,
		Expr: &parser.Call{
			Func: call.Func,
			Args: []parser.Expr{agg.Expr},
		},
		Param:    agg.Param,
		Grouping: agg.Grouping,
		Without:  agg.Without,
	}

	return newExpr, false, nil
}

func (pruner *queryPrunerHistogram) isSwitchableCall(callFunc *parser.Function) bool {
	return callFunc.Name == "histogram_sum" || callFunc.Name == "histogram_count" ||
		callFunc.Name == "histogram_avg"
}

func (pruner *queryPrunerHistogram) isSwitchableAgg(op parser.ItemType) bool {
	return op == parser.SUM || op == parser.AVG
}
