// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"slices"
	"strings"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
)

// NewRewriteAverageMapper rewrites sum/count to average wherever equivalent.
func NewRewriteAverageMapper() *astmapper.ASTExprMapperWithState {
	mapper := &rewriteAverage{}
	return astmapper.NewASTExprMapperWithState(mapper)
}

type rewriteAverage struct {
	changed bool

	countAvg          int
	countAvgOverTime  int
	countHistogramAvg int
}

func (mapper *rewriteAverage) HasChanged() bool {
	return mapper.changed
}

func (mapper *rewriteAverage) Stats() (int, int, int) {
	return mapper.countAvg, mapper.countAvgOverTime, mapper.countHistogramAvg
}

func (mapper *rewriteAverage) MapExpr(ctx context.Context, expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	e, ok := expr.(*parser.BinaryExpr)
	if !ok {
		return expr, false, nil
	}

	if e.Op != parser.DIV {
		return expr, false, nil
	}

	newExpr, changed := mapper.rewriteExpr(e)
	if !changed {
		return expr, false, nil
	}

	mapper.changed = true
	return newExpr, false, nil
}

func (mapper *rewriteAverage) rewriteExpr(e *parser.BinaryExpr) (parser.Expr, bool) {
	lhsAgg, rhsAgg, ok := mapper.areAggregateExprs(e.LHS, e.RHS)
	if ok {
		if newExpr := mapper.handleAggregateExprs(lhsAgg, rhsAgg); newExpr != nil {
			return newExpr, true
		}
		return e, false
	}
	lhsCall, rhsCall, ok := mapper.areFunctionCalls(e.LHS, e.RHS)
	if ok {
		if newExpr := mapper.handleFunctionCalls(lhsCall, rhsCall); newExpr != nil {
			return newExpr, true
		}
		return e, false
	}
	return e, false
}

func (mapper *rewriteAverage) areAggregateExprs(lhs, rhs parser.Expr) (*parser.AggregateExpr, *parser.AggregateExpr, bool) {
	lhsAgg, ok := lhs.(*parser.AggregateExpr)
	if !ok {
		return nil, nil, false
	}

	rhsAgg, ok := rhs.(*parser.AggregateExpr)
	if !ok {
		return nil, nil, false
	}

	return lhsAgg, rhsAgg, true
}

func (mapper *rewriteAverage) handleAggregateExprs(lhs, rhs *parser.AggregateExpr) parser.Expr {
	if lhs.Op != parser.SUM || rhs.Op != parser.COUNT {
		return nil
	}

	if !mapper.haveEqualContents(lhs, rhs) {
		return nil
	}

	mapper.countAvg++

	return &parser.AggregateExpr{
		Op:       parser.AVG,
		Expr:     lhs.Expr,
		Param:    lhs.Param,
		Grouping: lhs.Grouping,
		Without:  lhs.Without,
	}
}

func (mapper *rewriteAverage) areFunctionCalls(lhs, rhs parser.Expr) (*parser.Call, *parser.Call, bool) {
	lhsCall, ok := lhs.(*parser.Call)
	if !ok {
		return nil, nil, false
	}

	rhsCall, ok := rhs.(*parser.Call)
	if !ok {
		return nil, nil, false
	}

	return lhsCall, rhsCall, true
}

func (mapper *rewriteAverage) handleFunctionCalls(lhs, rhs *parser.Call) parser.Expr {
	var newFuncName string
	switch {
	case lhs.Func.Name == "histogram_sum" && rhs.Func.Name == "histogram_count":
		newFuncName = "histogram_avg"
	case lhs.Func.Name == "sum_over_time" && rhs.Func.Name == "count_over_time":
		newFuncName = "avg_over_time"
	default:
		return nil
	}

	if len(lhs.Args) != len(rhs.Args) {
		return nil
	}

	for i := range lhs.Args {
		if lhs.Args[i].String() != rhs.Args[i].String() {
			return nil
		}
	}

	switch newFuncName {
	case "histogram_avg":
		mapper.countHistogramAvg++
	case "avg_over_time":
		mapper.countAvgOverTime++
	}

	return &parser.Call{
		Func: &parser.Function{
			Name: newFuncName,
		},
		Args: lhs.Args,
	}
}

func (*rewriteAverage) haveEqualContents(lhs, rhs *parser.AggregateExpr) bool {
	if lhs.Without != rhs.Without {
		return false
	}

	slices.SortFunc(lhs.Grouping, func(l, r string) int {
		return strings.Compare(l, r)
	})
	slices.SortFunc(rhs.Grouping, func(l, r string) int {
		return strings.Compare(l, r)
	})
	if !slices.Equal(lhs.Grouping, rhs.Grouping) {
		return false
	}

	if lhs.Expr.String() != rhs.Expr.String() {
		return false
	}

	if lhs.Param != nil && rhs.Param != nil && lhs.Param.String() != rhs.Param.String() {
		return false
	}

	return true
}
