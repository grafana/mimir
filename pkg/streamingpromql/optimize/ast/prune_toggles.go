// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
)

// PruneToggles optimizes queries by pruning expressions that are toggled off, only
// targeting a very specific query pattern that is commonly used to toggle between
// classic and native histograms in our dashboards.
type PruneToggles struct{}

func (p *PruneToggles) Name() string {
	return "Toggled off expressions pruning"
}

func (p *PruneToggles) Apply(ctx context.Context, expr parser.Expr) (parser.Expr, error) {
	mapper := &pruneToggles{}
	ASTExprMapper := astmapper.NewASTExprMapper(mapper)
	return ASTExprMapper.Map(ctx, expr)
}

type pruneToggles struct{}

func (mapper *pruneToggles) MapExpr(ctx context.Context, expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	e, ok := expr.(*parser.BinaryExpr)
	if !ok {
		return expr, false, nil
	}

	if e.Op != parser.LAND || e.VectorMatching == nil ||
		!e.VectorMatching.On || len(e.VectorMatching.MatchingLabels) != 0 {
		// Return if not "<lhs> and on() <rhs>"
		return expr, false, nil
	}

	isConst, isEmpty := mapper.isConst(e.RHS)
	if !isConst {
		return expr, false, nil
	}
	if isEmpty {
		// The right hand side is empty, so the whole expression is empty due to
		// "and on()", return the right hand side.
		return e.RHS, false, nil
	}
	// The right hand side is const and not empty, so the whole expression is
	// just the left side.
	return e.LHS, false, nil
}

func (mapper *pruneToggles) isConst(expr parser.Expr) (isConst, isEmpty bool) {
	var lhs, rhs parser.Expr
	switch e := expr.(type) {
	case *parser.ParenExpr:
		return mapper.isConst(e.Expr)
	case *parser.BinaryExpr:
		if e.Op != parser.EQLC || e.ReturnBool {
			return false, false
		}
		lhs = e.LHS
		rhs = e.RHS
	default:
		return false, false
	}

	if vectorAndNumber, equals := mapper.isVectorAndNumberEqual(lhs, rhs); vectorAndNumber {
		return true, !equals
	}
	if vectorAndNumber, equals := mapper.isVectorAndNumberEqual(rhs, lhs); vectorAndNumber {
		return true, !equals
	}
	return false, false
}

// isVectorAndNumberEqual returns whether the lhs is a const vector like
// "vector(5)"" and the right hand size is a number like "2". Also returns
// if the values are equal.
func (mapper *pruneToggles) isVectorAndNumberEqual(lhs, rhs parser.Expr) (bool, bool) {
	lIsVector, lValue := mapper.isConstVector(lhs)
	if !lIsVector {
		return false, false
	}
	rIsConst, rValue := mapper.isNumber(rhs)
	if !rIsConst {
		return false, false
	}
	return true, rValue == lValue
}

func (mapper *pruneToggles) isConstVector(expr parser.Expr) (isVector bool, value float64) {
	switch e := expr.(type) {
	case *parser.ParenExpr:
		return mapper.isConstVector(e.Expr)
	case *parser.Call:
		if e.Func.Name != "vector" || len(e.Args) != 1 {
			return false, 0
		}
		lit, ok := e.Args[0].(*parser.NumberLiteral)
		if !ok {
			return false, 0
		}
		return true, lit.Val
	}
	return false, 0
}

func (mapper *pruneToggles) isNumber(expr parser.Expr) (isNumber bool, value float64) {
	switch e := expr.(type) {
	case *parser.ParenExpr:
		return mapper.isNumber(e.Expr)
	case *parser.NumberLiteral:
		return true, e.Val
	}
	return false, 0
}
