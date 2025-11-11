// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
)

// PruneToggles optimizes queries by pruning expressions that are toggled off, only
// targeting a very specific query pattern that is commonly used to toggle between
// classic and native histograms in our dashboards.
// e.g. `(avg(rate(foo[1m]))) and on() (vector(1) == 1)` -> `(avg(rate(foo[1m])))`
// e.g. `(avg(rate(foo[1m]))) and on() (vector(1) == -1)` -> `(vector(1) == -1)`
type PruneToggles struct {
	pruneTogglesAttempts prometheus.Counter
	pruneTogglesRewrites prometheus.Counter
}

func NewPruneToggles(reg prometheus.Registerer) *PruneToggles {
	return &PruneToggles{
		pruneTogglesAttempts: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_prune_toggles_attempted_total",
			Help: "Total number of queries that the optimization pass has attempted to rewrite by pruning toggles.",
		}),
		pruneTogglesRewrites: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_prune_toggles_rewritten_total",
			Help: "Total number of queries where the optimization pass has rewritten the query by pruning toggles.",
		}),
	}
}

func (p *PruneToggles) Name() string {
	return "Toggled off expressions pruning"
}

func (p *PruneToggles) Apply(ctx context.Context, expr parser.Expr) (parser.Expr, error) {
	p.pruneTogglesAttempts.Inc()
	mapper := NewPruneTogglesMapper()
	newExpr, err := mapper.Map(ctx, expr)
	if mapper.HasChanged() {
		p.pruneTogglesRewrites.Inc()
	}
	return newExpr, err
}

func NewPruneTogglesMapper() *astmapper.ASTExprMapperWithState {
	mapper := &pruneToggles{}
	return astmapper.NewASTExprMapperWithState(mapper)
}

type pruneToggles struct {
	changed bool
}

func (mapper *pruneToggles) HasChanged() bool {
	return mapper.changed
}

func (mapper *pruneToggles) Stats() (int, int, int) {
	return 0, 0, 0
}

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
		mapper.changed = true
		return e.RHS, false, nil
	}
	// The right hand side is const and not empty, so the whole expression is
	// just the left side.
	mapper.changed = true
	return e.LHS, false, nil
}

func (mapper *pruneToggles) isConst(expr parser.Expr) (isConst, isEmpty bool) {
	var lhs, rhs parser.Expr
	switch e := expr.(type) {
	case *parser.StepInvariantExpr:
		return mapper.isConst(e.Expr)
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
	case *parser.StepInvariantExpr:
		return mapper.isConstVector(e.Expr)
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
	case *parser.StepInvariantExpr:
		return mapper.isNumber(e.Expr)
	case *parser.ParenExpr:
		return mapper.isNumber(e.Expr)
	case *parser.NumberLiteral:
		return true, e.Val
	}
	return false, 0
}
