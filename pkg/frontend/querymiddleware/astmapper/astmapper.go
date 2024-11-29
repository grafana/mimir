// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/astmapper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/util/promqlext"
)

func init() {
	promqlext.ExtendPromQL()
}

// ASTMapper is the exported interface for mapping between multiple AST representations
type ASTMapper interface {
	// Map the input expr and returns the mapped expr.
	Map(expr parser.Expr) (mapped parser.Expr, err error)
}

// MultiMapper can compose multiple ASTMappers
type MultiMapper struct {
	mappers []ASTMapper
}

// Map implements ASTMapper
func (m *MultiMapper) Map(expr parser.Expr) (parser.Expr, error) {
	var result = expr
	var err error

	if len(m.mappers) == 0 {
		return nil, errors.New("MultiMapper: No mappers registered")
	}

	for _, x := range m.mappers {
		result, err = x.Map(result)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// Register adds ASTMappers into a multimapper.
// Since registered functions are applied in the order they're registered, it's advised to register them
// in decreasing priority and only operate on exprs that each function cares about, defaulting to cloneExpr.
func (m *MultiMapper) Register(xs ...ASTMapper) {
	m.mappers = append(m.mappers, xs...)
}

// NewMultiMapper instantiates an ASTMapper from multiple ASTMappers.
func NewMultiMapper(xs ...ASTMapper) *MultiMapper {
	m := &MultiMapper{}
	m.Register(xs...)
	return m
}

// cloneExpr is a helper function to clone an expr.
func cloneExpr(expr parser.Expr) (parser.Expr, error) {
	return parser.ParseExpr(expr.String())
}

func cloneAndMap(mapper ASTExprMapper, expr parser.Expr) (parser.Expr, error) {
	cloned, err := cloneExpr(expr)
	if err != nil {
		return nil, err
	}
	return mapper.Map(cloned)
}

type ExprMapper interface {
	// MapExpr either maps a single AST expr or returns the unaltered expr.
	// It returns a finished bool to signal whether no further recursion is necessary.
	MapExpr(expr parser.Expr) (mapped parser.Expr, finished bool, err error)
}

// NewASTExprMapper creates an ASTMapper from a ExprMapper
func NewASTExprMapper(mapper ExprMapper) ASTExprMapper {
	return ASTExprMapper{mapper}
}

// ASTExprMapper is an ASTMapper adapter which uses a ExprMapper internally.
type ASTExprMapper struct {
	ExprMapper
}

// Map implements ASTMapper from a ExprMapper
func (em ASTExprMapper) Map(expr parser.Expr) (parser.Expr, error) {
	expr, finished, err := em.MapExpr(expr)
	if err != nil {
		return nil, err
	}

	if finished {
		return expr, nil
	}

	switch e := expr.(type) {
	case nil:
		// nil handles cases where we check optional fields that are not set
		return nil, nil

	case *parser.AggregateExpr:
		expr, err := em.Map(e.Expr)
		if err != nil {
			return nil, err
		}
		e.Expr = expr
		return e, nil

	case *parser.BinaryExpr:
		lhs, err := em.Map(e.LHS)
		if err != nil {
			return nil, err
		}
		e.LHS = lhs

		rhs, err := em.Map(e.RHS)
		if err != nil {
			return nil, err
		}
		e.RHS = rhs

		return e, nil

	case *parser.Call:
		for i, arg := range e.Args {
			mapped, err := em.Map(arg)
			if err != nil {
				return nil, err
			}
			e.Args[i] = mapped
		}
		return e, nil

	case *parser.SubqueryExpr:
		mapped, err := em.Map(e.Expr)
		if err != nil {
			return nil, err
		}
		e.Expr = mapped
		return e, nil

	case *parser.ParenExpr:
		mapped, err := em.Map(e.Expr)
		if err != nil {
			return nil, err
		}
		e.Expr = mapped
		return e, nil

	case *parser.UnaryExpr:
		mapped, err := em.Map(e.Expr)
		if err != nil {
			return nil, err
		}
		e.Expr = mapped
		return e, nil

	case *parser.MatrixSelector:
		mapped, err := em.Map(e.VectorSelector)
		if err != nil {
			return nil, err
		}
		e.VectorSelector = mapped
		return e, nil

	case *parser.NumberLiteral, *parser.StringLiteral, *parser.VectorSelector:
		return e, nil

	default:
		return nil, errors.Errorf("ASTExprMapper: unhandled expr type %T", expr)
	}
}
