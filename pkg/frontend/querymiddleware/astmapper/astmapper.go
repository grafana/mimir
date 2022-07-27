// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/astmapper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql/parser"
)

// ASTMapper is the exported interface for mapping between multiple AST representations
type ASTMapper interface {
	// Map the input expr and returns the mapped expr.
	Map(expr parser.Expr, stats *MapperStats) (mapped parser.Expr, err error)
}

// MultiMapper can compose multiple ASTMappers
type MultiMapper struct {
	mappers []ASTMapper
}

// Map implements ASTMapper
func (m *MultiMapper) Map(expr parser.Expr, stats *MapperStats) (parser.Expr, error) {
	var result = expr
	var err error

	if len(m.mappers) == 0 {
		return nil, errors.New("MultiMapper: No mappers registered")
	}

	for _, x := range m.mappers {
		result, err = x.Map(result, stats)
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

func cloneAndMap(mapper ASTExprMapper, expr parser.Expr, stats *MapperStats) (parser.Expr, error) {
	cloned, err := cloneExpr(expr)
	if err != nil {
		return nil, err
	}
	return mapper.Map(cloned, stats)
}

type ExprMapper interface {
	// MapExpr either maps a single AST expr or returns the unaltered expr.
	// It returns a finished bool to signal whether no further recursion is necessary.
	MapExpr(expr parser.Expr, stats *MapperStats) (mapped parser.Expr, finished bool, err error)
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
func (nm ASTExprMapper) Map(expr parser.Expr, stats *MapperStats) (parser.Expr, error) {
	expr, finished, err := nm.MapExpr(expr, stats)
	if err != nil {
		return nil, err
	}

	if finished {
		return expr, nil
	}

	switch n := expr.(type) {
	case nil:
		// nil handles cases where we check optional fields that are not set
		return nil, nil

	case *parser.AggregateExpr:
		expr, err := nm.Map(n.Expr, stats)
		if err != nil {
			return nil, err
		}
		n.Expr = expr
		return n, nil

	case *parser.BinaryExpr:
		lhs, err := nm.Map(n.LHS, stats)
		if err != nil {
			return nil, err
		}
		n.LHS = lhs

		rhs, err := nm.Map(n.RHS, stats)
		if err != nil {
			return nil, err
		}
		n.RHS = rhs

		return n, nil

	case *parser.Call:
		for i, e := range n.Args {
			mapped, err := nm.Map(e, stats)
			if err != nil {
				return nil, err
			}
			n.Args[i] = mapped
		}
		return n, nil

	case *parser.SubqueryExpr:
		mapped, err := nm.Map(n.Expr, stats)
		if err != nil {
			return nil, err
		}
		n.Expr = mapped
		return n, nil

	case *parser.ParenExpr:
		mapped, err := nm.Map(n.Expr, stats)
		if err != nil {
			return nil, err
		}
		n.Expr = mapped
		return n, nil

	case *parser.UnaryExpr:
		mapped, err := nm.Map(n.Expr, stats)
		if err != nil {
			return nil, err
		}
		n.Expr = mapped
		return n, nil

	case *parser.NumberLiteral, *parser.StringLiteral, *parser.VectorSelector, *parser.MatrixSelector:
		return n, nil

	default:
		return nil, errors.Errorf("exprMapper: unhandled expr type %T", expr)
	}
}
