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
	// Map the input node and returns the mapped node.
	Map(node parser.Node, stats *MapperStats) (mapped parser.Node, err error)
}

// MultiMapper can compose multiple ASTMappers
type MultiMapper struct {
	mappers []ASTMapper
}

// Map implements ASTMapper
func (m *MultiMapper) Map(node parser.Node, stats *MapperStats) (parser.Node, error) {
	var result = node
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
// in decreasing priority and only operate on nodes that each function cares about, defaulting to cloneNode.
func (m *MultiMapper) Register(xs ...ASTMapper) {
	m.mappers = append(m.mappers, xs...)
}

// NewMultiMapper instantiates an ASTMapper from multiple ASTMappers.
func NewMultiMapper(xs ...ASTMapper) *MultiMapper {
	m := &MultiMapper{}
	m.Register(xs...)
	return m
}

// cloneNode is a helper function to clone a node.
func cloneNode(node parser.Node) (parser.Node, error) {
	return parser.ParseExpr(node.String())
}

func cloneAndMap(mapper ASTNodeMapper, node parser.Expr, stats *MapperStats) (parser.Node, error) {
	cloned, err := cloneNode(node)
	if err != nil {
		return nil, err
	}
	return mapper.Map(cloned, stats)
}

type NodeMapper interface {
	// MapNode either maps a single AST node or returns the unaltered node.
	// It returns a finished bool to signal whether no further recursion is necessary.
	MapNode(node parser.Node, stats *MapperStats) (mapped parser.Node, finished bool, err error)
}

// NewASTNodeMapper creates an ASTMapper from a NodeMapper
func NewASTNodeMapper(mapper NodeMapper) ASTNodeMapper {
	return ASTNodeMapper{mapper}
}

// ASTNodeMapper is an ASTMapper adapter which uses a NodeMapper internally.
type ASTNodeMapper struct {
	NodeMapper
}

// Map implements ASTMapper from a NodeMapper
func (nm ASTNodeMapper) Map(node parser.Node, stats *MapperStats) (parser.Node, error) {
	node, finished, err := nm.MapNode(node, stats)
	if err != nil {
		return nil, err
	}

	if finished {
		return node, nil
	}

	switch n := node.(type) {
	case nil:
		// nil handles cases where we check optional fields that are not set
		return nil, nil

	case parser.Expressions:
		for i, e := range n {
			mapped, err := nm.Map(e, stats)
			if err != nil {
				return nil, err
			}
			n[i] = mapped.(parser.Expr)
		}
		return n, nil

	case *parser.AggregateExpr:
		expr, err := nm.Map(n.Expr, stats)
		if err != nil {
			return nil, err
		}
		n.Expr = expr.(parser.Expr)
		return n, nil

	case *parser.BinaryExpr:
		lhs, err := nm.Map(n.LHS, stats)
		if err != nil {
			return nil, err
		}
		n.LHS = lhs.(parser.Expr)

		rhs, err := nm.Map(n.RHS, stats)
		if err != nil {
			return nil, err
		}
		n.RHS = rhs.(parser.Expr)

		return n, nil

	case *parser.Call:
		for i, e := range n.Args {
			mapped, err := nm.Map(e, stats)
			if err != nil {
				return nil, err
			}
			n.Args[i] = mapped.(parser.Expr)
		}
		return n, nil

	case *parser.SubqueryExpr:
		mapped, err := nm.Map(n.Expr, stats)
		if err != nil {
			return nil, err
		}
		n.Expr = mapped.(parser.Expr)
		return n, nil

	case *parser.ParenExpr:
		mapped, err := nm.Map(n.Expr, stats)
		if err != nil {
			return nil, err
		}
		n.Expr = mapped.(parser.Expr)
		return n, nil

	case *parser.UnaryExpr:
		mapped, err := nm.Map(n.Expr, stats)
		if err != nil {
			return nil, err
		}
		n.Expr = mapped.(parser.Expr)
		return n, nil

	case *parser.EvalStmt:
		mapped, err := nm.Map(n.Expr, stats)
		if err != nil {
			return nil, err
		}
		n.Expr = mapped.(parser.Expr)
		return n, nil

	case *parser.NumberLiteral, *parser.StringLiteral, *parser.VectorSelector, *parser.MatrixSelector:
		return n, nil

	default:
		return nil, errors.Errorf("nodeMapper: unhandled node type %T", node)
	}
}
