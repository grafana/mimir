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
	// Map the input node and returns the mapped node as well as whether the
	// returned mapped node has been rewritten in a shardable way.
	Map(node parser.Node) (mapped parser.Node, sharded bool, err error)
}

// MapperFunc is a function adapter for ASTMapper
type MapperFunc func(node parser.Node) (parser.Node, bool, error)

// Map applies a mapperfunc as an ASTMapper
func (fn MapperFunc) Map(node parser.Node) (parser.Node, bool, error) {
	return fn(node)
}

// MultiMapper can compose multiple ASTMappers
type MultiMapper struct {
	mappers []ASTMapper
}

// Map implements ASTMapper
func (m *MultiMapper) Map(node parser.Node) (parser.Node, bool, error) {
	var result parser.Node = node
	var err error

	if len(m.mappers) == 0 {
		return nil, false, errors.New("MultiMapper: No mappers registered")
	}

	sharded := false
	for _, x := range m.mappers {
		var s bool

		result, s, err = x.Map(result)
		if err != nil {
			return nil, false, err
		}

		sharded = sharded || s
	}

	return result, sharded, nil
}

// Register adds ASTMappers into a multimapper.
// Since registered functions are applied in the order they're registered, it's advised to register them
// in decreasing priority and only operate on nodes that each function cares about, defaulting to CloneNode.
func (m *MultiMapper) Register(xs ...ASTMapper) {
	m.mappers = append(m.mappers, xs...)
}

// NewMultiMapper instantiates an ASTMapper from multiple ASTMappers.
func NewMultiMapper(xs ...ASTMapper) *MultiMapper {
	m := &MultiMapper{}
	m.Register(xs...)
	return m
}

// CloneNode is a helper function to clone a node.
func CloneNode(node parser.Node) (parser.Node, error) {
	return parser.ParseExpr(node.String())
}

type NodeMapper interface {
	// MapNode either maps a single AST node or returns the unaltered node.
	// It returns a finished bool to signal that no further recursion is necessary,
	// and a sharded bool to signal whether the returned mapped node has been
	// rewritten in a shardable way.
	MapNode(node parser.Node) (mapped parser.Node, finished, sharded bool, err error)
}

// NodeMapperFunc is an adapter for NodeMapper
type NodeMapperFunc func(node parser.Node) (parser.Node, bool, bool, error)

// MapNode applies a NodeMapperFunc as a NodeMapper
func (f NodeMapperFunc) MapNode(node parser.Node) (parser.Node, bool, bool, error) {
	return f(node)
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
func (nm ASTNodeMapper) Map(node parser.Node) (parser.Node, bool, error) {
	node, finished, sharded, err := nm.MapNode(node)
	if err != nil {
		return nil, false, err
	}

	if finished {
		return node, sharded, nil
	}

	switch n := node.(type) {
	case nil:
		// nil handles cases where we check optional fields that are not set
		return nil, false, nil

	case parser.Expressions:
		for i, e := range n {
			mapped, s, err := nm.Map(e)
			if err != nil {
				return nil, false, err
			}
			n[i] = mapped.(parser.Expr)
			sharded = sharded || s
		}
		return n, sharded, nil

	case *parser.AggregateExpr:
		expr, s, err := nm.Map(n.Expr)
		if err != nil {
			return nil, false, err
		}
		n.Expr = expr.(parser.Expr)
		sharded = sharded || s
		return n, sharded, nil

	case *parser.BinaryExpr:
		lhs, s, err := nm.Map(n.LHS)
		if err != nil {
			return nil, false, err
		}
		n.LHS = lhs.(parser.Expr)
		sharded = sharded || s

		rhs, s, err := nm.Map(n.RHS)
		if err != nil {
			return nil, false, err
		}
		n.RHS = rhs.(parser.Expr)
		sharded = sharded || s

		return n, sharded, nil

	case *parser.Call:
		for i, e := range n.Args {
			mapped, s, err := nm.Map(e)
			if err != nil {
				return nil, false, err
			}
			n.Args[i] = mapped.(parser.Expr)
			sharded = sharded || s
		}
		return n, sharded, nil

	case *parser.SubqueryExpr:
		mapped, s, err := nm.Map(n.Expr)
		if err != nil {
			return nil, false, err
		}
		n.Expr = mapped.(parser.Expr)
		sharded = sharded || s
		return n, sharded, nil

	case *parser.ParenExpr:
		mapped, s, err := nm.Map(n.Expr)
		if err != nil {
			return nil, false, err
		}
		n.Expr = mapped.(parser.Expr)
		sharded = sharded || s
		return n, sharded, nil

	case *parser.UnaryExpr:
		mapped, s, err := nm.Map(n.Expr)
		if err != nil {
			return nil, false, err
		}
		n.Expr = mapped.(parser.Expr)
		sharded = sharded || s
		return n, sharded, nil

	case *parser.EvalStmt:
		mapped, s, err := nm.Map(n.Expr)
		if err != nil {
			return nil, false, err
		}
		n.Expr = mapped.(parser.Expr)
		sharded = sharded || s
		return n, sharded, nil

	case *parser.NumberLiteral, *parser.StringLiteral, *parser.VectorSelector, *parser.MatrixSelector:
		return n, false, nil

	default:
		return nil, false, errors.Errorf("nodeMapper: unhandled node type %T", node)
	}
}
