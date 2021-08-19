// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/subtree_folder.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"github.com/prometheus/prometheus/promql/parser"
)

// subtreeFolder is a NodeMapper which embeds an entire parser.Node in an embedded query,
// if it does not contain any previously embedded queries. This allows the query-frontend
// to "zip up" entire subtrees of an AST that have not already been parallelized.
type subtreeFolder struct{}

// newSubtreeFolder creates a subtreeFolder which can reduce an AST
// to one embedded query if it contains no embedded queries yet.
func newSubtreeFolder() ASTMapper {
	return NewASTNodeMapper(&subtreeFolder{})
}

// MapNode implements NodeMapper. This function always returns sharded=false because it's not
// its responsibility to rewrite the node in a shardable way.
func (f *subtreeFolder) MapNode(node parser.Node) (mapped parser.Node, finished, sharded bool, err error) {
	switch n := node.(type) {
	// Do not attempt to fold number or string leaf nodes.
	case *parser.NumberLiteral, *parser.StringLiteral:
		return n, true, false, nil
	}

	hasEmbeddedQueries, err := EvalPredicate(node, hasEmbeddedQueries)
	if err != nil {
		return nil, true, false, err
	}

	// Don't change the node if it already contains embedded queries.
	if hasEmbeddedQueries {
		return node, false, false, nil
	}

	expr, err := vectorSquasher(node)
	return expr, true, false, err
}

// hasEmbeddedQueries returns whether the node has embedded queries.
func hasEmbeddedQueries(node parser.Node) (bool, error) {
	switch n := node.(type) {
	case *parser.VectorSelector:
		if n.Name == EmbeddedQueriesMetricName {
			return true, nil
		}

	case *parser.MatrixSelector:
		return hasEmbeddedQueries(n.VectorSelector)
	}
	return false, nil
}

// EvalPredicate is a helper which walks the input node and returns true if any node in the subtree
// returns true for the specified predicate function.
func EvalPredicate(node parser.Node, fn predicate) (bool, error) {
	v := &visitor{
		fn: fn,
	}

	if err := parser.Walk(v, node, nil); err != nil {
		return false, err
	}
	return v.result, nil
}

type predicate = func(parser.Node) (bool, error)

type visitor struct {
	fn     predicate
	result bool
}

// Visit implements parser.Visitor
func (v *visitor) Visit(node parser.Node, path []parser.Node) (parser.Visitor, error) {
	// if the visitor has already seen a predicate success, don't overwrite
	if v.result {
		return nil, nil
	}

	var err error

	v.result, err = v.fn(node)
	if err != nil {
		return nil, err
	}
	if v.result {
		return nil, nil
	}
	return v, nil
}
