// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/subtree_folder.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"github.com/prometheus/prometheus/promql/parser"
)

// subtreeFolder is a ExprMapper which embeds an entire parser.Expr in an embedded query,
// if it does not contain any previously embedded queries. This allows the query-frontend
// to "zip up" entire subtrees of an AST that have not already been parallelized.
type subtreeFolder struct{}

// newSubtreeFolder creates a subtreeFolder which can reduce an AST
// to one embedded query if it contains no embedded queries yet.
func newSubtreeFolder() ASTMapper {
	return NewASTExprMapper(&subtreeFolder{})
}

// MapExpr implements ExprMapper.
func (f *subtreeFolder) MapExpr(expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	hasEmbeddedQueries, err := anyNode(expr, hasEmbeddedQueries)
	if err != nil {
		return nil, true, err
	}

	// Don't change the expr if it already contains embedded queries.
	if hasEmbeddedQueries {
		return expr, false, nil
	}

	hasVectorSelector, err := anyNode(expr, isVectorSelector)
	if err != nil {
		return nil, true, err
	}

	// Change the expr if it contains vector selectors, as only those need to be embedded.
	if hasVectorSelector {
		expr, err := VectorSquasher(NewEmbeddedQuery(expr.String(), nil))
		return expr, true, err
	}
	return expr, false, nil
}

// hasEmbeddedQueries returns whether the expr has embedded queries.
func hasEmbeddedQueries(node parser.Node) (bool, error) {
	switch n := node.(type) {
	case *parser.VectorSelector:
		if n.Name == EmbeddedQueriesMetricName {
			return true, nil
		}
	}
	return false, nil
}

// hasEmbeddedQueries returns whether the expr is a vector selector.
func isVectorSelector(n parser.Node) (bool, error) {
	_, ok := n.(*parser.VectorSelector)
	return ok, nil
}

// anyNode is a helper which walks the input node and returns true if any node in the subtree
// returns true for the specified predicate function.
func anyNode(node parser.Node, fn predicate) (bool, error) {
	v := &visitor{
		fn: fn,
	}

	if err := parser.Walk(v, node, nil); err != nil {
		return false, err
	}
	return v.result, nil
}

// visitNode recursively traverse the node's subtree and call fn for each node encountered.
func visitNode(node parser.Node, fn func(node parser.Node)) {
	_ = parser.Walk(&visitor{fn: func(node parser.Node) (bool, error) {
		fn(node)
		return false, nil
	}}, node, nil)
}

type predicate = func(parser.Node) (bool, error)

type visitor struct {
	fn     predicate
	result bool
}

// Visit implements parser.Visitor
func (v *visitor) Visit(node parser.Node, _ []parser.Node) (parser.Visitor, error) {
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
