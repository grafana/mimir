// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/subtree_folder.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser"
)

// subtreeFolder is a ExprMapper which embeds an entire parser.Expr in an embedded query,
// if it does not contain any previously embedded queries. This allows the query-frontend
// to "zip up" entire subtrees of an AST that have not already been parallelized.
type subtreeFolder struct {
	squasher Squasher
}

// newSubtreeFolder creates a subtreeFolder which can reduce an AST
// to one embedded query if it contains no embedded queries yet.
func newSubtreeFolder(squasher Squasher) ASTMapper {
	return NewASTExprMapper(&subtreeFolder{squasher: squasher})
}

// MapExpr implements ExprMapper.
func (f *subtreeFolder) MapExpr(ctx context.Context, expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	// Don't change the expr if it already contains embedded queries.
	if anyNode(expr, f.squasher.ContainsSquashedExpression) {
		return expr, false, nil
	}

	// Change the expr if it contains vector selectors, as only those need to be embedded.
	if anyNode(expr, isVectorSelector) {
		expr, err := f.squasher.Squash(NewEmbeddedQuery(expr, nil))
		return expr, true, err
	}
	return expr, false, nil
}

// isVectorSelector returns whether the expr is a vector selector.
func isVectorSelector(n parser.Node) bool {
	_, ok := n.(*parser.VectorSelector)
	return ok
}

// anyNode is a helper which walks the input node and returns true if any node in the subtree
// returns true for the specified predicate function.
func anyNode(node parser.Node, fn predicate) bool {
	if fn(node) {
		return true
	}

	for node := range parser.ChildrenIter(node) {
		if anyNode(node, fn) {
			return true
		}
	}
	return false
}

type predicate = func(parser.Node) bool
