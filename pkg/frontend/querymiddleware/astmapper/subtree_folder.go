// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/subtree_folder.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"github.com/prometheus/prometheus/promql/parser"
)

// isVectorSelector returns whether the expr is a vector selector.
//
// It will never return an error, but does so to satisfy the predicate function signature for anyNode.
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
