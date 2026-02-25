// SPDX-License-Identifier: AGPL-3.0-only

package optimize

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

// Walk visits the specified node and all children, depth first.
// Visiting is stopped if visitor returns an error at any point.
func Walk(node planning.Node, visitor Visitor) error {
	// The size of 4 elements here is picked to not have to resize
	// for simple plans and matches the value used in Prometheus'
	// function for walking an AST.
	var path [4]planning.Node
	return walk(node, path[:0], visitor)
}

func walk(node planning.Node, path []planning.Node, visitor Visitor) error {
	if err := visitor.Visit(node, path); err != nil {
		return err
	}

	path = append(path, node)
	for child := range planning.ChildrenIter(node) {
		if err := walk(child, path, visitor); err != nil {
			return err
		}
	}

	return nil
}

// Visitor is used to visit a single node at time in a query plan being walked
// as part of Walk.
type Visitor interface {
	// Visit examines node and has access to the path of nodes visited
	// before node was reached, not including node, with the root in the
	// first index and closest parent in the last.
	Visit(node planning.Node, path []planning.Node) error
}

type VisitorFunc func(node planning.Node, path []planning.Node) error

func (f VisitorFunc) Visit(node planning.Node, path []planning.Node) error {
	return f(node, path)
}

type InspectSelectorsResult struct {
	// HasSelectors indicates if any node in the tree has matrix or vector selectors.
	HasSelectors bool
	// IsRewrittenByMiddleware indicates if the query has been rewritten by query sharding or by
	// subquery spin-off middlewares.
	IsRewrittenByMiddleware bool
}

// InspectSelectors traverses a tree of Nodes and returns a result that indicates if the query
// can or should be modified by optimization passes based on the type of selectors it has, if any.
// It is up to each optimization pass to decide if this is needed or if the values in
// InspectSelectorsResult matter to the pass.
func InspectSelectors(node planning.Node) InspectSelectorsResult {
	var res InspectSelectorsResult

	_ = Walk(node, VisitorFunc(func(n planning.Node, _ []planning.Node) error {
		switch e := n.(type) {
		case *core.MatrixSelector:
			res.HasSelectors = true
			res.IsRewrittenByMiddleware = res.IsRewrittenByMiddleware || IsSpunOff(e)
		case *core.VectorSelector:
			res.HasSelectors = true
			res.IsRewrittenByMiddleware = res.IsRewrittenByMiddleware || isSharded(e)
		}
		return nil
	}))

	return res
}

func isSharded(s *core.VectorSelector) bool {
	for _, m := range s.Matchers {
		if m.Name == model.MetricNameLabel && m.Type == labels.MatchEqual && m.Value == astmapper.EmbeddedQueriesMetricName {
			return true
		}
	}

	return false
}

func IsSpunOff(s *core.MatrixSelector) bool {
	for _, m := range s.Matchers {
		if m.Name == model.MetricNameLabel && m.Type == labels.MatchEqual && m.Value == astmapper.SubqueryMetricName {
			return true
		}
	}

	return false
}
