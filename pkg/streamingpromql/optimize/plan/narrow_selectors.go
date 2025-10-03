// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"
	"slices"

	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// NarrowSelectorsOptimizationPass examines a QueryPlan to determine if there are any
// labels that can be used to reduce the amount of data fetched on one side of binary
// expression and propagates those labels as a Hint on the binary expression.
type NarrowSelectorsOptimizationPass struct {
	logger log.Logger
}

func NewNarrowSelectorsOptimizationPass(logger log.Logger) *NarrowSelectorsOptimizationPass {
	return &NarrowSelectorsOptimizationPass{
		logger: logger,
	}
}

func (n *NarrowSelectorsOptimizationPass) Name() string {
	return "narrow selectors"
}

func (n *NarrowSelectorsOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan) (*planning.QueryPlan, error) {
	// If this query plan doesn't contain any selectors for us to apply hints for or if the
	// query has been rewritten to be sharded or spun off, don't attempt to generate any query
	// hints since there are no selectors that we understand and can add matchers to.
	res := optimize.Inspect(plan.Root)
	if !res.HasSelectors || res.IsRewrittenByMiddleware {
		return plan, nil
	}

	if err := n.applyToNode(ctx, plan.Root); err != nil {
		return nil, err
	}

	return plan, nil
}

func (n *NarrowSelectorsOptimizationPass) applyToNode(ctx context.Context, node planning.Node) error {
	switch e := node.(type) {
	case *core.BinaryExpression:
		// Set hints for a binary expression based on the expression itself and any
		// children from the left hand side of the expression. Note that this stops
		// after finding the first node that allows us to generate hints for the query
		// (binary expressions or aggregations).
		e.Hints = n.hintsFromNode(ctx, e)
		if e.Hints != nil {
			sl := spanlogger.FromContext(ctx, n.logger)
			sl.DebugLog("msg", "setting query hint on binary expression", "labels", e.Hints.GetInclude())
		}
	}

	// Set hints for any child binary expressions of the current node.
	for _, child := range node.Children() {
		if err := n.applyToNode(ctx, child); err != nil {
			return err
		}
	}

	return nil
}

func (n *NarrowSelectorsOptimizationPass) hintsFromNode(ctx context.Context, node planning.Node) *core.BinaryExpressionHints {
	switch e := node.(type) {
	case *core.BinaryExpression:
		if e.VectorMatching != nil && e.VectorMatching.On && len(e.VectorMatching.MatchingLabels) > 0 {
			return &core.BinaryExpressionHints{
				Include: slices.Clone(e.VectorMatching.MatchingLabels),
			}
		}

		// If this is a binary expression with no matching, try to find a suitable query hint from
		// the left side (such as an aggregation), don't bother checking the right side since we use
		// the left side to generate extra matchers for the right side in the operator.
		return n.hintsFromNode(ctx, e.LHS)
	case *core.AggregateExpression:
		if !e.Without && len(e.Grouping) > 0 {
			return &core.BinaryExpressionHints{
				Include: slices.Clone(e.Grouping),
			}
		}
	}

	// If the current node isn't a binary expression or aggregation, keep looking at the
	// children to see if there are any that we can use to find a suitable query hint.
	for _, child := range node.Children() {
		if h := n.hintsFromNode(ctx, child); h != nil {
			return h
		}
	}

	return nil
}
