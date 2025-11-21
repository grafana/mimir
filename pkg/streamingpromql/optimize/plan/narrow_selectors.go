// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// NarrowSelectorsOptimizationPass examines a QueryPlan to determine if there are any
// labels that can be used to reduce the amount of data fetched on one side of binary
// expression and propagates those labels as a Hint on the binary expression.
type NarrowSelectorsOptimizationPass struct {
	attempts prometheus.Counter
	modified prometheus.Counter
	logger   log.Logger
}

func NewNarrowSelectorsOptimizationPass(reg prometheus.Registerer, logger log.Logger) *NarrowSelectorsOptimizationPass {
	return &NarrowSelectorsOptimizationPass{
		attempts: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_narrow_selectors_attempted_total",
			Help: "Total number of queries that the optimization pass has attempted to add hints to narrow selectors for.",
		}),
		modified: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_narrow_selectors_modified_total",
			Help: "Total number of queries where the optimization pass has been able to add hints to narrow selectors for.",
		}),
		logger: logger,
	}
}

func (n *NarrowSelectorsOptimizationPass) Name() string {
	return "narrow selectors"
}

func (n *NarrowSelectorsOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, _ planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	// If this query plan doesn't contain any selectors for us to apply hints for, if the
	// query has been rewritten to be sharded or spun off, don't attempt to generate any
	// query hints since there are no selectors that we understand and can add matchers to.
	res := optimize.Inspect(plan.Root)
	if !res.HasSelectors || res.IsRewrittenByMiddleware {
		return plan, nil
	}

	n.attempts.Inc()
	addedHint, err := n.applyToNode(ctx, plan.Root, res)
	if err != nil {
		return nil, err
	}

	if addedHint {
		n.modified.Inc()
	}

	return plan, nil
}

func (n *NarrowSelectorsOptimizationPass) applyToNode(ctx context.Context, node planning.Node, res optimize.InspectResult) (bool, error) {
	addedHint := false

	switch e := node.(type) {
	case *core.BinaryExpression:
		// Set hints for a binary expression based on the expression itself and any
		// children from the left hand side of the expression. Note that this stops
		// after finding the first node that allows us to generate hints for the query
		// (binary expressions or aggregations).
		e.Hints = n.hintsFromNode(ctx, e, res)
		if e.Hints != nil {
			sl := spanlogger.FromContext(ctx, n.logger)
			sl.DebugLog("msg", "setting query hint on binary expression", "labels", e.Hints.GetInclude())
			addedHint = true
		}
	}

	// Set hints for any child binary expressions of the current node.
	for child := range planning.ChildrenIter(node) {
		childHint, err := n.applyToNode(ctx, child, res)
		if err != nil {
			return false, err
		}

		addedHint = addedHint || childHint
	}

	return addedHint, nil
}

func (n *NarrowSelectorsOptimizationPass) hintsFromNode(ctx context.Context, node planning.Node, res optimize.InspectResult) *core.BinaryExpressionHints {
	switch e := node.(type) {
	case *core.BinaryExpression:
		if e.VectorMatching != nil && e.VectorMatching.On && len(e.VectorMatching.MatchingLabels) > 0 {
			// Only create a hint for matching labels that haven't been created by a function
			// like label_replace or label_join.
			if filtered := filterCreatedLabels(e.VectorMatching.MatchingLabels, res); len(filtered) > 0 {
				return &core.BinaryExpressionHints{
					Include: filtered,
				}
			}
		}

		// If this is a binary expression with no matching, try to find a suitable query hint from
		// the left side (such as an aggregation), don't bother checking the right side since we use
		// the left side to generate extra matchers for the right side in the operator.
		return n.hintsFromNode(ctx, e.LHS, res)
	case *core.AggregateExpression:
		if !e.Without && len(e.Grouping) > 0 {
			// Only create a hint for matching labels that haven't been created by a function
			// like label_replace or label_join.
			if filtered := filterCreatedLabels(e.Grouping, res); len(filtered) > 0 {
				return &core.BinaryExpressionHints{
					Include: filtered,
				}
			}
		}
	}

	// If the current node isn't a binary expression or aggregation, keep looking at the
	// children to see if there are any that we can use to find a suitable query hint.
	for child := range planning.ChildrenIter(node) {
		if h := n.hintsFromNode(ctx, child, res); h != nil {
			return h
		}
	}

	return nil
}

// filterCreatedLabels returns the subset of lbls that is not a label created by a PromQL
// function like label_replace or label_join.
func filterCreatedLabels(lbls []string, res optimize.InspectResult) []string {
	out := make([]string, 0, len(lbls))
	for _, v := range lbls {
		if _, ok := res.CreatedLabels[v]; !ok {
			out = append(out, v)
		}
	}

	return out
}
