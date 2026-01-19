// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var disallowedOperations = map[core.BinaryOperation]struct{}{
	core.BINARY_LOR:     {},
	core.BINARY_LUNLESS: {},
}

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
	res := optimize.InspectSelectors(plan.Root)
	if !res.HasSelectors || res.IsRewrittenByMiddleware {
		return plan, nil
	}

	n.attempts.Inc()
	addedHint := false

	_ = optimize.Walk(plan.Root, optimize.VisitorFunc(func(node planning.Node, _ []planning.Node) error {
		if e, ok := node.(*core.BinaryExpression); ok {
			// Only find hints for this binary expression if it is an operation that is compatible
			// with adding extra selectors to the right side of the expression. For example, "logical
			// or" includes series from the right side only when they _don't_ have matching label sets
			// on the left side.
			if _, disallowed := disallowedOperations[e.Op]; !disallowed {
				// If this is a binary expression, try to find appropriate labels to use as hints
				// based on joins or aggregations being performed by child nodes. We start with an
				// empty "created" set of labels that we _cannot_ use as hints. This is populated
				// and checked when generating hints.
				if include := n.includeFromNode(ctx, e, nil); len(include) > 0 {
					if e.Hints == nil {
						e.Hints = &core.BinaryExpressionHints{}
					}

					e.Hints.Include = include
					sl := spanlogger.FromContext(ctx, n.logger)
					sl.DebugLog("msg", "setting query hint on binary expression", "labels", include)
					addedHint = true
				}
			}
		}
		return nil
	}))

	if addedHint {
		n.modified.Inc()
	}

	return plan, nil
}

func (n *NarrowSelectorsOptimizationPass) includeFromNode(ctx context.Context, node planning.Node, created map[string]struct{}) []string {
	switch e := node.(type) {
	case *core.BinaryExpression:
		// The current node is a binary expression: we only want to exclude created labels
		// (via label_replace or label_join) from hints if they are created by the left or
		// right side of the current binary expression. Labels created by a function call in
		// a parent expression shouldn't affect hints applied to this expression.
		created = createdLabels(e)
		if e.VectorMatching != nil && e.VectorMatching.On && len(e.VectorMatching.MatchingLabels) > 0 {
			if filtered := filterLabels(e.VectorMatching.MatchingLabels, created); len(filtered) > 0 {
				return filtered
			}

			return nil
		}

		// If we aren't joining sides of this binary expression by particular labels, look for an
		// aggregation on the left side of this binary expression to see if there's a label we could
		// use as a hint. We pass along any labels created from label_replace or label_join on the
		// left or right side of this binary expression when trying to find labels for hints to make
		// sure we don't use them.
		return n.includeFromNode(ctx, e.LHS, created)
	case *core.AggregateExpression:
		if !e.Without && len(e.Grouping) > 0 {
			// Make sure to remove labels from potential hints that have been created by a function
			// call (via label_replace or label_join) in a parent expression.
			if filtered := filterLabels(e.Grouping, created); len(filtered) > 0 {
				return filtered
			}

			return nil
		}
	}

	// If the current node isn't a binary expression or aggregation, keep looking at the
	// children to see if there are any that we can use to find a suitable query hint.
	for child := range planning.ChildrenIter(node) {
		if i := n.includeFromNode(ctx, child, created); len(i) > 0 {
			return i
		}
	}

	return nil
}

// filterLabels returns a new slice of labels that does not include any label in the created set.
func filterLabels(lbls []string, created map[string]struct{}) []string {
	out := make([]string, 0, len(lbls))
	for _, lbl := range lbls {
		if _, ok := created[lbl]; !ok {
			out = append(out, lbl)
		}
	}

	return out
}

// createdLabels returns a set of label names created by a call to label_replace or label_join
// by any children of the given node.
func createdLabels(node planning.Node) map[string]struct{} {
	created := make(map[string]struct{})

	_ = optimize.Walk(node, optimize.VisitorFunc(func(n planning.Node, path []planning.Node) error {
		if f, ok := n.(*core.FunctionCall); ok {
			if (f.Function == functions.FUNCTION_LABEL_REPLACE || f.Function == functions.FUNCTION_LABEL_JOIN) && len(f.Args) > 1 {
				// The second parameter for both label_replace and label_join is the destination label.
				if lbl, ok := f.Args[1].(*core.StringLiteral); ok {
					created[lbl.Value] = struct{}{}
				}
			}
		}

		return nil
	}))

	return created
}
