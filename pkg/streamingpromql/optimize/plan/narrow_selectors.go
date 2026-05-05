// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"
	"slices"

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
	core.BINARY_LOR: {},
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
			Help: "Total number of queries where the optimization pass has added hints to narrow selectors for. Incremented whenever any binary expression in the query receives either on-matching include hints or exclude hints.",
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
		e, ok := node.(*core.BinaryExpression)
		if !ok {
			return nil
		}

		// Only set hints for operations that are compatible with adding extra selectors to
		// the right side of the expression. For example, "logical or" includes series from
		// the right side only when they _don't_ have matching label sets on the left side.
		if _, disallowed := disallowedOperations[e.Op]; disallowed {
			return nil
		}

		if e.VectorMatching == nil {
			return nil
		}

		// Labels created by label_replace or label_join anywhere within this binary
		// expression's subtree. We must not generate matchers for these labels because they
		// don't exist on the raw series fetched from storage.
		created := createdLabels(e)

		if e.VectorMatching.On && len(e.VectorMatching.MatchingLabels) > 0 {
			// "on (labels)" matching: use only the matching labels as Include hints, filtered
			// by any labels that are synthesised by label_replace/label_join.
			include := filterLabels(e.VectorMatching.MatchingLabels, created)
			if len(include) > 0 {
				if e.Hints == nil {
					e.Hints = &core.BinaryExpressionHints{}
				}
				e.Hints.Include = include
				sl := spanlogger.FromContext(ctx, n.logger)
				sl.DebugLog("msg", "setting on-matching query hint on binary expression", "labels", include)
				addedHint = true
			}
		} else if !e.VectorMatching.On {
			// Note: "on ()" with an empty label list falls through to neither branch.
			// The first condition requires On=true AND MatchingLabels>0, which excludes "on ()".
			// This branch requires On=false, which also excludes "on ()".
			// This is intentional: "on ()" matches all series regardless of labels,
			// so no narrowing hint is useful.
			// "without (labels)" / "ignoring (labels)" / default (no on/without) matching:
			// tell the operator to build RHS matchers from all LHS labels at query time,
			// excluding both the without/ignoring labels and any synthesised labels.
			// Setting Exclude with an empty Include signals exclude-matching mode.
			exclude := slices.Clone(e.VectorMatching.MatchingLabels)
			for lbl := range created {
				if !slices.Contains(exclude, lbl) {
					exclude = append(exclude, lbl)
				}
			}
			slices.Sort(exclude)
			if e.Hints == nil {
				e.Hints = &core.BinaryExpressionHints{}
			}
			e.Hints.Exclude = exclude
			sl := spanlogger.FromContext(ctx, n.logger)
			sl.DebugLog("msg", "setting exclude-matching query hint on binary expression", "excluded_labels", exclude)
			addedHint = true
		}

		return nil
	}))

	if addedHint {
		n.modified.Inc()
	}

	return plan, nil
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
