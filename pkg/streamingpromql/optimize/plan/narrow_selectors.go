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

	_ = optimize.Walk(plan.Root, optimize.VisitorFunc(func(node planning.Node, _ []planning.Node) (bool, error) {
		e, ok := node.(*core.BinaryExpression)
		if !ok {
			return true, nil
		}

		// Only set hints for operations that are compatible with adding extra selectors to
		// the right side of the expression. For example, "logical or" includes series from
		// the right side only when they _don't_ have matching label sets on the left side.
		if _, disallowed := disallowedOperations[e.Op]; disallowed {
			return true, nil
		}

		if e.VectorMatching == nil {
			return true, nil
		}

		// Skip narrowing when a fill value is set: narrowing the filled side would drop the unmatched
		// series that fill relies on to produce output.
		// TODO: still narrow the non-filled side, which is safe.
		if e.VectorMatching.FillValues.RhsSet || e.VectorMatching.FillValues.LhsSet {
			return nil
		}

		// Labels created by label_replace or label_join anywhere within this binary
		// expression's subtree. We must not generate matchers for these labels because they
		// don't exist on the raw series fetched from storage.
		created := createdLabels(e)

		// Note: "on ()" with an empty label list is intentionally not handled by either
		// branch. It matches all series regardless of labels, so no narrowing hint is useful.
		if e.VectorMatching.On {
			addedHint = n.hintsForOn(ctx, e, created) || addedHint
		} else {
			addedHint = n.hintsForIgnoring(ctx, e, created) || addedHint
		}

		return true, nil
	}))

	if addedHint {
		n.modified.Inc()
	}

	return plan, nil
}

// hintsForOn handles "on (labels)" matching: it uses the matching labels as Include
// hints, filtered by any labels synthesised by label_replace/label_join. Returns true
// if a hint was set.
func (n *NarrowSelectorsOptimizationPass) hintsForOn(ctx context.Context, e *core.BinaryExpression, created map[string]struct{}) bool {
	if len(e.VectorMatching.MatchingLabels) == 0 {
		return false
	}

	include := filterLabels(e.VectorMatching.MatchingLabels, created)
	if len(include) == 0 {
		return false
	}

	if e.Hints == nil {
		e.Hints = &core.BinaryExpressionHints{}
	}
	e.Hints.Include = include

	sl := spanlogger.FromContext(ctx, n.logger)
	sl.DebugLog("msg", "setting on-matching query hint on binary expression", "labels", include)
	return true
}

// hintsForIgnoring handles "ignoring (labels)" and default (no on/ignoring) matching.
// It first tries to derive Include hints from LHS aggregation grouping labels. If that
// fails, it falls back to exclude-matching mode, telling the operator to build RHS
// matchers from all LHS labels at query time (excluding ignored and synthesised labels).
// Returns true if a hint was set.
func (n *NarrowSelectorsOptimizationPass) hintsForIgnoring(ctx context.Context, e *core.BinaryExpression, created map[string]struct{}) bool {
	// Try to derive Include hints from LHS aggregation grouping labels.
	// If the LHS subtree contains an aggregation with a "by" clause, we know the
	// exact output labels statically and can use them as Include hints (after
	// filtering out any labels synthesised by label_replace/label_join AND any
	// labels listed in the ignoring clause, since those are not used for matching).
	include := includeFromLHS(e.LHS, created)
	if len(include) > 0 && len(e.VectorMatching.MatchingLabels) > 0 {
		// Remove ignoring labels from the include set.
		filtered := make([]string, 0, len(include))
		for _, lbl := range include {
			if !slices.Contains(e.VectorMatching.MatchingLabels, lbl) {
				filtered = append(filtered, lbl)
			}
		}
		include = filtered
	}

	if len(include) > 0 {
		if e.Hints == nil {
			e.Hints = &core.BinaryExpressionHints{}
		}
		e.Hints.Include = include

		sl := spanlogger.FromContext(ctx, n.logger)
		sl.DebugLog("msg", "setting include query hint on binary expression from LHS aggregation", "labels", include)
		return true
	}

	// No LHS aggregation labels found: fall back to exclude-matching mode.
	// Tell the operator to build RHS matchers from all LHS labels at query time,
	// excluding both the ignoring labels and any synthesised labels.
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
	return true
}

// includeFromLHS walks the LHS subtree of a binary expression looking for an
// aggregation with a "by" clause. If found, it returns the grouping labels
// (filtered by created labels) as candidate Include hints. It recurses through
// nested binary expressions (following their LHS) so that chains like
// "sum by (region) (X) / (sum(Y) + sum(Z))" still find the aggregation.
func includeFromLHS(node planning.Node, created map[string]struct{}) []string {
	switch e := node.(type) {
	case *core.AggregateExpression:
		if !e.Without && len(e.Grouping) > 0 {
			return filterLabels(e.Grouping, created)
		}
	case *core.BinaryExpression:
		return includeFromLHS(e.LHS, created)
	}

	// If the current node isn't a binary expression or aggregation, look at
	// children to find a suitable aggregation (e.g. through a function call wrapper).
	for child := range planning.ChildrenIter(node) {
		if include := includeFromLHS(child, created); len(include) > 0 {
			return include
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

// createdLabels returns a set of label names created by a call to label_replace, label_join or
// histogram_quantiles by any children of the given node.
func createdLabels(node planning.Node) map[string]struct{} {
	created := make(map[string]struct{})

	_ = optimize.Walk(node, optimize.VisitorFunc(func(n planning.Node, path []planning.Node) (bool, error) {
		if f, ok := n.(*core.FunctionCall); ok {
			switch f.Function {
			case functions.FUNCTION_LABEL_REPLACE, functions.FUNCTION_LABEL_JOIN, functions.FUNCTION_HISTOGRAM_QUANTILES:
				// The second parameter for label_replace, label_join and histogram_quantiles is the
				// destination label. It is synthesised by the function and does not exist on the raw
				// series fetched from storage, so we must not generate a matcher for it.
				if len(f.Args) > 1 {
					if lbl, ok := f.Args[1].(*core.StringLiteral); ok {
						created[lbl.Value] = struct{}{}
					}
				}
			}
		}

		return true, nil
	}))

	return created
}
