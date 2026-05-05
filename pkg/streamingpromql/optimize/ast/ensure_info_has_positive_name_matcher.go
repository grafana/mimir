// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
)

const (
	targetInfoName        = "target_info"
	infoMetricNamePattern = ".+_info"
)

// EnsureInfoHasPositiveNameMatcher rewrites info() calls so that, after this
// pass, every call has exactly two arguments and the second argument's
// LabelMatchers contains at least one positive (MatchEqual or MatchRegexp)
// __name__ matcher. Two failure modes the pass guards against:
//
//   - info(metric) — no second argument, so no info-series selector at all.
//     The pass appends a default target_info selector.
//   - info(metric, {__name__!~".+_info", ...}) — only-negative __name__
//     matchers, which would let storage return every non-info series. The
//     pass appends a synthetic .+_info regex matcher so storage selects only
//     info series.
//
// The pass is the AST-level analogue of upstream Prometheus' effectiveInfoNameMatchers
// rule. Doing it at AST optimization time (rather than at query evaluation) ensures
// the synthetic matcher is part of the planned query that reaches selectors,
// including under EagerLoad / query sharding where Prepare() loads series before
// any runtime injection could happen.
//
// The pass is idempotent — re-running on its own output is a no-op because each
// branch produces a positive __name__ matcher that the predicate then detects.
type EnsureInfoHasPositiveNameMatcher struct{}

func (h *EnsureInfoHasPositiveNameMatcher) Name() string {
	return "Ensure info has positive name matcher"
}

func (h *EnsureInfoHasPositiveNameMatcher) Apply(_ context.Context, root parser.Expr) (parser.Expr, error) {
	if err := parser.Walk(h, root, nil); err != nil {
		return nil, err
	}
	return root, nil
}

func (h *EnsureInfoHasPositiveNameMatcher) Visit(node parser.Node, _ []parser.Node) (parser.Visitor, error) {
	expr, isCall := node.(*parser.Call)
	if !isCall {
		return h, nil
	}
	if expr.Func.Name != "info" {
		return h, nil
	}
	switch length := len(expr.Args); length {
	case 1:
		// No data-label selector supplied; default to target_info. The new argument
		// has no parser position, so use the position of the info call itself.
		expr.Args = append(expr.Args, defaultTargetInfoSelector(expr.PosRange))
	case 2:
		dataLabelMatchersExpr, ok := expr.Args[1].(*parser.VectorSelector)
		if !ok {
			return nil, fmt.Errorf("expected second argument to 'info' function to be a VectorSelector, got %T", expr.Args[1])
		}
		hasPositive, hasNegative := false, false
		for _, m := range dataLabelMatchersExpr.LabelMatchers {
			if m.Name != model.MetricNameLabel {
				continue
			}
			if m.Type == labels.MatchEqual || m.Type == labels.MatchRegexp {
				hasPositive = true
				break
			}
			hasNegative = true
		}
		switch {
		case hasPositive:
			// Already has a positive __name__ matcher; nothing to do.
		case hasNegative:
			// Only negative __name__ matchers; append a synthetic .+_info matcher so
			// storage selects only info series.
			dataLabelMatchersExpr.LabelMatchers = append(dataLabelMatchersExpr.LabelMatchers, labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, infoMetricNamePattern))
		default:
			// No __name__ matchers at all; default to target_info=.
			dataLabelMatchersExpr.LabelMatchers = append(dataLabelMatchersExpr.LabelMatchers, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, targetInfoName))
		}
	default:
		return nil, fmt.Errorf("expected 'info' function to have 1 or 2 arguments, got %d", length)
	}
	return h, nil
}

func defaultTargetInfoSelector(infoPos posrange.PositionRange) *parser.VectorSelector {
	return &parser.VectorSelector{
		Name:     targetInfoName,
		PosRange: infoPos,
		LabelMatchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, targetInfoName),
		},
	}
}
