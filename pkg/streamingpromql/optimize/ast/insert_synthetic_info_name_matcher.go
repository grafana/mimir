// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// InsertSyntheticInfoNameMatcher ensures that the second argument of an info() call
// always contains a positive __name__ matcher. When the user supplies only negative
// __name__ matchers (e.g. {__name__!~".+_info"}), this pass appends a synthetic
// __name__=~".+_info" matcher so that storage selects only info series.
//
// This mirrors upstream Prometheus' effectiveInfoNameMatchers logic, but applies
// it at AST optimization time rather than at query evaluation. Doing so guarantees
// the synthetic matcher is part of the planned query that reaches selectors,
// including under EagerLoad / query sharding where Prepare() loads series before
// any runtime matcher injection could happen.
//
// This pass relies on InsertOmittedTargetInfoSelector running first to ensure
// info() always has two arguments and that a missing __name__ matcher has been
// supplied with the default target_info=.
type InsertSyntheticInfoNameMatcher struct{}

func (h *InsertSyntheticInfoNameMatcher) Name() string {
	return "Insert synthetic info name matcher"
}

func (h *InsertSyntheticInfoNameMatcher) Apply(_ context.Context, root parser.Expr) (parser.Expr, error) {
	if err := parser.Walk(h, root, nil); err != nil {
		return nil, err
	}
	return root, nil
}

func (h *InsertSyntheticInfoNameMatcher) Visit(node parser.Node, _ []parser.Node) (parser.Visitor, error) {
	expr, isCall := node.(*parser.Call)
	if !isCall {
		return h, nil
	}
	if expr.Func.Name != "info" {
		return h, nil
	}
	if len(expr.Args) != 2 {
		return h, nil
	}
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
	if !hasPositive && hasNegative {
		dataLabelMatchersExpr.LabelMatchers = append(dataLabelMatchersExpr.LabelMatchers, labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+_info"))
	}
	return h, nil
}
