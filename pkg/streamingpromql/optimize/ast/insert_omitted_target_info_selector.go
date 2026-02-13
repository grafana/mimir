// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

type InsertOmittedTargetInfoSelector struct{}

func (h *InsertOmittedTargetInfoSelector) Name() string {
	return "Insert omitted target info selector"
}

func (h *InsertOmittedTargetInfoSelector) Apply(_ context.Context, root parser.Expr) (parser.Expr, error) {
	if err := parser.Walk(h, root, nil); err != nil {
		return nil, err
	}
	return root, nil
}

func (h *InsertOmittedTargetInfoSelector) Visit(node parser.Node, _ []parser.Node) (parser.Visitor, error) {
	expr, isCall := node.(*parser.Call)
	if !isCall {
		return h, nil
	}
	if expr.Func.Name != "info" {
		return h, nil
	}
	length := len(expr.Args)
	switch {
	case length == 1:
		infoExpr, err := parser.ParseExpr("target_info")
		if err != nil {
			return nil, fmt.Errorf("failed to parse target_info expression: %v", err)
		}
		expr.Args = append(expr.Args, infoExpr)
	case length >= 2:
		dataLabelMatchersExpr, ok := expr.Args[1].(*parser.VectorSelector)
		if !ok {
			return nil, fmt.Errorf("expected second argument to 'info' function to be a VectorSelector, got %T", expr.Args[1])
		}
		hasMetricNameMatcher := false
		for _, m := range dataLabelMatchersExpr.LabelMatchers {
			if m.Name == model.MetricNameLabel {
				hasMetricNameMatcher = true
				break
			}
		}
		if !hasMetricNameMatcher {
			dataLabelMatchersExpr.LabelMatchers = append(dataLabelMatchersExpr.LabelMatchers, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "target_info"))
		}
	default:
		return nil, fmt.Errorf("expected 'info' function to have 1 or 2 arguments, got %d", length)
	}
	return h, nil
}
