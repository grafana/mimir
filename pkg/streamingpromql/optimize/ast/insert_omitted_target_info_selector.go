// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"fmt"
	"slices"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
)

const (
	targetInfoName = "target_info"
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
	switch length := len(expr.Args); length {
	case 1:
		expr.Args = append(expr.Args, defaultTargetInfoSelector())
	case 2:
		dataLabelMatchersExpr, ok := expr.Args[1].(*parser.VectorSelector)
		if !ok {
			return nil, fmt.Errorf("expected second argument to 'info' function to be a VectorSelector, got %T", expr.Args[1])
		}

		if hasMetricNameMatcher := slices.ContainsFunc(dataLabelMatchersExpr.LabelMatchers, func(matcher *labels.Matcher) bool {
			return matcher.Name == model.MetricNameLabel
		}); !hasMetricNameMatcher {
			dataLabelMatchersExpr.LabelMatchers = append(dataLabelMatchersExpr.LabelMatchers, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, targetInfoName))
		}
	default:
		return nil, fmt.Errorf("expected 'info' function to have 1 or 2 arguments, got %d", length)
	}
	return h, nil
}

func defaultTargetInfoSelector() *parser.VectorSelector {
	return &parser.VectorSelector{
		Name: targetInfoName,
		PosRange: posrange.PositionRange{
			Start: 0,
			End:   posrange.Pos(len(targetInfoName)),
		},
		LabelMatchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, targetInfoName),
		},
	}
}
