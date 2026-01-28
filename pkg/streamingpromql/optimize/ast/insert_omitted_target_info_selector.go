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
	var inspectionError error
	parser.Inspect(root, func(node parser.Node, _ []parser.Node) error {
		switch expr := node.(type) {
		case *parser.Call:
			if expr.Func.Name == "info" {
				switch length := len(expr.Args); length {
				case 1:
					infoExpr, err := parser.ParseExpr("target_info")
					if err != nil {
						inspectionError = fmt.Errorf("failed to parse target_info expression: %v", err)
						return inspectionError
					}
					expr.Args = append(expr.Args, infoExpr)
				case 2:
					dataLabelMatchersExpr, ok := expr.Args[1].(*parser.VectorSelector)
					if !ok {
						inspectionError = fmt.Errorf("expected second argument to 'info' function to be a VectorSelector, got %T", expr.Args[1])
						return inspectionError
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
					inspectionError = fmt.Errorf("expected 'info' function to have exactly 2 arguments, got %d", length)
					return inspectionError
				}
			}
		}
		return nil
	})

	if inspectionError != nil {
		return nil, inspectionError
	}

	return root, nil
}
