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
	return "Insert Omitted Target Info Selector"
}

func (h *InsertOmittedTargetInfoSelector) Apply(_ context.Context, expr parser.Expr) (parser.Expr, error) {
	return h.apply(expr), nil
}

func (h *InsertOmittedTargetInfoSelector) apply(root parser.Expr) parser.Expr {
	parser.Inspect(root, func(node parser.Node, _ []parser.Node) error {
		switch expr := node.(type) {
		case *parser.Call:
			if expr.Func.Name == "info" {
				switch length := len(expr.Args); length {
				case 1:
					infoExpr, err := parser.ParseExpr("target_info")
					if err != nil {
						panic(fmt.Sprintf("failed to parse target_info expression: %v", err))
					}
					expr.Args = append(expr.Args, infoExpr)
				case 2:
					dataLabelMatchersExpr, ok := expr.Args[1].(*parser.VectorSelector)
					if !ok {
						panic(fmt.Sprintf("expected second argument to 'info' function to be a VectorSelector, got %T", expr.Args[1]))
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
					panic(fmt.Sprintf("expected 'info' function to have exactly 2 arguments, got %d", length))
				}
			}
			for i, arg := range expr.Args {
				expr.Args[i] = h.apply(arg)
			}
		case *parser.ParenExpr:
			expr.Expr = h.apply(expr.Expr)
		case *parser.UnaryExpr:
			expr.Expr = h.apply(expr.Expr)
		case *parser.BinaryExpr:
			expr.LHS = h.apply(expr.LHS)
			expr.RHS = h.apply(expr.RHS)
		case *parser.AggregateExpr:
			expr.Expr = h.apply(expr.Expr)
			expr.Param = h.apply(expr.Param)
		case *parser.SubqueryExpr:
			expr.Expr = h.apply(expr.Expr)
		case *parser.StepInvariantExpr:
			expr.Expr = h.apply(expr.Expr)
		case *parser.VectorSelector, *parser.MatrixSelector, *parser.NumberLiteral, *parser.StringLiteral, nil:
			// no-op
		default:
			panic(fmt.Sprintf("unknown expression type: %T", expr))
		}
		return nil
	})

	return root
}
