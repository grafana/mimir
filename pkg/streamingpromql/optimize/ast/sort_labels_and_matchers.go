// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"fmt"
	"slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

// SortLabelsAndMatchers is an optimization pass that ensures that all label names and matchers are sorted.
//
// This allows subsequent optimization passes to assume that label names and matchers are sorted, which simplifies their implementation.
type SortLabelsAndMatchers struct{}

func (s *SortLabelsAndMatchers) Name() string {
	return "Sort labels and matchers"
}

func (s *SortLabelsAndMatchers) Apply(_ context.Context, expr parser.Expr) (parser.Expr, error) {
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		switch expr := node.(type) {
		case *parser.VectorSelector:
			// Note that VectorSelectors sort their matchers when pretty printing, so this change may not be visible when printing as PromQL.
			slices.SortFunc(expr.LabelMatchers, compareMatchers)

		case *parser.MatrixSelector:
			// Note that MatrixSelectors sort their matchers when pretty printing, so this change may not be visible when printing as PromQL.
			slices.SortFunc(expr.VectorSelector.(*parser.VectorSelector).LabelMatchers, compareMatchers)

		case *parser.BinaryExpr:
			if expr.VectorMatching != nil {
				slices.Sort(expr.VectorMatching.MatchingLabels)
				slices.Sort(expr.VectorMatching.Include)
			}

		case *parser.AggregateExpr:
			slices.Sort(expr.Grouping)

		case *parser.Call, *parser.SubqueryExpr, *parser.UnaryExpr, *parser.ParenExpr, *parser.StepInvariantExpr, *parser.StringLiteral, *parser.NumberLiteral, nil:
			// Nothing to do.
		default:
			panic(fmt.Sprintf("unknown expression type: %T", expr))
		}

		return nil
	})

	return expr, nil
}

func compareMatchers(a, b *labels.Matcher) int {
	return core.CompareMatchers(a.Name, b.Name, a.Type, b.Type, a.Value, b.Value)
}
