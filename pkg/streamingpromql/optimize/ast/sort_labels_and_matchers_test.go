// SPDX-License-Identifier: AGPL-3.0-only

package ast_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
)

func TestSortLabelsAndMatchers_AggregateAndBinaryExpressions(t *testing.T) {
	testCases := map[string]string{
		// Aggregate expressions
		"sum(foo)":                      "sum(foo)",
		"sum by (blah) (foo)":           "sum by (blah) (foo)",
		"sum by (blah, baz) (foo)":      "sum by (baz, blah) (foo)",
		"sum by (baz, blah) (foo)":      "sum by (baz, blah) (foo)",
		"sum without (blah, baz) (foo)": "sum without (baz, blah) (foo)",

		// Binary expressions
		"2 * 3":                                    "2 * 3",
		"2 * bar":                                  "2 * bar",
		"foo * 3":                                  "foo * 3",
		"foo * bar":                                "foo * bar",
		"foo * on (blah) bar":                      "foo * on (blah) bar",
		"foo * on (blah, baz) bar":                 "foo * on (baz, blah) bar",
		"foo * on (baz, blah) bar":                 "foo * on (baz, blah) bar",
		"foo * ignoring (blah, baz) bar":           "foo * ignoring (baz, blah) bar",
		"foo * on (g) group_left (blah) bar":       "foo * on (g) group_left (blah) bar",
		"foo * on (g) group_left (blah, baz) bar":  "foo * on (g) group_left (baz, blah) bar",
		"foo * on (g) group_left (baz, blah) bar":  "foo * on (g) group_left (baz, blah) bar",
		"foo * on (g) group_right (blah, baz) bar": "foo * on (g) group_right (baz, blah) bar",

		// Nested versions of the above
		"abs(sum by (blah, baz) (foo))":     "abs(sum by (baz, blah) (foo))",
		"abs(foo * on (blah, baz) bar)":     "abs(foo * on (baz, blah) bar)",
		"(sum by (blah, baz) (foo))[1m:1m]": "(sum by (baz, blah) (foo))[1m:1m]",
		"(foo * on (blah, baz) bar)[1m:1m]": "(foo * on (baz, blah) bar)[1m:1m]",
		"-(sum by (blah, baz) (foo))":       "-(sum by (baz, blah) (foo))",
		"-(foo * on (blah, baz) bar)":       "-(foo * on (baz, blah) bar)",
		"(sum by (blah, baz) (foo))":        "(sum by (baz, blah) (foo))",
		"(foo * on (blah, baz) bar)":        "(foo * on (baz, blah) bar)",

		// Expressions that should be left as-is
		"abs(foo)":   "abs(foo)",
		"foo[1m:1m]": "foo[1m:1m]",
		"-foo":       "-foo",
		"-(foo)":     "-(foo)",
		`"abc"`:      `"abc"`,
		"2":          "2",
	}

	ctx := context.Background()
	sortLabelsAndMatchers := &ast.SortLabelsAndMatchers{}

	for input, expected := range testCases {
		t.Run(input, func(t *testing.T) {
			result := runASTOptimizationPassWithoutMetrics(t, ctx, input, sortLabelsAndMatchers)
			require.Equal(t, expected, result.String())
		})
	}
}

func TestSortLabelsAndMatchers_Selectors(t *testing.T) {
	// Prometheus' pretty printing of selectors sorts their matchers, so it's easier to test the behaviour here
	// by comparing ASTs rather than PromQL expression strings like we do in the test above.
	testCases := map[string]*parser.VectorSelector{
		`metric`: {
			Name: "metric",
			LabelMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric"),
			},
		},
		`metric{env="blah"}`: {
			Name: "metric",
			LabelMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric"),
				labels.MustNewMatcher(labels.MatchEqual, "env", "blah"),
			},
		},
		`metric{__env__="blah"}`: {
			Name: "metric",
			LabelMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__env__", "blah"),
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric"),
			},
		},
		`{__name__="metric", __env__="blah"}`: {
			Name: "",
			LabelMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__env__", "blah"),
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric"),
			},
		},
		`{__env__="blah", __name__="metric"}`: {
			Name: "",
			LabelMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__env__", "blah"),
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric"),
			},
		},
		`metric{env="blah", namespace="foo"}`: {
			Name: "metric",
			LabelMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric"),
				labels.MustNewMatcher(labels.MatchEqual, "env", "blah"),
				labels.MustNewMatcher(labels.MatchEqual, "namespace", "foo"),
			},
		},
		`metric{namespace="foo", env="blah"}`: {
			Name: "metric",
			LabelMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric"),
				labels.MustNewMatcher(labels.MatchEqual, "env", "blah"),
				labels.MustNewMatcher(labels.MatchEqual, "namespace", "foo"),
			},
		},

		// Multiple matchers for the same label with different types should sort by the type first, then value
		`metric{namespace="1", namespace!="2", namespace=~"3", namespace!~"5", namespace!~"4", namespace=~"6", namespace!="7", namespace="8"}`: {
			Name: "metric",
			LabelMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric"),
				labels.MustNewMatcher(labels.MatchEqual, "namespace", "1"),
				labels.MustNewMatcher(labels.MatchEqual, "namespace", "8"),
				labels.MustNewMatcher(labels.MatchNotEqual, "namespace", "2"),
				labels.MustNewMatcher(labels.MatchNotEqual, "namespace", "7"),
				labels.MustNewMatcher(labels.MatchRegexp, "namespace", "3"),
				labels.MustNewMatcher(labels.MatchRegexp, "namespace", "6"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "namespace", "4"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "namespace", "5"),
			},
		},

		// Multiple matchers for the same label and same type should sort by the value.
		`metric{namespace="3", namespace="4", namespace="1", namespace="2"}`: {
			Name: "metric",
			LabelMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric"),
				labels.MustNewMatcher(labels.MatchEqual, "namespace", "1"),
				labels.MustNewMatcher(labels.MatchEqual, "namespace", "2"),
				labels.MustNewMatcher(labels.MatchEqual, "namespace", "3"),
				labels.MustNewMatcher(labels.MatchEqual, "namespace", "4"),
			},
		},
	}

	ctx := context.Background()
	sortLabelsAndMatchers := &ast.SortLabelsAndMatchers{}

	run := func(t *testing.T, input string, expected parser.Expr) {
		_, result := runASTOptimizationPass(t, ctx, input, func(prometheus.Registerer) optimize.ASTOptimizationPass {
			return sortLabelsAndMatchers
		})

		// Compare the expected and result expressions formatted as strings for clearer diffs.
		// Note that we can't use the VectorSelector and MatrixSelector String() methods because these
		// sort the matchers when printing.
		require.Equal(t, formatSelector(expected), formatSelector(result))
	}

	for input, expected := range testCases {
		expected.PosRange.End = posrange.Pos(len(input))

		t.Run(input, func(t *testing.T) {
			run(t, input, expected)
		})

		matrixSelector := fmt.Sprintf(`%v[1m]`, input)
		matrixSelectorExpected := &parser.MatrixSelector{
			Range:          time.Minute,
			VectorSelector: expected,
			EndPos:         posrange.Pos(len(input) + 4),
		}

		t.Run(matrixSelector, func(t *testing.T) {
			run(t, matrixSelector, matrixSelectorExpected)
		})
	}
}

func formatSelector(expr parser.Expr) string {
	switch e := expr.(type) {
	case *parser.VectorSelector:
		return fmt.Sprintf("%v, name=%q, position=%v", e.LabelMatchers, e.Name, e.PositionRange())
	case *parser.MatrixSelector:
		vs := e.VectorSelector.(*parser.VectorSelector)
		return fmt.Sprintf("%v, name=%q, range=%v, position=%v", vs.LabelMatchers, vs.Name, e.Range, e.PositionRange())
	default:
		panic(fmt.Sprintf("unexpected expression type: %T", expr))
	}
}
