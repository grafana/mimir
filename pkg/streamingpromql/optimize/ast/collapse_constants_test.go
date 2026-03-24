// SPDX-License-Identifier: AGPL-3.0-only

package ast_test

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
)

func TestCollapseConstants(t *testing.T) {
	testCases := map[string]string{
		"2":                                  "2",
		"-2":                                 "-2",
		"-(2)":                               "-2",
		"+(2)":                               "2",
		"2 + 3":                              "5",
		"(2 + 3)":                            "5",
		"2 - 3":                              "-1",
		"2 * 3":                              "6",
		"10 + 2 * 3":                         "16", // Ensure order of operations is respected.
		"3 / 2":                              "1.5",
		"5 % 2":                              "1",
		"2 ^ 3":                              "8",
		"1 atan2 3":                          "0.3217505543966422",
		`Inf`:                                `+Inf`,
		`Inf * -1`:                           `-Inf`,
		`-1 * -Inf`:                          `+Inf`,
		"abs(vector(2 + 3))":                 "abs(vector(5))",
		"vector(2 + 3)":                      "vector(5)",
		"vector(2 + 3)[1m:1m]":               "vector(5)[1m:1m]",
		"sum(vector(2 + 3))":                 "sum(vector(5))",
		"sum(vector(-(2) + ((27) / ((3)))))": "sum(vector(7))",
		"topk(2 + 3, metric)":                "topk(5, metric)",

		// Check a complex expression with multiple operations, some of which can't be collapsed to respect the order of operations.
		`abs(foo) + (sum(vector(2+3)) / (sum(series) + sum(min(series)))) * 600 + 3000`: `abs(foo) + (sum(vector(5)) / (sum(series) + sum(min(series)))) * 600 + 3000`,

		"foo * 2":    "foo * 2",
		"2 * foo":    "2 * foo",
		"bar * foo":  "bar * foo",
		"2 > bool 3": "2 > bool 3",

		"metric":        "metric",
		"(metric)":      "(metric)",
		"-metric":       "-metric",
		"metric[1m]":    "metric[1m]",
		"metric[1m:1m]": "metric[1m:1m]",
		`"abc"`:         `"abc"`,
	}

	ctx := context.Background()
	collapseConstants := &ast.CollapseConstants{}

	for input, expected := range testCases {
		t.Run(input, func(t *testing.T) {
			result := runASTOptimizationPassWithoutMetrics(t, ctx, input, collapseConstants)
			require.Equal(t, expected, result.String())

			// Check for unnecessary unary expressions.
			if u, ok := result.(*parser.UnaryExpr); ok {
				_, isNumberLiteral := u.Expr.(*parser.NumberLiteral)
				require.Falsef(t, isNumberLiteral, "should not have a unary expression wrapping a number literal:\n%v", parser.Tree(result))
			}
		})
	}
}
