// SPDX-License-Identifier: AGPL-3.0-only

package ast_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
)

var testCasesRewriteAverage = map[string]string{
	// Rewrite
	`sum(foo) / count(foo)`:                                                                                             `avg(foo)`,
	`sum by (series) (foo) / count by (series) (foo)`:                                                                   `avg by (series) (foo)`,
	`sum by (lbl, series) (foo) / count by (lbl, series) (foo)`:                                                         `avg by (lbl, series) (foo)`,
	`sum by (lbl, series) (foo) / count by (series, lbl) (foo)`:                                                         `avg by (lbl, series) (foo)`,
	`sum_over_time(foo[5m]) / count_over_time(foo[5m])`:                                                                 `avg_over_time(foo[5m])`,
	`histogram_sum(foo_hist) / histogram_count(foo_hist)`:                                                               `histogram_avg(foo_hist)`,
	`max(sum by (series) (foo) / count by (series) (foo))`:                                                              `max(avg by (series) (foo))`,
	`sum(sum_over_time(foo[5m]) / count_over_time(foo[5m]))`:                                                            `sum(avg_over_time(foo[5m]))`,
	`sum(sum_over_time(foo[5m]) / count_over_time(foo[5m])) / count(sum_over_time(foo[5m]) / count_over_time(foo[5m]))`: `avg(avg_over_time(foo[5m]))`,
	// Ignore
	`sum(foo) / count(bar)`:                                          `sum(foo) / count(bar)`,
	`sum by (series) (foo) / count(foo)`:                             `sum by (series) (foo) / count(foo)`,
	`sum by (series) (foo) / count by (lbl) (foo)`:                   `sum by (series) (foo) / count by (lbl) (foo)`,
	`sum by (lbl, series) (foo) / count by (__name__, series) (foo)`: `sum by (lbl, series) (foo) / count by (__name__, series) (foo)`,
	`sum(sum_over_time(foo[5m])) / sum(count_over_time(foo[5m]))`:    `sum(sum_over_time(foo[5m])) / sum(count_over_time(foo[5m]))`,
	`sum(sum_over_time(foo[5m])) / count(count_over_time(foo[5m]))`:  `sum(sum_over_time(foo[5m])) / count(count_over_time(foo[5m]))`,
}

func TestRewriteAverage(t *testing.T) {
	ctx := context.Background()

	for input, expected := range testCasesRewriteAverage {
		t.Run(input, func(t *testing.T) {
			expectedExpr, err := parser.ParseExpr(expected)
			require.NoError(t, err)

			inputExpr, err := parser.ParseExpr(input)
			require.NoError(t, err)
			optimizer := ast.NewRewriteAverageMapper()
			outputExpr, err := optimizer.Map(ctx, inputExpr)
			require.NoError(t, err)

			require.Equal(t, expectedExpr.String(), outputExpr.String())
			require.Equal(t, input != expected, optimizer.HasChanged())
		})
	}
}

func TestRewriteAverageWithData(t *testing.T) {
	testASTOptimizationPassWithData(t, `
		load 1m
			foo{series="1", lbl="odd"} 0+1x<num samples>
			foo{series="2", lbl="even"} 0+2x<num samples>
			foo{series="3", lbl="odd"} 0+3x<num samples>
			foo{series="4", lbl="even"} 0+4x<num samples>
			foo{series="5", lbl="odd"} 0+5x<num samples>
			bar{series="1", lbl="odd"} 0+6x<num samples>
			bar{series="2", lbl="even"} 0+7x<num samples>
			bar{series="3", lbl="odd"} 0+8x<num samples>
			bar{series="4", lbl="even"} 0+9x<num samples>
			bar{series="5", lbl="odd"} 0+10x<num samples>
			foo_hist	{{schema:0 sum:4 count:4 buckets:[1 2 1]}}+{{sum:2 count:1 buckets:[1] offset:1}}x<num samples>
			bar_hist	{{schema:0 sum:4 count:4 buckets:[1 2 1]}}+{{sum:4 count:2 buckets:[1 2] offset:1}}x<num samples>
	`, testCasesRewriteAverage)
}

func TestCountRewriteAverage(t *testing.T) {
	ctx := context.Background()
	optimizer := ast.NewRewriteAverageMapper()

	for input, _ := range testCasesRewriteAverage {
		inputExpr, err := parser.ParseExpr(input)
		require.NoError(t, err)
		_, err = optimizer.Map(ctx, inputExpr)
		require.NoError(t, err)
	}

	fmt.Printf("total count: %d\n", len(testCasesRewriteAverage))
	fmt.Println(optimizer.Stats())
}
