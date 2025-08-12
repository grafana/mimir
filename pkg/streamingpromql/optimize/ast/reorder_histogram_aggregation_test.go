// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

var testCasesReorderHistogramAggregation = map[string]string{
	`histogram_sum(sum(foo))`:                   `sum(histogram_sum(foo))`,
	`sum(histogram_sum(foo))`:                   `sum(histogram_sum(foo))`,
	`histogram_sum(avg(foo))`:                   `avg(histogram_sum(foo))`,
	`avg(histogram_sum(foo))`:                   `avg(histogram_sum(foo))`,
	`histogram_sum(sum(rate(foo[2m])))`:         `sum(histogram_sum(rate(foo[2m])))`,
	`sum(histogram_sum(rate(foo[2m])))`:         `sum(histogram_sum(rate(foo[2m])))`,
	`histogram_sum(avg(rate(foo[2m])))`:         `avg(histogram_sum(rate(foo[2m])))`,
	`avg(histogram_sum(rate(foo[2m])))`:         `avg(histogram_sum(rate(foo[2m])))`,
	`histogram_count(sum(foo))`:                 `sum(histogram_count(foo))`,
	`sum(histogram_count(foo))`:                 `sum(histogram_count(foo))`,
	`histogram_count(avg(foo))`:                 `avg(histogram_count(foo))`,
	`avg(histogram_count(foo))`:                 `avg(histogram_count(foo))`,
	`histogram_count(sum(rate(foo[2m])))`:       `sum(histogram_count(rate(foo[2m])))`,
	`sum(histogram_count(rate(foo[2m])))`:       `sum(histogram_count(rate(foo[2m])))`,
	`histogram_count(avg(rate(foo[2m])))`:       `avg(histogram_count(rate(foo[2m])))`,
	`avg(histogram_count(rate(foo[2m])))`:       `avg(histogram_count(rate(foo[2m])))`,
	`(((histogram_sum(sum(foo)))))`:             `(((sum(histogram_sum(foo)))))`,
	`histogram_sum(sum(foo+bar))`:               `sum(histogram_sum(foo+bar))`,
	`histogram_sum(sum(foo)+sum(bar))`:          `histogram_sum(sum(foo)+sum(bar))`,
	"histogram_sum(sum by (job) (foo))":         "sum by (job) (histogram_sum(foo))",
	"histogram_sum(sum without (job) (foo))":    "sum without (job) (histogram_sum(foo))",
	"histogram_sum(rate(foo[2m]))":              "histogram_sum(rate(foo[2m]))",
	`3 + (((histogram_sum(sum(foo)))))`:         `3 + (((sum(histogram_sum(foo)))))`,
	`vector(3) + (((histogram_sum(sum(foo)))))`: `vector(3) + (((sum(histogram_sum(foo)))))`,
}

func TestReorderHistogramAggregation(t *testing.T) {
	ctx := context.Background()

	for input, expected := range testCasesReorderHistogramAggregation {
		t.Run(input, func(t *testing.T) {
			expectedExpr, err := parser.ParseExpr(expected)
			require.NoError(t, err)

			inputExpr, err := parser.ParseExpr(input)
			require.NoError(t, err)
			optimizer := NewReorderHistogramAggregation()
			outputExpr, err := optimizer.Apply(ctx, inputExpr)
			require.NoError(t, err)

			require.Equal(t, expectedExpr.String(), outputExpr.String())
			require.Equal(t, input != expected, optimizer.mapper.HasChanged())
		})
	}
}

func TestReorderHistogramAggregationWithData(t *testing.T) {
	testASTOptimizationPassWithData(t, `
		load 1m
			foo	{{schema:0 sum:4 count:4 buckets:[1 2 1]}}+{{sum:2 count:1 buckets:[1] offset:1}}x<num samples>
			bar	{{schema:0 sum:4 count:4 buckets:[1 2 1]}}+{{sum:4 count:2 buckets:[1 2] offset:1}}x<num samples>
	`, testCasesReorderHistogramAggregation)
}
