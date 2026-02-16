// SPDX-License-Identifier: AGPL-3.0-only

package ast_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
	"github.com/grafana/mimir/pkg/util/promqlext"
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
	`histogram_sum(sum(foo @ 1234 * bar))`:      `sum(histogram_sum(foo @ 1234 * bar))`,

	// Unsupported aggregations
	`histogram_sum(max(foo))`:     `histogram_sum(max(foo))`,
	`max(histogram_sum(foo))`:     `max(histogram_sum(foo))`,
	`histogram_count(max(foo))`:   `histogram_count(max(foo))`,
	`max(histogram_count(foo))`:   `max(histogram_count(foo))`,
	`histogram_sum(count(foo))`:   `histogram_sum(count(foo))`,
	`count(histogram_sum(foo))`:   `count(histogram_sum(foo))`,
	`histogram_count(group(foo))`: `histogram_count(group(foo))`,
	`group(histogram_count(foo))`: `group(histogram_count(foo))`,

	// Do not reorder when __name__ is used in grouping or matcher as the histogram function drops the metric name which will cause incorrect aggregations or vector cannot contain metrics with the same labelset error.
	`histogram_sum(sum by (__name__) (foo))`:            `histogram_sum(sum by (__name__) (foo))`,
	`histogram_sum(sum({__name__=~"foo.*"}))`:           `histogram_sum(sum({__name__=~"foo.*"}))`,
	`histogram_sum(sum(rate({__name__=~"foo.*"}[2m])))`: `histogram_sum(sum(rate({__name__=~"foo.*"}[2m])))`,
}

func TestReorderHistogramAggregation(t *testing.T) {
	ctx := context.Background()

	for input, expected := range testCasesReorderHistogramAggregation {
		t.Run(input, func(t *testing.T) {
			expectedExpr, err := promqlext.NewExperimentalParser().ParseExpr(expected)
			require.NoError(t, err)

			inputExpr, err := promqlext.NewExperimentalParser().ParseExpr(input)
			require.NoError(t, err)
			inputExpr, err = preprocessQuery(t, inputExpr)
			require.NoError(t, err)

			optimizer := ast.NewReorderHistogramAggregationMapper()
			outputExpr, err := optimizer.Map(ctx, inputExpr)
			require.NoError(t, err)

			require.Equal(t, expectedExpr.String(), outputExpr.String())
			require.Equal(t, input != expected, optimizer.HasChanged())
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
