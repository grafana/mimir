// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestReorderHistogramAgg(t *testing.T) {
	testCases := map[string]string{
		`histogram_sum(sum(foo))`:                `sum(histogram_sum(foo))`,
		`sum(histogram_sum(foo))`:                `sum(histogram_sum(foo))`,
		`histogram_sum(avg(foo))`:                `avg(histogram_sum(foo))`,
		`avg(histogram_sum(foo))`:                `avg(histogram_sum(foo))`,
		`histogram_sum(sum(rate(foo[5m])))`:      `sum(histogram_sum(rate(foo[5m])))`,
		`sum(histogram_sum(rate(foo[5m])))`:      `sum(histogram_sum(rate(foo[5m])))`,
		`histogram_sum(avg(rate(foo[5m])))`:      `avg(histogram_sum(rate(foo[5m])))`,
		`avg(histogram_sum(rate(foo[5m])))`:      `avg(histogram_sum(rate(foo[5m])))`,
		`histogram_count(sum(foo))`:              `sum(histogram_count(foo))`,
		`sum(histogram_count(foo))`:              `sum(histogram_count(foo))`,
		`histogram_count(avg(foo))`:              `avg(histogram_count(foo))`,
		`avg(histogram_count(foo))`:              `avg(histogram_count(foo))`,
		`histogram_count(sum(rate(foo[5m])))`:    `sum(histogram_count(rate(foo[5m])))`,
		`sum(histogram_count(rate(foo[5m])))`:    `sum(histogram_count(rate(foo[5m])))`,
		`histogram_count(avg(rate(foo[5m])))`:    `avg(histogram_count(rate(foo[5m])))`,
		`avg(histogram_count(rate(foo[5m])))`:    `avg(histogram_count(rate(foo[5m])))`,
		`(((histogram_sum(sum(foo)))))`:          `(((sum(histogram_sum(foo)))))`,
		`histogram_sum(sum(foo+bar))`:            `sum(histogram_sum(foo+bar))`,
		`histogram_sum(sum(foo)+sum(bar))`:       `histogram_sum(sum(foo)+sum(bar))`,
		"histogram_sum(sum by (job) (foo))":      "sum by (job) (histogram_sum(foo))",
		"histogram_sum(sum without (job) (foo))": "sum without (job) (histogram_sum(foo))",
		"histogram_sum(rate(foo[5m]))":           "histogram_sum(rate(foo[5m]))",
	}

	optimizer := &ReorderHistogramAggregation{}
	ctx := context.Background()

	for input, expected := range testCases {
		t.Run(input, func(t *testing.T) {
			expectedExpr, err := parser.ParseExpr(expected)
			require.NoError(t, err)

			inputExpr, err := parser.ParseExpr(input)
			require.NoError(t, err)
			outputExpr, err := optimizer.Apply(ctx, inputExpr)
			require.NoError(t, err)

			require.Equal(t, expectedExpr.String(), outputExpr.String())
		})
	}
}
