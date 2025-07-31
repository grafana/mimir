// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestPruneToggles(t *testing.T) {
	testCases := map[string]string{
		// Non-prunable expressions.
		`foo`:                `foo`,
		`foo[1m]`:            `foo[1m]`,
		`foo{bar="baz"}[1m]`: `foo{bar="baz"}[1m]`,
		`foo{bar="baz"}`:     `foo{bar="baz"}`,
		`quantile(0.9,foo)`:  `quantile(0.9,foo)`,
		`histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`: `histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`,
		`count(up)`:          `count(up)`,
		`avg(rate(foo[1m]))`: `avg(rate(foo[1m]))`,
		`vector(0)`:          `vector(0)`,
		`avg(rate(foo[1m])) or avg(rate(bar[1m]))`: `avg(rate(foo[1m])) or avg(rate(bar[1m]))`,
		// The const expression is on the wrong side.
		`(vector(0) == 1) and on() (avg(rate(foo[1m])))`: `(vector(0) == 1) and on() (avg(rate(foo[1m])))`,
		// Matching on labels.
		`(avg(rate(foo[1m]))) and on(id) (vector(0) == 1)`: `(avg(rate(foo[1m]))) and on(id) (vector(0) == 1)`,
		// Not "on" expression.
		`(avg(rate(foo[1m]))) and ignoring() (vector(0) == 1)`: `(avg(rate(foo[1m]))) and ignoring() (vector(0) == 1)`,
		// Pruned expressions.
		`(avg(rate(foo[1m]))) and on() (vector(0) == 1)`:     `(vector(0) == 1)`,
		`(avg(rate(foo[1m]))) and on() (vector(1) == 1)`:     `(avg(rate(foo[1m])))`,
		`(avg(rate(foo[1m]))) and on() (vector(3) == 4.5)`:   `(vector(3) == 4.5)`,
		`(avg(rate(foo[1m]))) and on() (vector(5.5) == 5.5)`: `(avg(rate(foo[1m])))`,
		// "and on()" is not on top level, "or" has lower precedence.
		// This could be further reduced by dropping the vector(0)==1 from
		// the "or", but it is intentionally not done as that would
		// complicate the algorithm and isn't required to reduce chunks
		// loading.
		`(avg(rate(foo[1m]))) and on() (vector(0) == 1) or avg(rate(bar[1m]))`: `(vector(0) == 1) or avg(rate(bar[1m]))`,
		// "and on()" is not on top level, due to left-right associativity.
		`(avg(rate(foo[1m]))) and on() (vector(0) == 1) and avg(rate(bar[1m]))`: `(vector(0) == 1) and avg(rate(bar[1m]))`,
		// "and on()" is not on top level.
		// This could be further reduced by dropping the vector(0)==1 from
		// the "or", but it is intentionally not done as that would
		// complicate the algorithm and isn't required to reduce chunks
		// loading.
		`(avg(rate(foo[1m]))) and on() (vector(0) == 1) or avg(rate(bar[1m])) and on() (vector(1) == 1)`: `(vector(0) == 1) or avg(rate(bar[1m]))`,
	}

	optimizer := &PruneToggles{}
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
