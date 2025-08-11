// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

var testCasesPruneToggles = map[string]string{
	// Non-prunable expressions.
	`foo`:                `foo`,
	`foo[2m]`:            `foo[2m]`,
	`foo{bar="baz"}[2m]`: `foo{bar="baz"}[2m]`,
	`foo{bar="baz"}`:     `foo{bar="baz"}`,
	`quantile(0.9,foo)`:  `quantile(0.9,foo)`,
	`histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`: `histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`,
	`count(up)`:          `count(up)`,
	`avg(rate(foo[2m]))`: `avg(rate(foo[2m]))`,
	`vector(0)`:          `vector(0)`,
	`avg(rate(foo[2m])) or avg(rate(bar[2m]))`: `avg(rate(foo[2m])) or avg(rate(bar[2m]))`,
	// The const expression is on the wrong side.
	`(vector(0) == 1) and on() (avg(rate(foo[2m])))`: `(vector(0) == 1) and on() (avg(rate(foo[2m])))`,
	// Matching on labels.
	`(avg(rate(foo[2m]))) and on(id) (vector(0) == 1)`: `(avg(rate(foo[2m]))) and on(id) (vector(0) == 1)`,
	// Not "on" expression.
	`(avg(rate(foo[2m]))) and ignoring() (vector(0) == 1)`: `(avg(rate(foo[2m]))) and ignoring() (vector(0) == 1)`,
	// Pruned expressions.
	`(avg(rate(foo[2m]))) and on() (vector(0) == 1)`:     `(vector(0) == 1)`,
	`(avg(rate(foo[2m]))) and on() (vector(1) == 1)`:     `(avg(rate(foo[2m])))`,
	`(avg(rate(foo[2m]))) and on() (vector(3) == 4.5)`:   `(vector(3) == 4.5)`,
	`(avg(rate(foo[2m]))) and on() (vector(5.5) == 5.5)`: `(avg(rate(foo[2m])))`,
	// "and on()" is not on top level, "or" has lower precedence.
	// This could be further reduced by dropping the vector(0)==1 from
	// the "or", but it is intentionally not done as that would
	// complicate the algorithm and isn't required to reduce chunks
	// loading.
	`(avg(rate(foo[2m]))) and on() (vector(0) == 1) or avg(rate(bar[2m]))`: `(vector(0) == 1) or avg(rate(bar[2m]))`,
	// "and on()" is not on top level, due to left-right associativity.
	`(avg(rate(foo[2m]))) and on() (vector(0) == 1) and avg(rate(bar[2m]))`: `(vector(0) == 1) and avg(rate(bar[2m]))`,
	// "and on()" is not on top level.
	// This could be further reduced by dropping the vector(0)==1 from
	// the "or", but it is intentionally not done as that would
	// complicate the algorithm and isn't required to reduce chunks
	// loading.
	`(avg(rate(foo[2m]))) and on() (vector(0) == 1) or avg(rate(bar[2m])) and on() (vector(1) == 1)`: `(vector(0) == 1) or avg(rate(bar[2m]))`,
}

func TestPruneToggles(t *testing.T) {
	optimizer := NewPruneToggles()
	ctx := context.Background()

	for input, expected := range testCasesPruneToggles {
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

func TestPruneTogglesWithData(t *testing.T) {
	testASTOptimizationPassWithData(t, `
		load 1m
			foo{series="1"} 0+1x<num samples>
			foo{series="2"} 0+2x<num samples>
			foo{series="3"} 0+3x<num samples>
			foo{series="4"} 0+4x<num samples>
			foo{series="5"} 0+5x<num samples>
			bar{series="1"} 0+6x<num samples>
			bar{series="2"} 0+7x<num samples>
			bar{series="3"} 0+8x<num samples>
			bar{series="4"} 0+9x<num samples>
			bar{series="5"} 0+10x<num samples>
	`, testCasesPruneToggles)
}
