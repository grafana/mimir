// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestQueryPruner(t *testing.T) {
	pruner := NewQueryPruner(context.Background(), log.NewNopLogger())

	for _, tt := range []struct {
		in  string
		out string
	}{
		// Non-prunable expressions.
		{
			`foo`,
			`foo`,
		},
		{
			`foo[1m]`,
			`foo[1m]`,
		},
		{
			`foo{bar="baz"}[1m]`,
			`foo{bar="baz"}[1m]`,
		},
		{
			`foo{bar="baz"}`,
			`foo{bar="baz"}`,
		},
		{
			`quantile(0.9,foo)`,
			`quantile(0.9,foo)`,
		},
		{
			`histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`,
			`histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`,
		},
		{
			`count(up)`,
			`count(up)`,
		},
		{
			`avg(rate(foo[1m]))`,
			`avg(rate(foo[1m]))`,
		},
		{
			`vector(0)`,
			`vector(0)`,
		},
		{
			`up < -Inf`,
			`up < -Inf`,
		},
		{
			`avg(rate(foo[1m])) < (-Inf)`,
			`avg(rate(foo[1m])) < (-Inf)`,
		},
		{
			`Inf * -1`,
			`Inf * -1`,
		},
		{
			`avg(rate(foo[1m])) < (-1 * +Inf)`,
			`avg(rate(foo[1m])) < (-1 * +Inf)`,
		},
		{
			`(-1 * -Inf) < avg(rate(foo[1m]))`,
			`(-1 * -Inf) < avg(rate(foo[1m]))`,
		},
		{
			`vector(0) < -Inf or avg(rate(foo[1m]))`,
			`vector(0) < -Inf or avg(rate(foo[1m]))`,
		},
		{
			`avg(rate(foo[1m])) or vector(0) < -Inf`,
			`avg(rate(foo[1m])) or vector(0) < -Inf`,
		},
		{
			`avg(rate(foo[1m])) or (vector(0) < -Inf)`,
			`avg(rate(foo[1m])) or (vector(0) < -Inf)`,
		},
		{
			`((-1 * -Inf) < avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`((-1 * -Inf) < avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
		},
		{
			`(avg(rate(foo[2m])) < (+1 * -Inf)) or avg(rate(foo[1m]))`,
			`(avg(rate(foo[2m])) < (+1 * -Inf)) or avg(rate(foo[1m]))`,
		},
		{
			`((-1 * -Inf) == avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`((-1 * -Inf) == avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
		},
		{
			`avg(rate(foo[1m])) unless ((-1 * -Inf) < avg(rate(foo[2m])))`,
			`avg(rate(foo[1m])) unless ((-1 * -Inf) < avg(rate(foo[2m])))`,
		},
		{
			`((2 * +Inf) < avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`((2 * +Inf) < avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
		},
		{
			`(((-1 * -Inf) < avg(rate(foo[3m]))) unless avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`(((-1 * -Inf) < avg(rate(foo[3m]))) unless avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
		},
		{
			`((((-1 * -Inf) < avg(rate(foo[4m]))) unless avg(rate(foo[3m]))) and avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`((((-1 * -Inf) < avg(rate(foo[4m]))) unless avg(rate(foo[3m]))) and avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
		},
		{
			`avg(rate(foo[1m])) or avg(rate(bar[1m]))`,
			`avg(rate(foo[1m])) or avg(rate(bar[1m]))`,
		},
		{ // The const expression is on the wrong side.
			`(vector(0) == 1) and on() (avg(rate(foo[1m])))`,
			`(vector(0) == 1) and on() (avg(rate(foo[1m])))`,
		},
		{ // Matching on labels.
			`(avg(rate(foo[1m]))) and on(id) (vector(0) == 1)`,
			`(avg(rate(foo[1m]))) and on(id) (vector(0) == 1)`,
		},
		{ // Not "on" expression.
			`(avg(rate(foo[1m]))) and ignoring() (vector(0) == 1)`,
			`(avg(rate(foo[1m]))) and ignoring() (vector(0) == 1)`,
		},
		// Pruned expressions.
		{
			`(avg(rate(foo[1m]))) and on() (vector(0) == 1)`,
			`(vector(0) == 1)`,
		},
		{
			`(avg(rate(foo[1m]))) and on() (vector(1) == 1)`,
			`(avg(rate(foo[1m])))`,
		},
		{
			`(avg(rate(foo[1m]))) and on() (vector(3) == 4.5)`,
			`(vector(3) == 4.5)`,
		},
		{
			`(avg(rate(foo[1m]))) and on() (vector(5.5) == 5.5)`,
			`(avg(rate(foo[1m])))`,
		},
		{
			// "and on()" is not on top level, "or" has lower precedence.
			// This could be further reduced by dropping the vector(0)==1 from
			// the "or", but it is intentionally not done as that would
			// complicate the algorithm and isn't required to reduce chunks
			// loading.
			`(avg(rate(foo[1m]))) and on() (vector(0) == 1) or avg(rate(bar[1m]))`,
			`(vector(0) == 1) or avg(rate(bar[1m]))`,
		},
		{
			// "and on()" is not on top level, due to left-right associativity.
			`(avg(rate(foo[1m]))) and on() (vector(0) == 1) and avg(rate(bar[1m]))`,
			`(vector(0) == 1) and avg(rate(bar[1m]))`,
		},
		{
			// "and on()" is not on top level.
			// This could be further reduced by dropping the vector(0)==1 from
			// the "or", but it is intentionally not done as that would
			// complicate the algorithm and isn't required to reduce chunks
			// loading.
			`(avg(rate(foo[1m]))) and on() (vector(0) == 1) or avg(rate(bar[1m])) and on() (vector(1) == 1)`,
			`(vector(0) == 1) or avg(rate(bar[1m]))`,
		},
	} {
		tt := tt

		t.Run(tt.in, func(t *testing.T) {
			expr, err := parser.ParseExpr(tt.in)
			require.NoError(t, err)
			out, err := parser.ParseExpr(tt.out)
			require.NoError(t, err)

			mapped, err := pruner.Map(expr)
			require.NoError(t, err)
			require.Equal(t, out.String(), mapped.String())
		})
	}
}
