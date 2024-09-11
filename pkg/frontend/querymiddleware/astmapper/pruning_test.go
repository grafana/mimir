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
			`vector(0) < -Inf`,
		},
		{
			`-Inf > up`,
			`vector(0) < -Inf`,
		},
		{
			`up > +Inf`,
			`vector(0) < -Inf`,
		},
		{
			`+Inf < up`,
			`vector(0) < -Inf`,
		},
		{
			`up < +Inf`,
			`up < +Inf`,
		},
		{
			`+Inf > up`,
			`+Inf > up`,
		},
		{
			`up > -Inf`,
			`up > -Inf`,
		},
		{
			`-Inf < up`,
			`-Inf < up`,
		},
		{
			`avg(rate(foo[1m])) < (-Inf)`,
			`vector(0) < -Inf`,
		},
		{
			`Inf * -1`,
			`-Inf`,
		},
		{
			`+1 * -Inf`,
			`-Inf`,
		},
		{
			`1 * +Inf`,
			`Inf`,
		},
		{
			`-Inf * -1`,
			`+Inf`,
		},
		{
			`avg(rate(foo[1m])) < (-1 * +Inf)`,
			`vector(0) < -Inf`,
		},
		{
			`avg(rate(foo[1m])) < (+1 * +Inf)`,
			`avg(rate(foo[1m])) < (+Inf)`,
		},
		{
			`avg(rate(foo[1m])) < (-1 * -Inf)`,
			`avg(rate(foo[1m])) < (+Inf)`,
		},
		{
			`avg(rate(foo[1m])) < (+1 * -Inf)`,
			`vector(0) < -Inf`,
		},
		{
			`(-1 * -Inf) < avg(rate(foo[1m]))`,
			`vector(0) < -Inf`,
		},
		{
			`vector(0) < -Inf or avg(rate(foo[1m]))`,
			`avg(rate(foo[1m]))`,
		},
		{
			`avg(rate(foo[1m])) or vector(0) < -Inf`,
			`avg(rate(foo[1m]))`,
		},
		{
			`avg(rate(foo[1m])) or (vector(0) < -Inf)`,
			`avg(rate(foo[1m]))`,
		},
		{
			`((-1 * -Inf) < avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`avg(rate(foo[1m]))`,
		},
		{
			`((-1 * -Inf) <= avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`((+Inf) <= avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
		},
		{
			`((-1 * -Inf) >= avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`((+Inf) >= avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
		},
		{
			`((-1 * -Inf) > avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`((+Inf) > avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
		},
		{
			`((-1 * +Inf) > avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`avg(rate(foo[1m]))`,
		},
		{
			`((+1 * -Inf) > avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`avg(rate(foo[1m]))`,
		},
		{
			`(avg(rate(foo[2m])) < (+1 * -Inf)) or avg(rate(foo[1m]))`,
			`avg(rate(foo[1m]))`,
		},
		{
			`((-1 * -Inf) == avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`((+Inf) == avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
		},
		{
			`((-1 * -Inf) != avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`((+Inf) != avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
		},
		{
			`((-1 * -Inf) < avg(rate(foo[2m]))) and avg(rate(foo[1m]))`,
			`(vector(0) < -Inf)`,
		},
		{
			`((-1 * -Inf) < avg(rate(foo[2m]))) unless avg(rate(foo[1m]))`,
			`(vector(0) < -Inf)`,
		},
		{
			`avg(rate(foo[1m])) unless ((-1 * -Inf) < avg(rate(foo[2m])))`,
			`avg(rate(foo[1m]))`,
		},
		{
			`((2 * +Inf) < avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`avg(rate(foo[1m]))`,
		},
		{
			`(((-1 * -Inf) < avg(rate(foo[3m]))) unless avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`avg(rate(foo[1m]))`,
		},
		{
			`((((-1 * -Inf) < avg(rate(foo[4m]))) unless avg(rate(foo[3m]))) and avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`avg(rate(foo[1m]))`,
		},
		{
			`(((-1 * -Inf) < avg(rate(foo[4m]))) unless (avg(rate(foo[3m])) and avg(rate(foo[2m])))) or avg(rate(foo[1m]))`,
			`avg(rate(foo[1m]))`,
		},
		{
			`(((-1 * -Inf) < avg(rate(foo[4m]))) unless avg(rate(foo[3m])) and avg(rate(foo[2m]))) or avg(rate(foo[1m]))`,
			`avg(rate(foo[1m]))`,
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
