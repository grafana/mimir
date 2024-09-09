// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/shard_summer_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

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
			`vector(0) > +Inf`,
		},
		{
			`+Inf < up`,
			`vector(0) > +Inf`,
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
			`vector(0) < (-Inf)`,
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
			`vector(0) < (-Inf)`,
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
			`vector(0) < (-Inf)`,
		},
		{
			`(-1 * -Inf) < avg(rate(foo[1m]))`,
			`vector(0) > (+Inf)`,
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
