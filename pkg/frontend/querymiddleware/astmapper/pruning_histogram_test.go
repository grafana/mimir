// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestQueryPrunerHistogram(t *testing.T) {
	pruner := NewQueryPrunerHistogram(context.Background(), log.NewNopLogger())

	for _, tt := range []struct {
		in  string
		out string
	}{
		{
			`histogram_sum(sum(foo))`,
			`sum(histogram_sum(foo))`,
		},
		{
			`sum(histogram_sum(foo))`,
			`sum(histogram_sum(foo))`,
		},
		{
			`histogram_sum(avg(foo))`,
			`avg(histogram_sum(foo))`,
		},
		{
			`avg(histogram_sum(foo))`,
			`avg(histogram_sum(foo))`,
		},
		{
			`histogram_sum(sum(rate(foo[5m])))`,
			`sum(histogram_sum(rate(foo[5m])))`,
		},
		{
			`sum(histogram_sum(rate(foo[5m])))`,
			`sum(histogram_sum(rate(foo[5m])))`,
		},
		{
			`histogram_sum(avg(rate(foo[5m])))`,
			`avg(histogram_sum(rate(foo[5m])))`,
		},
		{
			`avg(histogram_sum(rate(foo[5m])))`,
			`avg(histogram_sum(rate(foo[5m])))`,
		},
		{
			`histogram_count(sum(foo))`,
			`sum(histogram_count(foo))`,
		},
		{
			`sum(histogram_count(foo))`,
			`sum(histogram_count(foo))`,
		},
		{
			`histogram_count(avg(foo))`,
			`avg(histogram_count(foo))`,
		},
		{
			`avg(histogram_count(foo))`,
			`avg(histogram_count(foo))`,
		},
		{
			`histogram_count(sum(rate(foo[5m])))`,
			`sum(histogram_count(rate(foo[5m])))`,
		},
		{
			`sum(histogram_count(rate(foo[5m])))`,
			`sum(histogram_count(rate(foo[5m])))`,
		},
		{
			`histogram_count(avg(rate(foo[5m])))`,
			`avg(histogram_count(rate(foo[5m])))`,
		},
		{
			`avg(histogram_count(rate(foo[5m])))`,
			`avg(histogram_count(rate(foo[5m])))`,
		},
		{
			`histogram_avg(sum(foo))`,
			`sum(histogram_avg(foo))`,
		},
		{
			`sum(histogram_avg(foo))`,
			`sum(histogram_avg(foo))`,
		},
		{
			`histogram_avg(avg(foo))`,
			`avg(histogram_avg(foo))`,
		},
		{
			`avg(histogram_avg(foo))`,
			`avg(histogram_avg(foo))`,
		},
		{
			`histogram_avg(sum(rate(foo[5m])))`,
			`sum(histogram_avg(rate(foo[5m])))`,
		},
		{
			`sum(histogram_avg(rate(foo[5m])))`,
			`sum(histogram_avg(rate(foo[5m])))`,
		},
		{
			`histogram_avg(avg(rate(foo[5m])))`,
			`avg(histogram_avg(rate(foo[5m])))`,
		},
		{
			`avg(histogram_avg(rate(foo[5m])))`,
			`avg(histogram_avg(rate(foo[5m])))`,
		},
		{
			`(((histogram_sum(sum(foo)))))`,
			`(((sum(histogram_sum(foo)))))`,
		},
		{
			`histogram_sum(sum(foo+bar))`,
			`sum(histogram_sum(foo+bar))`,
		},
		{
			`histogram_sum(sum(foo)+sum(bar))`,
			`histogram_sum(sum(foo)+sum(bar))`,
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
