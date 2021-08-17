// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/shard_summer_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func Test_Mapping(t *testing.T) {
	for _, tt := range []struct {
		in  string
		out string
	}{
		{
			`quantile(0.9,foo)`,
			concat(`quantile(0.9,foo)`),
		},
		{

			`histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`,
			concat(`histogram_quantile(0.5,rate(bar1{baz="blip"}[30s]))`),
		},
		{
			`sum by (foo,bar) (min_over_time(bar1{baz="blip"}[1m]))`,
			`sum by(foo, bar) (` +
				concat(
					`sum by (foo,bar) (min_over_time(bar1{__query_shard__="0_of_3",baz="blip"}[1m]))`,
					`sum by (foo,bar) (min_over_time(bar1{__query_shard__="1_of_3",baz="blip"}[1m]))`,
					`sum by (foo,bar) (min_over_time(bar1{__query_shard__="2_of_3",baz="blip"}[1m]))`,
				) + `)`,
		},
		{
			"sum(rate(bar1[1m])) or rate(bar1[1m])",
			`sum(` +
				concat(
					`sum(rate(bar1{__query_shard__="0_of_3"}[1m]))`,
					`sum(rate(bar1{__query_shard__="1_of_3"}[1m]))`,
					`sum(rate(bar1{__query_shard__="2_of_3"}[1m]))`,
				) + `) or ` + concat(
				`rate(bar1[1m])`,
			),
		},
		{
			`histogram_quantile(0.5, sum(rate(cortex_cache_value_size_bytes_bucket[5m])) by (le))`,
			`histogram_quantile(0.5,sum  by (le) (` + concat(
				`sum  by (le) (rate(cortex_cache_value_size_bytes_bucket{__query_shard__="0_of_3"}[5m]))`,
				`sum  by (le) (rate(cortex_cache_value_size_bytes_bucket{__query_shard__="1_of_3"}[5m]))`,
				`sum  by (le) (rate(cortex_cache_value_size_bytes_bucket{__query_shard__="2_of_3"}[5m]))`,
			) + `))`,
		},
		{
			`sum(
				  count(
				    count(
				      bar1
				    )  by (drive,instance)
				  )  by (instance)
				)`,
			`sum(
				count(
				  sum(` + concat(
				`count(bar1{__query_shard__="0_of_3"})  by (drive,instance)`,
				`count(bar1{__query_shard__="1_of_3"})  by (drive,instance)`,
				`count(bar1{__query_shard__="2_of_3"})  by (drive,instance)`,
			) + `
				  )  by (drive,instance)
				)  by (instance)
			  )`,
		},
		{
			`sum(rate(foo[1m]))`,
			`sum(` +
				concat(
					`sum(rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`sum(rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`sum(rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
		},
		{
			`count(rate(foo[1m]))`,
			`sum(` +
				concat(
					`count(rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`count(rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`count(rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
		},
		{
			`count(up)`,
			`sum(` +
				concat(
					`count(up{__query_shard__="0_of_3"})`,
					`count(up{__query_shard__="1_of_3"})`,
					`count(up{__query_shard__="2_of_3"})`,
				) + `)`,
		},
		{
			`avg(count(test))`,
			`avg(sum(` +
				concat(
					`count(test{__query_shard__="0_of_3"})`,
					`count(test{__query_shard__="1_of_3"})`,
					`count(test{__query_shard__="2_of_3"})`,
				) + `))`,
		},
		{
			`count by (foo) (rate(foo[1m]))`,
			`sum by (foo) (` +
				concat(
					`count by (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`count by (foo)(rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`count by (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
		},
		{
			`count without (foo) (rate(foo[1m]))`,
			`sum without (foo) (` +
				concat(
					`count without (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`count without (foo)(rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`count without (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
		},
		{
			`max(rate(foo[1m]))`,
			`max(` +
				concat(
					`max(rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`max(rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`max(rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
		},
		{
			`max by (foo) (rate(foo[1m]))`,
			`max by (foo) (` +
				concat(
					`max by (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`max by (foo)(rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`max by (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
		},
		{
			`max without (foo) (rate(foo[1m]))`,
			`max without (foo) (` +
				concat(
					`max without (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`max without (foo)(rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`max without (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
		},
		{
			`sum by (foo) (rate(foo[1m]))`,
			`sum by (foo) (` +
				concat(
					`sum by  (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`sum by  (foo) (rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`sum by  (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
		},
		{
			`sum without (foo) (rate(foo[1m]))`,
			`sum without (foo) (` +
				concat(
					`sum without  (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`sum without  (foo) (rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`sum without  (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
		},
		{
			`avg without (foo) (rate(foo[1m]))`,
			`sum without (foo) (` +
				concat(
					`sum without  (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`sum without  (foo) (rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`sum without  (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)/ sum without (foo) (` +
				concat(
					`count without  (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`count without  (foo) (rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`count without  (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
		},
		{
			`avg by (foo) (rate(foo[1m]))`,
			`sum by (foo)(` +
				concat(
					`sum by  (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`sum by  (foo) (rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`sum by  (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `) / sum by (foo) (` +
				concat(
					`count by  (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`count by  (foo) (rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`count by  (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
		},
		{
			`avg(rate(foo[1m]))`,
			`sum(` +
				concat(
					`sum(rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`sum(rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`sum(rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)/sum(` +
				concat(
					`count(rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`count(rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`count(rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
		},
		{
			`topk(10,avg by (foo)(rate(foo[1m])))`,
			`topk(10, sum by (foo)(` +
				concat(
					`sum by (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`sum by (foo) (rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`sum by (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `) / sum by (foo)(` +
				concat(
					`count by (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`count by (foo) (rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`count by (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `))`,
		},

		{
			`sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series[7d]))`,
			`sum by (user, cluster, namespace)(` +
				concat(
					`sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series{__query_shard__="0_of_3"}[7d]))`,
					`sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series{__query_shard__="1_of_3"}[7d]))`,
					`sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series{__query_shard__="2_of_3"}[7d]))`,
				) + `)`,
		},
		{
			`quantile_over_time(0.99, cortex_ingester_active_series[1w])`,
			concat(`quantile_over_time(0.99, cortex_ingester_active_series[1w])`),
		},
	} {
		tt := tt

		t.Run(tt.in, func(t *testing.T) {
			mapper, err := NewSharding(3, nil)
			require.NoError(t, err)
			expr, err := parser.ParseExpr(tt.in)
			require.NoError(t, err)
			out, err := parser.ParseExpr(tt.out)
			require.NoError(t, err)
			mapped, err := mapper.Map(expr)
			require.NoError(t, err)
			require.Equal(t,
				out.String(),
				mapped.String(),
			)
		})
	}
}

func concat(queries ...string) string {
	nodes := make([]parser.Node, 0, len(queries))
	for _, q := range queries {
		n, err := parser.ParseExpr(q)
		if err != nil {
			panic(err)
		}
		nodes = append(nodes, n)

	}
	mapped, err := vectorSquasher(nodes...)
	if err != nil {
		panic(err)
	}
	return mapped.String()
}

func TestShardSummerWithEncoding(t *testing.T) {
	for i, c := range []struct {
		shards   int
		input    string
		expected string
	}{
		{
			shards:   3,
			input:    `sum(rate(bar1{baz="blip"}[1m]))`,
			expected: `sum(__embedded_queries__{__cortex_queries__="{\"Concat\":[\"sum(rate(bar1{__query_shard__=\\\"0_of_3\\\",baz=\\\"blip\\\"}[1m]))\",\"sum(rate(bar1{__query_shard__=\\\"1_of_3\\\",baz=\\\"blip\\\"}[1m]))\",\"sum(rate(bar1{__query_shard__=\\\"2_of_3\\\",baz=\\\"blip\\\"}[1m]))\"]}"})`,
		},
	} {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			summer, err := newShardSummer(c.shards, vectorSquasher, nil)
			require.Nil(t, err)
			expr, err := parser.ParseExpr(c.input)
			require.Nil(t, err)
			res, err := summer.Map(expr)
			require.Nil(t, err)

			expected, err := parser.ParseExpr(c.expected)
			require.Nil(t, err)

			require.Equal(t, expected.String(), res.String())
		})
	}
}
