// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/shard_summer_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardSummer(t *testing.T) {
	for _, tt := range []struct {
		in                     string
		out                    string
		expectedShardedQueries int
	}{
		{
			`quantile(0.9,foo)`,
			concat(`quantile(0.9,foo)`),
			0,
		},
		{
			`absent(foo)`,
			concat(`absent(foo)`),
			0,
		},
		{
			`absent_over_time(foo[1m])`,
			concat(`absent_over_time(foo[1m])`),
			0,
		},
		{

			`histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`,
			`histogram_quantile(0.5, ` + concat(
				`rate(bar1{__query_shard__="0_of_3",baz="blip"}[30s])`,
				`rate(bar1{__query_shard__="1_of_3",baz="blip"}[30s])`,
				`rate(bar1{__query_shard__="2_of_3",baz="blip"}[30s])`,
			) + `)`,
			3,
		},
		{
			`sum by (foo) (histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[10m])))`,
			`sum by (foo) (histogram_quantile(0.9,
				` + concat(
				`rate(http_request_duration_seconds_bucket{__query_shard__="0_of_3"}[10m])`,
				`rate(http_request_duration_seconds_bucket{__query_shard__="1_of_3"}[10m])`,
				`rate(http_request_duration_seconds_bucket{__query_shard__="2_of_3"}[10m])`,
			) + `))`,
			3,
		},
		{
			`sum by (foo,bar) (min_over_time(bar1{baz="blip"}[1m]))`,
			`sum by(foo, bar) (` +
				concat(
					`sum by (foo,bar) (min_over_time(bar1{__query_shard__="0_of_3",baz="blip"}[1m]))`,
					`sum by (foo,bar) (min_over_time(bar1{__query_shard__="1_of_3",baz="blip"}[1m]))`,
					`sum by (foo,bar) (min_over_time(bar1{__query_shard__="2_of_3",baz="blip"}[1m]))`,
				) + `)`,
			3,
		},
		{
			"sum(rate(bar1[1m])) or rate(bar2[1m])",
			`sum(` +
				concat(
					`sum(rate(bar1{__query_shard__="0_of_3"}[1m]))`,
					`sum(rate(bar1{__query_shard__="1_of_3"}[1m]))`,
					`sum(rate(bar1{__query_shard__="2_of_3"}[1m]))`,
				) + `) or ` + concat(
				`rate(bar2{__query_shard__="0_of_3"}[1m])`,
				`rate(bar2{__query_shard__="1_of_3"}[1m])`,
				`rate(bar2{__query_shard__="2_of_3"}[1m])`,
			),
			6,
		},
		{
			"sum(rate(bar1[1m])) or sum(rate(bar2[1m]))",
			`sum(` +
				concat(
					`sum(rate(bar1{__query_shard__="0_of_3"}[1m]))`,
					`sum(rate(bar1{__query_shard__="1_of_3"}[1m]))`,
					`sum(rate(bar1{__query_shard__="2_of_3"}[1m]))`,
				) + `) or sum(` +
				concat(
					`sum(rate(bar2{__query_shard__="0_of_3"}[1m]))`,
					`sum(rate(bar2{__query_shard__="1_of_3"}[1m]))`,
					`sum(rate(bar2{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
			6,
		},
		{
			`histogram_quantile(0.5, sum(rate(cortex_cache_value_size_bytes_bucket[5m])) by (le))`,
			`histogram_quantile(0.5,sum  by (le) (` + concat(
				`sum  by (le) (rate(cortex_cache_value_size_bytes_bucket{__query_shard__="0_of_3"}[5m]))`,
				`sum  by (le) (rate(cortex_cache_value_size_bytes_bucket{__query_shard__="1_of_3"}[5m]))`,
				`sum  by (le) (rate(cortex_cache_value_size_bytes_bucket{__query_shard__="2_of_3"}[5m]))`,
			) + `))`,
			3,
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
			3,
		},
		{
			`sum(rate(foo[1m]))`,
			`sum(` +
				concat(
					`sum(rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`sum(rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`sum(rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
			3,
		},
		{
			`count(rate(foo[1m]))`,
			`sum(` +
				concat(
					`count(rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`count(rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`count(rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
			3,
		},
		{
			`count(up)`,
			`sum(` +
				concat(
					`count(up{__query_shard__="0_of_3"})`,
					`count(up{__query_shard__="1_of_3"})`,
					`count(up{__query_shard__="2_of_3"})`,
				) + `)`,
			3,
		},
		{
			`avg(count(test))`,
			`avg(sum(` +
				concat(
					`count(test{__query_shard__="0_of_3"})`,
					`count(test{__query_shard__="1_of_3"})`,
					`count(test{__query_shard__="2_of_3"})`,
				) + `))`,
			3,
		},
		{
			`count by (foo) (rate(foo[1m]))`,
			`sum by (foo) (` +
				concat(
					`count by (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`count by (foo)(rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`count by (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
			3,
		},
		{
			`count without (foo) (rate(foo[1m]))`,
			`sum without (foo) (` +
				concat(
					`count without (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`count without (foo)(rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`count without (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
			3,
		},
		{
			`max(rate(foo[1m]))`,
			`max(` +
				concat(
					`max(rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`max(rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`max(rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
			3,
		},
		{
			`max by (foo) (rate(foo[1m]))`,
			`max by (foo) (` +
				concat(
					`max by (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`max by (foo)(rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`max by (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
			3,
		},
		{
			`max without (foo) (rate(foo[1m]))`,
			`max without (foo) (` +
				concat(
					`max without (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`max without (foo)(rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`max without (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
			3,
		},
		{
			`sum by (foo) (rate(foo[1m]))`,
			`sum by (foo) (` +
				concat(
					`sum by  (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`sum by  (foo) (rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`sum by  (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
			3,
		},
		{
			`sum without (foo) (rate(foo[1m]))`,
			`sum without (foo) (` +
				concat(
					`sum without  (foo) (rate(foo{__query_shard__="0_of_3"}[1m]))`,
					`sum without  (foo) (rate(foo{__query_shard__="1_of_3"}[1m]))`,
					`sum without  (foo) (rate(foo{__query_shard__="2_of_3"}[1m]))`,
				) + `)`,
			3,
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
			6,
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
			6,
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
			6,
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
			6,
		},
		{
			`min_over_time(metric_counter[5m])`,
			concat(
				`min_over_time(metric_counter{__query_shard__="0_of_3"}[5m])`,
				`min_over_time(metric_counter{__query_shard__="1_of_3"}[5m])`,
				`min_over_time(metric_counter{__query_shard__="2_of_3"}[5m])`,
			),
			3,
		},
		{
			`sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series[7d]))`,
			`sum by (user, cluster, namespace)(` +
				concat(
					`sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series{__query_shard__="0_of_3"}[7d]))`,
					`sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series{__query_shard__="1_of_3"}[7d]))`,
					`sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series{__query_shard__="2_of_3"}[7d]))`,
				) + `)`,
			3,
		},
		{
			`min_over_time(
				sum by(group_1) (
					rate(metric_counter[5m])
				)[10m:2m]
			)`,
			concat(
				`min_over_time(
					sum by(group_1) (
						rate(metric_counter[5m])
					)[10m:2m]
				)`,
			),
			0,
		},
		{
			`max_over_time(
				stddev_over_time(
					deriv(
						rate(metric_counter[10m])
					[5m:1m])
				[2m:])
			[10m:])`,
			concat(
				`max_over_time(
					stddev_over_time(
						deriv(
							rate(metric_counter{__query_shard__="0_of_3"}[10m])
						[5m:1m])
					[2m:])
				[10m:])`,
				`max_over_time(
					stddev_over_time(
						deriv(
							rate(metric_counter{__query_shard__="1_of_3"}[10m])
						[5m:1m])
					[2m:])
				[10m:])`,
				`max_over_time(
					stddev_over_time(
						deriv(
							rate(metric_counter{__query_shard__="2_of_3"}[10m])
						[5m:1m])
					[2m:])
				[10m:])`,
			),
			3,
		},
		{
			`rate(
				sum by(group_1) (
					rate(metric_counter[5m])
				)[10m:]
			)`,
			concat(
				`rate(
						sum by(group_1) (
							rate(metric_counter[5m])
						)[10m:]
					)`,
			),
			0,
		},
		{
			`quantile_over_time(0.99, cortex_ingester_active_series[1w])`,
			concat(
				`quantile_over_time(0.99, cortex_ingester_active_series{__query_shard__="0_of_3"}[1w])`,
				`quantile_over_time(0.99, cortex_ingester_active_series{__query_shard__="1_of_3"}[1w])`,
				`quantile_over_time(0.99, cortex_ingester_active_series{__query_shard__="2_of_3"}[1w])`,
			),
			3,
		},
		{
			`ceil(sum by (foo) (rate(cortex_ingester_active_series[1w])))`,
			`ceil(sum by (foo) (` +
				concat(
					`sum by (foo) (rate(cortex_ingester_active_series{__query_shard__="0_of_3"}[1w]))`,
					`sum by (foo) (rate(cortex_ingester_active_series{__query_shard__="1_of_3"}[1w]))`,
					`sum by (foo) (rate(cortex_ingester_active_series{__query_shard__="2_of_3"}[1w]))`,
				) + `))`,
			3,
		},
		{
			`ln(bar) - resets(foo[1d])`,
			concat(
				`ln(bar{__query_shard__="0_of_3"})`,
				`ln(bar{__query_shard__="1_of_3"})`,
				`ln(bar{__query_shard__="2_of_3"})`,
			) + ` - ` +
				concat(
					`resets(foo{__query_shard__="0_of_3"}[1d])`,
					`resets(foo{__query_shard__="1_of_3"}[1d])`,
					`resets(foo{__query_shard__="2_of_3"}[1d])`,
				),
			6,
		},
		{
			`predict_linear(foo[10m],3600)`,
			concat(
				`predict_linear(foo{__query_shard__="0_of_3"}[10m],3600)`,
				`predict_linear(foo{__query_shard__="1_of_3"}[10m],3600)`,
				`predict_linear(foo{__query_shard__="2_of_3"}[10m],3600)`,
			),
			3,
		},
		{
			`sum by (foo) (label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*"))`,
			`sum by (foo) (` + concat(
				`sum by (foo) (label_replace(up{__query_shard__="0_of_3",job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*"))`,
				`sum by (foo) (label_replace(up{__query_shard__="1_of_3",job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*"))`,
				`sum by (foo) (label_replace(up{__query_shard__="2_of_3",job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*"))`,
			) + `)`,
			3,
		},
		{
			`label_join(up{job="api-server",service="a:c"}, "foo", ",", "service", "job")`,
			concat(
				`label_join(up{job="api-server",service="a:c"}, "foo", ",", "service", "job")`,
			),
			0,
		},
		{
			`label_join(up{job="api-server",service="a:c"}, "foo", ",", "service", "job") > 20`,
			concat(
				`label_join(up{job="api-server",service="a:c"}, "foo", ",", "service", "job") > 20`,
			),
			0,
		},
		{
			`ln(exp(label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")))`,
			concat(
				`ln(exp(label_replace(up{__query_shard__="0_of_3",job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")))`,
				`ln(exp(label_replace(up{__query_shard__="1_of_3",job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")))`,
				`ln(exp(label_replace(up{__query_shard__="2_of_3",job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")))`,
			),
			3,
		},
		{
			`ln(
				label_replace(
					sum by (cluster) (up{job="api-server",service="a:c"})
				, "foo", "$1", "service", "(.*):.*")
			)`,
			`ln(
				label_replace(
					sum by (cluster) ( ` +
				concat(
					`sum by (cluster) (up{__query_shard__="0_of_3",job="api-server",service="a:c"})`,
					`sum by (cluster) (up{__query_shard__="1_of_3",job="api-server",service="a:c"})`,
					`sum by (cluster) (up{__query_shard__="2_of_3",job="api-server",service="a:c"})`,
				) + `)
				, "foo", "$1", "service", "(.*):.*")
			)`,
			3,
		},
	} {
		tt := tt

		t.Run(tt.in, func(t *testing.T) {
			mapper, err := NewSharding(3)
			require.NoError(t, err)
			expr, err := parser.ParseExpr(tt.in)
			require.NoError(t, err)
			out, err := parser.ParseExpr(tt.out)
			require.NoError(t, err)

			stats := NewMapperStats()
			mapped, err := mapper.Map(expr, stats)
			require.NoError(t, err)
			require.Equal(t, out.String(), mapped.String())
			assert.Equal(t, tt.expectedShardedQueries, stats.GetShardedQueries())
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
			expected: `sum(__embedded_queries__{__queries__="{\"Concat\":[\"sum(rate(bar1{__query_shard__=\\\"0_of_3\\\",baz=\\\"blip\\\"}[1m]))\",\"sum(rate(bar1{__query_shard__=\\\"1_of_3\\\",baz=\\\"blip\\\"}[1m]))\",\"sum(rate(bar1{__query_shard__=\\\"2_of_3\\\",baz=\\\"blip\\\"}[1m]))\"]}"})`,
		},
	} {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			summer, err := newShardSummer(c.shards, vectorSquasher)
			require.Nil(t, err)
			expr, err := parser.ParseExpr(c.input)
			require.Nil(t, err)

			stats := NewMapperStats()
			res, err := summer.Map(expr, stats)
			require.Nil(t, err)
			assert.Equal(t, c.shards, stats.GetShardedQueries())
			expected, err := parser.ParseExpr(c.expected)
			require.Nil(t, err)

			require.Equal(t, expected.String(), res.String())
		})
	}
}
