// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/shard_summer_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"fmt"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/sharding"
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
			concat(
				`histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`,
			),
			0,
		},
		{
			`sum by (foo) (histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[10m])))`,
			concat(
				`sum by (foo) (histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[10m])))`,
			),
			0,
		},
		{
			`sum by (foo,bar) (min_over_time(bar1{baz="blip"}[1m]))`,
			`sum by(foo, bar) (` +
				concatShards(3, `sum by (foo,bar) (min_over_time(bar1{__query_shard__="x_of_y",baz="blip"}[1m]))`) +
				`)`,
			3,
		},
		{
			"sum(rate(bar1[1m])) or rate(bar2[1m])",
			`sum(` + concatShards(3, `sum(rate(bar1{__query_shard__="x_of_y"}[1m]))`) + `) or ` + concat(`rate(bar2[1m])`),
			3,
		},
		{
			"sum(rate(bar1[1m])) or sum(rate(bar2[1m]))",
			`sum(` + concatShards(3, `sum(rate(bar1{__query_shard__="x_of_y"}[1m]))`) + `)` +
				` or sum(` + concatShards(3, `sum(rate(bar2{__query_shard__="x_of_y"}[1m]))`) + `)`,
			6,
		},
		{
			`histogram_quantile(0.5, sum(rate(cortex_cache_value_size_bytes_bucket[5m])) by (le))`,
			`histogram_quantile(0.5,sum  by (le) (` + concatShards(3, `sum  by (le) (rate(cortex_cache_value_size_bytes_bucket{__query_shard__="x_of_y"}[5m]))`) + `))`,
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
				  sum(` + concatShards(3, `count(bar1{__query_shard__="x_of_y"})  by (drive,instance)`) + `
				  )  by (drive,instance)
				)  by (instance)
			  )`,
			3,
		},
		{
			`sum(rate(foo[1m]))`,
			`sum(` + concatShards(3, `sum(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			3,
		},
		{
			`count(rate(foo[1m]))`,
			`sum(` +
				concatShards(3, `count(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			3,
		},
		{
			`count(up)`,
			`sum(` + concatShards(3, `count(up{__query_shard__="x_of_y"})`) + `)`,
			3,
		},
		{
			`avg(count(test))`,
			`avg(sum(` + concatShards(3, `count(test{__query_shard__="x_of_y"})`) + `))`,
			3,
		},
		{
			`count by (foo) (rate(foo[1m]))`,
			`sum by (foo) (` + concatShards(3, `count by (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			3,
		},
		{
			`count without (foo) (rate(foo[1m]))`,
			`sum without (foo) (` + concatShards(3, `count without (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			3,
		},
		{
			`max(rate(foo[1m]))`,
			`max(` + concatShards(3, `max(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			3,
		},
		{
			`max by (foo) (rate(foo[1m]))`,
			`max by (foo) (` + concatShards(3, `max by (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			3,
		},
		{
			`max without (foo) (rate(foo[1m]))`,
			`max without (foo) (` + concatShards(3, `max without (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			3,
		},
		{
			`sum by (foo) (rate(foo[1m]))`,
			`sum by (foo) (` + concatShards(3, `sum by  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			3,
		},
		{
			`sum without (foo) (rate(foo[1m]))`,
			`sum without (foo) (` + concatShards(3, `sum without  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			3,
		},
		{
			`avg without (foo) (rate(foo[1m]))`,
			`(sum without (foo) (` + concatShards(3, `sum without  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `) ` +
				`/ sum without (foo) (` + concatShards(3, `count without  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `))`,
			6,
		},
		{
			`avg by (foo) (rate(foo[1m]))`,
			`(sum by (foo)(` + concatShards(3, `sum by  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `) ` +
				`/ sum by (foo) (` + concatShards(3, `count by  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `))`,
			6,
		},
		{
			`avg(rate(foo[1m]))`,
			`(sum(` + concatShards(3, `sum(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `) / ` +
				`sum(` + concatShards(3, `count(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `))`,
			6,
		},
		{
			`topk(10,avg by (foo)(rate(foo[1m])))`,
			`topk(10, (sum by (foo)(` +
				concatShards(3, `sum by (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `) / ` +
				`sum by (foo)(` + concatShards(3, `count by (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `))` +
				`)`,
			6,
		},
		{
			`min_over_time(metric_counter[5m])`,
			concat(`min_over_time(metric_counter[5m])`),
			0,
		},
		{
			`sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series[7d]))`,
			`sum by (user, cluster, namespace)(` +
				concatShards(3, `sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series{__query_shard__="x_of_y"}[7d]))`) +
				`)`,
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
			concatShards(3, `max_over_time(
					stddev_over_time(
						deriv(
							rate(metric_counter{__query_shard__="x_of_y"}[10m])
						[5m:1m])
					[2m:])
				[10m:])`),
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
			`absent_over_time(rate(metric_counter[5m])[10m:])`,
			concat(
				`absent_over_time(rate(metric_counter[5m])[10m:])`,
			),
			0,
		},
		{
			`max_over_time(
				stddev_over_time(
					deriv(
						sort(metric_counter)
					[5m:1m])
				[2m:])
			[10m:])`,
			concat(
				`max_over_time(
					stddev_over_time(
						deriv(
							sort(metric_counter)
						[5m:1m])
					[2m:])
				[10m:])`,
			),
			0,
		},
		{
			`max_over_time(
				absent_over_time(
					deriv(
						rate(metric_counter[1m])
					[5m:1m])
				[2m:])
			[10m:])`,
			concat(
				`max_over_time(
					absent_over_time(
						deriv(
							rate(metric_counter[1m])
						[5m:1m])
					[2m:])
				[10m:])`,
			),
			0,
		},
		{
			`quantile_over_time(0.99, cortex_ingester_active_series[1w])`,
			concat(
				`quantile_over_time(0.99, cortex_ingester_active_series{}[1w])`,
			),
			0,
		},
		{
			`ceil(sum by (foo) (rate(cortex_ingester_active_series[1w])))`,
			`ceil(sum by (foo) (` + concatShards(3, `sum by (foo) (rate(cortex_ingester_active_series{__query_shard__="x_of_y"}[1w]))`) + `))`,
			3,
		},
		{
			`ln(bar) - resets(foo[1d])`,
			concat(`ln(bar) - resets(foo[1d]) `),
			0,
		},
		{
			`predict_linear(foo[10m],3600)`,
			concat(`predict_linear(foo[10m],3600)`),
			0,
		},
		{
			`label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")`,
			concat(`label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")`),
			0,
		},
		{
			`ln(exp(label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")))`,
			concat(`ln(exp(label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")))`),
			0,
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
				concatShards(3, `sum by (cluster) (up{__query_shard__="x_of_y",job="api-server",service="a:c"})`) + `)
				, "foo", "$1", "service", "(.*):.*")
			)`,
			3,
		},
		{
			`sum by (job)(rate(http_requests_total[1h] @ end()))`,
			`sum by (job)(` + concatShards(3, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] @ end()))`) + `)`,
			3,
		},
		{
			`sum by (job)(rate(http_requests_total[1h] offset 1w @ 10))`,
			`sum by (job)(` + concatShards(3, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] offset 1w @ 10))`) + `)`,
			3,
		},
		{
			`sum by (job)(rate(http_requests_total[1h] offset 1w @ 10)) / 2`,
			`sum by (job)(` + concatShards(3, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] offset 1w @ 10))`) + `) / 2`,
			3,
		},
		{
			`sum by (job)(rate(http_requests_total[1h] offset 1w @ 10)) / 2 ^ 2`,
			`sum by (job)(` + concatShards(3, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] offset 1w @ 10))`) + `) / 2 ^ 2`,
			3,
		},
		{
			`sum by (group_1) (rate(metric_counter[1m]) / time() * 2)`,
			`sum by (group_1) (` + concatShards(3, `sum by (group_1) (rate(metric_counter{__query_shard__="x_of_y"}[1m]) / time() * 2)`) + `)`,
			3,
		},
		{
			`sum by (group_1) (rate(metric_counter[1m])) / time() *2`,
			`sum by (group_1) (` + concatShards(3, `sum by (group_1) (rate(metric_counter{__query_shard__="x_of_y"}[1m]))`) + `) / time() *2`,
			3,
		},
		{
			`sum by (group_1) (rate(metric_counter[1m])) / time()`,
			`sum by (group_1) (` + concatShards(3, `sum by (group_1) (rate(metric_counter{__query_shard__="x_of_y"}[1m]))`) + `) / time()`,
			3,
		},
		{
			`sum by (group_1) (rate(metric_counter[1m])) / vector(3) ^ month()`,
			`sum by (group_1) (` + concatShards(3, `sum by (group_1) (rate(metric_counter{__query_shard__="x_of_y"}[1m]))`) + `)` +
				`/ vector(3) ^ month()`,
			3,
		},
		{
			`vector(3) ^ month()`,
			`vector(3) ^ month()`,
			0,
		},
		{
			`sum(rate(metric_counter[1m])) / vector(3) ^ year(foo)`,
			`sum(` + concatShards(3, `sum(rate(metric_counter{__query_shard__="x_of_y"}[1m]))`) + `)` +
				`/ ` + concat(`vector(3) ^ year(foo)`),
			3,
		},
		{
			// can't shard foo > bar,
			// because foo{__query_shard__="1_of_3"} won't have the matching labels in bar{__query_shard__="1_of_3"}
			`foo > bar`,
			concat(`foo > bar`),
			0,
		},
		{
			// we could shard foo * 2, but since it doesn't reduce the data set, we don't.
			`foo * 2`,
			concat(`foo * 2`),
			0,
		},
		{
			`foo > 0`,
			concatShards(3, `foo{__query_shard__="x_of_y"} > 0`),
			3,
		},
		{
			`0 <= foo`,
			concatShards(3, `0 <= foo{__query_shard__="x_of_y"}`),
			3,
		},
		{
			`foo > (2 * 2)`,
			concatShards(3, `foo{__query_shard__="x_of_y"} > (2 * 2)`),
			3,
		},
		{
			`sum by (label) (foo > 0)`,
			`sum by(label) (` + concatShards(3, `sum by (label) (foo{__query_shard__="x_of_y"} > 0)`) + `)`,
			3,
		},
		{
			`sum by (label) (foo > 0) > 0`,
			`sum by (label) (` + concatShards(3, `sum by (label) (foo{__query_shard__="x_of_y"} > 0)`) + `) > 0`,
			3,
		},
		{
			`sum_over_time(foo[1m]) > (2 * 2)`,
			concatShards(3, `sum_over_time(foo{__query_shard__="x_of_y"}[1m]) > (2 * 2)`),
			3,
		},
		{
			`foo > sum(bar)`,
			concat(`foo`) + ` > sum(` + concatShards(3, `sum(bar{__query_shard__="x_of_y"})`) + `)`,
			3,
		},
		{
			`foo > scalar(sum(bar))`,
			concat(`foo`) + `> scalar(sum(` + concatShards(3, `sum(bar{__query_shard__="x_of_y"})`) + `))`,
			3,
		},
		{
			`scalar(min(foo)) > bool scalar(sum(bar))`,
			`scalar(min(` + concatShards(3, `min(foo{__query_shard__="x_of_y"})`) + `)) ` +
				`> bool scalar(sum(` + concatShards(3, `sum(bar{__query_shard__="x_of_y"})`) + `))`,
			6,
		},
		{
			in: `foo * on(a, b) group_left(c) avg by(a, b, c) (bar)`,
			out: concat(`foo`) + ` * on(a, b) group_left(c) ` +
				`(` +
				`sum by(a, b, c) (` + concatShards(3, `sum by(a, b, c) (bar{__query_shard__="x_of_y"})`) + `)` +
				` / ` +
				`sum by(a, b, c) (` + concatShards(3, `count by(a, b, c) (bar{__query_shard__="x_of_y"})`) + `)` +
				`)`,
			expectedShardedQueries: 6,
		},
		{
			in:                     `vector(1) > 0 and vector(1)`,
			out:                    `vector(1) > 0 and vector(1)`,
			expectedShardedQueries: 0,
		},
		{
			in:                     `sum(foo) > 0 and vector(1)`,
			out:                    `sum(` + concatShards(3, `sum(foo{__query_shard__="x_of_y"})`) + `) > 0 and vector(1)`,
			expectedShardedQueries: 3,
		},
	} {
		tt := tt

		t.Run(tt.in, func(t *testing.T) {
			mapper, err := NewSharding(3, log.NewNopLogger())
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

func concatShards(shards int, queryTemplate string) string {
	queries := make([]string, shards)
	for shard := range queries {
		queries[shard] = strings.ReplaceAll(queryTemplate, "x_of_y", sharding.FormatShardIDLabelValue(uint64(shard), uint64(shards)))
	}
	return concat(queries...)
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
			expected: `sum(__embedded_queries__{__queries__="{\"Concat\":[\"sum(rate(bar1{__query_shard__=\\\"1_of_3\\\",baz=\\\"blip\\\"}[1m]))\",\"sum(rate(bar1{__query_shard__=\\\"2_of_3\\\",baz=\\\"blip\\\"}[1m]))\",\"sum(rate(bar1{__query_shard__=\\\"3_of_3\\\",baz=\\\"blip\\\"}[1m]))\"]}"})`,
		},
	} {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			summer, err := newShardSummer(c.shards, vectorSquasher, log.NewNopLogger())
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
