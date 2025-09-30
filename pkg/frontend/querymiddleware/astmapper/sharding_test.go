// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/shard_summer_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"context"
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
	runTest := func(t *testing.T, input string, expected string, shardCount int, inspectOnly bool, expectedShardedQueries int) {
		stats := NewMapperStats()
		summer := NewQueryShardSummer(shardCount, inspectOnly, EmbeddedQueriesSquasher, log.NewNopLogger(), stats)
		expr, err := parser.ParseExpr(input)
		require.NoError(t, err)
		expectedExpr, err := parser.ParseExpr(expected)
		require.NoError(t, err)

		ctx := context.Background()
		mapped, err := summer.Map(ctx, expr)
		require.NoError(t, err)
		require.Equal(t, expectedExpr.String(), mapped.String())
		require.Equal(t, expectedShardedQueries, stats.GetShardedQueries())
	}

	const shardCount = 3

	for _, tt := range []struct {
		in                       string
		out                      string
		expectedShardableQueries int
	}{
		{
			`quantile(0.9,foo)`,
			`quantile(0.9,foo)`,
			0,
		},
		{
			`absent(foo)`,
			`absent(foo)`,
			0,
		},
		{
			`absent_over_time(foo[1m])`,
			`absent_over_time(foo[1m])`,
			0,
		},
		{

			`histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`,
			`histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`,
			0,
		},
		{
			`sum by (foo) (histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[10m])))`,
			`sum by (foo) (histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[10m])))`,
			0,
		},
		{
			`sum by (foo,bar) (min_over_time(bar1{baz="blip"}[1m]))`,
			`sum by(foo, bar) (` +
				concatShards(t, shardCount, `sum by (foo,bar) (min_over_time(bar1{__query_shard__="x_of_y",baz="blip"}[1m]))`) +
				`)`,
			1,
		},
		{
			// This query is not parallelized because the leg "rate(bar2[1m])" is not aggregated and
			// could result in high cardinality results.
			`sum(rate(bar1[1m])) or rate(bar2[1m])`,
			`sum(rate(bar1[1m])) or rate(bar2[1m])`,
			0,
		},
		{
			"sum(rate(bar1[1m])) or sum(rate(bar2[1m]))",
			`sum(` + concatShards(t, shardCount, `sum(rate(bar1{__query_shard__="x_of_y"}[1m]))`) + `)` +
				` or sum(` + concatShards(t, shardCount, `sum(rate(bar2{__query_shard__="x_of_y"}[1m]))`) + `)`,
			2,
		},
		{
			`histogram_quantile(0.5, sum(rate(cortex_cache_value_size_bytes_bucket[5m])) by (le))`,
			`histogram_quantile(0.5,sum  by (le) (` + concatShards(t, shardCount, `sum  by (le) (rate(cortex_cache_value_size_bytes_bucket{__query_shard__="x_of_y"}[5m]))`) + `))`,
			1,
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
				  sum(` + concatShards(t, shardCount, `count(bar1{__query_shard__="x_of_y"})  by (drive,instance)`) + `
				  )  by (drive,instance)
				)  by (instance)
			  )`,
			1,
		},
		{
			`sum(rate(foo[1m]))`,
			`sum(` + concatShards(t, shardCount, `sum(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			1,
		},
		{
			`count(rate(foo[1m]))`,
			`sum(` +
				concatShards(t, shardCount, `count(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			1,
		},
		{
			`count(up)`,
			`sum(` + concatShards(t, shardCount, `count(up{__query_shard__="x_of_y"})`) + `)`,
			1,
		},
		{
			`avg(count(test))`,
			`avg(sum(` + concatShards(t, shardCount, `count(test{__query_shard__="x_of_y"})`) + `))`,
			1,
		},
		{
			`count by (foo) (rate(foo[1m]))`,
			`sum by (foo) (` + concatShards(t, shardCount, `count by (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			1,
		},
		{
			`count without (foo) (rate(foo[1m]))`,
			`sum without (foo) (` + concatShards(t, shardCount, `count without (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			1,
		},
		{
			`max(rate(foo[1m]))`,
			`max(` + concatShards(t, shardCount, `max(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			1,
		},
		{
			`max by (foo) (rate(foo[1m]))`,
			`max by (foo) (` + concatShards(t, shardCount, `max by (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			1,
		},
		{
			`max without (foo) (rate(foo[1m]))`,
			`max without (foo) (` + concatShards(t, shardCount, `max without (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			1,
		},
		{
			`sum by (foo) (rate(foo[1m]))`,
			`sum by (foo) (` + concatShards(t, shardCount, `sum by  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			1,
		},
		{
			`sum without (foo) (rate(foo[1m]))`,
			`sum without (foo) (` + concatShards(t, shardCount, `sum without  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			1,
		},
		{
			`avg without (foo) (rate(foo[1m]))`,
			`(sum without (foo) (` + concatShards(t, shardCount, `sum without  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `) ` +
				`/ sum without (foo) (` + concatShards(t, shardCount, `count without  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `))`,
			2,
		},
		{
			`avg by (foo) (rate(foo[1m]))`,
			`(sum by (foo)(` + concatShards(t, shardCount, `sum by  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `) ` +
				`/ sum by (foo) (` + concatShards(t, shardCount, `count by  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `))`,
			2,
		},
		{
			`avg(rate(foo[1m]))`,
			`(sum(` + concatShards(t, shardCount, `sum(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `) / ` +
				`sum(` + concatShards(t, shardCount, `count(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `))`,
			2,
		},
		{
			`topk(10,avg by (foo)(rate(foo[1m])))`,
			`topk(10, (sum by (foo)(` +
				concatShards(t, shardCount, `sum by (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `) / ` +
				`sum by (foo)(` + concatShards(t, shardCount, `count by (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `))` +
				`)`,
			2,
		},
		{
			`min_over_time(metric_counter[5m])`,
			`min_over_time(metric_counter[5m])`,
			0,
		},
		{
			`sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series[7d]))`,
			`sum by (user, cluster, namespace)(` +
				concatShards(t, shardCount, `sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series{__query_shard__="x_of_y"}[7d]))`) +
				`)`,
			1,
		},
		{
			`min_over_time(
				sum by(group_1) (
					rate(metric_counter[5m])
				)[10m:2m]
			)`,
			`min_over_time(
				sum by(group_1) (
					rate(metric_counter[5m])
				)[10m:2m]
			)`,
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
			concatShards(t, shardCount, `max_over_time(
					stddev_over_time(
						deriv(
							rate(metric_counter{__query_shard__="x_of_y"}[10m])
						[5m:1m])
					[2m:])
				[10m:])`),
			1,
		},
		{
			// Parenthesis expression between the parallelizeable function call and the subquery.
			`max_over_time((
				stddev_over_time(
					deriv(
						rate(metric_counter[10m])
					[5m:1m])
				[2m:])
			[10m:]))`,
			concatShards(t, shardCount, `max_over_time((
					stddev_over_time(
						deriv(
							rate(metric_counter{__query_shard__="x_of_y"}[10m])
						[5m:1m])
					[2m:])
				[10m:]))`),
			1,
		},
		{
			`rate(
				sum by(group_1) (
					rate(metric_counter[5m])
				)[10m:]
			)`,
			`rate(
				sum by(group_1) (
					rate(metric_counter[5m])
				)[10m:]
			)`,
			0,
		},
		{
			`absent_over_time(rate(metric_counter[5m])[10m:])`,
			`absent_over_time(rate(metric_counter[5m])[10m:])`,
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
			`max_over_time(
				stddev_over_time(
					deriv(
						sort(metric_counter)
					[5m:1m])
				[2m:])
			[10m:])`,
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
			`max_over_time(
				absent_over_time(
					deriv(
						rate(metric_counter[1m])
					[5m:1m])
				[2m:])
			[10m:])`,
			0,
		},
		{
			`quantile_over_time(0.99, cortex_ingester_active_series[1w])`,
			`quantile_over_time(0.99, cortex_ingester_active_series{}[1w])`,
			0,
		},
		{
			`ceil(sum by (foo) (rate(cortex_ingester_active_series[1w])))`,
			`ceil(sum by (foo) (` + concatShards(t, shardCount, `sum by (foo) (rate(cortex_ingester_active_series{__query_shard__="x_of_y"}[1w]))`) + `))`,
			1,
		},
		{
			`ln(bar) - resets(foo[1d])`,
			`ln(bar) - resets(foo[1d]) `,
			0,
		},
		{
			`predict_linear(foo[10m],3600)`,
			`predict_linear(foo[10m],3600)`,
			0,
		},
		{
			`label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")`,
			`label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")`,
			0,
		},
		{
			`ln(exp(label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")))`,
			`ln(exp(label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")))`,
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
				concatShards(t, shardCount, `sum by (cluster) (up{__query_shard__="x_of_y",job="api-server",service="a:c"})`) + `)
				, "foo", "$1", "service", "(.*):.*")
			)`,
			1,
		},
		{
			`sum by (job)(rate(http_requests_total[1h] @ end()))`,
			`sum by (job)(` + concatShards(t, shardCount, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] @ end()))`) + `)`,
			1,
		},
		{
			`sum by (job)(rate(http_requests_total[1h] offset 1w @ 10))`,
			`sum by (job)(` + concatShards(t, shardCount, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] offset 1w @ 10))`) + `)`,
			1,
		},
		{
			`sum by (job)(rate(http_requests_total[1h] offset 1w @ 10)) / 2`,
			`sum by (job)(` + concatShards(t, shardCount, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] offset 1w @ 10))`) + `) / 2`,
			1,
		},
		{
			`sum by (job)(rate(http_requests_total[1h] offset 1w @ 10)) / 2 ^ 2`,
			`sum by (job)(` + concatShards(t, shardCount, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] offset 1w @ 10))`) + `) / 2 ^ 2`,
			1,
		},
		{
			`sum by (group_1) (rate(metric_counter[1m]) / time() * 2)`,
			`sum by (group_1) (` + concatShards(t, shardCount, `sum by (group_1) (rate(metric_counter{__query_shard__="x_of_y"}[1m]) / time() * 2)`) + `)`,
			1,
		},
		{
			`sum by (group_1) (rate(metric_counter[1m])) / time() *2`,
			`sum by (group_1) (` + concatShards(t, shardCount, `sum by (group_1) (rate(metric_counter{__query_shard__="x_of_y"}[1m]))`) + `) / time() *2`,
			1,
		},
		{
			`sum by (group_1) (rate(metric_counter[1m])) / time()`,
			`sum by (group_1) (` + concatShards(t, shardCount, `sum by (group_1) (rate(metric_counter{__query_shard__="x_of_y"}[1m]))`) + `) / time()`,
			1,
		},
		{
			`sum by (group_1) (rate(metric_counter[1m])) / vector(3) ^ month()`,
			`sum by (group_1) (` + concatShards(t, shardCount, `sum by (group_1) (rate(metric_counter{__query_shard__="x_of_y"}[1m]))`) + `)` +
				`/ vector(3) ^ month()`,
			1,
		},
		{
			`vector(3) ^ month()`,
			`vector(3) ^ month()`,
			0,
		},
		{
			// This query is not parallelized because the leg "year(foo)" could result in high cardinality results.
			`sum(rate(metric_counter[1m])) / vector(3) ^ year(foo)`,
			`sum(rate(metric_counter[1m])) / vector(3) ^ year(foo)`,
			0,
		},
		{
			// can't shard foo > bar,
			// because foo{__query_shard__="1_of_3"} won't have the matching labels in bar{__query_shard__="1_of_3"}
			`foo > bar`,
			`foo > bar`,
			0,
		},
		{
			// we could shard foo * 2, but since it doesn't reduce the data set, we don't.
			`foo * 2`,
			`foo * 2`,
			0,
		},
		{
			`foo > 0`,
			concatShards(t, shardCount, `foo{__query_shard__="x_of_y"} > 0`),
			1,
		},
		{
			`0 <= foo`,
			concatShards(t, shardCount, `0 <= foo{__query_shard__="x_of_y"}`),
			1,
		},
		{
			`foo > (2 * 2)`,
			concatShards(t, shardCount, `foo{__query_shard__="x_of_y"} > (2 * 2)`),
			1,
		},
		{
			`sum by (label) (foo > 0)`,
			`sum by(label) (` + concatShards(t, shardCount, `sum by (label) (foo{__query_shard__="x_of_y"} > 0)`) + `)`,
			1,
		},
		{
			`sum by (label) (foo > 0) > 0`,
			`sum by (label) (` + concatShards(t, shardCount, `sum by (label) (foo{__query_shard__="x_of_y"} > 0)`) + `) > 0`,
			1,
		},
		{
			`sum_over_time(foo[1m]) > (2 * 2)`,
			concatShards(t, shardCount, `sum_over_time(foo{__query_shard__="x_of_y"}[1m]) > (2 * 2)`),
			1,
		},
		{
			// This query is not parallelized because the leg "foo" is not aggregated and
			// could result in high cardinality results.
			`foo > sum(bar)`,
			`foo > sum(bar)`,
			0,
		},
		{
			// This query is not parallelized because the leg "foo" is not aggregated and
			// could result in high cardinality results.
			`foo > scalar(sum(bar))`,
			`foo > scalar(sum(bar))`,
			0,
		},
		{
			`scalar(min(foo)) > bool scalar(sum(bar))`,
			`scalar(min(` + concatShards(t, shardCount, `min(foo{__query_shard__="x_of_y"})`) + `)) ` +
				`> bool scalar(sum(` + concatShards(t, shardCount, `sum(bar{__query_shard__="x_of_y"})`) + `))`,
			2,
		},
		{
			// This query is not parallelized because the leg "foo" is not aggregated and
			// could result in high cardinality results.
			in:                       `foo * on(a, b) group_left(c) avg by(a, b, c) (bar)`,
			out:                      `foo * on(a, b) group_left(c) avg by(a, b, c) (bar)`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `vector(1) > 0 and vector(1)`,
			out:                      `vector(1) > 0 and vector(1)`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `sum(foo) > 0 and vector(1)`,
			out:                      `sum(` + concatShards(t, shardCount, `sum(foo{__query_shard__="x_of_y"})`) + `) > 0 and vector(1)`,
			expectedShardableQueries: 1,
		},
		{
			// This query is not parallelized because the leg "pod:container_cpu_usage:sum" is not aggregated and
			// could result in high cardinality results.
			in: `max by(pod) (
                    max without(prometheus_replica, instance, node) (kube_pod_labels{namespace="test"})
                    *
                    on(cluster, pod, namespace) group_right() pod:container_cpu_usage:sum
            )`,
			out: `max by(pod) (
                    max without(prometheus_replica, instance, node) (kube_pod_labels{namespace="test"})
                    *
                    on(cluster, pod, namespace) group_right() pod:container_cpu_usage:sum
            )`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `sum(rate(metric[1m])) and max(metric) > 0`,
			out:                      `sum(` + concatShards(t, shardCount, `sum(rate(metric{__query_shard__="x_of_y"}[1m]))`) + `) and max(` + concatShards(t, shardCount, `max(metric{__query_shard__="x_of_y"})`) + `) > 0`,
			expectedShardableQueries: 2,
		},
		{
			in:                       `sum(rate(metric[1m])) > avg(rate(metric[1m]))`,
			out:                      `sum(` + concatShards(t, shardCount, `sum(rate(metric{__query_shard__="x_of_y"}[1m]))`) + `) > (sum(` + concatShards(t, shardCount, `sum(rate(metric{__query_shard__="x_of_y"}[1m]))`) + `) / sum(` + concatShards(t, shardCount, `count(rate(metric{__query_shard__="x_of_y"}[1m]))`) + `))`,
			expectedShardableQueries: 3,
		},
		{
			in:                       `group by (a, b) (metric)`,
			out:                      `group by (a, b) (` + concatShards(t, shardCount, `group by (a, b) (metric{__query_shard__="x_of_y"})`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `count by (a) (group by (a, b) (metric))`,
			out:                      `count by (a) (group by (a, b) (` + concatShards(t, shardCount, `group by (a, b) (metric{__query_shard__="x_of_y"})`) + `))`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `count(group without () ({namespace="foo"}))`,
			out:                      `count(group without() (` + concatShards(t, shardCount, `group without() ({namespace="foo",__query_shard__="x_of_y"})`) + `))`,
			expectedShardableQueries: 1,
		},
	} {
		t.Run(tt.in, func(t *testing.T) {
			runTest(t, tt.in, tt.out, shardCount, false, tt.expectedShardableQueries*shardCount)
		})

		t.Run(tt.in+" with 'inspect only' set", func(t *testing.T) {
			// The query should be unchanged, but the number of sharded queries should still be reported.
			runTest(t, tt.in, tt.in, 1, true, tt.expectedShardableQueries)
		})
	}
}

func concatShards(t *testing.T, shards int, queryTemplate string) string {
	queries := make([]EmbeddedQuery, shards)
	for shard := range queries {
		queryStr := strings.ReplaceAll(queryTemplate, "x_of_y", sharding.FormatShardIDLabelValue(uint64(shard), uint64(shards)))
		expr, err := parser.ParseExpr(queryStr)
		require.NoError(t, err)
		queries[shard] = NewEmbeddedQuery(expr, nil)
	}
	return concatInner(queries...)
}

func concatInner(rawQueries ...EmbeddedQuery) string {
	mapped, err := EmbeddedQueriesSquasher.Squash(rawQueries...)
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
			expected: `sum(__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"sum(rate(bar1{__query_shard__=\\\"1_of_3\\\",baz=\\\"blip\\\"}[1m]))\"},{\"Expr\":\"sum(rate(bar1{__query_shard__=\\\"2_of_3\\\",baz=\\\"blip\\\"}[1m]))\"},{\"Expr\":\"sum(rate(bar1{__query_shard__=\\\"3_of_3\\\",baz=\\\"blip\\\"}[1m]))\"}]}"})`,
		},
	} {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			stats := NewMapperStats()
			summer := NewQueryShardSummer(c.shards, false, EmbeddedQueriesSquasher, log.NewNopLogger(), stats)
			expr, err := parser.ParseExpr(c.input)
			require.Nil(t, err)

			ctx := context.Background()
			res, err := summer.Map(ctx, expr)
			require.Nil(t, err)
			assert.Equal(t, c.shards, stats.GetShardedQueries())
			expected, err := parser.ParseExpr(c.expected)
			require.Nil(t, err)

			require.Equal(t, expected.String(), res.String())
		})
	}
}

func TestIsSubqueryCall(t *testing.T) {
	tests := []struct {
		query    string
		expected bool
	}{
		{
			query:    `time()`,
			expected: false,
		}, {
			query:    `rate(metric[1m])`,
			expected: false,
		}, {
			query:    `rate(metric[1h:5m])`,
			expected: true,
		}, {
			query:    `quantile_over_time(1, metric[1h:5m])`,
			expected: true,
		}, {
			// Parenthesis expression between sum_over_time() and the subquery.
			query:    `sum_over_time((metric_counter[30m:5s]))`,
			expected: true,
		},
		{
			// Multiple parenthesis expressions between sum_over_time() and the subquery.
			query:    `sum_over_time((((metric_counter[30m:5s]))))`,
			expected: true,
		},
	}

	for _, testData := range tests {
		t.Run(testData.query, func(t *testing.T) {
			expr, err := parser.ParseExpr(testData.query)
			require.NoError(t, err)

			call, ok := expr.(*parser.Call)
			require.True(t, ok)
			assert.Equal(t, testData.expected, isSubqueryCall(call))
		})
	}
}
