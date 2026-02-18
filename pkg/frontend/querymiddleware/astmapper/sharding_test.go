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
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

func TestSharding(t *testing.T) {
	const shardCount = 3

	for _, tt := range []struct {
		in                       string
		out                      string
		outWithPreprocessing     string
		expectedShardableQueries int
	}{
		{
			in:                       `quantile(0.9,foo)`,
			out:                      `quantile(0.9,foo)`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `absent(foo)`,
			out:                      `absent(foo)`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `absent(sum(foo))`,
			out:                      `absent(sum(` + concatShards(t, shardCount, `sum(foo{__query_shard__="x_of_y"})`) + `))`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `absent_over_time(foo[1m])`,
			out:                      `absent_over_time(foo[1m])`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`,
			out:                      `histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `sum by (foo) (histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[10m])))`,
			out:                      `sum by (foo) (histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[10m])))`,
			expectedShardableQueries: 0,
		},
		{
			in: `sum by (foo,bar) (min_over_time(bar1{baz="blip"}[1m]))`,
			out: `sum by(foo, bar) (` +
				concatShards(t, shardCount, `sum by (foo,bar) (min_over_time(bar1{__query_shard__="x_of_y",baz="blip"}[1m]))`) +
				`)`,
			expectedShardableQueries: 1,
		},
		{
			// This query is not parallelized because the leg "rate(bar2[1m])" is not aggregated and
			// could result in high cardinality results.
			in:                       `sum(rate(bar1[1m])) or rate(bar2[1m])`,
			out:                      `sum(rate(bar1[1m])) or rate(bar2[1m])`,
			expectedShardableQueries: 0,
		},
		{
			in: "sum(rate(bar1[1m])) or sum(rate(bar2[1m]))",
			out: `sum(` + concatShards(t, shardCount, `sum(rate(bar1{__query_shard__="x_of_y"}[1m]))`) + `)` +
				` or sum(` + concatShards(t, shardCount, `sum(rate(bar2{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardableQueries: 2,
		},
		{
			in: "sum(bar1) or sum(bar2)",
			out: `sum(` + concatShards(t, shardCount, `sum(bar1{__query_shard__="x_of_y"})`) + `)` +
				` or sum(` + concatShards(t, shardCount, `sum(bar2{__query_shard__="x_of_y"})`) + `)`,
			expectedShardableQueries: 2,
		},
		{
			in: "count(bar1) or sum(bar2)",
			out: `sum(` + concatShards(t, shardCount, `count(bar1{__query_shard__="x_of_y"})`) + `)` +
				` or sum(` + concatShards(t, shardCount, `sum(bar2{__query_shard__="x_of_y"})`) + `)`,
			expectedShardableQueries: 2,
		},
		{
			in: "count(bar1) or (sum(bar2) * 0)",
			out: `sum(` + concatShards(t, shardCount, `count(bar1{__query_shard__="x_of_y"})`) + `)` +
				` or (sum(` + concatShards(t, shardCount, `sum(bar2{__query_shard__="x_of_y"})`) + `) * 0)`,
			expectedShardableQueries: 2,
		},
		{
			in:                       "sum(bar2) * 0",
			out:                      `sum(` + concatShards(t, shardCount, `sum(bar2{__query_shard__="x_of_y"})`) + `) * 0`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `histogram_quantile(0.5, sum(rate(cortex_cache_value_size_bytes_bucket[5m])) by (le))`,
			out:                      `histogram_quantile(0.5,sum  by (le) (` + concatShards(t, shardCount, `sum  by (le) (rate(cortex_cache_value_size_bytes_bucket{__query_shard__="x_of_y"}[5m]))`) + `))`,
			expectedShardableQueries: 1,
		},
		{
			in: `sum(
				  count(
				    count(
				      bar1
				    )  by (drive,instance)
				  )  by (instance)
				)`,
			out: `sum(
				count(
				  sum(` + concatShards(t, shardCount, `count(bar1{__query_shard__="x_of_y"})  by (drive,instance)`) + `
				  )  by (drive,instance)
				)  by (instance)
			  )`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `sum(rate(foo[1m]))`,
			out:                      `sum(` + concatShards(t, shardCount, `sum(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in: `count(rate(foo[1m]))`,
			out: `sum(` +
				concatShards(t, shardCount, `count(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `count(up)`,
			out:                      `sum(` + concatShards(t, shardCount, `count(up{__query_shard__="x_of_y"})`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `-count(up)`,
			out:                      `-sum(` + concatShards(t, shardCount, `count(up{__query_shard__="x_of_y"})`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `-up`,
			out:                      `-up`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `avg(count(test))`,
			out:                      `avg(sum(` + concatShards(t, shardCount, `count(test{__query_shard__="x_of_y"})`) + `))`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `count by (foo) (rate(foo[1m]))`,
			out:                      `sum by (foo) (` + concatShards(t, shardCount, `count by (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `count without (foo) (rate(foo[1m]))`,
			out:                      `sum without (foo) (` + concatShards(t, shardCount, `count without (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `max(rate(foo[1m]))`,
			out:                      `max(` + concatShards(t, shardCount, `max(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `max by (foo) (rate(foo[1m]))`,
			out:                      `max by (foo) (` + concatShards(t, shardCount, `max by (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `max without (foo) (rate(foo[1m]))`,
			out:                      `max without (foo) (` + concatShards(t, shardCount, `max without (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `sum by (foo) (rate(foo[1m]))`,
			out:                      `sum by (foo) (` + concatShards(t, shardCount, `sum by  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `sum without (foo) (rate(foo[1m]))`,
			out:                      `sum without (foo) (` + concatShards(t, shardCount, `sum without  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in: `avg without (foo) (rate(foo[1m]))`,
			out: `(sum without (foo) (` + concatShards(t, shardCount, `sum without  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `) ` +
				`/ sum without (foo) (` + concatShards(t, shardCount, `count without  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `))`,
			expectedShardableQueries: 2,
		},
		{
			in: `avg by (foo) (rate(foo[1m]))`,
			out: `(sum by (foo)(` + concatShards(t, shardCount, `sum by  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `) ` +
				`/ sum by (foo) (` + concatShards(t, shardCount, `count by  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `))`,
			expectedShardableQueries: 2,
		},
		{
			in: `avg(rate(foo[1m]))`,
			out: `(sum(` + concatShards(t, shardCount, `sum(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `) / ` +
				`sum(` + concatShards(t, shardCount, `count(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `))`,
			expectedShardableQueries: 2,
		},
		{
			in: `topk(10,avg by (foo)(rate(foo[1m])))`,
			out: `topk(10, (sum by (foo)(` +
				concatShards(t, shardCount, `sum by (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `) / ` +
				`sum by (foo)(` + concatShards(t, shardCount, `count by (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `))` +
				`)`,
			expectedShardableQueries: 2,
		},
		{
			in:                       `min_over_time(metric_counter[5m])`,
			out:                      `min_over_time(metric_counter[5m])`,
			expectedShardableQueries: 0,
		},
		{
			in: `sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series[7d]))`,
			out: `sum by (user, cluster, namespace)(` +
				concatShards(t, shardCount, `sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series{__query_shard__="x_of_y"}[7d]))`) +
				`)`,
			expectedShardableQueries: 1,
		},
		{
			in: `min_over_time(
				sum by(group_1) (
					rate(metric_counter[5m])
				)[10m:2m]
			)`,
			out: `min_over_time(
				sum by(group_1) (
					rate(metric_counter[5m])
				)[10m:2m]
			)`,
			expectedShardableQueries: 0,
		},
		{
			in: `max_over_time(
				stddev_over_time(
					deriv(
						rate(metric_counter[10m])
					[5m:1m])
				[2m:])
			[10m:])`,
			out: concatShards(t, shardCount, `max_over_time(
					stddev_over_time(
						deriv(
							rate(metric_counter{__query_shard__="x_of_y"}[10m])
						[5m:1m])
					[2m:])
				[10m:])`),
			expectedShardableQueries: 1,
		},
		{
			// Parenthesis expression between the parallelizeable function call and the subquery.
			in: `max_over_time((
				stddev_over_time(
					deriv(
						rate(metric_counter[10m])
					[5m:1m])
				[2m:])
			[10m:]))`,
			out: concatShards(t, shardCount, `max_over_time((
					stddev_over_time(
						deriv(
							rate(metric_counter{__query_shard__="x_of_y"}[10m])
						[5m:1m])
					[2m:])
				[10m:]))`),
			// Preprocessing will remove the superfluous parenthesis.
			outWithPreprocessing: concatShards(t, shardCount, `max_over_time(
					stddev_over_time(
						deriv(
							rate(metric_counter{__query_shard__="x_of_y"}[10m])
						[5m:1m])
					[2m:])
				[10m:])`),
			expectedShardableQueries: 1,
		},
		{
			in: `rate(
				sum by(group_1) (
					rate(metric_counter[5m])
				)[10m:]
			)`,
			out: `rate(
				sum by(group_1) (
					rate(metric_counter[5m])
				)[10m:]
			)`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `rate(metric_counter[5m])[10m:]`,
			out:                      `rate(metric_counter[5m])[10m:]`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `absent_over_time(rate(metric_counter[5m])[10m:])`,
			out:                      `absent_over_time(rate(metric_counter[5m])[10m:])`,
			expectedShardableQueries: 0,
		},
		{
			in: `max_over_time(
				stddev_over_time(
					deriv(
						sort(metric_counter)
					[5m:1m])
				[2m:])
			[10m:])`,
			out: `max_over_time(
				stddev_over_time(
					deriv(
						sort(metric_counter)
					[5m:1m])
				[2m:])
			[10m:])`,
			expectedShardableQueries: 0,
		},
		{
			in: `max_over_time(
				absent_over_time(
					deriv(
						rate(metric_counter[1m])
					[5m:1m])
				[2m:])
			[10m:])`,
			out: `max_over_time(
				absent_over_time(
					deriv(
						rate(metric_counter[1m])
					[5m:1m])
				[2m:])
			[10m:])`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `quantile_over_time(0.99, cortex_ingester_active_series[1w])`,
			out:                      `quantile_over_time(0.99, cortex_ingester_active_series{}[1w])`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `ceil(sum by (foo) (rate(cortex_ingester_active_series[1w])))`,
			out:                      `ceil(sum by (foo) (` + concatShards(t, shardCount, `sum by (foo) (rate(cortex_ingester_active_series{__query_shard__="x_of_y"}[1w]))`) + `))`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `ln(bar) - resets(foo[1d])`,
			out:                      `ln(bar) - resets(foo[1d]) `,
			expectedShardableQueries: 0,
		},
		{
			in:                       `predict_linear(foo[10m],3600)`,
			out:                      `predict_linear(foo[10m],3600)`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")`,
			out:                      `label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `ln(exp(label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")))`,
			out:                      `ln(exp(label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")))`,
			expectedShardableQueries: 0,
		},
		{
			in: `ln(
				label_replace(
					sum by (cluster) (up{job="api-server",service="a:c"})
				, "foo", "$1", "service", "(.*):.*")
			)`,
			out: `ln(
				label_replace(
					sum by (cluster) ( ` +
				concatShards(t, shardCount, `sum by (cluster) (up{__query_shard__="x_of_y",job="api-server",service="a:c"})`) + `)
				, "foo", "$1", "service", "(.*):.*")
			)`,
			expectedShardableQueries: 1,
		},
		{
			in:  `sum by (job)(rate(http_requests_total[1h] @ end()))`,
			out: `sum by (job)(` + concatShards(t, shardCount, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] @ end()))`) + `)`,
			// Preprocessing will replace 'end()' with the actual end timestamp in the input, so the output will have this too.
			outWithPreprocessing:     `sum by (job)(` + concatShards(t, shardCount, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] @ 301.000))`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `sum by (job)(rate(http_requests_total[1h] offset 1w @ 10))`,
			out:                      `sum by (job)(` + concatShards(t, shardCount, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] offset 1w @ 10))`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `sum by (job)(rate(http_requests_total[1h] offset 1w @ 10)) / 2`,
			out:                      `sum by (job)(` + concatShards(t, shardCount, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] offset 1w @ 10))`) + `) / 2`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `sum by (job)(rate(http_requests_total[1h] offset 1w @ 10)) / 2 ^ 2`,
			out:                      `sum by (job)(` + concatShards(t, shardCount, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] offset 1w @ 10))`) + `) / 2 ^ 2`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `sum by (group_1) (rate(metric_counter[1m]) / time() * 2)`,
			out:                      `sum by (group_1) (` + concatShards(t, shardCount, `sum by (group_1) (rate(metric_counter{__query_shard__="x_of_y"}[1m]) / time() * 2)`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `sum by (group_1) (rate(metric_counter[1m])) / time() *2`,
			out:                      `sum by (group_1) (` + concatShards(t, shardCount, `sum by (group_1) (rate(metric_counter{__query_shard__="x_of_y"}[1m]))`) + `) / time() *2`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `sum by (group_1) (rate(metric_counter[1m])) / time()`,
			out:                      `sum by (group_1) (` + concatShards(t, shardCount, `sum by (group_1) (rate(metric_counter{__query_shard__="x_of_y"}[1m]))`) + `) / time()`,
			expectedShardableQueries: 1,
		},
		{
			in: `sum by (group_1) (rate(metric_counter[1m])) / vector(3) ^ month()`,
			out: `sum by (group_1) (` + concatShards(t, shardCount, `sum by (group_1) (rate(metric_counter{__query_shard__="x_of_y"}[1m]))`) + `)` +
				`/ vector(3) ^ month()`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `vector(3) ^ month()`,
			out:                      `vector(3) ^ month()`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `month()`,
			out:                      `month()`,
			expectedShardableQueries: 0,
		},
		{
			// We could shard month(foo), but since it doesn't reduce the data set, we don't.
			in:                       `month(foo)`,
			out:                      `month(foo)`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `month(sum(foo))`,
			out:                      `month(sum(` + concatShards(t, shardCount, `sum(foo{__query_shard__="x_of_y"})`) + `))`,
			expectedShardableQueries: 1,
		},
		{
			// This query is not parallelized because the leg "year(foo)" could result in high cardinality results.
			in:                       `sum(rate(metric_counter[1m])) / vector(3) ^ year(foo)`,
			out:                      `sum(rate(metric_counter[1m])) / vector(3) ^ year(foo)`,
			expectedShardableQueries: 0,
		},
		{
			// can't shard foo > bar,
			// because foo{__query_shard__="1_of_3"} won't have the matching labels in bar{__query_shard__="1_of_3"}
			in:                       `foo > bar`,
			out:                      `foo > bar`,
			expectedShardableQueries: 0,
		},
		{
			// we could shard foo * 2, but since it doesn't reduce the data set, we don't.
			in:                       `foo * 2`,
			out:                      `foo * 2`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `foo > 0`,
			out:                      concatShards(t, shardCount, `foo{__query_shard__="x_of_y"} > 0`),
			expectedShardableQueries: 1,
		},
		{
			in:                       `0 <= foo`,
			out:                      concatShards(t, shardCount, `0 <= foo{__query_shard__="x_of_y"}`),
			expectedShardableQueries: 1,
		},
		{
			in:                       `foo > (2 * 2)`,
			out:                      concatShards(t, shardCount, `foo{__query_shard__="x_of_y"} > (2 * 2)`),
			expectedShardableQueries: 1,
		},
		{
			in:                       `sum by (label) (foo > 0)`,
			out:                      `sum by(label) (` + concatShards(t, shardCount, `sum by (label) (foo{__query_shard__="x_of_y"} > 0)`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `sum by (label) (foo > 0) > 0`,
			out:                      `sum by (label) (` + concatShards(t, shardCount, `sum by (label) (foo{__query_shard__="x_of_y"} > 0)`) + `) > 0`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `sum_over_time(foo[1m]) > (2 * 2)`,
			out:                      concatShards(t, shardCount, `sum_over_time(foo{__query_shard__="x_of_y"}[1m]) > (2 * 2)`),
			expectedShardableQueries: 1,
		},
		{
			// This query is not parallelized because the leg "foo" is not aggregated and
			// could result in high cardinality results.
			in:                       `foo > sum(bar)`,
			out:                      `foo > sum(bar)`,
			expectedShardableQueries: 0,
		},
		{
			// This query is not parallelized because the leg "foo" is not aggregated and
			// could result in high cardinality results.
			in:                       `foo > scalar(sum(bar))`,
			out:                      `foo > scalar(sum(bar))`,
			expectedShardableQueries: 0,
		},
		{
			in: `scalar(min(foo)) > bool scalar(sum(bar))`,
			out: `scalar(min(` + concatShards(t, shardCount, `min(foo{__query_shard__="x_of_y"})`) + `)) ` +
				`> bool scalar(sum(` + concatShards(t, shardCount, `sum(bar{__query_shard__="x_of_y"})`) + `))`,
			expectedShardableQueries: 2,
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
		{
			in:                       `vector(scalar(sum(foo)))`,
			out:                      `vector(scalar(sum(` + concatShards(t, shardCount, `sum(foo{__query_shard__="x_of_y"})`) + `)))`,
			expectedShardableQueries: 1,
		},
		{
			in: `
				topk(
					scalar(count(foo)),
					sum by(pod) (rate(bar[5m]))
				)
			`,
			out: `
				topk(
					scalar(sum(` + concatShards(t, shardCount, `count(foo{__query_shard__="x_of_y"})`) + `)),
					sum by(pod) (` + concatShards(t, shardCount, `sum by (pod) (rate(bar{__query_shard__="x_of_y"}[5m]))`) + `)
				)
			`,
			expectedShardableQueries: 2,
		},
		{
			in:                       `scalar(sum(foo))`,
			out:                      `scalar(sum(` + concatShards(t, shardCount, `sum(foo{__query_shard__="x_of_y"})`) + `))`,
			expectedShardableQueries: 1,
		},
		{
			in:                       `sum(bar2 @ 123)`,
			out:                      `sum(` + concatShards(t, shardCount, `sum(bar2{__query_shard__="x_of_y"} @ 123)`) + `)`,
			expectedShardableQueries: 1,
		},
		{
			in: `count(bar1) or (sum(bar2 @ 123))`,
			out: `sum(` + concatShards(t, shardCount, `count(bar1{__query_shard__="x_of_y"})`) + `)` +
				` or (sum(` + concatShards(t, shardCount, `sum(bar2{__query_shard__="x_of_y"} @ 123)`) + `))`,
			expectedShardableQueries: 2,
		},
		{
			in:                       `max_over_time((-absent(foo))[5m:])`,
			out:                      `max_over_time((-absent(foo))[5m:])`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `clamp_max(max(foo), scalar(bar))`,
			out:                      `clamp_max(max(foo), scalar(bar))`,
			expectedShardableQueries: 0,
		},
		{
			in:                       `clamp_max(max(foo), scalar(sum(bar)))`,
			out:                      `clamp_max(max(` + concatShards(t, shardCount, `max(foo{__query_shard__="x_of_y"})`) + `), scalar(sum(` + concatShards(t, shardCount, `sum(bar{__query_shard__="x_of_y"})`) + `)))`,
			expectedShardableQueries: 2,
		},
	} {
		t.Run(tt.in, func(t *testing.T) {
			for _, preprocess := range []bool{true, false} {
				startT := timestamp.Time(1000)
				endT := startT.Add(5 * time.Minute)

				parseInputAndPreprocess := func(t *testing.T) parser.Expr {
					expr, err := promqlext.NewPromQLParser().ParseExpr(tt.in)
					require.NoError(t, err)

					if preprocess {
						expr, err = promql.PreprocessExpr(expr, startT, endT, time.Minute)
						require.NoError(t, err)
					}

					return expr
				}

				out := tt.out
				if preprocess && tt.outWithPreprocessing != "" {
					out = tt.outWithPreprocessing
				}

				t.Run(fmt.Sprintf("preprocess expression=%v", preprocess), func(t *testing.T) {
					t.Run("applying sharding", func(t *testing.T) {
						stats := NewMapperStats()
						summer := NewQueryShardSummer(shardCount, EmbeddedQueriesSquasher, log.NewNopLogger(), stats)
						expr := parseInputAndPreprocess(t)
						expectedExpr, err := promqlext.NewPromQLParser().ParseExpr(out)
						require.NoError(t, err)

						ctx := context.Background()
						mapped, err := summer.Map(ctx, expr)
						require.NoError(t, err)
						require.Equal(t, expectedExpr.String(), mapped.String())
						require.Equal(t, tt.expectedShardableQueries*shardCount, stats.GetShardedQueries())
					})

					t.Run("analyzing", func(t *testing.T) {
						expr := parseInputAndPreprocess(t)
						analyzer := NewShardingAnalyzer(log.NewNopLogger())

						analysisResult, err := analyzer.Analyze(expr)
						require.NoError(t, err)
						hasVectorSelectors := AnyNode(expr, isVectorSelector)
						require.NoError(t, err)

						if hasVectorSelectors {
							require.Equal(t, tt.expectedShardableQueries > 0, analysisResult.WillShardAllSelectors, "Analyze should be true if the expression is shardable, and false otherwise")
							require.Equal(t, tt.expectedShardableQueries, analysisResult.ShardedSelectors)
						} else {
							require.True(t, analysisResult.WillShardAllSelectors, "expression has no selectors")
							require.Equal(t, 0, analysisResult.ShardedSelectors, "expression has no selectors")
						}
					})
				})
			}
		})
	}
}

func concatShards(t *testing.T, shards int, queryTemplate string) string {
	queries := make([]EmbeddedQuery, shards)
	for shard := range queries {
		queryStr := strings.ReplaceAll(queryTemplate, "x_of_y", sharding.FormatShardIDLabelValue(uint64(shard), uint64(shards)))
		expr, err := promqlext.NewPromQLParser().ParseExpr(queryStr)
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
			summer := NewQueryShardSummer(c.shards, EmbeddedQueriesSquasher, log.NewNopLogger(), stats)
			expr, err := promqlext.NewPromQLParser().ParseExpr(c.input)
			require.Nil(t, err)

			ctx := context.Background()
			res, err := summer.Map(ctx, expr)
			require.Nil(t, err)
			assert.Equal(t, c.shards, stats.GetShardedQueries())
			expected, err := promqlext.NewPromQLParser().ParseExpr(c.expected)
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
			expr, err := promqlext.NewPromQLParser().ParseExpr(testData.query)
			require.NoError(t, err)

			call, ok := expr.(*parser.Call)
			require.True(t, ok)
			assert.Equal(t, testData.expected, isSubqueryCall(call))
		})
	}
}
