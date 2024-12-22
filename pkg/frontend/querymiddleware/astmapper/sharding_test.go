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
	for _, tt := range []struct {
		in                     string
		out                    string
		expectedShardedQueries int
		spinOffSubqueries      bool
	}{

		{
			in:                     `max_over_time((max(hello) > max(hello2))[5m:1m])`,
			out:                    `max_over_time(__aggregated_subquery__{__aggregated_subquery__="(max(hello) > max(hello2))[5m:1m]"}[5m])`,
			expectedShardedQueries: 0,
			spinOffSubqueries:      true,
		},
		{
			in:                     `max_over_time((max by (job) (kube_state_metrics_total_shards) * max by (job) (kube_state_metrics_watch_total))[2w:1m])`,
			out:                    `max_over_time(__aggregated_subquery__{__aggregated_subquery__="(max by (job) (kube_state_metrics_total_shards) * max by (job) (kube_state_metrics_watch_total))[2w:1m]"}[2w])`,
			expectedShardedQueries: 0,
			spinOffSubqueries:      true,
		},
		{
			in: "max_over_time(hello[5m:1m]) * " +
				"max_over_time(hello[5m:1m]) / " +
				"(1 - max_over_time(hello[5m:1m]))",
			out: `max_over_time(__aggregated_subquery__{__aggregated_subquery__="hello[5m:1m]"}[5m]) * ` +
				`max_over_time(__aggregated_subquery__{__aggregated_subquery__="hello[5m:1m]"}[5m]) / ` +
				`(1 - max_over_time(__aggregated_subquery__{__aggregated_subquery__="hello[5m:1m]"}[5m]))`,
			expectedShardedQueries: 0,
			spinOffSubqueries:      true,
		},
		{
			in: `1 - grafana_slo_sli_6h{grafana_slo_uuid="q97zal7potjxfz38y6xcs"} > 1 * 0.050000000000000044 and 1 - grafana_slo_sli_3d{grafana_slo_uuid="q97zal7potjxfz38y6xcs"} > 1 * 0.050000000000000044 and 300 * (sum_over_time((grafana_slo_total_rate_5m{grafana_slo_uuid="q97zal7potjxfz38y6xcs"} < 1e+308)[3d:5m]) - sum_over_time((grafana_slo_success_rate_5m{grafana_slo_uuid="q97zal7potjxfz38y6xcs"} < 1e+308)[3d:5m])) > 4`,
			out: concat(`1 - grafana_slo_sli_6h{grafana_slo_uuid="q97zal7potjxfz38y6xcs"} > 1 * 0.050000000000000044 and 1 - grafana_slo_sli_3d{grafana_slo_uuid="q97zal7potjxfz38y6xcs"} > 1 * 0.050000000000000044`) +
				"and 300 * " +
				`(sum_over_time(__aggregated_subquery__{__aggregated_subquery__="(grafana_slo_total_rate_5m{grafana_slo_uuid=\"q97zal7potjxfz38y6xcs\"} < 1e+308)[3d:5m]"}[3d]) - ` +
				`sum_over_time(__aggregated_subquery__{__aggregated_subquery__="(grafana_slo_success_rate_5m{grafana_slo_uuid=\"q97zal7potjxfz38y6xcs\"} < 1e+308)[3d:5m]"}[3d])) > 4`,
			expectedShardedQueries: 0,
			spinOffSubqueries:      true,
		},
		{
			in:                     `min by (asserts_env, asserts_site, namespace) (1 - avg_over_time(((asserts:latency:p99{asserts_request_context!~"/v1/stack/(sanity|vendor-integration)",asserts_request_type=~"inbound|outbound",job="api-server"}) > bool 12)[1d:]))`,
			out:                    `min by (asserts_env,asserts_site,namespace) (1 - avg_over_time(__aggregated_subquery__{__aggregated_subquery__="((asserts:latency:p99{asserts_request_context!~\"/v1/stack/(sanity|vendor-integration)\",asserts_request_type=~\"inbound|outbound\",job=\"api-server\"}) > bool 12)[1d:]"}[1d]))`,
			expectedShardedQueries: 0,
			spinOffSubqueries:      true,
		},
		{
			in:                     `quantile(0.9,foo)`,
			out:                    concat(`quantile(0.9,foo)`),
			expectedShardedQueries: 0,
		},
		{
			in:                     `absent(foo)`,
			out:                    concat(`absent(foo)`),
			expectedShardedQueries: 0,
		},
		{
			in:                     `absent_over_time(foo[1m])`,
			out:                    concat(`absent_over_time(foo[1m])`),
			expectedShardedQueries: 0,
		},
		{

			in: `histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`,
			out: concat(
				`histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`,
			),
			expectedShardedQueries: 0,
		},
		{
			in: `sum by (foo) (histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[10m])))`,
			out: concat(
				`sum by (foo) (histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[10m])))`,
			),
			expectedShardedQueries: 0,
		},
		{
			in: `sum by (foo,bar) (min_over_time(bar1{baz="blip"}[1m]))`,
			out: `sum by(foo, bar) (` +
				concatShards(3, `sum by (foo,bar) (min_over_time(bar1{__query_shard__="x_of_y",baz="blip"}[1m]))`) +
				`)`,
			expectedShardedQueries: 3,
		},
		{
			// This query is not parallelized because the leg "rate(bar2[1m])" is not aggregated and
			// could result in high cardinality results.
			in:                     `sum(rate(bar1[1m])) or rate(bar2[1m])`,
			out:                    concat(`sum(rate(bar1[1m])) or rate(bar2[1m])`),
			expectedShardedQueries: 0,
		},
		{
			in: "sum(rate(bar1[1m])) or sum(rate(bar2[1m]))",
			out: `sum(` + concatShards(3, `sum(rate(bar1{__query_shard__="x_of_y"}[1m]))`) + `)` +
				` or sum(` + concatShards(3, `sum(rate(bar2{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardedQueries: 6,
		},
		{
			in:                     `histogram_quantile(0.5, sum(rate(cortex_cache_value_size_bytes_bucket[5m])) by (le))`,
			out:                    `histogram_quantile(0.5,sum  by (le) (` + concatShards(3, `sum  by (le) (rate(cortex_cache_value_size_bytes_bucket{__query_shard__="x_of_y"}[5m]))`) + `))`,
			expectedShardedQueries: 3,
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
				  sum(` + concatShards(3, `count(bar1{__query_shard__="x_of_y"})  by (drive,instance)`) + `
				  )  by (drive,instance)
				)  by (instance)
			  )`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `sum(rate(foo[1m]))`,
			out:                    `sum(` + concatShards(3, `sum(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardedQueries: 3,
		},
		{
			in: `count(rate(foo[1m]))`,
			out: `sum(` +
				concatShards(3, `count(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `count(up)`,
			out:                    `sum(` + concatShards(3, `count(up{__query_shard__="x_of_y"})`) + `)`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `avg(count(test))`,
			out:                    `avg(sum(` + concatShards(3, `count(test{__query_shard__="x_of_y"})`) + `))`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `count by (foo) (rate(foo[1m]))`,
			out:                    `sum by (foo) (` + concatShards(3, `count by (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `count without (foo) (rate(foo[1m]))`,
			out:                    `sum without (foo) (` + concatShards(3, `count without (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `max(rate(foo[1m]))`,
			out:                    `max(` + concatShards(3, `max(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `max by (foo) (rate(foo[1m]))`,
			out:                    `max by (foo) (` + concatShards(3, `max by (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `max without (foo) (rate(foo[1m]))`,
			out:                    `max without (foo) (` + concatShards(3, `max without (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `sum by (foo) (rate(foo[1m]))`,
			out:                    `sum by (foo) (` + concatShards(3, `sum by  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `sum without (foo) (rate(foo[1m]))`,
			out:                    `sum without (foo) (` + concatShards(3, `sum without  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `)`,
			expectedShardedQueries: 3,
		},
		{
			in: `avg without (foo) (rate(foo[1m]))`,
			out: `(sum without (foo) (` + concatShards(3, `sum without  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `) ` +
				`/ sum without (foo) (` + concatShards(3, `count without  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `))`,
			expectedShardedQueries: 6,
		},
		{
			in: `avg by (foo) (rate(foo[1m]))`,
			out: `(sum by (foo)(` + concatShards(3, `sum by  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `) ` +
				`/ sum by (foo) (` + concatShards(3, `count by  (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `))`,
			expectedShardedQueries: 6,
		},
		{
			in: `avg(rate(foo[1m]))`,
			out: `(sum(` + concatShards(3, `sum(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `) / ` +
				`sum(` + concatShards(3, `count(rate(foo{__query_shard__="x_of_y"}[1m]))`) + `))`,
			expectedShardedQueries: 6,
		},
		{
			in: `topk(10,avg by (foo)(rate(foo[1m])))`,
			out: `topk(10, (sum by (foo)(` +
				concatShards(3, `sum by (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `) / ` +
				`sum by (foo)(` + concatShards(3, `count by (foo) (rate(foo{__query_shard__="x_of_y"}[1m]))`) + `))` +
				`)`,
			expectedShardedQueries: 6,
		},
		{
			in:                     `min_over_time(metric_counter[5m])`,
			out:                    concat(`min_over_time(metric_counter[5m])`),
			expectedShardedQueries: 0,
		},
		{
			in: `sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series[7d]))`,
			out: `sum by (user, cluster, namespace)(` +
				concatShards(3, `sum by (user, cluster, namespace) (quantile_over_time(0.99, cortex_ingester_active_series{__query_shard__="x_of_y"}[7d]))`) +
				`)`,
			expectedShardedQueries: 3,
		},
		{
			in: `min_over_time(
				sum by(group_1) (
					rate(metric_counter[5m])
				)[10m:2m]
			)`,
			out: concat(
				`min_over_time(
					sum by(group_1) (
						rate(metric_counter[5m])
					)[10m:2m]
				)`,
			),
			expectedShardedQueries: 0,
		},
		{
			in: `max_over_time(
				stddev_over_time(
					deriv(
						rate(metric_counter[10m])
					[5m:1m])
				[2m:])
			[10m:])`,
			out: concatShards(3, `max_over_time(
					stddev_over_time(
						deriv(
							rate(metric_counter{__query_shard__="x_of_y"}[10m])
						[5m:1m])
					[2m:])
				[10m:])`),
			expectedShardedQueries: 3,
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
			out: concatShards(3, `max_over_time((
					stddev_over_time(
						deriv(
							rate(metric_counter{__query_shard__="x_of_y"}[10m])
						[5m:1m])
					[2m:])
				[10m:]))`),
			expectedShardedQueries: 3,
		},
		{
			in: `rate(
				sum by(group_1) (
					rate(metric_counter[5m])
				)[10m:]
			)`,
			out: concat(
				`rate(
						sum by(group_1) (
							rate(metric_counter[5m])
						)[10m:]
					)`,
			),
			expectedShardedQueries: 0,
		},
		{
			in: `absent_over_time(rate(metric_counter[5m])[10m:])`,
			out: concat(
				`absent_over_time(rate(metric_counter[5m])[10m:])`,
			),
			expectedShardedQueries: 0,
		},
		{
			in: `max_over_time(
				stddev_over_time(
					deriv(
						sort(metric_counter)
					[5m:1m])
				[2m:])
			[10m:])`,
			out: concat(
				`max_over_time(
					stddev_over_time(
						deriv(
							sort(metric_counter)
						[5m:1m])
					[2m:])
				[10m:])`,
			),
			expectedShardedQueries: 0,
		},
		{
			in: `max_over_time(
				absent_over_time(
					deriv(
						rate(metric_counter[1m])
					[5m:1m])
				[2m:])
			[10m:])`,
			out: concat(
				`max_over_time(
					absent_over_time(
						deriv(
							rate(metric_counter[1m])
						[5m:1m])
					[2m:])
				[10m:])`,
			),
			expectedShardedQueries: 0,
		},
		{
			in: `quantile_over_time(0.99, cortex_ingester_active_series[1w])`,
			out: concat(
				`quantile_over_time(0.99, cortex_ingester_active_series{}[1w])`,
			),
			expectedShardedQueries: 0,
		},
		{
			in:                     `ceil(sum by (foo) (rate(cortex_ingester_active_series[1w])))`,
			out:                    `ceil(sum by (foo) (` + concatShards(3, `sum by (foo) (rate(cortex_ingester_active_series{__query_shard__="x_of_y"}[1w]))`) + `))`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `ln(bar) - resets(foo[1d])`,
			out:                    concat(`ln(bar) - resets(foo[1d]) `),
			expectedShardedQueries: 0,
		},
		{
			in:                     `predict_linear(foo[10m],3600)`,
			out:                    concat(`predict_linear(foo[10m],3600)`),
			expectedShardedQueries: 0,
		},
		{
			in:                     `label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")`,
			out:                    concat(`label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")`),
			expectedShardedQueries: 0,
		},
		{
			in:                     `ln(exp(label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")))`,
			out:                    concat(`ln(exp(label_replace(up{job="api-server",service="a:c"}, "foo", "$1", "service", "(.*):.*")))`),
			expectedShardedQueries: 0,
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
				concatShards(3, `sum by (cluster) (up{__query_shard__="x_of_y",job="api-server",service="a:c"})`) + `)
				, "foo", "$1", "service", "(.*):.*")
			)`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `sum by (job)(rate(http_requests_total[1h] @ end()))`,
			out:                    `sum by (job)(` + concatShards(3, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] @ end()))`) + `)`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `sum by (job)(rate(http_requests_total[1h] offset 1w @ 10))`,
			out:                    `sum by (job)(` + concatShards(3, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] offset 1w @ 10))`) + `)`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `sum by (job)(rate(http_requests_total[1h] offset 1w @ 10)) / 2`,
			out:                    `sum by (job)(` + concatShards(3, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] offset 1w @ 10))`) + `) / 2`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `sum by (job)(rate(http_requests_total[1h] offset 1w @ 10)) / 2 ^ 2`,
			out:                    `sum by (job)(` + concatShards(3, `sum by (job)(rate(http_requests_total{__query_shard__="x_of_y"}[1h] offset 1w @ 10))`) + `) / 2 ^ 2`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `sum by (group_1) (rate(metric_counter[1m]) / time() * 2)`,
			out:                    `sum by (group_1) (` + concatShards(3, `sum by (group_1) (rate(metric_counter{__query_shard__="x_of_y"}[1m]) / time() * 2)`) + `)`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `sum by (group_1) (rate(metric_counter[1m])) / time() *2`,
			out:                    `sum by (group_1) (` + concatShards(3, `sum by (group_1) (rate(metric_counter{__query_shard__="x_of_y"}[1m]))`) + `) / time() *2`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `sum by (group_1) (rate(metric_counter[1m])) / time()`,
			out:                    `sum by (group_1) (` + concatShards(3, `sum by (group_1) (rate(metric_counter{__query_shard__="x_of_y"}[1m]))`) + `) / time()`,
			expectedShardedQueries: 3,
		},
		{
			in: `sum by (group_1) (rate(metric_counter[1m])) / vector(3) ^ month()`,
			out: `sum by (group_1) (` + concatShards(3, `sum by (group_1) (rate(metric_counter{__query_shard__="x_of_y"}[1m]))`) + `)` +
				`/ vector(3) ^ month()`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `vector(3) ^ month()`,
			out:                    `vector(3) ^ month()`,
			expectedShardedQueries: 0,
		},
		{
			// This query is not parallelized because the leg "year(foo)" could result in high cardinality results.
			in:                     `sum(rate(metric_counter[1m])) / vector(3) ^ year(foo)`,
			out:                    concat(`sum(rate(metric_counter[1m])) / vector(3) ^ year(foo)`),
			expectedShardedQueries: 0,
		},
		{
			// can't shard foo > bar,
			// because foo{__query_shard__="1_of_3"} won't have the matching labels in bar{__query_shard__="1_of_3"}
			in:                     `foo > bar`,
			out:                    concat(`foo > bar`),
			expectedShardedQueries: 0,
		},
		{
			// we could shard foo * 2, but since it doesn't reduce the data set, we don't.
			in:                     `foo * 2`,
			out:                    concat(`foo * 2`),
			expectedShardedQueries: 0,
		},
		{
			in:                     `foo > 0`,
			out:                    concatShards(3, `foo{__query_shard__="x_of_y"} > 0`),
			expectedShardedQueries: 3,
		},
		{
			in:                     `0 <= foo`,
			out:                    concatShards(3, `0 <= foo{__query_shard__="x_of_y"}`),
			expectedShardedQueries: 3,
		},
		{
			in:                     `foo > (2 * 2)`,
			out:                    concatShards(3, `foo{__query_shard__="x_of_y"} > (2 * 2)`),
			expectedShardedQueries: 3,
		},
		{
			in:                     `sum by (label) (foo > 0)`,
			out:                    `sum by(label) (` + concatShards(3, `sum by (label) (foo{__query_shard__="x_of_y"} > 0)`) + `)`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `sum by (label) (foo > 0) > 0`,
			out:                    `sum by (label) (` + concatShards(3, `sum by (label) (foo{__query_shard__="x_of_y"} > 0)`) + `) > 0`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `sum_over_time(foo[1m]) > (2 * 2)`,
			out:                    concatShards(3, `sum_over_time(foo{__query_shard__="x_of_y"}[1m]) > (2 * 2)`),
			expectedShardedQueries: 3,
		},
		{
			// This query is not parallelized because the leg "foo" is not aggregated and
			// could result in high cardinality results.
			in:                     `foo > sum(bar)`,
			out:                    concat(`foo > sum(bar)`),
			expectedShardedQueries: 0,
		},
		{
			// This query is not parallelized because the leg "foo" is not aggregated and
			// could result in high cardinality results.
			in:                     `foo > scalar(sum(bar))`,
			out:                    concat(`foo > scalar(sum(bar))`),
			expectedShardedQueries: 0,
		},
		{
			in: `scalar(min(foo)) > bool scalar(sum(bar))`,
			out: `scalar(min(` + concatShards(3, `min(foo{__query_shard__="x_of_y"})`) + `)) ` +
				`> bool scalar(sum(` + concatShards(3, `sum(bar{__query_shard__="x_of_y"})`) + `))`,
			expectedShardedQueries: 6,
		},
		{
			// This query is not parallelized because the leg "foo" is not aggregated and
			// could result in high cardinality results.
			in:                     `foo * on(a, b) group_left(c) avg by(a, b, c) (bar)`,
			out:                    concat(`foo * on(a, b) group_left(c) avg by(a, b, c) (bar)`),
			expectedShardedQueries: 0,
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
		{
			// This query is not parallelized because the leg "pod:container_cpu_usage:sum" is not aggregated and
			// could result in high cardinality results.
			in: `max by(pod) (
		            max without(prometheus_replica, instance, node) (kube_pod_labels{namespace="test"})
		            *
		            on(cluster, pod, namespace) group_right() pod:container_cpu_usage:sum
		    )`,
			out: concat(`max by(pod) (
		            max without(prometheus_replica, instance, node) (kube_pod_labels{namespace="test"})
		            *
		            on(cluster, pod, namespace) group_right() pod:container_cpu_usage:sum
		    )`),
			expectedShardedQueries: 0,
		},
		{
			in:                     `sum(rate(metric[1m])) and max(metric) > 0`,
			out:                    `sum(` + concatShards(3, `sum(rate(metric{__query_shard__="x_of_y"}[1m]))`) + `) and max(` + concatShards(3, `max(metric{__query_shard__="x_of_y"})`) + `) > 0`,
			expectedShardedQueries: 6,
		},
		{
			in:                     `sum(rate(metric[1m])) > avg(rate(metric[1m]))`,
			out:                    `sum(` + concatShards(3, `sum(rate(metric{__query_shard__="x_of_y"}[1m]))`) + `) > (sum(` + concatShards(3, `sum(rate(metric{__query_shard__="x_of_y"}[1m]))`) + `) / sum(` + concatShards(3, `count(rate(metric{__query_shard__="x_of_y"}[1m]))`) + `))`,
			expectedShardedQueries: 9,
		},
		{
			in:                     `group by (a, b) (metric)`,
			out:                    `group by (a, b) (` + concatShards(3, `group by (a, b) (metric{__query_shard__="x_of_y"})`) + `)`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `count by (a) (group by (a, b) (metric))`,
			out:                    `count by (a) (group by (a, b) (` + concatShards(3, `group by (a, b) (metric{__query_shard__="x_of_y"})`) + `))`,
			expectedShardedQueries: 3,
		},
		{
			in:                     `count(group without () ({namespace="foo"}))`,
			out:                    `count(group without() (` + concatShards(3, `group without() ({namespace="foo",__query_shard__="x_of_y"})`) + `))`,
			expectedShardedQueries: 3,
		},
	} {
		tt := tt

		t.Run(tt.in, func(t *testing.T) {
			stats := NewMapperStats()
			summer, err := NewQueryShardSummer(context.Background(), 3, VectorSquasher, log.NewNopLogger(), stats)
			require.NoError(t, err)

			var subqueryMapper ASTMapper
			if tt.spinOffSubqueries {
				subqueryMapper = NewSubqueryMapper(stats)
			}

			mapper := NewSharding(summer, subqueryMapper)
			expr, err := parser.ParseExpr(tt.in)
			require.NoError(t, err)
			out, err := parser.ParseExpr(tt.out)
			require.NoError(t, err, tt.out)

			mapped, err := mapper.Map(expr)
			require.NoError(t, err)
			require.Equal(t, out.String(), mapped.String())
			assert.Equal(t, tt.expectedShardedQueries, stats.GetShardedQueries())
		})
	}
}

func concatShards(shards int, queryTemplate string) string {
	queries := make([]EmbeddedQuery, shards)
	for shard := range queries {
		queryStr := strings.ReplaceAll(queryTemplate, "x_of_y", sharding.FormatShardIDLabelValue(uint64(shard), uint64(shards)))
		queries[shard] = NewEmbeddedQuery(queryStr, nil)
	}
	return concatInner(queries...)
}

func concat(queryStrs ...string) string {
	queries := make([]EmbeddedQuery, len(queryStrs))
	for i, query := range queryStrs {
		queries[i] = NewEmbeddedQuery(query, nil)
	}
	return concatInner(queries...)
}

func concatInner(rawQueries ...EmbeddedQuery) string {
	queries := make([]EmbeddedQuery, len(rawQueries))
	for i, q := range rawQueries {
		n, err := parser.ParseExpr(q.Expr)
		if err != nil {
			panic(err)
		}
		queries[i] = NewEmbeddedQuery(n.String(), q.Params)
	}
	mapped, err := VectorSquasher(queries...)
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
			summer, err := NewQueryShardSummer(context.Background(), c.shards, VectorSquasher, log.NewNopLogger(), stats)
			require.Nil(t, err)
			expr, err := parser.ParseExpr(c.input)
			require.Nil(t, err)

			res, err := summer.Map(expr)
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
