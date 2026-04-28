// SPDX-License-Identifier: AGPL-3.0-only

package ast_test

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

// These test cases are to be used in both TestPropagateMatchers and TestPropagateMatchersWithData, so the metrics and labels should be the same as the ones in the data loaded in TestPropagateMatchersWithData.
var testCasesPropagateMatchersWithData = map[string]string{
	`up`:                                 `up`,
	`up{foo="bar"}`:                      `up{foo="bar"}`,
	`up * on(foo) group_left down`:       `up * on(foo) group_left down`,
	`up * ignoring(foo) group_left down`: `up * ignoring(foo) group_left down`,
	`up{foo="bar"} * 1`:                  `up{foo="bar"} * 1`,
	`up * down`:                          `up * down`,
	`up{foo="bar"} * down{foo="bar"}`:    `up{foo="bar"} * down{foo="bar"}`,
	`up{foo="bar"} * down`:               `up{foo="bar"} * down{foo="bar"}`,
	`up * down{foo="bar"}`:               `up{foo="bar"} * down{foo="bar"}`,
	`up{foo="bar"} * down{baz="fob"}`:    `up{foo="bar", baz="fob"} * down{foo="bar", baz="fob"}`,
	`up{foo="bar"} * down{foo!="bar2"}`:  `up{foo="bar", foo!="bar2"} * down{foo="bar", foo!="bar2"}`,
	`up{foo="bar"} * ignoring(foo) down`: `up{foo="bar"} * ignoring(foo) down`,
	`up{foo="bar", baz="fob", boo="far", faf="bob"} * ignoring(foo, baz) down`: `up{foo="bar", baz="fob", boo="far", faf="bob"} * ignoring(foo, baz) down{boo="far", faf="bob"}`,
	`up * ignoring(foo) down{foo="bar"}`:                                       `up * ignoring(foo) down{foo="bar"}`,
	`(up{foo="bar"} * down)`:                                                   `(up{foo="bar"} * down{foo="bar"})`,
	`((((up{foo="bar"} * down))))`:                                             `((((up{foo="bar"} * down{foo="bar"}))))`,
	`((((((up{foo="bar"})) * (((down)))))))`:                                   `((((((up{foo="bar"})) * (((down{foo="bar"})))))))`,
	`up{foo="bar"} * down * 3`:                                                 `up{foo="bar"} * down{foo="bar"} * 3`,
	`up * down{foo="bar"} * 3`:                                                 `up{foo="bar"} * down{foo="bar"} * 3`,
	`(up{foo="bar"} * down) * 3`:                                               `(up{foo="bar"} * down{foo="bar"}) * 3`,
	`(up * down{foo="bar"}) * 3`:                                               `(up{foo="bar"} * down{foo="bar"}) * 3`,
	`3 * (up{foo="bar"} * down)`:                                               `3 * (up{foo="bar"} * down{foo="bar"})`,
	`3 * (up * down{foo="bar"})`:                                               `3 * (up{foo="bar"} * down{foo="bar"})`,
	`3 * up{foo="bar"} * down`:                                                 `3 * up{foo="bar"} * down{foo="bar"}`,
	`3 * up * down{foo="bar"}`:                                                 `3 * up{foo="bar"} * down{foo="bar"}`,
	`up{foo="bar"} * 3 * down`:                                                 `up{foo="bar"} * 3 * down{foo="bar"}`,
	`up * 3 * down{foo="bar"}`:                                                 `up{foo="bar"} * 3 * down{foo="bar"}`,
	`(up{foo="bar"} * 3) * down`:                                               `(up{foo="bar"} * 3) * down{foo="bar"}`,
	`up{foo="bar"} * (3 * down)`:                                               `up{foo="bar"} * (3 * down{foo="bar"})`,
	`(up * 3) * down{foo="bar"}`:                                               `(up{foo="bar"} * 3) * down{foo="bar"}`,
	`up * (3 * down{foo="bar"})`:                                               `up{foo="bar"} * (3 * down{foo="bar"})`,
	`up * on(foo) down{foo="bar", baz="fob"}`:                                  `up{foo="bar"} * on(foo) down{foo="bar", baz="fob"}`,
	`up * on(foo, baz) down{foo="bar", baz="fob", boo="far", faf="bob"}`:       `up{foo="bar", baz="fob"} * on(foo, baz) down{foo="bar", baz="fob", boo="far", faf="bob"}`,
	`up{foo="bar"} * down * left`:                                              `up{foo="bar"} * down{foo="bar"} * left{foo="bar"}`,
	`up * down{foo="bar"} * left`:                                              `up{foo="bar"} * down{foo="bar"} * left{foo="bar"}`,
	`up * down * left{foo="bar"}`:                                              `up{foo="bar"} * down{foo="bar"} * left{foo="bar"}`,
	`vector(3) / up{foo!="bar2"} + down`:                                       `vector(3) / up{foo!="bar2"} + down{foo!="bar2"}`,
	`vector(3) / on(foo) up{foo!="bar2"} + down`:                               `vector(3) / on(foo) up{foo!="bar2"} + down{foo!="bar2"}`,
	`vector(3) / on(foo) up{foo!="bar2"} + ignoring(foo) down`:                 `vector(3) / on(foo) up{foo!="bar2"} + ignoring(foo) down`,
	`left / ignoring(foo) up{foo!="bar2"} + on(foo) down`:                      `left / ignoring(foo) up{foo!="bar2"} + on(foo) down`,
	`left + ignoring(foo) up{foo!="bar2"} / on(foo) down`:                      `left + ignoring(foo) up{foo!="bar2"} / on(foo) down{foo!="bar2"}`,
	`up and vector(3) / down{foo=~"bar"}`:                                      `up{foo=~"bar"} and vector(3) / down{foo=~"bar"}`,
	`up and vector(3) / ignoring(foo) down{foo=~"bar"}`:                        `up{foo=~"bar"} and vector(3) / ignoring(foo) down{foo=~"bar"}`,
	`(up and vector(3)) / ignoring(foo) down{foo=~"bar"}`:                      `(up and vector(3)) / ignoring(foo) down{foo=~"bar"}`,
	`up and vector(3) or down{foo=~"bar"}`:                                     `up and vector(3) or down{foo=~"bar"}`,
	`up{foo="bar"} or down`:                                                    `up{foo="bar"} or down`,
	`up{foo="bar"} unless down`:                                                `up{foo="bar"} unless down{foo="bar"}`,
	`up unless down{foo="bar"}`:                                                `up unless down{foo="bar"}`,
	`up{foo="bar"} unless down{baz="fob"}`:                                     `up{foo="bar"} unless down{foo="bar", baz="fob"}`,
	`{__name__=~"left_side.*"} == ignoring(env) right_side`:                    `{__name__=~"left_side.*"} == ignoring(env) right_side`,
	`abs(up{foo="bar"}) + sin(down) - left{baz="fob"}`:                         `abs(up{foo="bar", baz="fob"}) + sin(down{foo="bar", baz="fob"}) - left{foo="bar", baz="fob"}`,
	`up{foo="bar"} / round(down{baz="fob"}, 1)`:                                `up{foo="bar", baz="fob"} / round(down{foo="bar", baz="fob"}, 1)`,
	`rate(up{foo="bar"}[2m]) + delta(down{baz="fob"}[4m])`:                     `rate(up{foo="bar", baz="fob"}[2m]) + delta(down{foo="bar", baz="fob"}[4m])`,
	`up * -down{foo="bar"}`:                                                    `up{foo="bar"} * -down{foo="bar"}`,
	`up{foo="bar"} @ 1 + down @ 2`:                                             `up{foo="bar"} @ 1 + down{foo="bar"} @ 2`,
	`up{foo="bar"} @ 1 + down`:                                                 `up{foo="bar"} @ 1 + down{foo="bar"}`,
	`minute() * down{baz="fob"}`:                                               `minute() * down{baz="fob"}`,
	`minute(up{foo="bar"}) * down{baz="fob"}`:                                  `minute(up{foo="bar", baz="fob"}) * down{foo="bar", baz="fob"}`,
	`round(up{foo="bar"}) * down{baz="fob"}`:                                   `round(up{foo="bar", baz="fob"}) * down{foo="bar", baz="fob"}`,
	`round(up{foo="bar"}, 1) * down{baz="fob"}`:                                `round(up{foo="bar", baz="fob"}, 1) * down{foo="bar", baz="fob"}`,

	// Subqueries
	`min_over_time(rate(up{foo="bar"}[5m])[30m:1m]) + min_over_time(rate(down[5m])[30m:1m])`: `min_over_time(rate(up{foo="bar"}[5m])[30m:1m]) + min_over_time(rate(down{foo="bar"}[5m])[30m:1m])`,

	// Aggregations
	`sum(up) / sum(down)`:                                                                                  `sum(up) / sum(down)`,
	`sum(up{foo!="bar2"}) / sum(down)`:                                                                     `sum(up{foo!="bar2"}) / sum(down)`,
	`sum by (foo) (up) / sum by (foo) (down)`:                                                              `sum by (foo) (up) / sum by (foo) (down)`,
	`sum by (foo) (up{foo!="bar2"}) / sum by (foo) (down)`:                                                 `sum by (foo) (up{foo!="bar2"}) / sum by (foo) (down{foo!="bar2"})`,
	`sum by (foo) (up) / sum by (foo) (down{boo="far"})`:                                                   `sum by (foo) (up) / sum by (foo) (down{boo="far"})`,
	`sum by (foo) (up{foo!="bar2", boo="far"}) / sum by (foo) (down)`:                                      `sum by (foo) (up{foo!="bar2", boo="far"}) / sum by (foo) (down{foo!="bar2"})`,
	`sum by (foo) (up) / sum(down)`:                                                                        `sum by (foo) (up) / sum(down)`,
	`sum by (foo) (up{foo!="bar2"}) / sum(down)`:                                                           `sum by (foo) (up{foo!="bar2"}) / sum(down)`,
	`sum by (foo) (up{foo!="bar2"}) / sum by (foo) (down{boo="far"})`:                                      `sum by (foo) (up{foo!="bar2"}) / sum by (foo) (down{foo!="bar2", boo="far"})`,
	`sum by (foo) (up{foo!="bar2"}) / sum by (boo) (down{boo="far"})`:                                      `sum by (foo) (up{foo!="bar2"}) / sum by (boo) (down{boo="far"})`,
	`sum by (foo, boo) (up{foo!="bar2"}) / sum by (foo, boo) (down{boo="far"})`:                            `sum by (foo, boo) (up{foo!="bar2", boo="far"}) / sum by (foo, boo) (down{foo!="bar2", boo="far"})`,
	`sum by (foo, boo) (up{foo!="bar2"}) / sum by (foo, soo) (down{boo="far"})`:                            `sum by (foo, boo) (up{foo!="bar2"}) / sum by (foo, soo) (down{foo!="bar2", boo="far"})`,
	`sum by (foo, boo) (up{foo!="bar2"}) / sum by (boo, soo) (down{boo="far"})`:                            `sum by (foo, boo) (up{foo!="bar2", boo="far"}) / sum by (boo, soo) (down{boo="far"})`,
	`sum by (boo) (up) / sum without (foo) (down{foo!="bar2", boo="far"})`:                                 `sum by (boo) (up{boo="far"}) / sum without (foo) (down{foo!="bar2", boo="far"})`,
	`sum without (foo) (up) / sum without (foo) (down{foo!="bar2", boo="far"})`:                            `sum without (foo) (up{boo="far"}) / sum without (foo) (down{foo!="bar2", boo="far"})`,
	`sum without (foo) (up{foo="bar", soo="sar"}) / sum without (foo) (down{foo!="bar2", boo="far"})`:      `sum without (foo) (up{foo="bar", soo="sar", boo="far"}) / sum without (foo) (down{foo!="bar2", boo="far", soo="sar"})`,
	`sum without (soo) (up{foo="bar", soo="sar"}) / sum without (foo) (down{foo!="bar2", boo="far"})`:      `sum without (soo) (up{foo="bar", soo="sar", boo="far"}) / sum without (foo) (down{foo!="bar2", boo="far"})`,
	`sum without (soo, boo) (up{foo="bar", soo="sar"}) / sum without (foo) (down{foo!="bar2", boo="far"})`: `sum without (soo, boo) (up{foo="bar", soo="sar"}) / sum without (foo) (down{foo!="bar2", boo="far"})`,
	`up * on(foo) sum(down{foo="bar", baz="fob"})`:                                                         `up * on(foo) sum(down{foo="bar", baz="fob"})`,
	`up * on(foo) sum by (foo) (down{foo="bar", baz="fob"})`:                                               `up{foo="bar"} * on(foo) sum by (foo) (down{foo="bar", baz="fob"})`,
	`up * on(foo) sum by (baz) (down{foo="bar", baz="fob"})`:                                               `up * on(foo) sum by (baz) (down{foo="bar", baz="fob"})`,
	`up * ignoring(foo) sum by (baz) (down{foo="bar", baz="fob"})`:                                         `up{baz="fob"} * ignoring(foo) sum by (baz) (down{foo="bar", baz="fob"})`,
	`up * ignoring(foo) sum without (baz) (down{foo="bar", baz="fob"})`:                                    `up * ignoring(foo) sum without (baz) (down{foo="bar", baz="fob"})`,
	`up{soo="sar"} * ignoring(foo) sum without (baz) (down{foo="bar", baz="fob"})`:                         `up{soo="sar"} * ignoring(foo) sum without (baz) (down{foo="bar", baz="fob", soo="sar"})`,
	`topk by (foo) (5, up{foo="bar"}) / bottomk by (baz) (5, down{baz="fob"})`:                             `topk by (foo) (5, up{foo="bar"}) / bottomk by (baz) (5, down{baz="fob"})`,
	`topk by (foo) (5, up{foo="bar"}) / bottomk by (foo) (5, down{baz="fob"})`:                             `topk by (foo) (5, up{foo="bar"}) / bottomk by (foo) (5, down{foo="bar", baz="fob"})`,
	`sum by (foo) (abs(up{foo="bar"})) / sum by (foo) (sin(down))`:                                         `sum by (foo) (abs(up{foo="bar"})) / sum by (foo) (sin(down{foo="bar"}))`,
	`max by (baz) (avg by (foo, baz) (up)) / down{foo="bar"}`:                                              `max by (baz) (avg by (foo, baz) (up)) / down{foo="bar"}`,
	`max by (foo) (avg by (foo, baz) (up{foo="bar"})) / down`:                                              `max by (foo) (avg by (foo, baz) (up{foo="bar"})) / down{foo="bar"}`,
	`max by (foo) (avg by (foo, baz) (up)) / down{foo="bar"}`:                                              `max by (foo) (avg by (foo, baz) (up{foo="bar"})) / down{foo="bar"}`,
	`max by (foo, baz) (avg by (foo) (up)) / down{foo="bar"}`:                                              `max by (foo, baz) (avg by (foo) (up{foo="bar"})) / down{foo="bar"}`,
	`max without (foo, baz) (avg without (foo) (up)) / down{foo="bar", baz="fob", soo="sar"}`:              `max without (foo, baz) (avg without (foo) (up{soo="sar"})) / down{foo="bar", baz="fob", soo="sar"}`,
	`max by (foo, baz) (avg without (foo) (up)) / down{foo="bar", baz="fob"}`:                              `max by (foo, baz) (avg without (foo) (up{baz="fob"})) / down{foo="bar", baz="fob"}`,
	`max without (baz) (avg by (foo, baz) (up)) / down{foo="bar", baz="fob"}`:                              `max without (baz) (avg by (foo, baz) (up{foo="bar"})) / down{foo="bar", baz="fob"}`,

	// Conflicting matchers should be allowed to propagate. They'd produce empty results anyway so might as well not fetch the data for the individual selectors.
	`up{foo="bar"} + up{foo="bar2"}`:                                     `up{foo="bar", foo="bar2"} + up{foo="bar", foo="bar2"}`,
	`sum by (foo) (up{foo="bar"}) + sum by (foo) (up{foo="bar2"})`:       `sum by (foo) (up{foo="bar", foo="bar2"}) + sum by (foo) (up{foo="bar", foo="bar2"})`,
	`sum without (x) (up{foo="bar"}) + sum without (x) (up{foo="bar2"})`: `sum without (x) (up{foo="bar", foo="bar2"}) + sum without (x) (up{foo="bar", foo="bar2"})`,
	// Note that they won't propagate when that label is excluded from the grouping.
	`sum by (x) (up{foo="bar"}) + sum by (x) (up{foo="bar2"})`:               `sum by (x) (up{foo="bar"}) + sum by (x) (up{foo="bar2"})`,
	`sum without (foo) (up{foo="bar"}) + sum without (foo) (up{foo="bar2"})`: `sum without (foo) (up{foo="bar"}) + sum without (foo) (up{foo="bar2"})`,

	// Not supported
	`scalar(up{foo="bar"} * down)`:                                       `scalar(up{foo="bar"} * down{foo="bar"})`,
	`scalar(up{foo="bar"}) * down`:                                       `scalar(up{foo="bar"}) * down`,
	`scalar(up) * down{foo="bar"}`:                                       `scalar(up) * down{foo="bar"}`,
	`absent(up{foo="bar99"}) + down{baz="fob"}`:                          `absent(up{foo="bar99"}) + down{baz="fob"}`,
	`absent(up{foo="bar99"}) + absent(down{baz="fob99"})`:                `absent(up{foo="bar99"}) + absent(down{baz="fob99"})`,
	`absent_over_time(up{foo="bar99"}[5m]) + absent_over_time(down[5m])`: `absent_over_time(up{foo="bar99"}[5m]) + absent_over_time(down[5m])`,
	`label_join(up{foo="bar"}, "joint", "-", "boo", "faf") + label_join(down{baz="fob"}, "joint", "-", "boo", "faf")`:       `label_join(up{foo="bar"}, "joint", "-", "boo", "faf") + label_join(down{baz="fob"}, "joint", "-", "boo", "faf")`,
	`label_replace(up{foo="bar"}, "zoo", "$1", "faf", "(.*)") + label_replace(down{baz="fob"}, "zoo", "$1", "faf", "(.*)")`: `label_replace(up{foo="bar"}, "zoo", "$1", "faf", "(.*)") + label_replace(down{baz="fob"}, "zoo", "$1", "faf", "(.*)")`,
	`info(up{foo="bar"}) + info(down{baz="fob"})`:                                 `info(up{foo="bar"}) + info(down{baz="fob"})`,
	`sort(up{foo="bar"}) + sort(down{baz="fob"})`:                                 `sort(up{foo="bar"}) + sort(down{baz="fob"})`,
	`sort_by_label(up{foo="bar"}, "boo") + sort_by_label(down{baz="fob"}, "faf")`: `sort_by_label(up{foo="bar"}, "boo") + sort_by_label(down{baz="fob"}, "faf")`,

	// Ensure matchers are propagated across different kinds of nested expressions.

	// Propagating matchers from the LHS of an "and on" into the RHS, inside a count() without grouping.
	// "soo" is intentionally outside the on() label set so it does not propagate back to the LHS.
	`count(up{foo="bar", baz="fob"} and on (foo, baz, boo, faf) down == 1)`:       `count(up{foo="bar", baz="fob"} and on (foo, baz, boo, faf) down{foo="bar", baz="fob"} == 1)`,
	`count(up{foo="bar", baz="fob"} and on (foo, baz, boo) left{soo="sar"} == 1)`: `count(up{foo="bar", baz="fob"} and on (foo, baz, boo) left{soo="sar", foo="bar", baz="fob"} == 1)`,
	// Single expression where the root is a BinaryExpr.
	`count(up{foo="bar", baz="fob"} and on (foo, baz, boo, faf) down == 1) / count(up{foo="bar", baz="fob"} and on (foo, baz, boo) left{soo="sar"} == 1) == 1`: `count(up{foo="bar", baz="fob"} and on (foo, baz, boo, faf) down{foo="bar", baz="fob"} == 1) / count(up{foo="bar", baz="fob"} and on (foo, baz, boo) left{soo="sar", foo="bar", baz="fob"} == 1) == 1`,
	// Propagation should work inside LOR arms even when or is the root or nested.
	// (Matchers cannot propagate across an `or`, but inner BinaryExprs within each arm should still be processed.)
	`(up{foo="bar"} * down) or left`:                      `(up{foo="bar"} * down{foo="bar"}) or left`,
	`up{foo="bar"} / ((left * down) or right{baz="fob"})`: `up{foo="bar"} / ((left * down) or right{baz="fob"})`,
	// Propagation should work inside unsupported Call args when the Call is nested in a BinaryExpr.
	`scalar(up{foo="bar"} * down) / left`: `scalar(up{foo="bar"} * down{foo="bar"}) / left`,
	`sort(up{foo="bar"} * down) / left`:   `sort(up{foo="bar"} * down{foo="bar"}) / left`,
}

// These test cases are only used in TestPropagateMatchers, so the metrics and labels can be freely chosen independently of the data loaded in TestPropagateMatchersWithData. Running them in TestPropagateMatchersWithData would be pointless as the results would be empty anyway.
var testCasesPropagateMatchersWithoutData = map[string]string{
	// Ensure that internal matchers are not propagated, as they are not actual labels on the data.
	`up{__query_shard__="bar"} + up{__query_shard__="bar2"}`:                                     `up{__query_shard__="bar"} + up{__query_shard__="bar2"}`,
	`sum without (x) (up{__query_shard__="bar"}) + sum without (x) (up{__query_shard__="bar2"})`: `sum without (x) (up{__query_shard__="bar"}) + sum without (x) (up{__query_shard__="bar2"})`,

	// Check more complicated cases based on real world queries.
	`count(kube_pod_container_info{pod=~"a|b|c", namespace="ns"} and on (cluster, namespace, pod, container) kube_pod_container_status_ready == 1)`:                                                                                                                                                 `count(kube_pod_container_info{pod=~"a|b|c", namespace="ns"} and on (cluster, namespace, pod, container) kube_pod_container_status_ready{pod=~"a|b|c", namespace="ns"} == 1)`,
	`count(kube_pod_container_info{pod=~"a|b|c", namespace="ns"} and on (cluster, namespace, pod) kube_pod_status_phase{phase!~"x|y|z"} == 1)`:                                                                                                                                                      `count(kube_pod_container_info{pod=~"a|b|c", namespace="ns"} and on (cluster, namespace, pod) kube_pod_status_phase{phase!~"x|y|z", pod=~"a|b|c", namespace="ns"} == 1)`,
	`count(kube_pod_container_info{pod=~"a|b|c", namespace="ns"} and on (cluster, namespace, pod, container) kube_pod_container_status_ready == 1) / count(kube_pod_container_info{pod=~"a|b|c", namespace="ns"} and on (cluster, namespace, pod) kube_pod_status_phase{phase!~"x|y|z"} == 1) == 1`: `count(kube_pod_container_info{pod=~"a|b|c", namespace="ns"} and on (cluster, namespace, pod, container) kube_pod_container_status_ready{pod=~"a|b|c", namespace="ns"} == 1) / count(kube_pod_container_info{pod=~"a|b|c", namespace="ns"} and on (cluster, namespace, pod) kube_pod_status_phase{phase!~"x|y|z", pod=~"a|b|c", namespace="ns"} == 1) == 1`,
}

// TestPropagateMatchers tests that queries are rewritten as expected, without running it on sample data.
func TestPropagateMatchers(t *testing.T) {
	ctx := context.Background()

	testCasesPropagateMatchers := make(map[string]string)
	for k, v := range testCasesPropagateMatchersWithData {
		testCasesPropagateMatchers[k] = v
	}
	for k, v := range testCasesPropagateMatchersWithoutData {
		testCasesPropagateMatchers[k] = v
	}

	for input, expected := range testCasesPropagateMatchers {
		t.Run(input, func(t *testing.T) {
			// We parse the expressions and compare them as strings below so we don't have to worry about formatting in the test cases.
			expectedExpr, err := promqlext.NewPromQLParser().ParseExpr(expected)
			require.NoError(t, err)

			inputExpr, err := promqlext.NewPromQLParser().ParseExpr(input)
			require.NoError(t, err)
			inputExpr, err = preprocessQuery(t, inputExpr)
			require.NoError(t, err)

			optimizer := ast.NewPropagateMatchersMapper()
			outputExpr, err := optimizer.Map(ctx, inputExpr)
			require.NoError(t, err)

			require.Equal(t, expectedExpr.String(), outputExpr.String())
			require.Equal(t, input != expected, optimizer.HasChanged())
		})
	}
}

// TestPropagateMatchersWithData tests that the rewritten query produces the same results as the original one on sample data.
func TestPropagateMatchersWithData(t *testing.T) {
	testASTOptimizationPassWithData(t, `
		load 1m
			up{foo="bar",baz="fob",boo="far",faf="bob"} 0+1x<num samples>
			down{foo="bar",baz="fob",boo="far",faf="bob"} 0+2x<num samples>
			left{foo="bar",baz="fob",boo="far",faf="bob"} 0+3x<num samples>
			right{foo="bar",baz="fob",boo="far",faf="bob"} 0+4x<num samples>
			up{foo="bar2",baz="fob2",boo="far2",faf="bob2"} 0+5x<num samples>
			down{foo="bar2",baz="fob2",boo="far2",faf="bob2"} 0+6x<num samples>
			left{foo="bar2",baz="fob2",boo="far2",faf="bob2"} 0+7x<num samples>
			right{foo="bar2",baz="fob2",boo="far2",faf="bob2"} 0+8x<num samples>
	`, testCasesPropagateMatchersWithData)
}

func TestFunctionsForVectorSelectorArgumentIndex(t *testing.T) {
	allowedValueTypes := []parser.ValueType{
		parser.ValueTypeVector,
		parser.ValueTypeMatrix,
	}
	for name, f := range parser.Functions {
		t.Run(name, func(t *testing.T) {
			i, err := ast.VectorSelectorArgumentIndex(f.Name)
			require.NoError(t, err)
			if i < 0 {
				return
			}
			require.Contains(t, allowedValueTypes, f.ArgTypes[i])
			require.Contains(t, allowedValueTypes, f.ReturnType)
		})
	}
}
