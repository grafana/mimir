// SPDX-License-Identifier: AGPL-3.0-only

// Different test package name to break import cycle
package plan_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

const expectedMetricsTemplate = `
	# HELP cortex_mimir_query_engine_narrow_selectors_attempted_total Total number of queries that the optimization pass has attempted to add hints to narrow selectors for.
    # TYPE cortex_mimir_query_engine_narrow_selectors_attempted_total counter
    cortex_mimir_query_engine_narrow_selectors_attempted_total %d
    # HELP cortex_mimir_query_engine_narrow_selectors_modified_total Total number of queries where the optimization pass has added hints to narrow selectors for. Incremented whenever any binary expression in the query receives either on-matching include hints or exclude hints.
    # TYPE cortex_mimir_query_engine_narrow_selectors_modified_total counter
    cortex_mimir_query_engine_narrow_selectors_modified_total %d
`

func TestNarrowSelectorsOptimizationPass(t *testing.T) {
	testCases := map[string]struct {
		expr             string
		expectedPlan     string
		expectedAttempts int
		expectedModified int
	}{
		"raw vector selector": {
			expr: `some_metric`,
			expectedPlan: `
				- VectorSelector: {__name__="some_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression raw vector selectors": {
			expr: `some_metric + some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS, hints exclude ()
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression on raw vector selectors": {
			expr: `some_metric + on (cluster) some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + on (cluster) RHS, hints include (cluster)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"nested binary expression on raw vector selectors": {
			expr: `some_metric + (some_other_metric / on (cluster) some_third_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS, hints exclude ()
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS / on (cluster) RHS, hints include (cluster)
						- LHS: VectorSelector: {__name__="some_other_metric"}
						- RHS: VectorSelector: {__name__="some_third_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression aggregation LHS raw vector selector RHS": {
			expr: `sum by (region) (some_metric) / some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints include (region)
					- LHS: AggregateExpression: sum by (region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression aggregation LHS aggregation RHS": {
			expr: `sum by (region) (some_metric) / sum(some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints include (region)
					- LHS: AggregateExpression: sum by (region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum
						- VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression aggregation LHS aggregation RHS aggregation": {
			expr: `sum by (region) (some_metric) / sum by (region) (some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints include (region)
					- LHS: AggregateExpression: sum by (region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum by (region)
						- VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression multiple aggregation LHS aggregation RHS": {
			expr: `sum by (region, env) (some_metric) / sum(some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints include (region, env)
					- LHS: AggregateExpression: sum by (region, env)
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum
						- VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression multiple aggregation LHS aggregation RHS aggregation": {
			expr: `sum by (region, env) (some_metric) / sum by (region, env) (some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints include (region, env)
					- LHS: AggregateExpression: sum by (region, env)
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum by (region, env)
						- VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression multiple aggregation LHS aggregation RHS aggregation different labels": {
			expr: `sum by (region, env) (some_metric) / sum by (region, cluster) (some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints include (region, env)
					- LHS: AggregateExpression: sum by (region, env)
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum by (region, cluster)
						- VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression on multiple aggregation LHS aggregation RHS aggregation": {
			expr: `sum by (region, env) (some_metric) / on(region) sum(some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (region) RHS, hints include (region)
					- LHS: AggregateExpression: sum by (region, env)
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum
						- VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression aggregation LHS aggregation and nested binary expression RHS": {
			expr: `sum by (region) (some_metric) / (sum(some_other_metric) + sum(some_third_metric))`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints include (region)
					- LHS: AggregateExpression: sum by (region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS + RHS, hints exclude ()
						- LHS: AggregateExpression: sum
							- VectorSelector: {__name__="some_other_metric"}
						- RHS: AggregateExpression: sum
							- VectorSelector: {__name__="some_third_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression aggregation LHS aggregation and nested binary expression RHS aggregation": {
			expr: `sum by (region) (some_metric) / (sum by (cluster) (some_other_metric) + sum(some_third_metric))`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints include (region)
					- LHS: AggregateExpression: sum by (region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS + RHS, hints include (cluster)
						- LHS: AggregateExpression: sum by (cluster)
							- VectorSelector: {__name__="some_other_metric"}
						- RHS: AggregateExpression: sum
							- VectorSelector: {__name__="some_third_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression LHS aggregation RHS aggregation": {
			expr: `sum(some_metric) / sum by (cluster) (some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints exclude ()
					- LHS: AggregateExpression: sum
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum by (cluster)
						- VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression with no selectors": {
			expr: `vector(1) + vector(0)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: FunctionCall: vector(...)
						- NumberLiteral: 1
					- RHS: FunctionCall: vector(...)
						- NumberLiteral: 0
			`,
			expectedAttempts: 0,
			expectedModified: 0,
		},
		"binary expression on with no selectors": {
			expr: `vector(1) + on (region) vector(0)`,
			expectedPlan: `
				- BinaryExpression: LHS + on (region) RHS
					- LHS: FunctionCall: vector(...)
						- NumberLiteral: 1
					- RHS: FunctionCall: vector(...)
						- NumberLiteral: 0
			`,
			expectedAttempts: 0,
			expectedModified: 0,
		},
		// Make sure we don't modify query plans that have been rewritten to be sharded
		"binary expression that has been sharded": {
			expr: `sum by (container) (__embedded_queries__{__queries__="something"}) / sum by (container) (__embedded_queries__{__queries__="something else"})`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS
					- LHS: AggregateExpression: sum by (container)
						- VectorSelector: {__queries__="something", __name__="__embedded_queries__"}
					- RHS: AggregateExpression: sum by (container)
						- VectorSelector: {__queries__="something else", __name__="__embedded_queries__"}
			`,
			expectedAttempts: 0,
			expectedModified: 0,
		},
		// Make sure we don't modify query plans that don't have a binary expression
		"aggregation and function call": {
			expr: `sum by (route) (rate(some_metric[5m]))`,
			expectedPlan: `
				- AggregateExpression: sum by (route)
					- DeduplicateAndMerge
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__="some_metric"}[5m0s]
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		// Make sure we don't modify query plans that modify labels
		"binary expression with label_replace on one side": {
			// statefulset is synthesised by label_replace on the RHS, so it is added to
			// Exclude to prevent incorrect matchers from being pushed to the RHS selector.
			expr: `sum by (statefulset) (kube_statefulset_replicas) - sum by (statefulset) (label_replace(not_ready, "statefulset", "$1", "job", ".+/(.+)"))`,
			expectedPlan: `
				- BinaryExpression: LHS - RHS, hints exclude (statefulset)
					- LHS: AggregateExpression: sum by (statefulset)
						- VectorSelector: {__name__="kube_statefulset_replicas"}
					- RHS: AggregateExpression: sum by (statefulset)
						- DeduplicateAndMerge
							- FunctionCall: label_replace(...)
								- param 0: VectorSelector: {__name__="not_ready"}
								- param 1: StringLiteral: "statefulset"
								- param 2: StringLiteral: "$1"
								- param 3: StringLiteral: "job"
								- param 4: StringLiteral: ".+/(.+)"
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression with label_join on one side": {
			// statefulset is synthesised by label_join on the RHS, so it is added to
			// Exclude to prevent incorrect matchers from being pushed to the RHS selector.
			expr: `sum by (statefulset) (kube_statefulset_replicas) - sum by (statefulset) (label_join(not_ready, "statefulset", "job", "workload"))`,
			expectedPlan: `
				- BinaryExpression: LHS - RHS, hints exclude (statefulset)
					- LHS: AggregateExpression: sum by (statefulset)
						- VectorSelector: {__name__="kube_statefulset_replicas"}
					- RHS: AggregateExpression: sum by (statefulset)
						- DeduplicateAndMerge
							- FunctionCall: label_join(...)
								- param 0: VectorSelector: {__name__="not_ready"}
								- param 1: StringLiteral: "statefulset"
								- param 2: StringLiteral: "job"
								- param 3: StringLiteral: "workload"
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression with label_replace on one side for non-hint label": {
			// region is synthesised by label_replace on the RHS, so it is excluded from the
			// LHS aggregation labels. The remaining label (env) is used as an Include hint.
			expr: `sum by (env, region) (first_metric) - sum by (env, region) (label_replace(second_metric, "region", "$1", "job", ".+/(.+)"))`,
			expectedPlan: `
				- BinaryExpression: LHS - RHS, hints include (env)
					- LHS: AggregateExpression: sum by (env, region)
						- VectorSelector: {__name__="first_metric"}
					- RHS: AggregateExpression: sum by (env, region)
						- DeduplicateAndMerge
							- FunctionCall: label_replace(...)
								- param 0: VectorSelector: {__name__="second_metric"}
								- param 1: StringLiteral: "region"
								- param 2: StringLiteral: "$1"
								- param 3: StringLiteral: "job"
								- param 4: StringLiteral: ".+/(.+)"
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression with label_join on one side for non-hint label": {
			// region is synthesised by label_join on the RHS, so it is excluded from the
			// LHS aggregation labels. The remaining label (env) is used as an Include hint.
			expr: `sum by (env, region) (first_metric) - sum by (env, region) (label_join(second_metric, "region", "job", "workload"))`,
			expectedPlan: `
				- BinaryExpression: LHS - RHS, hints include (env)
					- LHS: AggregateExpression: sum by (env, region)
						- VectorSelector: {__name__="first_metric"}
					- RHS: AggregateExpression: sum by (env, region)
						- DeduplicateAndMerge
							- FunctionCall: label_join(...)
								- param 0: VectorSelector: {__name__="second_metric"}
								- param 1: StringLiteral: "region"
								- param 2: StringLiteral: "job"
								- param 3: StringLiteral: "workload"
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression with histogram_quantiles on one side for non-hint label": {
			// q is synthesised by histogram_quantiles on the RHS, so it is excluded from the
			// LHS aggregation labels. The remaining label (env) is used as an Include hint.
			expr: `sum by (env, q) (first_metric) - sum by (env, q) (histogram_quantiles(second_metric, "q", 0.5))`,
			expectedPlan: `
				- BinaryExpression: LHS - RHS, hints include (env)
					- LHS: AggregateExpression: sum by (env, q)
						- VectorSelector: {__name__="first_metric"}
					- RHS: AggregateExpression: sum by (env, q)
						- DeduplicateAndMerge
							- FunctionCall: histogram_quantiles(...)
								- param 0: VectorSelector: {__name__="second_metric"}
								- param 1: StringLiteral: "q"
								- param 2: NumberLiteral: 0.5
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"multiple binary expressions with label_replace on only a single one": {
			expr: `(first_metric * on (env, region) second_metric) * on (env, region) label_replace(third_metric, "region", "$1", "cluster", ".*")`,
			expectedPlan: `
				- BinaryExpression: LHS * on (env, region) RHS, hints include (env)
					- LHS: BinaryExpression: LHS * on (env, region) RHS, hints include (env, region)
						- LHS: VectorSelector: {__name__="first_metric"}
						- RHS: VectorSelector: {__name__="second_metric"}
					- RHS: DeduplicateAndMerge
						- FunctionCall: label_replace(...)
							- param 0: VectorSelector: {__name__="third_metric"}
							- param 1: StringLiteral: "region"
							- param 2: StringLiteral: "$1"
							- param 3: StringLiteral: "cluster"
							- param 4: StringLiteral: ".*"
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"multiple binary expressions with label_join on only a single one": {
			expr: `(sum by (env, region)(rate(first_metric[5m])) * sum(rate(second_metric[5m]))) / on (env, region) label_join(rate(third_metric[5m]), "region", "job", "workload")`,
			expectedPlan: `
				- BinaryExpression: LHS / on (env, region) RHS, hints include (env)
					- LHS: BinaryExpression: LHS * RHS, hints include (env, region)
						- LHS: AggregateExpression: sum by (env, region)
							- DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="first_metric"}[5m0s]
						- RHS: AggregateExpression: sum
							- DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="second_metric"}[5m0s]
					- RHS: DeduplicateAndMerge
						- FunctionCall: label_join(...)
							- param 0: DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="third_metric"}[5m0s]
							- param 1: StringLiteral: "region"
							- param 2: StringLiteral: "job"
							- param 3: StringLiteral: "workload"
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"multiple binary expressions with label_replace on only a single one on LHS": {
			expr: `label_replace(first_metric, "region", "$1", "cluster", ".*") * on (env, region) (second_metric * on (env, region) third_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS * on (env, region) RHS, hints include (env)
					- LHS: DeduplicateAndMerge
						- FunctionCall: label_replace(...)
							- param 0: VectorSelector: {__name__="first_metric"}
							- param 1: StringLiteral: "region"
							- param 2: StringLiteral: "$1"
							- param 3: StringLiteral: "cluster"
							- param 4: StringLiteral: ".*"
					- RHS: BinaryExpression: LHS * on (env, region) RHS, hints include (env, region)
						- LHS: VectorSelector: {__name__="second_metric"}
						- RHS: VectorSelector: {__name__="third_metric"}

			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"multiple binary expressions with nested label_replace on LHS": {
			expr: `
				label_replace(label_replace(first_metric, "region", "$1", "cluster", ".*"), "env", "$1", "deployment", ".*")
				* on (env, region)
				(second_metric * on (env, region) third_metric)
			`,
			expectedPlan: `
				- BinaryExpression: LHS * on (env, region) RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: label_replace(...)
							- param 0: DeduplicateAndMerge
								- FunctionCall: label_replace(...)
									- param 0: VectorSelector: {__name__="first_metric"}
									- param 1: StringLiteral: "region"
									- param 2: StringLiteral: "$1"
									- param 3: StringLiteral: "cluster"
									- param 4: StringLiteral: ".*"
							- param 1: StringLiteral: "env"
							- param 2: StringLiteral: "$1"
							- param 3: StringLiteral: "deployment"
							- param 4: StringLiteral: ".*"
					- RHS: BinaryExpression: LHS * on (env, region) RHS, hints include (env, region)
						- LHS: VectorSelector: {__name__="second_metric"}
						- RHS: VectorSelector: {__name__="third_metric"}

			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"logical or binary expression should not have hints added": {
			expr: `
				first_metric
				or on (env, region)
				second_metric
			`,
			expectedPlan: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS or on (env, region) RHS
						- LHS: VectorSelector: {__name__="first_metric"}
						- RHS: VectorSelector: {__name__="second_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"logical unless binary expression should have hints added": {
			expr: `
				first_metric
				unless on (env, region)
				second_metric
			`,
			expectedPlan: `
				- BinaryExpression: LHS unless on (env, region) RHS, hints include (env, region)
					- LHS: VectorSelector: {__name__="first_metric"}
					- RHS: VectorSelector: {__name__="second_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"group_left binary expression on raw vector selectors should have hints added": {
			expr: `many_side * on (env) group_left () one_side`,
			expectedPlan: `
				- BinaryExpression: LHS * on (env) group_left () RHS, hints include (env)
					- LHS: VectorSelector: {__name__="many_side"}
					- RHS: VectorSelector: {__name__="one_side"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"group_right binary expression on raw vector selectors should have hints added": {
			expr: `one_side * on (env) group_right () many_side`,
			expectedPlan: `
				- BinaryExpression: LHS * on (env) group_right () RHS, hints include (env)
					- LHS: VectorSelector: {__name__="one_side"}
					- RHS: VectorSelector: {__name__="many_side"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"group_left binary expression with ignoring () matching should have exclude hints added": {
			expr: `many_side * ignoring () group_left () one_side`,
			expectedPlan: `
				- BinaryExpression: LHS * group_left () RHS, hints exclude ()
					- LHS: VectorSelector: {__name__="many_side"}
					- RHS: VectorSelector: {__name__="one_side"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"group_right binary expression with ignoring () matching should have exclude hints added": {
			expr: `one_side * ignoring () group_right () many_side`,
			expectedPlan: `
				- BinaryExpression: LHS * group_right () RHS, hints exclude ()
					- LHS: VectorSelector: {__name__="one_side"}
					- RHS: VectorSelector: {__name__="many_side"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"group_left binary expression with ignoring labels should exclude those labels from without hints": {
			expr: `many_side * ignoring (region) group_left () one_side`,
			expectedPlan: `
				- BinaryExpression: LHS * ignoring (region) group_left () RHS, hints exclude (region)
					- LHS: VectorSelector: {__name__="many_side"}
					- RHS: VectorSelector: {__name__="one_side"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression with on () (empty explicit on) should get no hints": {
			expr: `some_metric * on () some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS * on () RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression with on (synthesized_label) gets no hints: synthesized label cannot narrow storage selectors": {
			// "foo" is produced by label_replace on the LHS; it does not exist on the raw
			// series in storage. filterLabels removes it from the MatchingLabels list, leaving
			// an empty Include set, so no hint is emitted.
			expr: `label_replace(some_metric, "foo", "$1", "bar", ".*") + on (foo) some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + on (foo) RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: label_replace(...)
							- param 0: VectorSelector: {__name__="some_metric"}
							- param 1: StringLiteral: "foo"
							- param 2: StringLiteral: "$1"
							- param 3: StringLiteral: "bar"
							- param 4: StringLiteral: ".*"
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression with explicit ignoring labels": {
			expr: `some_metric + ignoring (foo) some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + ignoring (foo) RHS, hints exclude (foo)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression with explicit ignoring labels and aggregation LHS": {
			// foo is not in the LHS aggregation's by-labels, so it doesn't affect
			// the include hints derived from the aggregation.
			expr: `sum by (env, region) (some_metric) + ignoring (foo) some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + ignoring (foo) RHS, hints include (env, region)
					- LHS: AggregateExpression: sum by (env, region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression with ignoring label that overlaps aggregation LHS by-labels": {
			// region is in both the ignoring clause and the LHS aggregation's by-labels.
			// Since ignoring(region) means the binary operation does NOT match on region,
			// we must NOT include region in the hints — it would incorrectly filter out
			// valid RHS series. Only env survives as an include hint.
			expr: `sum by (env, region) (some_metric) + ignoring (region) some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + ignoring (region) RHS, hints include (env)
					- LHS: AggregateExpression: sum by (env, region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression with ignoring labels that remove all aggregation LHS by-labels": {
			// All LHS aggregation by-labels are in the ignoring clause, so no include
			// hints can be derived. Falls back to exclude-matching mode.
			expr: `sum by (env, region) (some_metric) + ignoring (env, region) some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + ignoring (env, region) RHS, hints exclude (env, region)
					- LHS: AggregateExpression: sum by (env, region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"logical unless binary expression with ignoring matching": {
			expr: `
				first_metric
				unless ignoring (env)
				second_metric
			`,
			expectedPlan: `
				- BinaryExpression: LHS unless ignoring (env) RHS, hints exclude (env)
					- LHS: VectorSelector: {__name__="first_metric"}
					- RHS: VectorSelector: {__name__="second_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			observer := streamingpromql.NoopPlanningObserver{}

			opts := streamingpromql.NewTestEngineOpts()
			planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)
			planner.RegisterQueryPlanOptimizationPass(plan.NewNarrowSelectorsOptimizationPass(opts.CommonOpts.Reg, opts.Logger))

			p, err := planner.NewQueryPlan(ctx, testCase.expr, timeRange, streamingpromql.DefaultLookbackDelta, false, observer)
			require.NoError(t, err)
			actual := p.String()
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)

			expectedMetrics := fmt.Sprintf(expectedMetricsTemplate, testCase.expectedAttempts, testCase.expectedModified)
			reg := opts.CommonOpts.Reg.(*prometheus.Registry)
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), "cortex_mimir_query_engine_narrow_selectors_attempted_total", "cortex_mimir_query_engine_narrow_selectors_modified_total"))
		})
	}
}
