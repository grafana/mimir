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
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/multiaggregation"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/rangevectorsplitting"
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

		// The engine runs common subexpression elimination before narrowing, so the narrowed side can
		// contain the nodes that this pass adds.
		enableCommonSubexpressionElimination bool

		// Multi-aggregation needs common subexpression elimination.
		enableMultiAggregation bool

		// Splitting a subquery needs common subexpression elimination.
		enableRangeVectorSplitting bool
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
		"binary expression on metric name and other label": {
			expr: `some_metric + on (__name__, cluster) some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + on (__name__, cluster) RHS, hints include (cluster)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression on metric name only": {
			expr: `some_metric + on (__name__) some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + on (__name__) RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"nested binary expression on raw vector selectors": {
			// The output series of the RHS only have the cluster label.
			expr: `some_metric + (some_other_metric / on (cluster) some_third_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS, hints include (cluster)
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
		"binary expression aggregation with metric name grouping": {
			expr: `sum by (__name__, region) (some_metric) / some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints include (region)
					- LHS: AggregateExpression: sum by (__name__, region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression nested grouped aggregation below ungrouped aggregation": {
			expr: `sum(max by (region) (some_metric)) / some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints exclude ()
					- LHS: AggregateExpression: sum
						- AggregateExpression: max by (region)
							- VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression grouped aggregation below without aggregation": {
			expr: `sum without (instance) (max by (region, instance) (some_metric)) / some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints exclude ()
					- LHS: AggregateExpression: sum without (instance)
						- AggregateExpression: max by (region, instance)
							- VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression grouped aggregation below unary expression": {
			expr: `(-sum by (region) (some_metric)) / some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints include (region)
					- LHS: DeduplicateAndMerge
						- UnaryExpression: -
							- AggregateExpression: sum by (region)
								- VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression grouped aggregation below label-changing function": {
			expr: `absent(sum by (region) (some_metric)) / some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints exclude ()
					- LHS: FunctionCall: absent(...)
						- AggregateExpression: sum by (region)
							- VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression aggregation LHS aggregation RHS": {
			// The RHS removes the region label, so a matcher for it would select the wrong input series.
			expr: `sum by (region) (some_metric) / sum(some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS
					- LHS: AggregateExpression: sum by (region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum
						- VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
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
				- BinaryExpression: LHS / RHS
					- LHS: AggregateExpression: sum by (region, env)
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum
						- VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
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
				- BinaryExpression: LHS / RHS, hints include (region)
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
				- BinaryExpression: LHS / on (region) RHS
					- LHS: AggregateExpression: sum by (region, env)
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum
						- VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression aggregation LHS aggregation and nested binary expression RHS": {
			expr: `sum by (region) (some_metric) / (sum(some_other_metric) + sum(some_third_metric))`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS
					- LHS: AggregateExpression: sum by (region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS + RHS
						- LHS: AggregateExpression: sum
							- VectorSelector: {__name__="some_other_metric"}
						- RHS: AggregateExpression: sum
							- VectorSelector: {__name__="some_third_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression aggregation LHS aggregation and nested binary expression RHS aggregation": {
			expr: `sum by (region) (some_metric) / (sum by (cluster) (some_other_metric) + sum(some_third_metric))`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS
					- LHS: AggregateExpression: sum by (region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS + RHS
						- LHS: AggregateExpression: sum by (cluster)
							- VectorSelector: {__name__="some_other_metric"}
						- RHS: AggregateExpression: sum
							- VectorSelector: {__name__="some_third_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression LHS aggregation RHS aggregation": {
			// The RHS only keeps the cluster label, which is a matching label, so the matcher
			// for it only accepts RHS series without it.
			expr: `sum(some_metric) / sum by (cluster) (some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints include (cluster)
					- LHS: AggregateExpression: sum
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum by (cluster)
						- VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression on RHS histogram_quantile": {
			// histogram_quantile removes the le label.
			expr: `some_metric / on (cluster, le) histogram_quantile(0.9, some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster, le) RHS, hints include (cluster)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_quantile(...)
							- param 0: NumberLiteral: 0.9
							- param 1: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression RHS histogram_quantile": {
			expr: `some_metric / histogram_quantile(0.9, some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints exclude (le)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_quantile(...)
							- param 0: NumberLiteral: 0.9
							- param 1: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression on RHS absent": {
			// The output of absent depends on whether there are any input series, not on their labels.
			expr: `some_metric / on (cluster) absent(some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster) RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: FunctionCall: absent(...)
						- VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression on RHS ungrouped topk": {
			// topk selects series in each group, so a matcher for a label that is not a grouping label changes which series it selects.
			expr: `some_metric / on (cluster) topk(1, some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster) RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: topk
						- expression: VectorSelector: {__name__="some_other_metric"}
						- parameter: NumberLiteral: 1
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression on RHS ungrouped limitk": {
			// limitk also selects series in each group.
			expr: `some_metric / on (cluster) limitk(1, some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster) RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: limitk
						- expression: VectorSelector: {__name__="some_other_metric"}
						- parameter: NumberLiteral: 1
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression on RHS topk by matching label": {
			expr: `some_metric / on (cluster) topk by (cluster) (1, some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster) RHS, hints include (cluster)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: topk by (cluster)
						- expression: VectorSelector: {__name__="some_other_metric"}
						- parameter: NumberLiteral: 1
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression on RHS ungrouped limit_ratio": {
			// limit_ratio selects each series from a hash of its own labels, so it keeps all labels.
			expr: `some_metric / on (cluster) limit_ratio(0.5, some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster) RHS, hints include (cluster)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: limit_ratio
						- expression: VectorSelector: {__name__="some_other_metric"}
						- parameter: NumberLiteral: 0.5
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression on RHS count_values": {
			// count_values sets its label to the sample value.
			expr: `some_metric / on (cluster, value) count_values by (cluster, value) ("value", some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster, value) RHS, hints include (cluster)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: count_values by (cluster, value)
						- expression: VectorSelector: {__name__="some_other_metric"}
						- parameter: StringLiteral: "value"
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression on RHS ungrouped count_values": {
			expr: `some_metric / on (value) count_values("value", some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (value) RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: count_values
						- expression: VectorSelector: {__name__="some_other_metric"}
						- parameter: StringLiteral: "value"
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression on RHS count_values without": {
			expr: `some_metric / on (cluster, value) count_values without (pod) ("value", some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster, value) RHS, hints include (cluster)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: count_values without (pod)
						- expression: VectorSelector: {__name__="some_other_metric"}
						- parameter: StringLiteral: "value"
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression on RHS without aggregation": {
			expr: `some_metric / on (cluster) sum without (pod) (some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster) RHS, hints include (cluster)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum without (pod)
						- VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression on RHS without aggregation of matching label": {
			expr: `some_metric / on (pod) sum without (pod) (some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (pod) RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum without (pod)
						- VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression on RHS one-to-one binary expression on other label": {
			// The output series of the RHS only have the region label.
			expr: `some_metric / on (cluster) (some_other_metric * on (region) some_third_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster) RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS * on (region) RHS, hints include (region)
						- LHS: VectorSelector: {__name__="some_other_metric"}
						- RHS: VectorSelector: {__name__="some_third_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression on RHS group_left included label": {
			// The included node label comes from the "one" side.
			expr: `some_metric / on (cluster, node) (some_other_metric * on (cluster) group_left (node) some_third_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster, node) RHS, hints include (cluster)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS * on (cluster) group_left (node) RHS, hints include (cluster)
						- LHS: VectorSelector: {__name__="some_other_metric"}
						- RHS: VectorSelector: {__name__="some_third_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression on RHS group_left label from many side": {
			// With on (...), only the matchers for the matching labels go to the "one" side, and the pod label comes from the "many" side.
			expr: `some_metric / on (cluster, pod) (some_other_metric * on (cluster) group_left some_third_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster, pod) RHS, hints include (cluster, pod)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS * on (cluster) group_left () RHS, hints include (cluster)
						- LHS: VectorSelector: {__name__="some_other_metric"}
						- RHS: VectorSelector: {__name__="some_third_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression on RHS or on other label": {
			// A series from the RHS of or is only in the output if no LHS series has the same region, so a matcher for cluster can add RHS series.
			expr: `some_metric / on (cluster) (some_other_metric or on (region) some_third_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster) RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: DeduplicateAndMerge
						- BinaryExpression: LHS or on (region) RHS
							- LHS: VectorSelector: {__name__="some_other_metric"}
							- RHS: VectorSelector: {__name__="some_third_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression on RHS and on other label": {
			// The output series of and are the LHS series, and only the LHS receives the matchers.
			expr: `some_metric / on (cluster) (some_other_metric and on (region) some_third_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster) RHS, hints include (cluster)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS and on (region) RHS, hints include (region)
						- LHS: VectorSelector: {__name__="some_other_metric"}
						- RHS: VectorSelector: {__name__="some_third_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression on RHS info with data label": {
			// info can change the value of its data labels.
			expr: `some_metric / on (k8s_cluster_name, cluster) info(some_other_metric, {k8s_cluster_name=~".+"})`,
			expectedPlan: `
				- BinaryExpression: LHS / on (k8s_cluster_name, cluster) RHS, hints include (cluster)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: FunctionCall: info(...)
						- param 0: VectorSelector: {__name__="some_other_metric"}
						- param 1: DataLabelSelector: {k8s_cluster_name=~".+"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression on RHS common subexpression aggregation": {
			// The narrowed side keeps the labels that the node inside the Duplicate keeps.
			expr: `(some_metric / on (cluster) sum(some_other_metric)) + (some_third_metric / on (cluster) sum(some_other_metric))`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS / on (cluster) RHS
						- LHS: VectorSelector: {__name__="some_metric"}
						- RHS: ref#1 Duplicate
							- AggregateExpression: sum
								- VectorSelector: {__name__="some_other_metric"}
					- RHS: BinaryExpression: LHS / on (cluster) RHS
						- LHS: VectorSelector: {__name__="some_third_metric"}
						- RHS: ref#1 Duplicate ...
			`,
			expectedAttempts:                     1,
			expectedModified:                     0,
			enableCommonSubexpressionElimination: true,
		},
		"binary expression on RHS common subexpression aggregation by same label": {
			expr: `(some_metric / on (cluster) sum by (cluster) (some_other_metric)) + (some_third_metric / on (cluster) sum by (cluster) (some_other_metric))`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS, hints include (cluster)
					- LHS: BinaryExpression: LHS / on (cluster) RHS, hints include (cluster)
						- LHS: VectorSelector: {__name__="some_metric"}
						- RHS: ref#1 Duplicate
							- AggregateExpression: sum by (cluster)
								- VectorSelector: {__name__="some_other_metric"}
					- RHS: BinaryExpression: LHS / on (cluster) RHS, hints include (cluster)
						- LHS: VectorSelector: {__name__="some_third_metric"}
						- RHS: ref#1 Duplicate ...
			`,
			expectedAttempts:                     1,
			expectedModified:                     1,
			enableCommonSubexpressionElimination: true,
		},
		"binary expression on RHS aggregation of subset selector": {
			// The narrowed side keeps the labels that the node inside the DuplicateFilter keeps.
			expr: `some_metric / on (cluster) sum(some_metric{env="prod"})`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster) RHS
					- LHS: ref#1 Duplicate
						- VectorSelector: {__name__="some_metric"}, subsets: {env="prod"} ({__name__="some_metric", env="prod"})
					- RHS: AggregateExpression: sum
						- DuplicateFilter: {env="prod"}, subset index: 0
							- ref#1 Duplicate ...
			`,
			expectedAttempts:                     1,
			expectedModified:                     0,
			enableCommonSubexpressionElimination: true,
		},
		"binary expression on RHS aggregation by same label of subset selector": {
			expr: `some_metric / on (cluster) sum by (cluster) (some_metric{env="prod"})`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster) RHS, hints include (cluster)
					- LHS: ref#1 Duplicate
						- VectorSelector: {__name__="some_metric"}, subsets: {env="prod"} ({__name__="some_metric", env="prod"})
					- RHS: AggregateExpression: sum by (cluster)
						- DuplicateFilter: {env="prod"}, subset index: 0
							- ref#1 Duplicate ...
			`,
			expectedAttempts:                     1,
			expectedModified:                     1,
			enableCommonSubexpressionElimination: true,
		},
		"binary expression on RHS multi-aggregation": {
			// The narrowed side keeps the labels that both the MultiAggregationInstance and the node inside its group keep.
			expr: `some_metric / on (cluster) (sum(some_other_metric) + max(some_other_metric))`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster) RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS + RHS
						- LHS: MultiAggregationInstance: sum
							- ref#1 MultiAggregationGroup
								- VectorSelector: {__name__="some_other_metric"}
						- RHS: MultiAggregationInstance: max
							- ref#1 MultiAggregationGroup ...
			`,
			expectedAttempts:                     1,
			expectedModified:                     0,
			enableCommonSubexpressionElimination: true,
			enableMultiAggregation:               true,
		},
		"binary expression on RHS multi-aggregation by same label": {
			expr: `some_metric / on (cluster) (sum by (cluster) (some_other_metric) + max by (cluster) (some_other_metric))`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster) RHS, hints include (cluster)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS + RHS, hints include (cluster)
						- LHS: MultiAggregationInstance: sum by (cluster)
							- ref#1 MultiAggregationGroup
								- VectorSelector: {__name__="some_other_metric"}
						- RHS: MultiAggregationInstance: max by (cluster)
							- ref#1 MultiAggregationGroup ...
			`,
			expectedAttempts:                     1,
			expectedModified:                     1,
			enableCommonSubexpressionElimination: true,
			enableMultiAggregation:               true,
		},
		"binary expression on RHS multi-aggregation by same label of aggregation by other label": {
			expr: `some_metric / on (cluster) (sum by (cluster) (sum by (region) (some_other_metric)) + max by (cluster) (sum by (region) (some_other_metric)))`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster) RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS + RHS
						- LHS: MultiAggregationInstance: sum by (cluster)
							- ref#1 MultiAggregationGroup
								- AggregateExpression: sum by (region)
									- VectorSelector: {__name__="some_other_metric"}
						- RHS: MultiAggregationInstance: max by (cluster)
							- ref#1 MultiAggregationGroup ...
			`,
			expectedAttempts:                     1,
			expectedModified:                     0,
			enableCommonSubexpressionElimination: true,
			enableMultiAggregation:               true,
		},
		"binary expression on RHS split range vector function": {
			// The narrowed side keeps the labels that the node inside the SplitFunctionCall keeps.
			expr: `some_metric / on (cluster) sum_over_time(some_other_metric[5h])`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster) RHS, hints include (cluster)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: DeduplicateAndMerge
						- SplitFunctionCall
							- FunctionCall: sum_over_time(...)
								- MatrixSelector: {__name__="some_other_metric"}[5h0m0s]
			`,
			expectedAttempts:           1,
			expectedModified:           1,
			enableRangeVectorSplitting: true,
		},
		"binary expression on RHS split range vector function of aggregation subquery": {
			expr: `some_metric / on (cluster) sum_over_time(sum(some_other_metric)[5h:10m])`,
			expectedPlan: `
				- BinaryExpression: LHS / on (cluster) RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: DeduplicateAndMerge
						- SplitFunctionCall
							- FunctionCall: sum_over_time(...)
								- Subquery: [5h0m0s:10m0s]
									- Duplicate
										- AggregateExpression: sum
											- VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts:                     1,
			expectedModified:                     0,
			enableRangeVectorSplitting:           true,
			enableCommonSubexpressionElimination: true,
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
			// statefulset is synthesised by label_replace on the RHS, and it is the only label that the RHS
			// keeps, so no hint is set.
			expr: `sum by (statefulset) (kube_statefulset_replicas) - sum by (statefulset) (label_replace(not_ready, "statefulset", "$1", "job", ".+/(.+)"))`,
			expectedPlan: `
				- BinaryExpression: LHS - RHS
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
			expectedModified: 0,
		},
		"binary expression with label_join on one side": {
			// statefulset is synthesised by label_join on the RHS, and it is the only label that the RHS
			// keeps, so no hint is set.
			expr: `sum by (statefulset) (kube_statefulset_replicas) - sum by (statefulset) (label_join(not_ready, "statefulset", "job", "workload"))`,
			expectedPlan: `
				- BinaryExpression: LHS - RHS
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
			expectedModified: 0,
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
					- LHS: BinaryExpression: LHS * RHS
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
		// Regression: the inner "* on () group_left" is correctly left unhinted, but the outer
		// "+" still gets an empty-exclude hint. Whose matchers must not reach the one-side
		// selector is enforced at the operator level; see TestNarrowSelectorsOnEmptyGroupLeftBoundary
		// for the end-to-end result-correctness check.
		"outer exclude-matching binop over inner on() group_left cross-metric": {
			expr: `prediction + prediction * on() group_left threshold_margin`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS, hints exclude ()
					- LHS: VectorSelector: {__name__="prediction"}
					- RHS: BinaryExpression: LHS * on () group_left () RHS
						- LHS: VectorSelector: {__name__="prediction"}
						- RHS: VectorSelector: {__name__="threshold_margin"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression with on (synthesized_label) gets no hints: synthesized label cannot narrow storage selectors": {
			// "foo" is produced by label_replace on the LHS; it does not exist on the raw
			// series in storage. filterLabelsForDefaultMatching removes it from the MatchingLabels list, leaving
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
		// A filled side must not be narrowed, so no hint is added.
		"binary expression with fill on both sides is not narrowed": {
			expr: `some_metric + on (cluster) fill(0) some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + on (cluster) fill (0) RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression with fill_right is not narrowed": {
			expr: `some_metric + on (cluster) fill_right(0) some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + on (cluster) fill_right (0) RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression with fill_left is not narrowed": {
			expr: `some_metric + ignoring (env) fill_left(0) some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + ignoring (env) fill_left (0) RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
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
			if testCase.enableRangeVectorSplitting {
				planner.RegisterQueryPlanOptimizationPass(rangevectorsplitting.NewOptimizationPass(2*time.Hour, true, opts.CommonOpts.Reg, opts.Logger))
			}
			if testCase.enableCommonSubexpressionElimination {
				planner.RegisterASTOptimizationPass(&ast.SortLabelsAndMatchers{})
				planner.RegisterQueryPlanOptimizationPass(commonsubexpressionelimination.NewOptimizationPass(opts.EnableSubsetSelectorElimination, opts.EnableRangeQueryRangeVectorCommonSubexpressionElimination, opts.CommonOpts.Reg, opts.Logger))
			}
			if testCase.enableMultiAggregation {
				planner.RegisterQueryPlanOptimizationPass(multiaggregation.NewOptimizationPass(opts.CommonOpts.Reg))
			}
			planner.RegisterQueryPlanOptimizationPass(plan.NewNarrowSelectorsOptimizationPass(opts.CommonOpts.Reg, opts.Logger))

			p, err := planner.NewQueryPlan(ctx, testCase.expr, timeRange, streamingpromql.DefaultLookbackDelta, false, nil, observer)
			require.NoError(t, err)
			actual := p.String()
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)

			expectedMetrics := fmt.Sprintf(expectedMetricsTemplate, testCase.expectedAttempts, testCase.expectedModified)
			reg := opts.CommonOpts.Reg.(*prometheus.Registry)
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), "cortex_mimir_query_engine_narrow_selectors_attempted_total", "cortex_mimir_query_engine_narrow_selectors_modified_total"))
		})
	}
}
