// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
	_ "github.com/grafana/mimir/pkg/streamingpromql/optimize/ast/sharding" // Imported for side effects: registering the __sharded_concat__ function with the parser.
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestOptimizationPass(t *testing.T) {
	enableExtendedRangeSelectors := parser.EnableExtendedRangeSelectors
	enableExperimentalFunctions := parser.EnableExperimentalFunctions
	defer func() {
		parser.EnableExtendedRangeSelectors = enableExtendedRangeSelectors
		parser.EnableExperimentalFunctions = enableExperimentalFunctions
	}()
	parser.EnableExtendedRangeSelectors = true
	parser.EnableExperimentalFunctions = true

	testCases := map[string]struct {
		expr                                 string
		rangeQuery                           bool
		expectedPlan                         string
		expectUnchanged                      bool
		expectedDuplicateNodes               int // Check that we don't do unnecessary work introducing a duplicate node multiple times when starting from different selectors (eg. when handling (a+b) + (a+b)).
		expectedDuplicateSelectorsEliminated int
		expectedSubsetSelectorsEliminated    int
		expectedSelectorsInspected           int
	}{
		"single vector selector": {
			expr:                       `foo`,
			expectUnchanged:            true,
			expectedSelectorsInspected: 1,
		},
		"single matrix selector": {
			expr:                       `foo[5m]`,
			expectUnchanged:            true,
			expectedSelectorsInspected: 1,
		},
		"single subquery": {
			expr:                       `foo[5m:10s]`,
			expectUnchanged:            true,
			expectedSelectorsInspected: 1,
		},
		"duplicated numeric literal": {
			expr:            `1 + 1`,
			expectUnchanged: true,
		},
		"duplicated string literal": {
			expr:                       `label_join(foo, "abc", "-") + label_join(bar, "def", ",")`,
			expectUnchanged:            true,
			expectedSelectorsInspected: 2,
		},
		"vector selector duplicated twice": {
			expr: `foo + foo`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- VectorSelector: {__name__="foo"}
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"vector selector duplicated twice with other selector": {
			expr: `foo + foo + bar`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: ref#1 Duplicate
							- VectorSelector: {__name__="foo"}
						- RHS: ref#1 Duplicate ...
					- RHS: VectorSelector: {__name__="bar"}
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           3,
		},
		"vector selector duplicated three times": {
			expr: `foo + foo + foo`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: ref#1 Duplicate
							- VectorSelector: {__name__="foo"}
						- RHS: ref#1 Duplicate ...
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 2,
			expectedSelectorsInspected:           3,
		},
		"vector selector duplicated many times": {
			expr: `foo + foo + foo + bar + foo`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: BinaryExpression: LHS + RHS
							- LHS: BinaryExpression: LHS + RHS
								- LHS: ref#1 Duplicate
									- VectorSelector: {__name__="foo"}
								- RHS: ref#1 Duplicate ...
							- RHS: ref#1 Duplicate ...
						- RHS: VectorSelector: {__name__="bar"}
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 3,
			expectedSelectorsInspected:           5,
		},
		"duplicated vector selector with different aggregations": {
			expr: `max(foo) - min(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS - RHS
					- LHS: AggregateExpression: max
						- ref#1 Duplicate
							- VectorSelector: {__name__="foo"}
					- RHS: AggregateExpression: min
						- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"duplicated vector selector with same aggregations": {
			expr: `max(foo) + max(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- AggregateExpression: max
							- VectorSelector: {__name__="foo"}
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"multiple levels of duplication: vector selector and aggregation": {
			expr: `a + sum(a) + sum(a)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: ref#1 Duplicate
							- VectorSelector: {__name__="a"}
						- RHS: ref#2 Duplicate
							- AggregateExpression: sum
								- ref#1 Duplicate ...
					- RHS: ref#2 Duplicate ...
			`,
			expectedDuplicateNodes:               2,
			expectedDuplicateSelectorsEliminated: 2,
			expectedSelectorsInspected:           3,
		},
		"multiple levels of duplication: vector selector and binary operation": {
			expr: `(a - a) + (a - a)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#2 Duplicate
						- BinaryExpression: LHS - RHS
							- LHS: ref#1 Duplicate
								- VectorSelector: {__name__="a"}
							- RHS: ref#1 Duplicate ...
					- RHS: ref#2 Duplicate ...
			`,
			expectedDuplicateNodes:               2,
			expectedDuplicateSelectorsEliminated: 3,
			expectedSelectorsInspected:           4,
		},
		"multiple levels of duplication: multiple vector selectors and binary operation": {
			expr: `(a - a) + (a - a) + (a * b) + (a * b)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: BinaryExpression: LHS + RHS
							- LHS: ref#2 Duplicate
								- BinaryExpression: LHS - RHS
									- LHS: ref#1 Duplicate
										- VectorSelector: {__name__="a"}
									- RHS: ref#1 Duplicate ...
							- RHS: ref#2 Duplicate ...
						- RHS: ref#3 Duplicate
							- BinaryExpression: LHS * RHS
								- LHS: ref#1 Duplicate ...
								- RHS: VectorSelector: {__name__="b"}
					- RHS: ref#3 Duplicate ...
			`,
			expectedDuplicateNodes:               3,
			expectedDuplicateSelectorsEliminated: 6, // 5 instances of 'a', and one instance of 'b'
			expectedSelectorsInspected:           8,
		},
		"duplicated binary operation with different vector selectors": {
			expr: `(a - b) + (a - b)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- BinaryExpression: LHS - RHS
							- LHS: VectorSelector: {__name__="a"}
							- RHS: VectorSelector: {__name__="b"}
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1, // This test ensures that we don't do unnecessary work when traversing up from both the a and b selectors.
			expectedDuplicateSelectorsEliminated: 2,
			expectedSelectorsInspected:           4,
		},
		"duplicated binary operation with different matrix selectors": {
			expr: `(rate(a[5m]) - rate(b[5m])) + (rate(a[5m]) - rate(b[5m]))`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- BinaryExpression: LHS - RHS
							- LHS: DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="a"}[5m0s]
							- RHS: DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="b"}[5m0s]
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1, // This test ensures that we don't do unnecessary work when traversing up from both the a and b selectors.
			expectedDuplicateSelectorsEliminated: 2,
			expectedSelectorsInspected:           4,
		},
		"same selector used for both vector and matrix selector": {
			expr:                       `foo + rate(foo[5m])`,
			expectUnchanged:            true,
			expectedSelectorsInspected: 2,
		},
		"duplicate matrix selectors with different outer function in instant query": {
			expr:       `rate(foo[5m]) + increase(foo[5m])`,
			rangeQuery: false,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: rate(...)
							- ref#1 Duplicate
								- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: DeduplicateAndMerge
						- FunctionCall: increase(...)
							- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"duplicate matrix selectors with different outer function in range query": {
			// We do not want to deduplicate matrix selectors in range queries.
			expr:                       `rate(foo[5m]) + increase(foo[5m])`,
			rangeQuery:                 true,
			expectUnchanged:            true,
			expectedSelectorsInspected: 2,
		},
		"duplicate matrix selectors, some with different outer function in instant query": {
			expr: `rate(foo[5m]) + increase(foo[5m]) + rate(foo[5m])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: ref#2 Duplicate
							- DeduplicateAndMerge
								- FunctionCall: rate(...)
									- ref#1 Duplicate
										- MatrixSelector: {__name__="foo"}[5m0s]
						- RHS: DeduplicateAndMerge
							- FunctionCall: increase(...)
								- ref#1 Duplicate ...
					- RHS: ref#2 Duplicate ...
			`,
			rangeQuery:                           false,
			expectedDuplicateNodes:               2,
			expectedDuplicateSelectorsEliminated: 2,
			expectedSelectorsInspected:           3,
		},
		"duplicate matrix selectors, some with different outer function in range query": {
			// We do not want to deduplicate matrix selectors themselves in range queries, but do want to deduplicate duplicate functions over matrix selectors.
			expr: `rate(foo[5m]) + increase(foo[5m]) + rate(foo[5m])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: ref#1 Duplicate
							- DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="foo"}[5m0s]
						- RHS: DeduplicateAndMerge
							- FunctionCall: increase(...)
								- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: ref#1 Duplicate ...
			`,
			rangeQuery:                           true,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1, // We only eliminate one 'foo' selector in the duplicate rate(...) expression.
			expectedSelectorsInspected:           3,
		},
		"duplicate matrix selectors with same outer function": {
			expr: `rate(foo[5m]) + rate(foo[5m])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- DeduplicateAndMerge
							- FunctionCall: rate(...)
								- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"duplicate subqueries with different outer function in instant query": {
			expr: `rate(foo[5m:]) + increase(foo[5m:])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: rate(...)
							- ref#1 Duplicate
								- Subquery: [5m0s:1m0s]
									- VectorSelector: {__name__="foo"}
					- RHS: DeduplicateAndMerge
						- FunctionCall: increase(...)
							- ref#1 Duplicate ...
			`,
			rangeQuery:                           false,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"duplicate subqueries with different outer function in range query": {
			// We do not want to deduplicate subqueries directly in range queries, but do want to deduplicate their contents if they are the same.
			expr: `rate(foo[5m:]) + increase(foo[5m:])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: rate(...)
							- Subquery: [5m0s:1m0s]
								- ref#1 Duplicate
									- VectorSelector: {__name__="foo"}
					- RHS: DeduplicateAndMerge
						- FunctionCall: increase(...)
							- Subquery: [5m0s:1m0s]
								- ref#1 Duplicate ...
			`,
			rangeQuery:                           true,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"duplicate subqueries with different outer function and multiple child selectors in instant query": {
			expr: `rate((a - b)[5m:]) + increase((a - b)[5m:])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: rate(...)
							- ref#1 Duplicate
								- Subquery: [5m0s:1m0s]
									- BinaryExpression: LHS - RHS
										- LHS: VectorSelector: {__name__="a"}
										- RHS: VectorSelector: {__name__="b"}
					- RHS: DeduplicateAndMerge
						- FunctionCall: increase(...)
							- ref#1 Duplicate ...
			`,
			rangeQuery:                           false,
			expectedDuplicateNodes:               1, // This test ensures that we don't do unnecessary work when traversing up from both the a and b selectors.
			expectedDuplicateSelectorsEliminated: 2,
			expectedSelectorsInspected:           4,
		},
		"duplicate subqueries with different outer function and multiple child selectors in range query": {
			expr: `rate((a - b)[5m:]) + increase((a - b)[5m:])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: rate(...)
							- Subquery: [5m0s:1m0s]
								- ref#1 Duplicate
									- BinaryExpression: LHS - RHS
										- LHS: VectorSelector: {__name__="a"}
										- RHS: VectorSelector: {__name__="b"}
					- RHS: DeduplicateAndMerge
						- FunctionCall: increase(...)
							- Subquery: [5m0s:1m0s]
								- ref#1 Duplicate ...
			`,
			rangeQuery:                           true,
			expectedDuplicateNodes:               1, // This test ensures that we don't do unnecessary work when traversing up from both the a and b selectors.
			expectedDuplicateSelectorsEliminated: 2,
			expectedSelectorsInspected:           4,
		},
		"duplicate subqueries with same outer function": {
			expr: `rate(foo[5m:]) + rate(foo[5m:])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- DeduplicateAndMerge
							- FunctionCall: rate(...)
								- Subquery: [5m0s:1m0s]
									- VectorSelector: {__name__="foo"}
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"duplicate nested subqueries": {
			expr: `max_over_time(rate(foo[5m:])[10m:]) + max_over_time(rate(foo[5m:])[10m:])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- DeduplicateAndMerge
							- FunctionCall: max_over_time(...)
								- Subquery: [10m0s:1m0s]
									- DeduplicateAndMerge
										- FunctionCall: rate(...)
											- Subquery: [5m0s:1m0s]
												- VectorSelector: {__name__="foo"}
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"duplicate subqueries with different ranges": {
			expr:                       `max_over_time(rate(foo[5m:])[10m:]) + max_over_time(rate(foo[5m:])[7m:])`,
			expectUnchanged:            true, // We don't support deduplicating common expressions that are evaluated over different ranges.
			expectedSelectorsInspected: 2,
		},
		"duplicate selectors, both with timestamp()": {
			expr: `timestamp(foo) + timestamp(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- DeduplicateAndMerge
							- FunctionCall: timestamp(...)
								- VectorSelector: {__name__="foo"}, return sample timestamps
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"duplicate selectors, one with timestamp()": {
			expr:                       `timestamp(foo) + foo`,
			expectUnchanged:            true, // In the future, we might be able to deduplicate these, but for now, treat them as unique expressions.
			expectedSelectorsInspected: 2,
		},
		"duplicate selectors, one with timestamp() over an intermediate expression": {
			expr: `timestamp(abs(foo)) + foo`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: timestamp(...)
							- DeduplicateAndMerge
								- FunctionCall: abs(...)
									- ref#1 Duplicate
										- VectorSelector: {__name__="foo"}
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"duplicate operation with different children": {
			expr: `topk(3, foo) + topk(5, foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: AggregateExpression: topk
						- expression: ref#1 Duplicate
							- VectorSelector: {__name__="foo"}
						- parameter: NumberLiteral: 3
					- RHS: AggregateExpression: topk
						- expression: ref#1 Duplicate ...
						- parameter: NumberLiteral: 5
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"duplicate expression with multiple children": {
			expr: `topk(5, foo) + topk(5, foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- AggregateExpression: topk
							- expression: VectorSelector: {__name__="foo"}
							- parameter: NumberLiteral: 5
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"duplicate expression where 'skip histogram decoding' applies to one expression": {
			expr: `histogram_count(some_metric) * histogram_quantile(0.5, some_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_count(...)
							- ref#1 Duplicate
								- VectorSelector: {__name__="some_metric"}
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_quantile(...)
							- param 0: NumberLiteral: 0.5
							- param 1: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"duplicate expression where 'skip histogram decoding' applies to both expressions": {
			expr: `histogram_count(some_metric) * histogram_sum(some_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_count(...)
							- ref#1 Duplicate
								- VectorSelector: {__name__="some_metric"}
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"duplicate expression with nested functions calls, where inner and outer functions are same": {
			expr: `clamp_min(rate(foo[5m]), 1) + clamp_min(rate(foo[5m]), 1)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- DeduplicateAndMerge
							- FunctionCall: clamp_min(...)
								- param 0: DeduplicateAndMerge
									- FunctionCall: rate(...)
										- MatrixSelector: {__name__="foo"}[5m0s]
								- param 1: NumberLiteral: 1
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"duplicate expression with nested functions calls, where only inner functions are same": {
			expr: `clamp_min(rate(foo[5m]), 1) + clamp_max(rate(foo[5m]), 1)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: clamp_min(...)
							- param 0: ref#1 Duplicate
								- DeduplicateAndMerge
									- FunctionCall: rate(...)
										- MatrixSelector: {__name__="foo"}[5m0s]
							- param 1: NumberLiteral: 1
					- RHS: DeduplicateAndMerge
						- FunctionCall: clamp_max(...)
							- param 0: ref#1 Duplicate ...
							- param 1: NumberLiteral: 1
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"duplicate expression with duplicate aggregation over a duplicate function call": {
			expr: `max(rate(foo[5m])) + min(rate(foo[5m]))`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: AggregateExpression: max
						- ref#1 Duplicate
							- DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: AggregateExpression: min
						- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"duplicate expression with different aggregation over a duplicate function call": {
			expr: `max(rate(foo[5m])) + min(rate(foo[5m]))`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: AggregateExpression: max
						- ref#1 Duplicate
							- DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: AggregateExpression: min
						- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           2,
		},
		"equivalent expressions that differ in the number of children but are otherwise duplicated (first side with fewer arguments)": {
			// This test ensures that we don't incorrectly deduplicate the __sharded_concat__ calls.
			expr: `__sharded_concat__(foo, bar) + __sharded_concat__(foo, bar, baz)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: FunctionCall: __sharded_concat__(...)
						- param 0: ref#1 Duplicate
							- VectorSelector: {__name__="foo"}
						- param 1: ref#2 Duplicate
							- VectorSelector: {__name__="bar"}
					- RHS: FunctionCall: __sharded_concat__(...)
						- param 0: ref#1 Duplicate ...
						- param 1: ref#2 Duplicate ...
						- param 2: VectorSelector: {__name__="baz"}
			`,
			expectedDuplicateNodes:               2,
			expectedDuplicateSelectorsEliminated: 2,
			expectedSelectorsInspected:           5,
		},
		"equivalent expressions that differ in the number of children but are otherwise duplicated (first side with more arguments)": {
			// This test ensures that we don't incorrectly deduplicate the __sharded_concat__ calls.
			expr: `__sharded_concat__(foo, bar, baz) + __sharded_concat__(foo, bar)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: FunctionCall: __sharded_concat__(...)
						- param 0: ref#1 Duplicate
							- VectorSelector: {__name__="foo"}
						- param 1: ref#2 Duplicate
							- VectorSelector: {__name__="bar"}
						- param 2: VectorSelector: {__name__="baz"}
					- RHS: FunctionCall: __sharded_concat__(...)
						- param 0: ref#1 Duplicate ...
						- param 1: ref#2 Duplicate ...
			`,
			expectedDuplicateNodes:               2,
			expectedDuplicateSelectorsEliminated: 2,
			expectedSelectorsInspected:           5,
		},
		// In this case the foo[5m] smoothed provides step data with both a collection of points but also the alternate smoothed head and tail points.
		// delta() will use the returned points, but rate() will substitute in the smoothed head/tail points to its calculation.
		// It is important that if the matrix selector is shared between the functions, that the smoothed head/tail alternate points
		// are not substituted into the points' collection.
		"duplicate matrix selectors with smoothed head/tail points": {
			expr:       `delta(foo[5m] smoothed) + rate(foo[5m] smoothed)`,
			rangeQuery: true,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: delta(...)
							- MatrixSelector: {__name__="foo"}[5m0s] smoothed
					- RHS: DeduplicateAndMerge
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__="foo"}[5m0s] smoothed counter aware
			`,
			expectedDuplicateNodes:               0,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSelectorsInspected:           2,
		},
		// These tests verify that CSE optimization correctly skips the 2nd argument to info function,
		// allowing only the 1st argument to be deduplicated.
		"info function with arguments included separately": {
			expr: `foo + {k8s_cluster_name="cluster1"} + info(foo, {k8s_cluster_name="cluster1"})`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: ref#1 Duplicate
							- VectorSelector: {__name__="foo"}
						- RHS: VectorSelector: {k8s_cluster_name="cluster1"}
					- RHS: FunctionCall: info(...)
						- param 0: ref#1 Duplicate ...
						- param 1: VectorSelector: {k8s_cluster_name="cluster1"}, return sample timestamps preserving histograms
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSelectorsInspected:           3,
		},
		"info function called twice with same 2nd argument but different 1st arguments": {
			expr: `info(foo, {k8s_cluster_name="cluster1"}) + info(bar, {k8s_cluster_name="cluster1"})`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: FunctionCall: info(...)
						- param 0: VectorSelector: {__name__="foo"}
						- param 1: VectorSelector: {k8s_cluster_name="cluster1"}, return sample timestamps preserving histograms
					- RHS: FunctionCall: info(...)
						- param 0: VectorSelector: {__name__="bar"}
						- param 1: VectorSelector: {k8s_cluster_name="cluster1"}, return sample timestamps preserving histograms
			`,
			expectedDuplicateNodes:               0,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSelectorsInspected:           2,
		},
		"subset vector selectors": {
			expr: `some_metric + some_metric{env="bar"}`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- VectorSelector: {__name__="some_metric"}
					- RHS: DuplicateFilter: {env="bar"}
						- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSubsetSelectorsEliminated:    1,
			expectedSelectorsInspected:           2,
		},
		"subset vector selector with broader selector repeated": {
			expr: `some_metric + some_metric{env="bar"} * some_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS * RHS
						- LHS: DuplicateFilter: {env="bar"}
							- ref#1 Duplicate ...
						- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSubsetSelectorsEliminated:    1,
			expectedSelectorsInspected:           3,
		},
		"subset vector selector with narrower selector repeated": {
			expr: `some_metric + some_metric{env="bar"} * some_metric{env="bar"}`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS * RHS
						- LHS: DuplicateFilter: {env="bar"}
							- ref#1 Duplicate ...
						- RHS: DuplicateFilter: {env="bar"}
							- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSubsetSelectorsEliminated:    2,
			expectedSelectorsInspected:           3,
		},
		"subset matrix selectors": {
			expr: `count_over_time(some_metric[5m]) + sum_over_time(some_metric{env="bar"}[5m])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: count_over_time(...)
							- ref#1 Duplicate
								- MatrixSelector: {__name__="some_metric"}[5m0s]
					- RHS: DeduplicateAndMerge
						- FunctionCall: sum_over_time(...)
							- DuplicateFilter: {env="bar"}
								- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSubsetSelectorsEliminated:    1,
			expectedSelectorsInspected:           2,
		},
		"subset matrix selector with broader selector repeated": {
			expr: `count_over_time(some_metric[5m]) + sum_over_time(some_metric{env="bar"}[5m]) * max_over_time(some_metric[5m])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: count_over_time(...)
							- ref#1 Duplicate
								- MatrixSelector: {__name__="some_metric"}[5m0s]
					- RHS: BinaryExpression: LHS * RHS
						- LHS: DeduplicateAndMerge
							- FunctionCall: sum_over_time(...)
								- DuplicateFilter: {env="bar"}
									- ref#1 Duplicate ...
						- RHS: DeduplicateAndMerge
							- FunctionCall: max_over_time(...)
								- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSubsetSelectorsEliminated:    1,
			expectedSelectorsInspected:           3,
		},
		"subset matrix selector with narrower selector repeated": {
			expr: `count_over_time(some_metric[5m]) + sum_over_time(some_metric{env="bar"}[5m]) * max_over_time(some_metric{env="bar"}[5m])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: count_over_time(...)
							- ref#1 Duplicate
								- MatrixSelector: {__name__="some_metric"}[5m0s]
					- RHS: BinaryExpression: LHS * RHS
						- LHS: DeduplicateAndMerge
							- FunctionCall: sum_over_time(...)
								- DuplicateFilter: {env="bar"}
									- ref#1 Duplicate ...
						- RHS: DeduplicateAndMerge
							- FunctionCall: max_over_time(...)
								- DuplicateFilter: {env="bar"}
									- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSubsetSelectorsEliminated:    2,
			expectedSelectorsInspected:           3,
		},
		"subset matrix selector with multiple narrower selectors": {
			expr: `count_over_time(some_metric[5m]) + sum_over_time(some_metric{env="bar"}[5m]) * max_over_time(some_metric{env="foo"}[5m])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: count_over_time(...)
							- ref#1 Duplicate
								- MatrixSelector: {__name__="some_metric"}[5m0s]
					- RHS: BinaryExpression: LHS * RHS
						- LHS: DeduplicateAndMerge
							- FunctionCall: sum_over_time(...)
								- DuplicateFilter: {env="bar"}
									- ref#1 Duplicate ...
						- RHS: DeduplicateAndMerge
							- FunctionCall: max_over_time(...)
								- DuplicateFilter: {env="foo"}
									- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSubsetSelectorsEliminated:    2,
			expectedSelectorsInspected:           3,
		},
		"subset selector, broader one is vector selector": {
			expr:                                 `some_metric + sum_over_time(some_metric{env="bar"}[5m])`,
			expectUnchanged:                      true,
			expectedDuplicateNodes:               0,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSubsetSelectorsEliminated:    0,
			expectedSelectorsInspected:           2,
		},
		"subset selector, broader one is matrix selector": {
			expr:                                 `some_metric{env="bar"} + sum_over_time(some_metric[5m])`,
			expectUnchanged:                      true,
			expectedDuplicateNodes:               0,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSubsetSelectorsEliminated:    0,
			expectedSelectorsInspected:           2,
		},
		"subset selectors wrapped in function ordinarily safe to run before filtering": {
			expr: `rate(foo{status="success"}[5m]) / rate(foo[5m])`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS
					- LHS: DeduplicateAndMerge
						- DuplicateFilter: {status="success"}
							- ref#1 Duplicate
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: DeduplicateAndMerge
						- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSubsetSelectorsEliminated:    1,
			expectedSelectorsInspected:           2,
		},
		"subset selectors wrapped in function never safe to run before filtering": {
			expr: `absent(foo{status="success"}) + absent(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: FunctionCall: absent(...) with labels {status="success"}
						- DuplicateFilter: {status="success"}
							- ref#1 Duplicate
								- VectorSelector: {__name__="foo"}
					- RHS: FunctionCall: absent(...)
						- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSubsetSelectorsEliminated:    1,
			expectedSelectorsInspected:           2,
		},
		"subset selectors wrapped in function ordinarily safe to run before filtering, but filter is on __name__": {
			// This test case assumes delayed name removal is disabled.
			// If delayed name removal is enabled, then we can run filtering after the rate() call given that it will return __name__ if delayed name removal is enabled.
			expr: `rate({__name__=~"foo.*",__name__!="foo_2"}[5m]) / rate({__name__=~"foo.*"}[5m])`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: rate(...)
							- DuplicateFilter: {__name__!="foo_2"}
								- ref#1 Duplicate
									- MatrixSelector: {__name__=~"foo.*"}[5m0s]
					- RHS: DeduplicateAndMerge
						- FunctionCall: rate(...)
							- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSubsetSelectorsEliminated:    1,
			expectedSelectorsInspected:           2,
		},
		"subset selectors wrapped in aggregation": {
			expr: `sum(foo{status="success"}) / sum(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS
					- LHS: AggregateExpression: sum
						- DuplicateFilter: {status="success"}
							- ref#1 Duplicate
								- VectorSelector: {__name__="foo"}
					- RHS: AggregateExpression: sum
						- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSubsetSelectorsEliminated:    1,
			expectedSelectorsInspected:           2,
		},
		"subset selectors wrapped in aggregation and function ordinarily safe to run before filtering": {
			expr: `sum(rate(foo{status="success"}[5m])) / sum(rate(foo[5m]))`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS
					- LHS: AggregateExpression: sum
						- DeduplicateAndMerge
							- DuplicateFilter: {status="success"}
								- ref#1 Duplicate
									- FunctionCall: rate(...)
										- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: AggregateExpression: sum
						- DeduplicateAndMerge
							- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSubsetSelectorsEliminated:    1,
			expectedSelectorsInspected:           2,
		},
		"subset selectors where broader selector can be further deduplicated": {
			expr: `abs(max(foo)) / (abs(max(foo{env="prod"})) + abs(max(foo)))`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS
					- LHS: ref#2 Duplicate
						- DeduplicateAndMerge
							- FunctionCall: abs(...)
								- AggregateExpression: max
									- ref#1 Duplicate
										- VectorSelector: {__name__="foo"}
					- RHS: BinaryExpression: LHS + RHS
						- LHS: DeduplicateAndMerge
							- FunctionCall: abs(...)
								- AggregateExpression: max
									- DuplicateFilter: {env="prod"}
										- ref#1 Duplicate ...
						- RHS: ref#2 Duplicate ...
			`,
			expectedDuplicateNodes:               2,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSubsetSelectorsEliminated:    1,
			expectedSelectorsInspected:           3,
		},
		"subset selectors where narrower selector can be further deduplicated": {
			expr: `abs(max(foo{env="prod"})) / (abs(max(foo{env="prod"})) + abs(max(foo)))`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS
					- LHS: ref#1 Duplicate
						- DeduplicateAndMerge
							- FunctionCall: abs(...)
								- AggregateExpression: max
									- DuplicateFilter: {env="prod"}
										- ref#2 Duplicate
											- VectorSelector: {__name__="foo"}
					- RHS: BinaryExpression: LHS + RHS
						- LHS: ref#1 Duplicate ...
						- RHS: DeduplicateAndMerge
							- FunctionCall: abs(...)
								- AggregateExpression: max
									- ref#2 Duplicate ...
			`,
			expectedDuplicateNodes:               2,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSubsetSelectorsEliminated:    2,
			expectedSelectorsInspected:           3,
		},
		"multiple subsets of same selector, broadest selector last": {
			expr: `foo{env="bar"} + foo{env="baz"} + foo`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: DuplicateFilter: {env="bar"}
							- ref#1 Duplicate
								- VectorSelector: {__name__="foo"}
						- RHS: DuplicateFilter: {env="baz"}
							- ref#1 Duplicate ...
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSubsetSelectorsEliminated:    2,
			expectedSelectorsInspected:           3,
		},
		"multiple subsets of same selector, broadest selector first": {
			expr: `foo + foo{env="baz"} + foo{env="bar"}`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: ref#1 Duplicate
							- VectorSelector: {__name__="foo"}
						- RHS: DuplicateFilter: {env="baz"}
							- ref#1 Duplicate ...
					- RHS: DuplicateFilter: {env="bar"}
						- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSubsetSelectorsEliminated:    2,
			expectedSelectorsInspected:           3,
		},
		"multiple subsets of same selector, broadest selector neither first nor last": {
			expr: `foo{env="baz"} + foo + foo{env="bar"}`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: DuplicateFilter: {env="baz"}
							- ref#1 Duplicate
								- VectorSelector: {__name__="foo"}
						- RHS: ref#1 Duplicate ...
					- RHS: DuplicateFilter: {env="bar"}
						- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 0,
			expectedSubsetSelectorsEliminated:    2,
			expectedSelectorsInspected:           3,
		},
		"duplicated expressions containing subset selector children and other siblings, other siblings are the same": {
			expr: `topk(5, foo) + topk(5, foo{env="bar"}) + topk(5, foo) + topk(5, foo{env="bar"})`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: BinaryExpression: LHS + RHS
							- LHS: ref#2 Duplicate
								- AggregateExpression: topk
									- expression: ref#1 Duplicate
										- VectorSelector: {__name__="foo"}
									- parameter: NumberLiteral: 5
							- RHS: ref#3 Duplicate
								- AggregateExpression: topk
									- expression: DuplicateFilter: {env="bar"}
										- ref#1 Duplicate ...
									- parameter: NumberLiteral: 5
						- RHS: ref#2 Duplicate ...
					- RHS: ref#3 Duplicate ...
			`,
			expectedDuplicateNodes:               3,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSubsetSelectorsEliminated:    2,
			expectedSelectorsInspected:           4,
		},
		"duplicated expressions containing subset selector children and other siblings, other siblings are not the same": {
			expr: `topk(5, foo) + topk(5, foo{env="bar"}) + topk(3, foo) + topk(3, foo{env="bar"})`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: BinaryExpression: LHS + RHS
							- LHS: AggregateExpression: topk
								- expression: ref#1 Duplicate
									- VectorSelector: {__name__="foo"}
								- parameter: NumberLiteral: 5
							- RHS: AggregateExpression: topk
								- expression: DuplicateFilter: {env="bar"}
									- ref#1 Duplicate ...
								- parameter: NumberLiteral: 5
						- RHS: AggregateExpression: topk
							- expression: ref#1 Duplicate ...
							- parameter: NumberLiteral: 3
					- RHS: AggregateExpression: topk
						- expression: DuplicateFilter: {env="bar"}
							- ref#1 Duplicate ...
						- parameter: NumberLiteral: 3
			`,
			expectedDuplicateNodes:               1,
			expectedDuplicateSelectorsEliminated: 1,
			expectedSubsetSelectorsEliminated:    2,
			expectedSelectorsInspected:           4,
		},
		"subset vector selectors with different time ranges": {
			expr:                       `foo + foo{env="bar"} offset 10m`,
			expectUnchanged:            true,
			expectedSelectorsInspected: 2,
		},
		"subset matrix selectors with different time ranges": {
			expr:                       `rate(foo[2m]) + rate(foo{env="bar"}[2m] offset 10m)`,
			expectUnchanged:            true,
			expectedSelectorsInspected: 2,
		},
	}

	ctx := context.Background()
	observer := streamingpromql.NoopPlanningObserver{}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			optsWithoutOptimizationPass := streamingpromql.NewTestEngineOpts()
			plannerWithoutOptimizationPass, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(optsWithoutOptimizationPass, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)
			plannerWithoutOptimizationPass.RegisterASTOptimizationPass(&ast.SortLabelsAndMatchers{})
			plannerWithoutOptimizationPass.RegisterASTOptimizationPass(&ast.CollapseConstants{})

			optsWithOptimizationPass := streamingpromql.NewTestEngineOpts()
			plannerWithOptimizationPass, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(optsWithOptimizationPass, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)
			plannerWithOptimizationPass.RegisterASTOptimizationPass(&ast.SortLabelsAndMatchers{})
			plannerWithOptimizationPass.RegisterASTOptimizationPass(&ast.CollapseConstants{})
			plannerWithOptimizationPass.RegisterQueryPlanOptimizationPass(commonsubexpressionelimination.NewOptimizationPass(true, optsWithOptimizationPass.CommonOpts.Reg, optsWithOptimizationPass.Logger))

			var timeRange types.QueryTimeRange

			if testCase.rangeQuery {
				timeRange = types.NewRangeQueryTimeRange(time.Now(), time.Now().Add(time.Hour), time.Minute)
			} else {
				timeRange = types.NewInstantQueryTimeRange(time.Now())
			}

			if testCase.expectUnchanged {
				p, err := plannerWithoutOptimizationPass.NewQueryPlan(ctx, testCase.expr, timeRange, false, observer)
				require.NoError(t, err)
				testCase.expectedPlan = p.String()
			}

			p, err := plannerWithOptimizationPass.NewQueryPlan(ctx, testCase.expr, timeRange, false, observer)
			require.NoError(t, err)
			actual := p.String()
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)

			// Type assertion since we need a Gatherer need to verify the metrics emitted by the optimization pass.
			reg := optsWithOptimizationPass.CommonOpts.Reg.(*prometheus.Registry)
			requireDuplicateNodeCount(t, reg, testCase.expectedDuplicateNodes)
			requireSelectorCounts(t, reg, testCase.expectedSelectorsInspected, testCase.expectedDuplicateSelectorsEliminated, testCase.expectedSubsetSelectorsEliminated)
		})
	}
}

func requireDuplicateNodeCount(t *testing.T, g prometheus.Gatherer, expected int) {
	const metricName = "cortex_mimir_query_engine_common_subexpression_elimination_duplication_nodes_introduced_total"

	expectedMetrics := fmt.Sprintf(`# HELP %v Number of duplication nodes introduced by the common subexpression elimination optimization pass.
# TYPE %v counter
%v %v
`, metricName, metricName, metricName, expected)

	require.NoError(t, testutil.GatherAndCompare(g, strings.NewReader(expectedMetrics), metricName))
}

func requireSelectorCounts(t *testing.T, g prometheus.Gatherer, expectedInspected int, expectedDuplicatesEliminated int, expectedSubsetsEliminated int) {
	const inspectedMetricName = "cortex_mimir_query_engine_common_subexpression_elimination_selectors_inspected_total"
	const eliminatedMetricName = "cortex_mimir_query_engine_common_subexpression_elimination_selectors_eliminated_total"

	expectedMetrics := fmt.Sprintf(`# HELP %[1]v Number of selectors inspected by the common subexpression elimination optimization pass, before elimination.
# TYPE %[1]v counter
%[1]v %[2]v
# HELP %[3]v Number of selectors eliminated by the common subexpression elimination optimization pass.
# TYPE %[3]v counter
%[3]v{reason="duplicate"} %[4]v
%[3]v{reason="subset"} %[5]v
`, inspectedMetricName, expectedInspected, eliminatedMetricName, expectedDuplicatesEliminated, expectedSubsetsEliminated)

	require.NoError(t, testutil.GatherAndCompare(g, strings.NewReader(expectedMetrics), inspectedMetricName, eliminatedMetricName))
}

func TestOptimizationPass_HintsHandling(t *testing.T) {
	testCases := map[string]struct {
		expr         string
		expectedPlan string
	}{
		"duplicate vector selector not eligible for skipping histogram decoding due to nesting": {
			expr: `histogram_sum(some_metric * histogram_quantile(0.5, some_metric))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_sum(...)
						- BinaryExpression: LHS * RHS
							- LHS: ref#1 Duplicate
								- VectorSelector: {__name__="some_metric"}
							- RHS: DeduplicateAndMerge
								- FunctionCall: histogram_quantile(...)
									- param 0: NumberLiteral: 0.5
									- param 1: ref#1 Duplicate ...
			`,
		},
		"duplicate vector selector with multiple levels of duplication": {
			expr: `histogram_sum(foo) + histogram_sum(foo) + histogram_count(foo) + histogram_count(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: BinaryExpression: LHS + RHS
							- LHS: ref#1 Duplicate
								- DeduplicateAndMerge
									- FunctionCall: histogram_sum(...)
										- ref#2 Duplicate
											- VectorSelector: {__name__="foo"}, skip histogram buckets
							- RHS: ref#1 Duplicate ...
						- RHS: ref#3 Duplicate
							- DeduplicateAndMerge
								- FunctionCall: histogram_count(...)
									- ref#2 Duplicate ...
					- RHS: ref#3 Duplicate ...
			`,
		},
		"duplicate vector selector, both eligible for skipping histogram decoding": {
			expr: `histogram_sum(some_metric) * histogram_count(some_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- ref#1 Duplicate
								- VectorSelector: {__name__="some_metric"}, skip histogram buckets
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_count(...)
							- ref#1 Duplicate ...
			`,
		},
		"duplicate vector selector, only first instance eligible for skipping histogram decoding": {
			expr: `histogram_sum(some_metric) * histogram_quantile(0.5, some_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- ref#1 Duplicate
								- VectorSelector: {__name__="some_metric"}
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_quantile(...)
							- param 0: NumberLiteral: 0.5
							- param 1: ref#1 Duplicate ...
			`,
		},
		"duplicate vector selector, only second instance eligible for skipping histogram decoding": {
			expr: `histogram_quantile(0.5, some_metric) * histogram_sum(some_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_quantile(...)
							- param 0: NumberLiteral: 0.5
							- param 1: ref#1 Duplicate
								- VectorSelector: {__name__="some_metric"}
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- ref#1 Duplicate ...
			`,
		},
		"duplicate matrix selector not eligible for skipping histogram decoding due to nesting": {
			expr: `histogram_sum(rate(some_metric[1m]) * histogram_quantile(0.5, rate(some_metric[1m])))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_sum(...)
						- BinaryExpression: LHS * RHS
							- LHS: ref#1 Duplicate
								- DeduplicateAndMerge
									- FunctionCall: rate(...)
										- MatrixSelector: {__name__="some_metric"}[1m0s]
							- RHS: DeduplicateAndMerge
								- FunctionCall: histogram_quantile(...)
									- param 0: NumberLiteral: 0.5
									- param 1: ref#1 Duplicate ...
			`,
		},
		"duplicate matrix selector, both eligible for skipping histogram decoding": {
			expr: `histogram_sum(rate(some_metric[1m])) * histogram_count(rate(some_metric[1m]))`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- ref#1 Duplicate
								- DeduplicateAndMerge
									- FunctionCall: rate(...)
										- MatrixSelector: {__name__="some_metric"}[1m0s], skip histogram buckets
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_count(...)
							- ref#1 Duplicate ...
			`,
		},
		"duplicate matrix selector, only first instance eligible for skipping histogram decoding": {
			expr: `histogram_sum(rate(some_metric[1m])) * histogram_quantile(0.5, rate(some_metric[1m]))`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- ref#1 Duplicate
								- DeduplicateAndMerge
									- FunctionCall: rate(...)
										- MatrixSelector: {__name__="some_metric"}[1m0s]
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_quantile(...)
							- param 0: NumberLiteral: 0.5
							- param 1: ref#1 Duplicate ...
			`,
		},
		"duplicate matrix selector, only second instance eligible for skipping histogram decoding": {
			expr: `histogram_quantile(0.5, rate(some_metric[1m])) * histogram_sum(rate(some_metric[1m]))`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_quantile(...)
							- param 0: NumberLiteral: 0.5
							- param 1: ref#1 Duplicate
								- DeduplicateAndMerge
									- FunctionCall: rate(...)
										- MatrixSelector: {__name__="some_metric"}[1m0s]
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- ref#1 Duplicate ...
			`,
		},
		"subset vector selector not eligible for skipping histogram decoding due to nesting": {
			expr: `histogram_sum(some_metric * histogram_quantile(0.5, some_metric{env="bar"}))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_sum(...)
						- BinaryExpression: LHS * RHS
							- LHS: ref#1 Duplicate
								- VectorSelector: {__name__="some_metric"}
							- RHS: DeduplicateAndMerge
								- FunctionCall: histogram_quantile(...)
									- param 0: NumberLiteral: 0.5
									- param 1: DuplicateFilter: {env="bar"}
										- ref#1 Duplicate ...
			`,
		},
		"subset vector selectors, both eligible for skipping histogram decoding": {
			expr: `histogram_sum(some_metric) * histogram_count(some_metric{env="bar"})`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- ref#1 Duplicate
								- VectorSelector: {__name__="some_metric"}, skip histogram buckets
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_count(...)
							- DuplicateFilter: {env="bar"}
								- ref#1 Duplicate ...
			`,
		},
		"subset vector selectors, only broader instance eligible for skipping histogram decoding": {
			expr: `histogram_sum(some_metric) * histogram_quantile(0.5, some_metric{env="bar"})`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- ref#1 Duplicate
								- VectorSelector: {__name__="some_metric"}
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_quantile(...)
							- param 0: NumberLiteral: 0.5
							- param 1: DuplicateFilter: {env="bar"}
								- ref#1 Duplicate ...
			`,
		},
		"subset vector selectors, only narrower instance eligible for skipping histogram decoding": {
			expr: `histogram_sum(some_metric{env="bar"}) * histogram_quantile(0.5, some_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- DuplicateFilter: {env="bar"}
								- ref#1 Duplicate
									- VectorSelector: {__name__="some_metric"}
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_quantile(...)
							- param 0: NumberLiteral: 0.5
							- param 1: ref#1 Duplicate ...
			`,
		},
	}

	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	observer := streamingpromql.NoopPlanningObserver{}

	opts := streamingpromql.NewTestEngineOpts()
	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	planner.RegisterASTOptimizationPass(&ast.SortLabelsAndMatchers{})
	planner.RegisterQueryPlanOptimizationPass(plan.NewSkipHistogramDecodingOptimizationPass())
	planner.RegisterQueryPlanOptimizationPass(commonsubexpressionelimination.NewOptimizationPass(true, nil, opts.Logger))

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {

			p, err := planner.NewQueryPlan(ctx, testCase.expr, timeRange, false, observer)
			require.NoError(t, err)
			actual := p.String()
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)
		})
	}
}

func TestOptimizationPass_SubsetSelectorEliminationDisabled(t *testing.T) {
	runTest := func(t *testing.T, expr string, enabled bool, maxSupportedQueryPlanVersion planning.QueryPlanVersion, expectedPlan string) {
		ctx := context.Background()
		timeRange := types.NewInstantQueryTimeRange(time.Now())
		observer := streamingpromql.NoopPlanningObserver{}

		opts := streamingpromql.NewTestEngineOpts()
		planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewStaticQueryPlanVersionProvider(maxSupportedQueryPlanVersion))
		require.NoError(t, err)
		planner.RegisterASTOptimizationPass(&ast.SortLabelsAndMatchers{})
		planner.RegisterQueryPlanOptimizationPass(commonsubexpressionelimination.NewOptimizationPass(enabled, nil, opts.Logger))

		plan, err := planner.NewQueryPlan(ctx, expr, timeRange, false, observer)
		require.NoError(t, err)
		require.Equal(t, testutils.TrimIndent(expectedPlan), plan.String())
	}

	expr := `foo + foo{env="bar"}`

	expectedPlanWithoutSSE := `
		- BinaryExpression: LHS + RHS
			- LHS: VectorSelector: {__name__="foo"}
			- RHS: VectorSelector: {__name__="foo", env="bar"}
	`

	expectedPlanWithSSE := `
		- BinaryExpression: LHS + RHS
			- LHS: ref#1 Duplicate
				- VectorSelector: {__name__="foo"}
			- RHS: DuplicateFilter: {env="bar"}
				- ref#1 Duplicate ...
	`

	t.Run("feature enabled, querier supports SSE", func(t *testing.T) {
		runTest(t, expr, true, planning.QueryPlanV6, expectedPlanWithSSE)
	})

	t.Run("feature disabled, querier supports SSE", func(t *testing.T) {
		runTest(t, expr, false, planning.QueryPlanV6, expectedPlanWithoutSSE)
	})

	t.Run("feature enabled, querier does not support SSE", func(t *testing.T) {
		runTest(t, expr, true, planning.QueryPlanV5, expectedPlanWithoutSSE)
	})

	t.Run("feature disabled, querier does not support SSE", func(t *testing.T) {
		runTest(t, expr, false, planning.QueryPlanV5, expectedPlanWithoutSSE)
	})
}

func BenchmarkOptimizationPass(b *testing.B) {
	testCases := []string{
		`foo`,
		`foo[5m]`,
		`foo[5m:10s]`,
		`1 + 1`,
		`label_join(foo, "abc", "-") + label_join(bar, "def", ",")`,
		`foo + foo`,
		`foo + foo + bar`,
		`foo + foo + foo`,
		`foo + foo + foo + bar + foo`,
		`max(foo) - min(foo)`,
		`max(foo) + max(foo)`,
		`a + sum(a) + sum(a)`,
		`(a - a) + (a - a)`,
		`(a - a) + (a - a) + (a * b) + (a * b)`,
		`(a - b) + (a - b)`,
		`(rate(a[5m]) - rate(b[5m])) + (rate(a[5m]) - rate(b[5m]))`,
		`foo + rate(foo[5m])`,
		`rate(foo[5m]) + increase(foo[5m])`,
		`rate(foo[5m]) + increase(foo[5m]) + rate(foo[5m])`,
		`rate(foo[5m]) + rate(foo[5m])`,
		`rate(foo[5m:]) + increase(foo[5m:])`,
		`rate((a - b)[5m:]) + increase((a - b)[5m:])`,
		`rate(foo[5m:]) + rate(foo[5m:])`,
		`max_over_time(rate(foo[5m:])[10m:]) + max_over_time(rate(foo[5m:])[10m:])`,
		`max_over_time(rate(foo[5m:])[10m:]) + max_over_time(rate(foo[5m:])[7m:])`,
		`timestamp(foo) + timestamp(foo)`,
		`timestamp(foo) + foo`,
		`timestamp(abs(foo)) + foo`,
		`topk(3, foo) + topk(5, foo)`,
		`topk(5, foo) + topk(5, foo)`,
		`histogram_count(some_metric) * histogram_quantile(0.5, some_metric)`,
		`histogram_count(some_metric) * histogram_sum(some_metric)`,
		`clamp_min(rate(foo[5m]), 1) + clamp_min(rate(foo[5m]), 1)`,
		`clamp_min(rate(foo[5m]), 1) + clamp_max(rate(foo[5m]), 1)`,
		`max(rate(foo[5m])) + min(rate(foo[5m]))`,
		`foo + foo + foo + foo + foo + foo + foo + foo + foo + foo + foo + foo + foo + foo + foo + foo + foo + foo + foo + foo + foo + foo + foo + foo + foo`,
		`a_1 + a_2 + a_3 + a_4 + a_5 + a_6 + a_7 + a_8 + a_9 + a_10 + a_11 + a_12 + a_13 + a_14 + a_15 + a_16 + a_17 + a_18 + a_19 + a_20 + a_21 + a_22 + a_23 + a_24 + a_25`,
		`__sharded_concat__(a, b, c, d) + __sharded_concat__(a, b, c, d)`,
		`__sharded_concat__(a, b, c, d, e, f, g, h) + __sharded_concat__(a, b, c, d, e, f, g, h)`,
	}

	opts := streamingpromql.NewTestEngineOpts()
	ctx := context.Background()
	observer := streamingpromql.NoopPlanningObserver{}

	reg := prometheus.NewPedanticRegistry()
	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(b, err)
	planner.RegisterQueryPlanOptimizationPass(commonsubexpressionelimination.NewOptimizationPass(true, reg, opts.Logger))

	timeRange := types.NewInstantQueryTimeRange(time.Now())

	for _, expr := range testCases {
		b.Run(expr, func(b *testing.B) {
			for b.Loop() {
				_, err := planner.NewQueryPlan(ctx, expr, timeRange, false, observer)

				if err != nil {
					require.NoError(b, err)
				}
			}
		})
	}
}

func TestShouldSkipChild(t *testing.T) {
	pass := commonsubexpressionelimination.NewOptimizationPass(true, nil, nil)

	// Test info function - should skip only 2nd children
	infoFunctionCall := &core.FunctionCall{
		FunctionCallDetails: &core.FunctionCallDetails{
			Function: functions.FUNCTION_INFO,
		},
	}

	require.False(t, pass.ShouldSkipChild(infoFunctionCall, 0), "1st argument to info function should not be skipped")
	require.True(t, pass.ShouldSkipChild(infoFunctionCall, 1), "2nd argument to info function should be skipped")

	// Test other function - should not skip any children
	otherFunctionCall := &core.FunctionCall{
		FunctionCallDetails: &core.FunctionCallDetails{
			Function: functions.FUNCTION_RATE,
		},
	}

	require.False(t, pass.ShouldSkipChild(otherFunctionCall, 0), "1st argument to other function should not be skipped")
	require.False(t, pass.ShouldSkipChild(otherFunctionCall, 1), "2nd argument to other function should not be skipped")

	// Test non-function node - should not skip any children
	nonFunctionNode := &core.VectorSelector{}
	require.False(t, pass.ShouldSkipChild(nonFunctionNode, 0), "non-function node children should not be skipped")
}

func TestSelectorsAreDuplicateOrSubset(t *testing.T) {
	testCases := map[string]struct {
		firstSelector          string
		secondSelector         string
		expectedResult         commonsubexpressionelimination.SelectorRelationship
		expectedSubsetMatchers []*core.LabelMatcher
	}{
		"empty matchers": {
			firstSelector:  `{}`,
			secondSelector: `{}`,
			expectedResult: commonsubexpressionelimination.ExactDuplicateSelectors,
		},
		"duplicate selectors, single matcher": {
			firstSelector:  `{foo="bar"}`,
			secondSelector: `{foo="bar"}`,
			expectedResult: commonsubexpressionelimination.ExactDuplicateSelectors,
		},
		"duplicate selectors, multiple matchers": {
			firstSelector:  `{foo="bar", baz="qux"}`,
			secondSelector: `{foo="bar", baz="qux"}`,
			expectedResult: commonsubexpressionelimination.ExactDuplicateSelectors,
		},
		"different selectors, one matcher with different label value": {
			firstSelector:  `{foo="bar"}`,
			secondSelector: `{foo="baz"}`,
			expectedResult: commonsubexpressionelimination.NotDuplicateOrSubset,
		},
		"different selectors, one matcher with different label name": {
			firstSelector:  `{foo="bar"}`,
			secondSelector: `{baz="bar"}`,
			expectedResult: commonsubexpressionelimination.NotDuplicateOrSubset,
		},
		"different selectors, one matcher with different match type": {
			firstSelector:  `{foo="bar"}`,
			secondSelector: `{foo!="bar"}`,
			expectedResult: commonsubexpressionelimination.NotDuplicateOrSubset,
		},
		"different selectors, one matcher completely different": {
			firstSelector:  `{foo="bar"}`,
			secondSelector: `{baz!="qux"}`,
			expectedResult: commonsubexpressionelimination.NotDuplicateOrSubset,
		},
		"different selectors, some matchers the same, some not": {
			firstSelector:  `{foo="bar", baz="qux"}`,
			secondSelector: `{foo="bar", baz="bar"}`,
			expectedResult: commonsubexpressionelimination.NotDuplicateOrSubset,
		},
		"different selectors, different number of matchers": {
			firstSelector:  `{foo="bar"}`,
			secondSelector: `{bar="abc", baz="qux"}`,
			expectedResult: commonsubexpressionelimination.NotDuplicateOrSubset,
		},
		"second selector is subset of first, first matcher is different": {
			firstSelector:  `{b="1", d="3"}`,
			secondSelector: `{a="0", b="1", d="3"}`,
			expectedResult: commonsubexpressionelimination.SubsetSelectors,
			expectedSubsetMatchers: []*core.LabelMatcher{
				{Name: "a", Type: labels.MatchEqual, Value: "0"},
			},
		},
		"second selector is subset of first, middle matcher is different": {
			firstSelector:  `{b="1", d="3"}`,
			secondSelector: `{b="1", c="2", d="3"}`,
			expectedResult: commonsubexpressionelimination.SubsetSelectors,
			expectedSubsetMatchers: []*core.LabelMatcher{
				{Name: "c", Type: labels.MatchEqual, Value: "2"},
			},
		},
		"second selector is subset of first, last matcher is different": {
			firstSelector:  `{b="1", d="3"}`,
			secondSelector: `{b="1", d="3", e="4"}`,
			expectedResult: commonsubexpressionelimination.SubsetSelectors,
			expectedSubsetMatchers: []*core.LabelMatcher{
				{Name: "e", Type: labels.MatchEqual, Value: "4"},
			},
		},
		"second selector is subset of first, multiple additional matchers": {
			firstSelector:  `{c="2", f="5"}`,
			secondSelector: `{a="0", "b"="1", c="2", d="3", e="4", f="5", g="6", h="7"}`,
			expectedResult: commonsubexpressionelimination.SubsetSelectors,
			expectedSubsetMatchers: []*core.LabelMatcher{
				{Name: "a", Type: labels.MatchEqual, Value: "0"},
				{Name: "b", Type: labels.MatchEqual, Value: "1"},
				{Name: "d", Type: labels.MatchEqual, Value: "3"},
				{Name: "e", Type: labels.MatchEqual, Value: "4"},
				{Name: "g", Type: labels.MatchEqual, Value: "6"},
				{Name: "h", Type: labels.MatchEqual, Value: "7"},
			},
		},

		// FIXME: it'd be nice to support this case, but this is currently not supported.
		"second selector is subset of first when considering regex, narrower selector uses regex": {
			firstSelector:  `{a=~"(a|b|c)"}`,
			secondSelector: `{a=~"(a|b)"}`,
			expectedResult: commonsubexpressionelimination.NotDuplicateOrSubset,
		},
		"second selector is subset of first when considering regex, narrower selector uses exact matcher": {
			firstSelector:  `{a=~"(a|b|c)"}`,
			secondSelector: `{a="a"}`,
			expectedResult: commonsubexpressionelimination.NotDuplicateOrSubset,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			switch testCase.expectedResult {
			case commonsubexpressionelimination.ExactDuplicateSelectors, commonsubexpressionelimination.NotDuplicateOrSubset:
				require.Empty(t, testCase.expectedSubsetMatchers, "invalid test case: must have no subset matchers when not subset selectors")
			case commonsubexpressionelimination.SubsetSelectors:
				require.NotEmpty(t, testCase.expectedSubsetMatchers, "invalid test case: must have subset matchers for subset selectors")
			}

			first := parseSelector(t, testCase.firstSelector)
			second := parseSelector(t, testCase.secondSelector)

			result, subsetMatchers := commonsubexpressionelimination.SelectorsAreDuplicateOrSubset(first, second)
			require.Equal(t, testCase.expectedResult, result)
			require.Equal(t, testCase.expectedSubsetMatchers, subsetMatchers)

			switch testCase.expectedResult {
			case commonsubexpressionelimination.ExactDuplicateSelectors, commonsubexpressionelimination.NotDuplicateOrSubset:
				// Check that the order doesn't matter.
				result, subsetMatchers := commonsubexpressionelimination.SelectorsAreDuplicateOrSubset(second, first)
				require.Equal(t, testCase.expectedResult, result)
				require.Empty(t, subsetMatchers)
			case commonsubexpressionelimination.SubsetSelectors:
				// Running the same call with the arguments in the other order should return no match.
				result, subsetMatchers := commonsubexpressionelimination.SelectorsAreDuplicateOrSubset(second, first)
				require.Equal(t, commonsubexpressionelimination.NotDuplicateOrSubset, result)
				require.Empty(t, subsetMatchers)
			}
		})
	}
}

func parseSelector(t *testing.T, selector string) []*core.LabelMatcher {
	matchers, err := parser.ParseMetricSelector(selector)
	require.NoError(t, err)

	slices.SortFunc(matchers, func(a, b *labels.Matcher) int {
		return core.CompareMatchers(a.Name, b.Name, a.Type, b.Type, a.Value, b.Value)
	})

	return core.LabelMatchersFromPrometheusType(matchers)
}

var (
	groupWithNoFilters = commonsubexpressionelimination.SharedSelectorGroup{}

	groupWithFilterOnEnvLabel = commonsubexpressionelimination.SharedSelectorGroup{
		Filters: [][]*core.LabelMatcher{
			{
				&core.LabelMatcher{
					Name:  "env",
					Type:  labels.MatchEqual,
					Value: "foo",
				},
			},
		},
	}

	groupWithFilterOnMetricName = commonsubexpressionelimination.SharedSelectorGroup{
		Filters: [][]*core.LabelMatcher{
			{
				&core.LabelMatcher{
					Name:  "__name__",
					Type:  labels.MatchEqual,
					Value: "foo",
				},
			},
		},
	}
)

func TestIsSafeToApplyFilteringAfter(t *testing.T) {
	groupWithFilterOnManyLabels := commonsubexpressionelimination.SharedSelectorGroup{
		Filters: [][]*core.LabelMatcher{
			{
				&core.LabelMatcher{
					Name:  "env",
					Type:  labels.MatchEqual,
					Value: "foo",
				},
				&core.LabelMatcher{
					Name:  "region",
					Type:  labels.MatchEqual,
					Value: "foo",
				},
			},
			{
				&core.LabelMatcher{
					Name:  "cluster",
					Type:  labels.MatchEqual,
					Value: "foo",
				},
			},
		},
	}

	testCases := map[string]struct {
		node                                       planning.Node
		group                                      commonsubexpressionelimination.SharedSelectorGroup
		expectedSafeWithDelayedNameRemovalDisabled bool
		expectedSafeWithDelayedNameRemovalEnabled  bool
	}{
		"unary expression with no filters": {
			node: &core.UnaryExpression{
				UnaryExpressionDetails: &core.UnaryExpressionDetails{
					Op: core.UNARY_SUB,
				},
			},
			group: groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"unary expression with filter on __name__": {
			node: &core.UnaryExpression{
				UnaryExpressionDetails: &core.UnaryExpressionDetails{
					Op: core.UNARY_SUB,
				},
			},
			group: groupWithFilterOnMetricName,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"unary expression with filter on other label": {
			node: &core.UnaryExpression{
				UnaryExpressionDetails: &core.UnaryExpressionDetails{
					Op: core.UNARY_SUB,
				},
			},
			group: groupWithFilterOnEnvLabel,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},

		"aggregation with 'by' and no filter": {
			node: &core.AggregateExpression{
				AggregateExpressionDetails: &core.AggregateExpressionDetails{
					Op: core.AGGREGATION_SUM,
				},
			},
			group: groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"aggregation with 'by' and sole filter label does not appear in grouping labels": {
			node: &core.AggregateExpression{
				AggregateExpressionDetails: &core.AggregateExpressionDetails{
					Op:       core.AGGREGATION_SUM,
					Grouping: []string{"app"},
				},
			},
			group: groupWithFilterOnEnvLabel,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  false,
		},
		"aggregation with 'by' and sole filter label does appear in grouping labels": {
			node: &core.AggregateExpression{
				AggregateExpressionDetails: &core.AggregateExpressionDetails{
					Op:       core.AGGREGATION_SUM,
					Grouping: []string{"env"},
				},
			},
			group: groupWithFilterOnEnvLabel,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"aggregation with 'by' and only some filter labels appear in grouping labels": {
			node: &core.AggregateExpression{
				AggregateExpressionDetails: &core.AggregateExpressionDetails{
					Op:       core.AGGREGATION_SUM,
					Grouping: []string{"env"},
				},
			},
			group: groupWithFilterOnManyLabels,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  false,
		},
		"aggregation with 'by' and all filter labels appear in grouping labels": {
			node: &core.AggregateExpression{
				AggregateExpressionDetails: &core.AggregateExpressionDetails{
					Op:       core.AGGREGATION_SUM,
					Grouping: []string{"cluster", "env", "region"},
				},
			},
			group: groupWithFilterOnManyLabels,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},

		"aggregation with 'without' and no filter": {
			node: &core.AggregateExpression{
				AggregateExpressionDetails: &core.AggregateExpressionDetails{
					Op:      core.AGGREGATION_SUM,
					Without: true,
				},
			},
			group: groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"aggregation with 'without' and sole filter label does not appear in grouping labels": {
			node: &core.AggregateExpression{
				AggregateExpressionDetails: &core.AggregateExpressionDetails{
					Op:       core.AGGREGATION_SUM,
					Grouping: []string{"app"},
					Without:  true,
				},
			},
			group: groupWithFilterOnEnvLabel,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"aggregation with 'without' and sole filter label does appear in grouping labels": {
			node: &core.AggregateExpression{
				AggregateExpressionDetails: &core.AggregateExpressionDetails{
					Op:       core.AGGREGATION_SUM,
					Grouping: []string{"env"},
					Without:  true,
				},
			},
			group: groupWithFilterOnEnvLabel,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  false,
		},
		"aggregation with 'without' and no filter labels appear in grouping labels": {
			node: &core.AggregateExpression{
				AggregateExpressionDetails: &core.AggregateExpressionDetails{
					Op:       core.AGGREGATION_SUM,
					Grouping: []string{"app"},
					Without:  true,
				},
			},
			group: groupWithFilterOnManyLabels,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"aggregation with 'without' and only some filter labels appear in grouping labels": {
			node: &core.AggregateExpression{
				AggregateExpressionDetails: &core.AggregateExpressionDetails{
					Op:       core.AGGREGATION_SUM,
					Grouping: []string{"env"},
					Without:  true,
				},
			},
			group: groupWithFilterOnManyLabels,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  false,
		},
		"aggregation with 'without' and all filter labels appear in grouping labels": {
			node: &core.AggregateExpression{
				AggregateExpressionDetails: &core.AggregateExpressionDetails{
					Op:       core.AGGREGATION_SUM,
					Grouping: []string{"cluster", "env", "region"},
					Without:  true,
				},
			},
			group: groupWithFilterOnManyLabels,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  false,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, testCase.expectedSafeWithDelayedNameRemovalDisabled, commonsubexpressionelimination.IsSafeToApplyFilteringAfter(testCase.node, testCase.group, false))
			require.Equal(t, testCase.expectedSafeWithDelayedNameRemovalEnabled, commonsubexpressionelimination.IsSafeToApplyFilteringAfter(testCase.node, testCase.group, true))
		})
	}
}

func TestIsSafeToApplyFilteringAfterFunction(t *testing.T) {
	groupWithFilterOnBucketLabel := commonsubexpressionelimination.SharedSelectorGroup{
		Filters: [][]*core.LabelMatcher{
			{
				&core.LabelMatcher{
					Name:  "le",
					Type:  labels.MatchEqual,
					Value: "0.5",
				},
			},
		},
	}

	testCases := map[string]struct {
		function                                   functions.Function
		args                                       []string
		group                                      commonsubexpressionelimination.SharedSelectorGroup
		expectedSafeWithDelayedNameRemovalDisabled bool
		expectedSafeWithDelayedNameRemovalEnabled  bool
	}{
		"rate() with no filters": {
			function: functions.FUNCTION_RATE,
			group:    groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"rate() with some filters, none for __name__": {
			function: functions.FUNCTION_RATE,
			group:    groupWithFilterOnEnvLabel,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"rate() with some filters, some for __name__": {
			function: functions.FUNCTION_RATE,
			group:    groupWithFilterOnMetricName,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},

		"absent()": {
			function: functions.FUNCTION_ABSENT,
			group:    groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  false,
		},
		"absent_over_time()": {
			function: functions.FUNCTION_ABSENT_OVER_TIME,
			group:    groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  false,
		},

		"last_over_time()": {
			function: functions.FUNCTION_LAST_OVER_TIME,
			group:    groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"first_over_time()": {
			function: functions.FUNCTION_FIRST_OVER_TIME,
			group:    groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"sort()": {
			function: functions.FUNCTION_SORT,
			group:    groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"sort_desc()": {
			function: functions.FUNCTION_SORT_DESC,
			group:    groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"sort_by_label()": {
			function: functions.FUNCTION_SORT_BY_LABEL,
			group:    groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"sort_by_label_desc()": {
			function: functions.FUNCTION_SORT_BY_LABEL_DESC,
			group:    groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},

		"scalar()": {
			function: functions.FUNCTION_SCALAR,
			group:    groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  false,
		},
		"vector()": {
			function: functions.FUNCTION_VECTOR,
			group:    groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  false,
		},
		"info()": {
			function: functions.FUNCTION_INFO,
			group:    groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  false,
		},
		"pi()": {
			function: functions.FUNCTION_SCALAR,
			group:    groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  false,
		},
		"time()": {
			function: functions.FUNCTION_SCALAR,
			group:    groupWithNoFilters,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  false,
		},

		"label_join() where destination label isn't present in filters": {
			function: functions.FUNCTION_LABEL_JOIN,
			group:    groupWithNoFilters,
			args:     []string{"env", "-", "region"},
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"label_join() where destination label is present in filters": {
			function: functions.FUNCTION_LABEL_JOIN,
			group:    groupWithFilterOnEnvLabel,
			args:     []string{"env", "-", "region"},
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  false,
		},
		"label_replace() where destination label isn't present in filters": {
			function: functions.FUNCTION_LABEL_REPLACE,
			group:    groupWithNoFilters,
			args:     []string{"env", "$1", "region", "(.*)"},
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"label_replace() where destination label is present in filters": {
			function: functions.FUNCTION_LABEL_REPLACE,
			group:    groupWithFilterOnEnvLabel,
			args:     []string{"env", "$1", "region", "(.*)"},
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  false,
		},

		"histogram_fraction() with filter on le": {
			function: functions.FUNCTION_HISTOGRAM_FRACTION,
			group:    groupWithFilterOnBucketLabel,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  false,
		},
		"histogram_fraction() with filter on __name__": {
			function: functions.FUNCTION_HISTOGRAM_FRACTION,
			group:    groupWithFilterOnMetricName,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"histogram_fraction() with filtering on neither le nor __name__": {
			function: functions.FUNCTION_HISTOGRAM_FRACTION,
			group:    groupWithFilterOnEnvLabel,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"histogram_quantile() with filter on le": {
			function: functions.FUNCTION_HISTOGRAM_QUANTILE,
			group:    groupWithFilterOnBucketLabel,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  false,
		},
		"histogram_quantile() with filter on __name__": {
			function: functions.FUNCTION_HISTOGRAM_QUANTILE,
			group:    groupWithFilterOnMetricName,
			expectedSafeWithDelayedNameRemovalDisabled: false,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
		"histogram_quantile() with filtering on neither le nor __name__": {
			function: functions.FUNCTION_HISTOGRAM_QUANTILE,
			group:    groupWithFilterOnEnvLabel,
			expectedSafeWithDelayedNameRemovalDisabled: true,
			expectedSafeWithDelayedNameRemovalEnabled:  true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			fn := &core.FunctionCall{
				FunctionCallDetails: &core.FunctionCallDetails{
					Function: testCase.function,
				},
			}

			if len(testCase.args) > 0 {
				args := make([]planning.Node, 0, len(testCase.args)+1)
				args = append(args, &core.VectorSelector{})
				for _, arg := range testCase.args {
					args = append(args, &core.StringLiteral{
						StringLiteralDetails: &core.StringLiteralDetails{
							Value: arg,
						},
					})
				}

				fn.Args = args
			}

			t.Run("delayed name removal disabled", func(t *testing.T) {
				safe, knownFunction := commonsubexpressionelimination.IsSafeToApplyFilteringAfterFunction(fn, testCase.group, false)
				require.True(t, knownFunction)
				require.Equal(t, testCase.expectedSafeWithDelayedNameRemovalDisabled, safe)
			})

			t.Run("delayed name removal enabled", func(t *testing.T) {
				safe, knownFunction := commonsubexpressionelimination.IsSafeToApplyFilteringAfterFunction(fn, testCase.group, true)
				require.True(t, knownFunction)
				require.Equal(t, testCase.expectedSafeWithDelayedNameRemovalEnabled, safe)
			})
		})
	}
}

func TestIsSafeToApplyFilteringAfterFunction_HandlesAllKnownFunctions(t *testing.T) {
	group := commonsubexpressionelimination.SharedSelectorGroup{}

	for name, function := range functions.Function_value {
		if functions.Function(function) == functions.FUNCTION_UNKNOWN {
			continue
		}

		t.Run(name, func(t *testing.T) {
			fn := &core.FunctionCall{
				FunctionCallDetails: &core.FunctionCallDetails{
					Function: functions.Function(function),
				},
			}

			_, knownFunction := commonsubexpressionelimination.IsSafeToApplyFilteringAfterFunction(fn, group, false)
			require.True(t, knownFunction, "IsSafeToApplyFilteringAfterFunction does not know how to handle this function")
		})
	}
}
