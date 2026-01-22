// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
	_ "github.com/grafana/mimir/pkg/streamingpromql/optimize/ast/sharding" // Imported for side effects: registering the __sharded_concat__ function with the parser.
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
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
		expr                        string
		rangeQuery                  bool
		expectedPlan                string
		expectUnchanged             bool
		expectedDuplicateNodes      int // Check that we don't do unnecessary work introducing a duplicate node multiple times when starting from different selectors (eg. when handling (a+b) + (a+b)).
		expectedSelectorsEliminated int
		expectedSelectorsInspected  int
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  3,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 2,
			expectedSelectorsInspected:  3,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 3,
			expectedSelectorsInspected:  5,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			expectedDuplicateNodes:      2,
			expectedSelectorsEliminated: 2,
			expectedSelectorsInspected:  3,
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
			expectedDuplicateNodes:      2,
			expectedSelectorsEliminated: 3,
			expectedSelectorsInspected:  4,
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
			expectedDuplicateNodes:      3,
			expectedSelectorsEliminated: 6, // 5 instances of 'a', and one instance of 'b'
			expectedSelectorsInspected:  8,
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
			expectedDuplicateNodes:      1, // This test ensures that we don't do unnecessary work when traversing up from both the a and b selectors.
			expectedSelectorsEliminated: 2,
			expectedSelectorsInspected:  4,
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
			expectedDuplicateNodes:      1, // This test ensures that we don't do unnecessary work when traversing up from both the a and b selectors.
			expectedSelectorsEliminated: 2,
			expectedSelectorsInspected:  4,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			rangeQuery:                  false,
			expectedDuplicateNodes:      2,
			expectedSelectorsEliminated: 2,
			expectedSelectorsInspected:  3,
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
			rangeQuery:                  true,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1, // We only eliminate one 'foo' selector in the duplicate rate(...) expression.
			expectedSelectorsInspected:  3,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			rangeQuery:                  false,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			rangeQuery:                  true,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			rangeQuery:                  false,
			expectedDuplicateNodes:      1, // This test ensures that we don't do unnecessary work when traversing up from both the a and b selectors.
			expectedSelectorsEliminated: 2,
			expectedSelectorsInspected:  4,
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
			rangeQuery:                  true,
			expectedDuplicateNodes:      1, // This test ensures that we don't do unnecessary work when traversing up from both the a and b selectors.
			expectedSelectorsEliminated: 2,
			expectedSelectorsInspected:  4,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
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
			expectedDuplicateNodes:      2,
			expectedSelectorsEliminated: 2,
			expectedSelectorsInspected:  5,
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
			expectedDuplicateNodes:      2,
			expectedSelectorsEliminated: 2,
			expectedSelectorsInspected:  5,
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
			expectedDuplicateNodes:      0,
			expectedSelectorsEliminated: 0,
			expectedSelectorsInspected:  2,
		},
		"info function": {
			// This test verifies that CSE optimization correctly skips the 2nd argument to info function,
			// allowing only the 1st argument to be deduplicated.
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  3,
		},
	}

	ctx := context.Background()
	observer := streamingpromql.NoopPlanningObserver{}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			opts1 := streamingpromql.NewTestEngineOpts()
			plannerWithoutOptimizationPass, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts1, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)
			plannerWithoutOptimizationPass.RegisterASTOptimizationPass(&ast.CollapseConstants{})

			opts2 := streamingpromql.NewTestEngineOpts()
			plannerWithOptimizationPass, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts2, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)
			plannerWithOptimizationPass.RegisterASTOptimizationPass(&ast.CollapseConstants{})
			plannerWithOptimizationPass.RegisterQueryPlanOptimizationPass(commonsubexpressionelimination.NewOptimizationPass(true, opts2.CommonOpts.Reg, opts2.Logger))

			var timeRange types.QueryTimeRange

			if testCase.rangeQuery {
				timeRange = types.NewRangeQueryTimeRange(time.Now(), time.Now().Add(time.Hour), time.Minute)
			} else {
				timeRange = types.NewInstantQueryTimeRange(time.Now())
			}

			if testCase.expectUnchanged {
				p, err := plannerWithoutOptimizationPass.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
				require.NoError(t, err)
				testCase.expectedPlan = p.String()
			}

			p, err := plannerWithOptimizationPass.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
			require.NoError(t, err)
			actual := p.String()
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)

			// Type assertion since we need a Gatherer need to verify the metrics emitted by the optimization pass.
			reg := opts2.CommonOpts.Reg.(*prometheus.Registry)
			requireDuplicateNodeCount(t, reg, testCase.expectedDuplicateNodes)
			requireSelectorCounts(t, reg, testCase.expectedSelectorsInspected, testCase.expectedSelectorsEliminated)
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

func requireSelectorCounts(t *testing.T, g prometheus.Gatherer, expectedInspected int, expectedEliminated int) {
	const inspectedMetricName = "cortex_mimir_query_engine_common_subexpression_elimination_selectors_inspected_total"
	const eliminatedMetricName = "cortex_mimir_query_engine_common_subexpression_elimination_selectors_eliminated_total"

	expectedMetrics := fmt.Sprintf(`# HELP %[1]v Number of selectors inspected by the common subexpression elimination optimization pass, before elimination.
# TYPE %[1]v counter
%[1]v %[2]v
# HELP %[3]v Number of selectors eliminated by the common subexpression elimination optimization pass.
# TYPE %[3]v counter
%[3]v %v
`, inspectedMetricName, expectedInspected, eliminatedMetricName, expectedEliminated)

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
	}

	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	observer := streamingpromql.NoopPlanningObserver{}

	opts := streamingpromql.NewTestEngineOpts()
	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	planner.RegisterQueryPlanOptimizationPass(plan.NewSkipHistogramDecodingOptimizationPass())
	planner.RegisterQueryPlanOptimizationPass(commonsubexpressionelimination.NewOptimizationPass(true, nil, opts.Logger))

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {

			p, err := planner.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
			require.NoError(t, err)
			actual := p.String()
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)
		})
	}
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
				_, err := planner.NewQueryPlan(ctx, expr, timeRange, observer)

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
