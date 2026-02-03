// SPDX-License-Identifier: AGPL-3.0-only

package plan_test

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestEliminateDeduplicateAndMergeOptimizationPassPlan(t *testing.T) {
	testCases := map[string]struct {
		expr                                     string
		expectedPlanWithoutDelayedNameRemoval    string
		expectedPlanWithDelayedNameRemoval       string
		nodesEliminatedWithoutDelayedNameRemoval int
		nodesEliminatedWithDelayedNameRemoval    int
	}{
		"function where selector has exact name matcher": {
			// DeduplicateAndMerge is eliminated in both plans, exact name matcher guarantees unique series.
			expr: `rate(foo[5m])`,
			expectedPlanWithoutDelayedNameRemoval: `
				- FunctionCall: rate(...)
					- MatrixSelector: {__name__="foo"}[5m0s]
			`,
			expectedPlanWithDelayedNameRemoval: `
				- DropName
					- FunctionCall: rate(...)
						- MatrixSelector: {__name__="foo"}[5m0s]
			`,
			nodesEliminatedWithoutDelayedNameRemoval: 1,
			nodesEliminatedWithDelayedNameRemoval:    1,
		},
		"function where selector has no name matcher": {
			// DeduplicateAndMerge is kept in both plans, matcher without exact name might produce non-unique series.
			expr: `rate({job="test"}[5m])`,
			expectedPlanWithoutDelayedNameRemoval: `
				- DeduplicateAndMerge
					- FunctionCall: rate(...)
						- MatrixSelector: {job="test"}[5m0s]
			`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- FunctionCall: rate(...)
							- MatrixSelector: {job="test"}[5m0s]
			`,
			nodesEliminatedWithoutDelayedNameRemoval: 0,
			nodesEliminatedWithDelayedNameRemoval:    0,
		},
		"function where selector has regex name matcher": {
			// DeduplicateAndMerge is kept in both plans, regex matcher might produce non-unique series.
			expr: `rate({__name__=~"(foo|bar)"}[5m])`,
			expectedPlanWithoutDelayedNameRemoval: `
				- DeduplicateAndMerge
					- FunctionCall: rate(...)
						- MatrixSelector: {__name__=~"(foo|bar)"}[5m0s]
			`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__=~"(foo|bar)"}[5m0s]
			`,
			nodesEliminatedWithoutDelayedNameRemoval: 0,
			nodesEliminatedWithDelayedNameRemoval:    0,
		},
		"nested functions where selector has exact name matcher": {
			// DeduplicateAndMerge is eliminated for both of the functions in both plans, exact name matcher guarantees unique series.
			expr: `abs(rate(foo[5m]))`,
			expectedPlanWithoutDelayedNameRemoval: `	
				- FunctionCall: abs(...)
					- FunctionCall: rate(...)
						- MatrixSelector: {__name__="foo"}[5m0s]
			`,
			expectedPlanWithDelayedNameRemoval: `
				- DropName
					- FunctionCall: abs(...)
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__="foo"}[5m0s]
			`,
			nodesEliminatedWithoutDelayedNameRemoval: 2,
			nodesEliminatedWithDelayedNameRemoval:    2,
		},
		"nested functions where selector has no name matcher": {
			expr: `abs(rate({job="test"}[5m]))`,
			// DeduplicateAndMerge nodes eliminated, except the closest to the selector.
			// Name is dropped immediately, so first DeduplicateAndMerge node deduplicates series and there shouldn't be any duplicates after it.
			expectedPlanWithoutDelayedNameRemoval: `
				- FunctionCall: abs(...)
					- DeduplicateAndMerge
						- FunctionCall: rate(...)
							- MatrixSelector: {job="test"}[5m0s]
			`,
			// DeduplicateAndMerge nodes eliminated, except the root one.
			// Duplicates might appear after __name__ is dropped, which happens at the very end of query execution, so we deduplicate after it.
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- FunctionCall: abs(...)
							- FunctionCall: rate(...)
								- MatrixSelector: {job="test"}[5m0s]
			`,
			nodesEliminatedWithoutDelayedNameRemoval: 1,
			nodesEliminatedWithDelayedNameRemoval:    1,
		},
		"nested functions where selector has regex name matcher": {
			expr: `abs(rate({__name__=~"(foo|bar)"}[5m]))`,
			// DeduplicateAndMerge nodes eliminated, except the closest to the selector.
			// Name is dropped immediately, so first DeduplicateAndMerge node deduplicates series and there shouldn't be any duplicates after it.
			expectedPlanWithoutDelayedNameRemoval: `
				- FunctionCall: abs(...)
					- DeduplicateAndMerge
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__=~"(foo|bar)"}[5m0s]
			`,
			// DeduplicateAndMerge nodes eliminated, except the root one.
			// Duplicates might appear after __name__ is dropped, which happens at the very end of query execution, so we deduplicate after it.
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- FunctionCall: abs(...)
							- FunctionCall: rate(...)
								- MatrixSelector: {__name__=~"(foo|bar)"}[5m0s]
			`,
			nodesEliminatedWithoutDelayedNameRemoval: 1,
			nodesEliminatedWithDelayedNameRemoval:    1,
		},
		"deeply nested functions where selector has no name matcher": {
			expr: `abs(ceil(rate({job="test"}[5m])))`,
			// DeduplicateAndMerge nodes eliminated, except the closest to the selector.
			// Name is dropped immediately, so first DeduplicateAndMerge node deduplicates series and there shouldn't be any duplicates after it.
			expectedPlanWithoutDelayedNameRemoval: `
					- FunctionCall: abs(...)
						- FunctionCall: ceil(...)
							- DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {job="test"}[5m0s]
				`,
			// DeduplicateAndMerge nodes eliminated, except the root one.
			// Duplicates might appear after __name__ is dropped, which happens at the very end of query execution, so we deduplicate after it.
			expectedPlanWithDelayedNameRemoval: `
					- DeduplicateAndMerge
						- DropName
							- FunctionCall: abs(...)
								- FunctionCall: ceil(...)
									- FunctionCall: rate(...)
										- MatrixSelector: {job="test"}[5m0s]
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 2,
			nodesEliminatedWithDelayedNameRemoval:    2,
		},
		"function over subquery with exact name matcher": {
			expr: `max_over_time(foo[5m:1m])`,
			expectedPlanWithoutDelayedNameRemoval: `
				- FunctionCall: max_over_time(...)
					- Subquery: [5m0s:1m0s]
						- VectorSelector: {__name__="foo"}
			`,
			expectedPlanWithDelayedNameRemoval: `
				- DropName
					- FunctionCall: max_over_time(...)
						- Subquery: [5m0s:1m0s]
							- VectorSelector: {__name__="foo"}
			`,
			nodesEliminatedWithoutDelayedNameRemoval: 1,
			nodesEliminatedWithDelayedNameRemoval:    1,
		},
		"function over subquery without exact name matcher": {
			expr: `max_over_time({job="test"}[5m:1m])`,
			expectedPlanWithoutDelayedNameRemoval: `
				- DeduplicateAndMerge
					- FunctionCall: max_over_time(...)
						- Subquery: [5m0s:1m0s]
							- VectorSelector: {job="test"}
			`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- FunctionCall: max_over_time(...)
							- Subquery: [5m0s:1m0s]
								- VectorSelector: {job="test"}
			`,
			nodesEliminatedWithoutDelayedNameRemoval: 0,
			nodesEliminatedWithDelayedNameRemoval:    0,
		},
		"function over two selectors": {
			expr: `abs({__name__=~"bar.+"} + foo)`,
			expectedPlanWithoutDelayedNameRemoval: `
				- DeduplicateAndMerge
					- FunctionCall: abs(...)
						- BinaryExpression: LHS + RHS
							- LHS: VectorSelector: {__name__=~"bar.+"}
							- RHS: VectorSelector: {__name__="foo"}
			`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- FunctionCall: abs(...)
							- BinaryExpression: LHS + RHS
								- LHS: VectorSelector: {__name__=~"bar.+"}
								- RHS: VectorSelector: {__name__="foo"}
			`,
			nodesEliminatedWithoutDelayedNameRemoval: 0,
			nodesEliminatedWithDelayedNameRemoval: 0,
		},
		"unary negation with exact name matcher": {
			// DeduplicateAndMerge is eliminated in both plans, exact name matcher guarantees unique series.
			expr: `-foo`,
			expectedPlanWithoutDelayedNameRemoval: `
					- UnaryExpression: -
						- VectorSelector: {__name__="foo"}
				`,
			expectedPlanWithDelayedNameRemoval: `
					- DropName
						- UnaryExpression: -
							- VectorSelector: {__name__="foo"}
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 1,
			nodesEliminatedWithDelayedNameRemoval:    1,
		},
		"unary negation without exact name matcher": {
			// DeduplicateAndMerge is kept in both plans, matcher without exact name might produce non-unique series.
			expr: `-{job="test"}`,
			expectedPlanWithoutDelayedNameRemoval: `
					- DeduplicateAndMerge
						- UnaryExpression: -
							- VectorSelector: {job="test"}
				`,
			expectedPlanWithDelayedNameRemoval: `
					- DeduplicateAndMerge
						- DropName
							- UnaryExpression: -
								- VectorSelector: {job="test"}
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 0,
			nodesEliminatedWithDelayedNameRemoval:    0,
		},
		"unary negation with regex name matcher": {
			// DeduplicateAndMerge is kept in both plans, regex matcher might produce non-unique series.
			expr: `-{__name__=~"(foo|bar)"}`,
			expectedPlanWithoutDelayedNameRemoval: `
					- DeduplicateAndMerge
						- UnaryExpression: -
							- VectorSelector: {__name__=~"(foo|bar)"}
				`,
			expectedPlanWithDelayedNameRemoval: `
					- DeduplicateAndMerge
						- DropName
							- UnaryExpression: -
								- VectorSelector: {__name__=~"(foo|bar)"}
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 0,
			nodesEliminatedWithDelayedNameRemoval:    0,
		},
		"nested unary after function": {
			expr: `-(rate({job="test"}[5m]))`,
			// DeduplicateAndMerge nodes eliminated, except the closest to the selector.
			// Name is dropped immediately, so first DeduplicateAndMerge node deduplicates series and there shouldn't be any duplicates after it.
			expectedPlanWithoutDelayedNameRemoval: `
					- UnaryExpression: -
						- DeduplicateAndMerge
							- FunctionCall: rate(...)
								- MatrixSelector: {job="test"}[5m0s]
				`,
			// DeduplicateAndMerge nodes eliminated, except the root one.
			// Duplicates might appear after __name__ drop, which happens at the very end of query execution, so deduplicate after it.
			expectedPlanWithDelayedNameRemoval: `
					- DeduplicateAndMerge
						- DropName
							- UnaryExpression: -
								- FunctionCall: rate(...)
									- MatrixSelector: {job="test"}[5m0s]
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 1,
			nodesEliminatedWithDelayedNameRemoval:    1,
		},
		"label_replace": {
			// DeduplicateAndMerge is kept around label_replace in both plans.
			expr: `label_replace(foo, "dst", "$1", "src", "(.*)")`,
			expectedPlanWithoutDelayedNameRemoval: `
						- DeduplicateAndMerge
							- FunctionCall: label_replace(...)
								- param 0: VectorSelector: {__name__="foo"}
								- param 1: StringLiteral: "dst"
								- param 2: StringLiteral: "$1"
								- param 3: StringLiteral: "src"
								- param 4: StringLiteral: "(.*)"
					`,
			expectedPlanWithDelayedNameRemoval: `
						- DeduplicateAndMerge
							- DropName
								- FunctionCall: label_replace(...)
									- param 0: VectorSelector: {__name__="foo"}
									- param 1: StringLiteral: "dst"
									- param 2: StringLiteral: "$1"
									- param 3: StringLiteral: "src"
									- param 4: StringLiteral: "(.*)"
					`,
			nodesEliminatedWithoutDelayedNameRemoval: 0,
			nodesEliminatedWithDelayedNameRemoval:    0,
		},
		"label_join": {
			// DeduplicateAndMerge is kept around label_join in both plans.
			expr: `label_join(foo, "dst", ",", "a", "b")`,
			expectedPlanWithoutDelayedNameRemoval: `
						- DeduplicateAndMerge
							- FunctionCall: label_join(...)
								- param 0: VectorSelector: {__name__="foo"}
								- param 1: StringLiteral: "dst"
								- param 2: StringLiteral: ","
								- param 3: StringLiteral: "a"
								- param 4: StringLiteral: "b"
					`,
			expectedPlanWithDelayedNameRemoval: `
						- DeduplicateAndMerge
							- DropName
								- FunctionCall: label_join(...)
									- param 0: VectorSelector: {__name__="foo"}
									- param 1: StringLiteral: "dst"
									- param 2: StringLiteral: ","
									- param 3: StringLiteral: "a"
									- param 4: StringLiteral: "b"
					`,
			nodesEliminatedWithoutDelayedNameRemoval: 0,
			nodesEliminatedWithDelayedNameRemoval:    0,
		},
		"label_replace enclosing function which drops name, selector without name matcher": {
			// keeps:
			// - DeduplicateAndMerge around label_replace - to deduplicate after label_replace might modify labels and therefore potentially introduce duplicates.
			// - DeduplicateAndMerge around rate - to deduplicate after rate drops __name__ label and selector without exact name matcher might produce non-unique series.
			expr: `label_replace(rate({job="test"}[5m]), "dst", "$1", "src", "(.*)")`,
			expectedPlanWithoutDelayedNameRemoval: `
						- DeduplicateAndMerge
							- FunctionCall: label_replace(...)
								- param 0: DeduplicateAndMerge
									- FunctionCall: rate(...)
										- MatrixSelector: {job="test"}[5m0s]
								- param 1: StringLiteral: "dst"
								- param 2: StringLiteral: "$1"
								- param 3: StringLiteral: "src"
								- param 4: StringLiteral: "(.*)"
					`,
			// should keep:
			// - DeduplicateAndMerge at the root - to deduplicate after __name__ is dropped, because selector without exact name matcher might produce non-unique series.
			// Note, that since label_replace is last operation before __name__ is dropped, the DeduplicateAndMerge at the root deduplicates it results.
			// drops:
			// - DeduplicateAndMerge wrapping rate is dropped, because despite selector has NO exact name matcher, name is not dropped immediately and therefore duplicates won't appear.
			expectedPlanWithDelayedNameRemoval: `
					- DeduplicateAndMerge
						- DropName
							- FunctionCall: label_replace(...)
								- param 0: FunctionCall: rate(...)
									- MatrixSelector: {job="test"}[5m0s]
								- param 1: StringLiteral: "dst"
								- param 2: StringLiteral: "$1"
								- param 3: StringLiteral: "src"
								- param 4: StringLiteral: "(.*)"
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 0,
			nodesEliminatedWithDelayedNameRemoval:    1,
		},
		"function call enclosing label_replace, selector with exact name matcher": {
			// keeps:
			// - inner DeduplicateAndMerge - to deduplicate after rate drops __name__ label.
			// - DeduplicateAndMerge around label_replace - to deduplicate after label_replace might modify labels and therefore potentially introduce duplicates.
			// - DeduplicateAndMerge wrapping closest to label_replace __name__ dropping operation - to deduplicate in case __name__ is reintroduced by label_replace and operation's result should be deduplicated.
			// drops:
			// - DeduplicateAndMerge wrapping rate is dropped, because selector has exact name matcher.
			expr: `abs(label_replace(rate(foo[5m]), "dst", "$1", "src", "(.*)"))`,
			expectedPlanWithoutDelayedNameRemoval: `
				- DeduplicateAndMerge
					- FunctionCall: abs(...)
						- DeduplicateAndMerge
							- FunctionCall: label_replace(...)
								- param 0: FunctionCall: rate(...)
									- MatrixSelector: {__name__="foo"}[5m0s]
								- param 1: StringLiteral: "dst"
								- param 2: StringLiteral: "$1"
								- param 3: StringLiteral: "src"
								- param 4: StringLiteral: "(.*)"
				`,
			// keeps:
			// - DeduplicateAndMerge around label_replace - to deduplicate after label_replace might modify labels and therefore potentially introduce duplicates.
			// drops:
			// - DeduplicateAndMerge wrapping rate is dropped, because selector has exact name matcher.
			// - DeduplicateAndMerge at the root is dropped, because selector has exact name matcher. label_replace can't reintroduce __name__ label because it's dropped after. Even if it will modify it, DeduplicateAndMerge right after will hanlde this.
			expectedPlanWithDelayedNameRemoval: `
				- DropName
					- FunctionCall: abs(...)
						- DeduplicateAndMerge
							- FunctionCall: label_replace(...)
								- param 0: FunctionCall: rate(...)
									- MatrixSelector: {__name__="foo"}[5m0s]
								- param 1: StringLiteral: "dst"
								- param 2: StringLiteral: "$1"
								- param 3: StringLiteral: "src"
								- param 4: StringLiteral: "(.*)"
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 1,
			nodesEliminatedWithDelayedNameRemoval:    2,
		},
		"function call enclosing label_replace, selector without name matcher": {
			expr: `abs(label_replace(rate({job="test"}[5m]), "dst", "$1", "src", "(.*)"))`,
			// keeps:
			// - DeduplicateAndMerge closest to the selector - to deduplicate after rate drops __name__ label.
			// - DeduplicateAndMerge around label_replace - to deduplicate after label_replace might modify labels and therefore potentially introduce duplicates.
			// - DeduplicateAndMerge wrapping closest to label_replace __name__ dropping operation - to deduplicate in case __name__ is reintroduced by label_replace and operation's result should be deduplicated.
			expectedPlanWithoutDelayedNameRemoval: `
					- DeduplicateAndMerge
						- FunctionCall: abs(...)
							- DeduplicateAndMerge
								- FunctionCall: label_replace(...)
									- param 0: DeduplicateAndMerge
										- FunctionCall: rate(...)
											- MatrixSelector: {job="test"}[5m0s]
									- param 1: StringLiteral: "dst"
									- param 2: StringLiteral: "$1"
									- param 3: StringLiteral: "src"
									- param 4: StringLiteral: "(.*)"
					`,
			// keeps:
			// - DeduplicateAndMerge around label_replace - to deduplicate after label_replace might modify labels and therefore potentially introduce duplicates.
			// - DeduplicateAndMerge at the root - to deduplicate after __name__ is dropped, because selector without exact name matcher might produce non-unique series.
			// drops:
			// - DeduplicateAndMerge wrapping rate is dropped, because despite selector has NO exact name matcher, name is not dropped immediately and therefore duplicates won't appear.
			expectedPlanWithDelayedNameRemoval: `
					- DeduplicateAndMerge
						- DropName
							- FunctionCall: abs(...)
								- DeduplicateAndMerge
									- FunctionCall: label_replace(...)
										- param 0: FunctionCall: rate(...)
											- MatrixSelector: {job="test"}[5m0s]
										- param 1: StringLiteral: "dst"
										- param 2: StringLiteral: "$1"
										- param 3: StringLiteral: "src"
										- param 4: StringLiteral: "(.*)"
					`,
			nodesEliminatedWithoutDelayedNameRemoval: 0,
			nodesEliminatedWithDelayedNameRemoval:    1,
		},
		"nested function calls enclosing label_replace": {
			expr: `abs(ceil(label_replace(rate(foo[5m]), "dst", "$1", "src", "(.*)")))`,
			// keeps:
			// - DeduplicateAndMerge closest to the selector - to deduplicate after rate drops __name__ label.
			// - DeduplicateAndMerge around label_replace - to deduplicate after label_replace might modify labels and therefore potentially introduce duplicates.
			// - DeduplicateAndMerge wrapping closest to label_replace __name__ dropping operation - to deduplicate in case __name__ is reintroduced by label_replace and operation's result should be deduplicated.
			expectedPlanWithoutDelayedNameRemoval: `
				- FunctionCall: abs(...)
					- DeduplicateAndMerge
						- FunctionCall: ceil(...)
							- DeduplicateAndMerge
								- FunctionCall: label_replace(...)
									- param 0: FunctionCall: rate(...)
										- MatrixSelector: {__name__="foo"}[5m0s]
									- param 1: StringLiteral: "dst"
									- param 2: StringLiteral: "$1"
									- param 3: StringLiteral: "src"
									- param 4: StringLiteral: "(.*)"
			`,
			// keeps:
			// - DeduplicateAndMerge around label_replace - to deduplicate after label_replace might modify labels and therefore potentially introduce duplicates.
			// drops:
			// - DeduplicateAndMerge wrapping rate is dropped, because despite selector has NO exact name matcher, name is not dropped immediately and therefore duplicates won't appear.
			// - DeduplicateAndMerge at the root is dropped, because selector has exact name matcher. label_replace can't reintroduce __name__ label because it's dropped after. Even if it will modify it, DeduplicateAndMerge right after will hanlde this.
			expectedPlanWithDelayedNameRemoval: `
					- DropName
						- FunctionCall: abs(...)
							- FunctionCall: ceil(...)
								- DeduplicateAndMerge
									- FunctionCall: label_replace(...)
										- param 0: FunctionCall: rate(...)
											- MatrixSelector: {__name__="foo"}[5m0s]
										- param 1: StringLiteral: "dst"
										- param 2: StringLiteral: "$1"
										- param 3: StringLiteral: "src"
										- param 4: StringLiteral: "(.*)"
			`,
			nodesEliminatedWithoutDelayedNameRemoval: 2,
			nodesEliminatedWithDelayedNameRemoval:    3,
		},
		"function which does not drop name after label_replace": {
			expr: `sort(label_replace(rate(foo[5m]), "dst", "$1", "src", "(.*)"))`,
			// keeps:
			// - DeduplicateAndMerge around label_replace - to deduplicate after label_replace might modify labels and therefore potentially introduce duplicates.
			// drops:
			// - DeduplicateAndMerge wrapping rate is dropped because selector has exact name matcher.
			expectedPlanWithoutDelayedNameRemoval: `
								- FunctionCall: sort(...)
									- DeduplicateAndMerge
										- FunctionCall: label_replace(...)
											- param 0: FunctionCall: rate(...)
												- MatrixSelector: {__name__="foo"}[5m0s]
											- param 1: StringLiteral: "dst"
											- param 2: StringLiteral: "$1"
											- param 3: StringLiteral: "src"
											- param 4: StringLiteral: "(.*)"
					`,
			// keeps:
			// - DeduplicateAndMerge around label_replace - to deduplicate after label_replace might modify labels and therefore potentially introduce duplicates.
			// drops:
			// - DeduplicateAndMerge wrapping rate is dropped because selector has exact name matcher.
			// - DeduplicateAndMerge at the root is dropped, because selector has exact name matcher. label_replace can't reintroduce __name__ label because it's dropped after. Even if it will modify it, DeduplicateAndMerge right after will hanlde this.

			expectedPlanWithDelayedNameRemoval: `
									- DropName
										- FunctionCall: sort(...)
											- DeduplicateAndMerge
												- FunctionCall: label_replace(...)
													- param 0: FunctionCall: rate(...)
														- MatrixSelector: {__name__="foo"}[5m0s]
													- param 1: StringLiteral: "dst"
													- param 2: StringLiteral: "$1"
													- param 3: StringLiteral: "src"
													- param 4: StringLiteral: "(.*)"
					`,
			nodesEliminatedWithoutDelayedNameRemoval: 1,
			nodesEliminatedWithDelayedNameRemoval:    2,
		},
		"nested function calls enclosing label_replace, function in between doesn't drop __name__": {
			expr: `abs(sort(label_replace(rate(foo[5m]), "dst", "$1", "src", "(.*)")))`,
			// keeps:
			// - DeduplicateAndMerge around label_replace - to deduplicate after label_replace might modify labels and therefore potentially introduce duplicates.
			// - DeduplicateAndMerge around enclosing function call which drops name after label_replace.
			// drops:
			// - DeduplicateAndMerge wrapping rate is dropped because selector has exact name matcher.
			expectedPlanWithoutDelayedNameRemoval: `
				- DeduplicateAndMerge
					- FunctionCall: abs(...)
						- FunctionCall: sort(...)
							- DeduplicateAndMerge
								- FunctionCall: label_replace(...)
									- param 0: FunctionCall: rate(...)
										- MatrixSelector: {__name__="foo"}[5m0s]
									- param 1: StringLiteral: "dst"
									- param 2: StringLiteral: "$1"
									- param 3: StringLiteral: "src"
									- param 4: StringLiteral: "(.*)"
				`,
			// keeps:
			// - DeduplicateAndMerge around label_replace - to deduplicate after label_replace might modify labels and therefore potentially introduce duplicates.
			// drops:
			// - DeduplicateAndMerge wrapping rate is dropped because selector has exact name matcher.
			// - DeduplicateAndMerge at the root is dropped, because selector has exact name matcher. label_replace can't reintroduce __name__ label because it's dropped after. Even if it will modify it, DeduplicateAndMerge right after will hanlde this.

			expectedPlanWithDelayedNameRemoval: `
				- DropName
					- FunctionCall: abs(...)
						- FunctionCall: sort(...)
							- DeduplicateAndMerge
								- FunctionCall: label_replace(...)
									- param 0: FunctionCall: rate(...)
										- MatrixSelector: {__name__="foo"}[5m0s]
									- param 1: StringLiteral: "dst"
									- param 2: StringLiteral: "$1"
									- param 3: StringLiteral: "src"
									- param 4: StringLiteral: "(.*)"
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 1,
			nodesEliminatedWithDelayedNameRemoval:    2,
		},
		"nested label_replace": {
			// keeps:
			// - DeduplicateAndMerge around  both label_replace - to deduplicate after label_replace might modify labels and therefore potentially introduce duplicates.
			// - DeduplicateAndMerge around ceil() function enclosing label_replace - to deduplicate after function drops name after label_replace.
			// drops:
			// - DeduplicateAndMerge wrapping rate is dropped because selector has exact name matcher.
			// - DeduplicateAndMerge wrapping abs function is dropped because selector has exact name matcher and series deduplicated previously.
			expr: `abs(ceil(label_replace(label_replace(rate(foo[5m]), "dst1", "$1", "src1", "(.*)"), "dst2", "$1", "dst1", "(.*)")))`,
			expectedPlanWithoutDelayedNameRemoval: `
					- FunctionCall: abs(...)
						- DeduplicateAndMerge
							- FunctionCall: ceil(...)
								- DeduplicateAndMerge
									- FunctionCall: label_replace(...)
										- param 0: DeduplicateAndMerge
											- FunctionCall: label_replace(...)
												- param 0: FunctionCall: rate(...)
													- MatrixSelector: {__name__="foo"}[5m0s]
												- param 1: StringLiteral: "dst1"
												- param 2: StringLiteral: "$1"
												- param 3: StringLiteral: "src1"
												- param 4: StringLiteral: "(.*)"
										- param 1: StringLiteral: "dst2"
										- param 2: StringLiteral: "$1"
										- param 3: StringLiteral: "dst1"
										- param 4: StringLiteral: "(.*)"
				`,
			// keeps:
			// - DeduplicateAndMerge around  both label_replace - to deduplicate after label_replace might modify labels and therefore potentially introduce duplicates.
			// drops:
			// - DeduplicateAndMerge wrapping rate is dropped because selector has exact name matcher.
			// - DeduplicateAndMerge wrapping ceil function is dropped because name removal is delayed and series will be deduplicated later.
			// - DeduplicateAndMerge at the root is dropped, because selector has exact name matcher. label_replace can't reintroduce __name__ label because it's dropped after. Even if it will modify it, DeduplicateAndMerge right after will hanlde this.
			expectedPlanWithDelayedNameRemoval: `
			- DropName
				- FunctionCall: abs(...)
					- FunctionCall: ceil(...)
						- DeduplicateAndMerge
							- FunctionCall: label_replace(...)
								- param 0: DeduplicateAndMerge
									- FunctionCall: label_replace(...)
										- param 0: FunctionCall: rate(...)
											- MatrixSelector: {__name__="foo"}[5m0s]
										- param 1: StringLiteral: "dst1"
										- param 2: StringLiteral: "$1"
										- param 3: StringLiteral: "src1"
										- param 4: StringLiteral: "(.*)"
								- param 1: StringLiteral: "dst2"
								- param 2: StringLiteral: "$1"
								- param 3: StringLiteral: "dst1"
								- param 4: StringLiteral: "(.*)"
			`,
			nodesEliminatedWithoutDelayedNameRemoval: 2,
			nodesEliminatedWithDelayedNameRemoval:    3,
		},

		// Test cases to confirm we're skipping optimization when expression has a binary operation.
		"or operator - should keep DeduplicateAndMerge": {
			expr: `foo or bar`,
			expectedPlanWithoutDelayedNameRemoval: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS or RHS
						- LHS: VectorSelector: {__name__="foo"}
						- RHS: VectorSelector: {__name__="bar"}
				`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- BinaryExpression: LHS or RHS
							- LHS: VectorSelector: {__name__="foo"}
							- RHS: VectorSelector: {__name__="bar"}
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 0,
			nodesEliminatedWithDelayedNameRemoval:    0,
		},
		"or operator with functions - should keep top-level DeduplicateAndMerge": {
			// Note: we could remove the DeduplicateAndMerge nodes on each side of the binary operation,
			// but we currently don't for simplicity.
			expr: `rate(foo[5m]) or rate(bar[5m])`,
			expectedPlanWithoutDelayedNameRemoval: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS or RHS
						- LHS: FunctionCall: rate(...)
							- MatrixSelector: {__name__="foo"}[5m0s]
						- RHS: FunctionCall: rate(...)
							- MatrixSelector: {__name__="bar"}[5m0s]
				`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- BinaryExpression: LHS or RHS
							- LHS: FunctionCall: rate(...)
								- MatrixSelector: {__name__="foo"}[5m0s]
							- RHS: FunctionCall: rate(...)
								- MatrixSelector: {__name__="bar"}[5m0s]
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 2,
			nodesEliminatedWithDelayedNameRemoval:    2,
		},
		"vector-scalar comparison with bool modifier should keep DeduplicateAndMerge": {
			expr: `foo == bool 6`,
			expectedPlanWithoutDelayedNameRemoval: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS == bool RHS
						- LHS: VectorSelector: {__name__="foo"}
						- RHS: NumberLiteral: 6
				`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- BinaryExpression: LHS == bool RHS
							- LHS: VectorSelector: {__name__="foo"}
							- RHS: NumberLiteral: 6
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 0,
			nodesEliminatedWithDelayedNameRemoval:    0,
		},
		"vector-scalar arithmetic with exact name matcher should keep DeduplicateAndMerge": {
			expr: `foo * 2`,
			expectedPlanWithoutDelayedNameRemoval: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS * RHS
						- LHS: VectorSelector: {__name__="foo"}
						- RHS: NumberLiteral: 2
				`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- BinaryExpression: LHS * RHS
							- LHS: VectorSelector: {__name__="foo"}
							- RHS: NumberLiteral: 2
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 0,
			nodesEliminatedWithDelayedNameRemoval:    0,
		},
		"binary operation with nested rate functions": {
			expr: `rate(foo[5m]) + rate(bar[5m])`,
			expectedPlanWithoutDelayedNameRemoval: `
				- BinaryExpression: LHS + RHS
					- LHS: FunctionCall: rate(...)
						- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: FunctionCall: rate(...)
						- MatrixSelector: {__name__="bar"}[5m0s]
				`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- BinaryExpression: LHS + RHS
							- LHS: FunctionCall: rate(...)
								- MatrixSelector: {__name__="foo"}[5m0s]
							- RHS: FunctionCall: rate(...)
								- MatrixSelector: {__name__="bar"}[5m0s]
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 2,
			nodesEliminatedWithDelayedNameRemoval:    2,
		},
		"binary operation with aggregations": {
			expr: `sum(rate(foo[5m])) / sum(rate(bar[5m]))`,
			expectedPlanWithoutDelayedNameRemoval: `
				- BinaryExpression: LHS / RHS
					- LHS: AggregateExpression: sum
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: AggregateExpression: sum
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__="bar"}[5m0s]
				`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- BinaryExpression: LHS / RHS
							- LHS: AggregateExpression: sum
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="foo"}[5m0s]
							- RHS: AggregateExpression: sum
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="bar"}[5m0s]
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 2,
			nodesEliminatedWithDelayedNameRemoval:    2,
		},
		"binary operation and expression": {
			expr: `rate(foo[5m]) and rate(bar[5m])`,
			expectedPlanWithoutDelayedNameRemoval: `
				- BinaryExpression: LHS and RHS
					- LHS: FunctionCall: rate(...)
						- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: FunctionCall: rate(...)
						- MatrixSelector: {__name__="bar"}[5m0s]
				`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- BinaryExpression: LHS and RHS
							- LHS: FunctionCall: rate(...)
								- MatrixSelector: {__name__="foo"}[5m0s]
							- RHS: FunctionCall: rate(...)
								- MatrixSelector: {__name__="bar"}[5m0s]
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 2,
			nodesEliminatedWithDelayedNameRemoval:    2,
		},
		"binary operation * expression": {
			expr: `rate(foo[5m]) * rate(bar[5m])`,
			expectedPlanWithoutDelayedNameRemoval: `
				- BinaryExpression: LHS * RHS
					- LHS: FunctionCall: rate(...)
						- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: FunctionCall: rate(...)
						- MatrixSelector: {__name__="bar"}[5m0s]
				`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- BinaryExpression: LHS * RHS
							- LHS: FunctionCall: rate(...)
								- MatrixSelector: {__name__="foo"}[5m0s]
							- RHS: FunctionCall: rate(...)
								- MatrixSelector: {__name__="bar"}[5m0s]
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 2,
			nodesEliminatedWithDelayedNameRemoval:    2,
		},
		"binary operation with nested rate functions and non-name equal matcher": {
			expr: `rate(foo[5m]) + rate({__name__=~"bar.*"}[5m])`,
			expectedPlanWithoutDelayedNameRemoval: `
				- BinaryExpression: LHS + RHS
					- LHS: FunctionCall: rate(...)
						- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: DeduplicateAndMerge
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__=~"bar.*"}[5m0s]
				`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- BinaryExpression: LHS + RHS
							- LHS: FunctionCall: rate(...)
								- MatrixSelector: {__name__="foo"}[5m0s]
							- RHS: FunctionCall: rate(...)
								- MatrixSelector: {__name__=~"bar.*"}[5m0s]
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 1,
			nodesEliminatedWithDelayedNameRemoval:    2,
		},
		"nested binary operations with only name selectors": {
			expr: `foo or bar or baz`,
			expectedPlanWithoutDelayedNameRemoval: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS or RHS
						- LHS: DeduplicateAndMerge
							- BinaryExpression: LHS or RHS
								- LHS: VectorSelector: {__name__="foo"}
								- RHS: VectorSelector: {__name__="bar"}
						- RHS: VectorSelector: {__name__="baz"}
				`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- BinaryExpression: LHS or RHS
							- LHS: BinaryExpression: LHS or RHS
								- LHS: VectorSelector: {__name__="foo"}
								- RHS: VectorSelector: {__name__="bar"}
							- RHS: VectorSelector: {__name__="baz"}
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 0,
			nodesEliminatedWithDelayedNameRemoval:    1,
		},
		"nested binary operations with rate functions": {
			expr: `rate(foo[5m]) or rate(bar[5m]) or rate(baz[5m])`,
			expectedPlanWithoutDelayedNameRemoval: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS or RHS
						- LHS: DeduplicateAndMerge
							- BinaryExpression: LHS or RHS
								- LHS: FunctionCall: rate(...)
									- MatrixSelector: {__name__="foo"}[5m0s]
								- RHS: FunctionCall: rate(...)
									- MatrixSelector: {__name__="bar"}[5m0s]
						- RHS: FunctionCall: rate(...)
							- MatrixSelector: {__name__="baz"}[5m0s]
				`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- BinaryExpression: LHS or RHS
							- LHS: BinaryExpression: LHS or RHS
								- LHS: FunctionCall: rate(...)
									- MatrixSelector: {__name__="foo"}[5m0s]
								- RHS: FunctionCall: rate(...)
									- MatrixSelector: {__name__="bar"}[5m0s]
							- RHS: FunctionCall: rate(...)
								- MatrixSelector: {__name__="baz"}[5m0s]
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 3,
			nodesEliminatedWithDelayedNameRemoval:    4,
		},
		"nested binary operations not wrapped in DeduplicateAndMerge, with one eligible for elimination": {
			expr: `rate(bar[5m]) / (baz * foo)`,
			expectedPlanWithoutDelayedNameRemoval: `
				- BinaryExpression: LHS / RHS
					- LHS: FunctionCall: rate(...)
						- MatrixSelector: {__name__="bar"}[5m0s]
					- RHS: BinaryExpression: LHS * RHS
						- LHS: VectorSelector: {__name__="baz"}
						- RHS: VectorSelector: {__name__="foo"}
				`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- BinaryExpression: LHS / RHS
							- LHS: FunctionCall: rate(...)
								- MatrixSelector: {__name__="bar"}[5m0s]
							- RHS: BinaryExpression: LHS * RHS
								- LHS: VectorSelector: {__name__="baz"}
								- RHS: VectorSelector: {__name__="foo"}
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 1,
			nodesEliminatedWithDelayedNameRemoval:    1,
		},
		"nested binary operations wrapped in DeduplicateAndMerge, with one eligible for elimination": {
			expr: `rate(bar[5m]) or (baz or foo)`,
			expectedPlanWithoutDelayedNameRemoval: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS or RHS
						- LHS: FunctionCall: rate(...)
							- MatrixSelector: {__name__="bar"}[5m0s]
						- RHS: DeduplicateAndMerge
							- BinaryExpression: LHS or RHS
								- LHS: VectorSelector: {__name__="baz"}
								- RHS: VectorSelector: {__name__="foo"}
				`,
			expectedPlanWithDelayedNameRemoval: `
				- DeduplicateAndMerge
					- DropName
						- BinaryExpression: LHS or RHS
							- LHS: FunctionCall: rate(...)
								- MatrixSelector: {__name__="bar"}[5m0s]
							- RHS: BinaryExpression: LHS or RHS
								- LHS: VectorSelector: {__name__="baz"}
								- RHS: VectorSelector: {__name__="foo"}
				`,
			nodesEliminatedWithoutDelayedNameRemoval: 1,
			nodesEliminatedWithDelayedNameRemoval:    2,
		},
	}

	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	observer := streamingpromql.NoopPlanningObserver{}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			runTest := func(t *testing.T, enableDelayedNameRemoval bool) {
				// First, create a plan without optimization to count original nodes
				opts1 := streamingpromql.NewTestEngineOpts()
				plannerNoOpt, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts1, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
				require.NoError(t, err)
				planBefore, err := plannerNoOpt.NewQueryPlan(ctx, testCase.expr, timeRange, enableDelayedNameRemoval, observer)
				require.NoError(t, err)
				nodesBefore := countDeduplicateAndMergeNodes(planBefore.Root)

				// Then, create a plan with optimization
				opts2 := streamingpromql.NewTestEngineOpts()
				plannerWithOpt, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts2, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
				require.NoError(t, err)
				plannerWithOpt.RegisterQueryPlanOptimizationPass(plan.NewEliminateDeduplicateAndMergeOptimizationPass(opts2.CommonOpts.Reg))
				planAfter, err := plannerWithOpt.NewQueryPlan(ctx, testCase.expr, timeRange, enableDelayedNameRemoval, observer)
				require.NoError(t, err)
				nodesAfter := countDeduplicateAndMergeNodes(planAfter.Root)

				// Select expected values based on configuration
				var expectedPlan string
				var expectedNodesEliminated int
				if enableDelayedNameRemoval {
					expectedPlan = testCase.expectedPlanWithDelayedNameRemoval
					expectedNodesEliminated = testCase.nodesEliminatedWithDelayedNameRemoval
				} else {
					expectedPlan = testCase.expectedPlanWithoutDelayedNameRemoval
					expectedNodesEliminated = testCase.nodesEliminatedWithoutDelayedNameRemoval
				}

				// Check the plan structure
				actual := planAfter.String()
				require.Equal(t, testutils.TrimIndent(expectedPlan), actual, "Query: %s", testCase.expr)

				// Check the number of nodes eliminated
				actualEliminated := nodesBefore - nodesAfter
				require.Equal(t, expectedNodesEliminated, actualEliminated,
					"Query: %s\nExpected to eliminate %d nodes, but eliminated %d (before: %d, after: %d)",
					testCase.expr, expectedNodesEliminated, actualEliminated, nodesBefore, nodesAfter)

				var expectedModified int
				if expectedNodesEliminated > 0 {
					expectedModified = 1
				}

				reg := opts2.CommonOpts.Reg.(*prometheus.Registry)
				require.NoError(t, testutil.CollectAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_mimir_query_engine_eliminate_dedupe_modified_total Total number of queries where the optimization pass has been able to eliminate DeduplicateAndMerge nodes for.
					# TYPE cortex_mimir_query_engine_eliminate_dedupe_modified_total counter
					cortex_mimir_query_engine_eliminate_dedupe_modified_total %d
					`, expectedModified)), "cortex_mimir_query_engine_eliminate_dedupe_modified_total",
				))
			}

			t.Run("delayed name removal disabled", func(t *testing.T) {
				runTest(t, false)
			})

			t.Run("delayed name removal enabled", func(t *testing.T) {
				runTest(t, true)
			})
		})
	}
}

func TestEliminateDeduplicateAndMergeOptimizationPassCorrectness(t *testing.T) {
	testCases := map[string]struct {
		data           string
		expr           string
		expectedResult string
		expectedError  string
	}{
		"series with different labelset after name drop": {
			data: `
				load 1m
					a{env="prod"} 0+1x10
					a{env="test"} 0+2x10
			`,
			expr: `abs(rate(a[5m]))`,
			expectedResult: `
				{env="prod"} => 0.016666666666666666 @[600000]
				{env="test"} => 0.03333333333333333 @[600000]
			`,
		},
		"series with same labelset after name drop, but datapoints at different timestamps - should be merged": {
			data: `
				load 1m
					a{env="prod"} _ 0 1 2 _ _ _ _ _ _
					b{env="prod"} _ _ _ _ _ 0 1 2 3 4
			`,
			expr: `abs(rate({__name__=~"(a|b)"}[5m]))`,
			expectedResult: `
				{env="prod"} => 0.016666666666666666 @[600000]
			`,
		},
		"series with same labelset after name drop, but datapoints at same timestamp - should error": {
			data: `
				load 1m
					a{env="prod"} 0 1 2 3 4 5 6 7 8 9
					b{env="prod"} 0 1 2 3 4 5 6 7 8 9
			`,
			expr:          `abs(rate({__name__=~"(a|b)"}[5m]))`,
			expectedError: "vector cannot contain metrics with the same labelset",
		},
		"nested function calls over series with different labelset after name drop": {
			data: `
				load 1m
					a{env="prod"} 0+1x10
					a{env="test"} 0+2x10
			`,
			expr: `abs(rate(a[5m]))`,
			expectedResult: `
				{env="prod"} => 0.016666666666666666 @[600000]
				{env="test"} => 0.03333333333333333 @[600000]
			`,
		},
		"label_replace with exact name matcher, no conflicts": {
			data: `
				load 1m
					a{env="prod", src="value1"} 0+1x10
					a{env="test", src="value2"} 0+2x10
			`,
			expr: `abs(label_replace(rate(a[5m]), "dst", "$1", "src", "(.*)"))`,
			expectedResult: `
				{dst="value1", env="prod", src="value1"} => 0.016666666666666666 @[600000]
				{dst="value2", env="test", src="value2"} => 0.03333333333333333 @[600000]
			`,
		},
		"label_replace that creates duplicate labelsets - should error": {
			data: `
				load 1m
					a{env="prod", src="value"} 0 1 2 3 4 5 6 7 8 9
					b{env="prod", src="value"} 0 1 2 3 4 5 6 7 8 9
			`,
			expr:          `abs(label_replace(rate({__name__=~"(a|b)"}[5m]), "dst", "same", "src", "(.*)"))`,
			expectedError: "vector cannot contain metrics with the same labelset",
		},
		"label_replace where series will be merged": {
			// This tests that label_replace can create duplicate labelsets that get merged successfully.
			// Step by step:
			// 1. Initial: two series a{env="prod"} at timestamps 1-3m and a{env="test"} at timestamps 5-9m (same __name__, different env, non-overlapping timestamps)
			// 2. label_replace overwrites env label: both become a{env="merged"} (now identical labelsets)
			// 3. Since labelsets are identical and timestamps don't overlap, the series merge into one: a{env="merged"}
			data: `
				load 1m
					a{env="prod"} _ 0 1 2 _ _ _ _ _ _
					a{env="test"} _ _ _ _ _ 0 1 2 3 4
			`,
			expr: `label_replace(a, "env", "merged", "env", "(.*)")`,
			expectedResult: `
				{__name__="a", env="merged"} => 4 @[600000]
			`,
		},
		"label_replace re-introduces __name__ after rate drops it": {
			data: `
				load 1m
					a{env="prod"} 0+1x10
					a{env="test"} 0+2x10
			`,
			expr: `label_replace(rate(a[5m]), "__name__", "my_metric", "", "")`,
			expectedResult: `
				{__name__="my_metric", env="prod"} => 0.016666666666666666 @[600000]
				{__name__="my_metric", env="test"} => 0.03333333333333333 @[600000]
			`,
		},
		"label_replace re-introduces __name__, then name-dropping operation merges series": {
			// This tests the full cycle: drop name -> re-introduce name with label change -> drop name again -> merge.
			// Step by step:
			// 1. Initial: a{env="prod"} at timestamps 1-3m and a{env="test"} at timestamps 5-9m (different env, non-overlapping)
			// 2. rate() drops __name__: {env="prod"} and {env="test"} (still two separate series)
			// 3. label_replace re-introduces __name__ AND changes env: {__name__="my_metric", env="my_metric"} for both
			// 4. abs() drops __name__ again: {env="my_metric"} for both (now identical labelsets!)
			// 5. Since labelsets are identical and timestamps don't overlap, series merge into one: {env="my_metric"}
			data: `
				load 1m
					a{env="prod"} _ 0 1 2 _ _ _ _ _ _
					a{env="test"} _ _ _ _ _ 0 1 2 3 4
			`,
			expr: `abs(label_replace(label_replace(rate(a[5m]), "__name__", "my_metric", "", ""), "env", "my_metric", "", ""))`,
			expectedResult: `
				{env="my_metric"} => 0.016666666666666666 @[600000]
			`,
		},
		"label_replace re-introduces __name__, then name-dropping operation causes error": {
			// This tests the error case: drop name -> re-introduce name with label change -> drop name again -> error.
			// Step by step:
			// 1. Initial: a{env="prod"} and a{env="test"} both at all timestamps (different env, overlapping data)
			// 2. rate() drops __name__: {env="prod"} and {env="test"} (still two separate series)
			// 3. label_replace re-introduces __name__ AND changes env: {__name__="my_metric", env="my_metric"} for both
			// 4. abs() drops __name__ again: {env="my_metric"} for both (now identical labelsets!)
			// 5. Since labelsets are identical but timestamps overlap, this should error
			data: `
				load 1m
					a{env="prod"} 0 1 2 3 4 5 6 7 8 9
					a{env="test"} 0 1 2 3 4 5 6 7 8 9
			`,
			expr:          `abs(label_replace(label_replace(rate(a[5m]), "__name__", "my_metric", "", ""), "env", "my_metric", "", ""))`,
			expectedError: "vector cannot contain metrics with the same labelset",
		},
	}

	ctx := context.Background()
	end := timestamp.Time(0).Add(10 * time.Minute)

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			storage := promqltest.LoadedStorage(t, testCase.data)
			t.Cleanup(func() { require.NoError(t, storage.Close()) })

			runTest := func(t *testing.T, withOptimization bool, enableDelayedNameRemoval bool) {
				opts := streamingpromql.NewTestEngineOpts()
				planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
				require.NoError(t, err)

				if withOptimization {
					planner.RegisterQueryPlanOptimizationPass(plan.NewEliminateDeduplicateAndMergeOptimizationPass(opts.CommonOpts.Reg))
				}

				engine, err := streamingpromql.NewEngine(opts, streamingpromql.NewStaticQueryLimitsProvider(0, enableDelayedNameRemoval), stats.NewQueryMetrics(nil), planner)
				require.NoError(t, err)

				q, err := engine.NewInstantQuery(ctx, storage, nil, testCase.expr, end)
				require.NoError(t, err)
				defer q.Close()

				result := q.Exec(ctx)

				if testCase.expectedError != "" {
					require.Error(t, result.Err)
					require.Contains(t, result.Err.Error(), testCase.expectedError)
				} else {
					require.NoError(t, result.Err)
					require.Equal(t, testutils.TrimIndent(testCase.expectedResult), result.String())
				}
			}

			t.Run("delayed name removal disabled", func(t *testing.T) {
				t.Run("without optimization", func(t *testing.T) {
					runTest(t, false, false)
				})

				t.Run("with optimization", func(t *testing.T) {
					runTest(t, true, false)
				})
			})

			t.Run("delayed name removal enabled", func(t *testing.T) {
				t.Run("without optimization", func(t *testing.T) {
					runTest(t, false, true)
				})

				t.Run("with optimization", func(t *testing.T) {
					runTest(t, true, true)
				})
			})
		})
	}
}

// Test runs upstream and our test cases with delayed name removal disabled, ensuring that the optimization pass doesn't cause regressions.
// It's needed because in TestUpstreamTestCases and TestOurTestCase delayed name removal is enabled but the optimization pass behaves differently when DelayedNameRemoval is disabled.
func TestEliminateDeduplicateAndMergeOptimizationWithDelayedNameRemovalDisabled(t *testing.T) {
	runTestCasesWithDelayedNameRemovalDisabled(t, "upstream/*.test")
	runTestCasesWithDelayedNameRemovalDisabled(t, "ours*/*.test")
}

func runTestCasesWithDelayedNameRemovalDisabled(t *testing.T, globPattern string) {
	types.EnableManglingReturnedSlices = true
	parser.ExperimentalDurationExpr = true
	parser.EnableExperimentalFunctions = true
	parser.EnableExtendedRangeSelectors = true
	t.Cleanup(func() {
		types.EnableManglingReturnedSlices = false
		parser.ExperimentalDurationExpr = false
		parser.EnableExperimentalFunctions = false
		parser.EnableExtendedRangeSelectors = false
	})

	testdataFS := os.DirFS("../../testdata")
	testFiles, err := fs.Glob(testdataFS, globPattern)
	require.NoError(t, err)

	for _, testFile := range testFiles {
		t.Run(testFile, func(t *testing.T) {
			if strings.Contains(testFile, "name_label_dropping") {
				t.Skip("name_label_dropping tests require delayed name removal to be enabled, but this test exercises the optimization pass with delayed name removal disabled")
			}
			// Note that we get the equivalent test coverage from ours/native_histograms_delayed_name_removal_disabled.test
			if strings.Contains(testFile, "upstream/native_histograms.test") {
				t.Skip("upstream/native_histograms.test tests require delayed name removal to be enabled, but this test exercises the optimization pass with delayed name removal disabled")
			}

			f, err := testdataFS.Open(testFile)
			require.NoError(t, err)
			defer f.Close()

			b, err := io.ReadAll(f)
			require.NoError(t, err)

			testScript := string(b)
			opts := streamingpromql.NewTestEngineOpts()
			planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)
			engine, err := streamingpromql.NewEngine(opts, streamingpromql.NewStaticQueryLimitsProvider(0, false), stats.NewQueryMetrics(nil), planner)
			require.NoError(t, err)
			promqltest.RunTest(t, testScript, engine)
		})
	}
}

func countDeduplicateAndMergeNodes(node planning.Node) int {
	count := 0
	if _, ok := node.(*core.DeduplicateAndMerge); ok {
		count = 1
	}
	for child := range planning.ChildrenIter(node) {
		count += countDeduplicateAndMergeNodes(child)
	}
	return count
}
