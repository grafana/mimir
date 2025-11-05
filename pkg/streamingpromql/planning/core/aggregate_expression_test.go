// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

func TestAggregateExpression_Describe(t *testing.T) {
	testCases := map[string]struct {
		node     *AggregateExpression
		expected string
	}{
		"'by' with no grouping labels": {
			node: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op: AGGREGATION_COUNT,
				},
			},
			expected: `count`,
		},
		"'by' with one grouping label": {
			node: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:       AGGREGATION_COUNT,
					Grouping: []string{"foo"},
				},
			},
			expected: `count by (foo)`,
		},
		"'by' with many grouping labels": {
			node: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:       AGGREGATION_COUNT,
					Grouping: []string{"foo", "bar", "baz"},
				},
			},
			expected: `count by (foo, bar, baz)`,
		},
		"'without' with no grouping labels": {
			node: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:      AGGREGATION_COUNT,
					Without: true,
				},
			},
			expected: `count without ()`,
		},
		"'without' with one grouping label": {
			node: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:       AGGREGATION_COUNT,
					Without:  true,
					Grouping: []string{"foo"},
				},
			},
			expected: `count without (foo)`,
		},
		"'without' with many grouping labels": {
			node: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:       AGGREGATION_COUNT,
					Without:  true,
					Grouping: []string{"foo", "bar", "baz"},
				},
			},
			expected: `count without (foo, bar, baz)`,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.node.Describe()
			require.Equal(t, testCase.expected, actual)
		})
	}
}

func TestAggregateExpression_Equivalence(t *testing.T) {
	testCases := map[string]struct {
		a                planning.Node
		b                planning.Node
		expectEquivalent bool
	}{
		"identical, no parameter or grouping labels": {
			a: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			expectEquivalent: true,
		},
		"identical, has parameter": {
			a: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
				Param: numberLiteralOf(2),
			},
			b: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
				Param: numberLiteralOf(2),
			},
			expectEquivalent: true,
		},
		"identical, has grouping labels": {
			a: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					Grouping:           []string{"foo"},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					Grouping:           []string{"foo"},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			expectEquivalent: true,
		},
		"different expression position": {
			a: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					ExpressionPosition: PositionRange{Start: 3, End: 4},
				},
				Inner: numberLiteralOf(12),
			},
			expectEquivalent: true,
		},
		"different operation": {
			a: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_AVG,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			expectEquivalent: false,
		},
		"different type": {
			a: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b:                numberLiteralOf(12),
			expectEquivalent: false,
		},
		"different child node": {
			a: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(13),
			},
			expectEquivalent: true,
		},
		"different parameter child node": {
			a: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
				Param: numberLiteralOf(2),
			},
			b: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
				Param: numberLiteralOf(3),
			},
			expectEquivalent: true,
		},
		"one with parameter, one without": {
			a: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
				Param: numberLiteralOf(2),
			},
			b: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			expectEquivalent: true,
		},
		"one with 'on', one with 'without'": {
			a: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					Without:            true,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			expectEquivalent: false,
		},
		"different grouping labels": {
			a: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					Grouping:           []string{"foo"},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &AggregateExpression{
				AggregateExpressionDetails: &AggregateExpressionDetails{
					Op:                 AGGREGATION_COUNT,
					Grouping:           []string{"bar"},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			expectEquivalent: false,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, testCase.expectEquivalent, testCase.a.EquivalentToIgnoringHintsAndChildren(testCase.b), "a.EquivalentToIgnoringHintsAndChildren(b) did not return expected value")
			require.Equal(t, testCase.expectEquivalent, testCase.b.EquivalentToIgnoringHintsAndChildren(testCase.a), "b.EquivalentToIgnoringHintsAndChildren(a) did not return expected value")

			require.True(t, testCase.a.EquivalentToIgnoringHintsAndChildren(testCase.a), "a should be equivalent to itself")
			require.True(t, testCase.b.EquivalentToIgnoringHintsAndChildren(testCase.b), "b should be equivalent to itself")
		})
	}
}
