// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"

	"github.com/stretchr/testify/require"
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
