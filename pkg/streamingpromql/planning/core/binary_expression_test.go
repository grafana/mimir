// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestBinaryExpression_Describe(t *testing.T) {
	testCases := map[string]struct {
		node     *BinaryExpression
		expected string
	}{
		"no vector matching": {
			node: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
				},
			},
			expected: `LHS + RHS`,
		},
		"no vector matching with 'bool'": {
			node: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:         BINARY_GTR,
					ReturnBool: true,
				},
			},
			expected: `LHS > bool RHS`,
		},
		"vector matching with 'ignoring' and no matching labels or included labels": {
			node: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:             BINARY_ADD,
					VectorMatching: &VectorMatching{},
				},
			},
			expected: `LHS + RHS`,
		},
		"vector matching with 'ignoring', one matching label and no included labels": {
			node: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						MatchingLabels: []string{"foo"},
					},
				},
			},
			expected: `LHS + ignoring (foo) RHS`,
		},
		"vector matching with 'ignoring', many matching labels and no included labels": {
			node: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						MatchingLabels: []string{"foo", "bar", "baz"},
					},
				},
			},
			expected: `LHS + ignoring (foo, bar, baz) RHS`,
		},

		"vector matching with 'on' and no matching labels or included labels": {
			node: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						On:             true,
						MatchingLabels: []string{},
					},
				},
			},
			expected: `LHS + on () RHS`,
		},
		"vector matching with 'on', one matching label and no included labels": {
			node: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						On:             true,
						MatchingLabels: []string{"foo"},
					},
				},
			},
			expected: `LHS + on (foo) RHS`,
		},
		"vector matching with 'on', many matching labels and no included labels": {
			node: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						On:             true,
						MatchingLabels: []string{"foo", "bar", "baz"},
					},
				},
			},
			expected: `LHS + on (foo, bar, baz) RHS`,
		},

		"vector matching with group_left and no included labels": {
			node: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						MatchingLabels: []string{"foo"},
						Card:           parser.CardManyToOne,
					},
				},
			},
			expected: `LHS + ignoring (foo) group_left () RHS`,
		},
		"vector matching with group_left and one included label": {
			node: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						MatchingLabels: []string{"foo"},
						Card:           parser.CardManyToOne,
						Include:        []string{"bar"},
					},
				},
			},
			expected: `LHS + ignoring (foo) group_left (bar) RHS`,
		},
		"vector matching with group_left and many included labels": {
			node: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						MatchingLabels: []string{"foo"},
						Card:           parser.CardManyToOne,
						Include:        []string{"bar", "baz", "quux"},
					},
				},
			},
			expected: `LHS + ignoring (foo) group_left (bar, baz, quux) RHS`,
		},

		"vector matching with group_right and no included labels": {
			node: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						MatchingLabels: []string{"foo"},
						Card:           parser.CardOneToMany,
					},
				},
			},
			expected: `LHS + ignoring (foo) group_right () RHS`,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.node.Describe()
			require.Equal(t, testCase.expected, actual)
		})
	}
}
