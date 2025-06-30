// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
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

func TestBinaryExpression_Equivalence(t *testing.T) {
	testCases := map[string]struct {
		a                planning.Node
		b                planning.Node
		expectEquivalent bool
	}{
		"identical, without vector matching": {
			a: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_ADD,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			b: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_ADD,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			expectEquivalent: true,
		},
		"identical, has vector matching": {
			a: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_ADD,
					VectorMatching:     &VectorMatching{},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			b: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_ADD,
					VectorMatching:     &VectorMatching{},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			expectEquivalent: true,
		},
		"different expression position": {
			a: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_ADD,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			b: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_ADD,
					ExpressionPosition: PositionRange{Start: 3, End: 4},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			expectEquivalent: true,
		},
		"different operation": {
			a: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_ADD,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			b: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_SUB,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			expectEquivalent: false,
		},
		"different type": {
			a: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_ADD,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			b:                numberLiteralOf(12),
			expectEquivalent: false,
		},
		"different left-hand side": {
			a: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_ADD,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			b: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_ADD,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(13),
				RHS: numberLiteralOf(14),
			},
			expectEquivalent: false,
		},
		"different right-hand side": {
			a: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_ADD,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			b: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_ADD,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(13),
				RHS: numberLiteralOf(14),
			},
			expectEquivalent: false,
		},
		"one with 'bool', one without": {
			a: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_ADD,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			b: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_ADD,
					ReturnBool:         true,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			expectEquivalent: false,
		},
		"one with vector matching, one without": {
			a: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_ADD,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			b: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op:                 BINARY_ADD,
					VectorMatching:     &VectorMatching{},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			expectEquivalent: false,
		},
		"different cardinality": {
			a: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						Card: parser.CardManyToOne,
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			b: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						Card: parser.CardManyToMany,
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			expectEquivalent: false,
		},
		"different matching labels": {
			a: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						MatchingLabels: []string{"foo"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			b: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						MatchingLabels: []string{"bar"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			expectEquivalent: false,
		},
		"one with 'on', one with 'ignoring'": {
			a: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						On: true,
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			b: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						On: false,
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			expectEquivalent: false,
		},
		"different included labels": {
			a: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						Include: []string{"foo"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			b: &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Op: BINARY_ADD,
					VectorMatching: &VectorMatching{
						Include: []string{"bar"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				LHS: numberLiteralOf(12),
				RHS: numberLiteralOf(14),
			},
			expectEquivalent: false,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, testCase.expectEquivalent, testCase.a.EquivalentTo(testCase.b))
			require.Equal(t, testCase.expectEquivalent, testCase.b.EquivalentTo(testCase.a))

			require.True(t, testCase.a.EquivalentTo(testCase.a))
			require.True(t, testCase.b.EquivalentTo(testCase.b))
		})
	}
}
