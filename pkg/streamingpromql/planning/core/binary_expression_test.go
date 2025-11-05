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
			expectEquivalent: true,
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
			expectEquivalent: true,
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
			require.Equal(t, testCase.expectEquivalent, testCase.a.EquivalentToIgnoringHintsAndChildren(testCase.b), "a.EquivalentToIgnoringHintsAndChildren(b) did not return expected value")
			require.Equal(t, testCase.expectEquivalent, testCase.b.EquivalentToIgnoringHintsAndChildren(testCase.a), "b.EquivalentToIgnoringHintsAndChildren(a) did not return expected value")

			require.True(t, testCase.a.EquivalentToIgnoringHintsAndChildren(testCase.a), "a should be equivalent to itself")
			require.True(t, testCase.b.EquivalentToIgnoringHintsAndChildren(testCase.b), "b should be equivalent to itself")
		})
	}
}

func TestBinaryExpression_MergeHints(t *testing.T) {
	testCases := map[string]struct {
		first       *BinaryExpressionHints
		second      *BinaryExpressionHints
		expected    *BinaryExpressionHints
		expectError bool
	}{
		"both have nil hints": {
			first:    nil,
			second:   nil,
			expected: nil,
		},
		"first has nil hints, other has empty hints": {
			first:    nil,
			second:   &BinaryExpressionHints{},
			expected: nil,
		},
		"second has nil hints, other has empty hints": {
			first:    &BinaryExpressionHints{},
			second:   nil,
			expected: &BinaryExpressionHints{},
		},
		"first has empty hints, other does not": {
			first:       &BinaryExpressionHints{},
			second:      &BinaryExpressionHints{Include: []string{"foo"}},
			expectError: true,
		},
		"second has empty hints, other does not": {
			first:       &BinaryExpressionHints{Include: []string{"foo"}},
			second:      &BinaryExpressionHints{},
			expectError: true,
		},
		"both have hints, no overlap in labels": {
			first:       &BinaryExpressionHints{Include: []string{"first_1", "first_2"}},
			second:      &BinaryExpressionHints{Include: []string{"second_1", "second_2"}},
			expectError: true,
		},
		"both have the same labels": {
			first:    &BinaryExpressionHints{Include: []string{"foo", "bar"}},
			second:   &BinaryExpressionHints{Include: []string{"foo", "bar"}},
			expected: &BinaryExpressionHints{Include: []string{"foo", "bar"}},
		},
		"both have the same labels in a different order": {
			first:       &BinaryExpressionHints{Include: []string{"foo", "bar"}},
			second:      &BinaryExpressionHints{Include: []string{"bar", "foo"}},
			expectError: true,
		},
		"both have hints, some overlap in labels": {
			first:       &BinaryExpressionHints{Include: []string{"first", "foo"}},
			second:      &BinaryExpressionHints{Include: []string{"second", "foo"}},
			expectError: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			first := &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Hints: testCase.first,
				},
			}

			second := &BinaryExpression{
				BinaryExpressionDetails: &BinaryExpressionDetails{
					Hints: testCase.second,
				},
			}

			err := first.MergeHints(second)
			if testCase.expectError {
				require.EqualError(t, err, "cannot merge hints for binary expressions with different included labels")
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.expected, first.Hints)
			}
		})
	}
}
