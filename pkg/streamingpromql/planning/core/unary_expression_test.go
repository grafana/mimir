// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

func TestUnaryExpression_Equivalence(t *testing.T) {
	testCases := map[string]struct {
		a                planning.Node
		b                planning.Node
		expectEquivalent bool
	}{
		"identical": {
			a: &UnaryExpression{
				UnaryExpressionDetails: &UnaryExpressionDetails{
					Op:                 UNARY_SUB,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &UnaryExpression{
				UnaryExpressionDetails: &UnaryExpressionDetails{
					Op:                 UNARY_SUB,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			expectEquivalent: true,
		},
		"different expression position": {
			a: &UnaryExpression{
				UnaryExpressionDetails: &UnaryExpressionDetails{
					Op:                 UNARY_SUB,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &UnaryExpression{
				UnaryExpressionDetails: &UnaryExpressionDetails{
					Op:                 UNARY_SUB,
					ExpressionPosition: PositionRange{Start: 3, End: 4},
				},
				Inner: numberLiteralOf(12),
			},
			expectEquivalent: true,
		},
		"different operation": {
			a: &UnaryExpression{
				UnaryExpressionDetails: &UnaryExpressionDetails{
					Op:                 UNARY_SUB,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &UnaryExpression{
				UnaryExpressionDetails: &UnaryExpressionDetails{
					Op:                 UNARY_UNKNOWN,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			expectEquivalent: false,
		},
		"different type": {
			a: &UnaryExpression{
				UnaryExpressionDetails: &UnaryExpressionDetails{
					Op:                 UNARY_SUB,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b:                numberLiteralOf(12),
			expectEquivalent: false,
		},
		"different child node": {
			a: &UnaryExpression{
				UnaryExpressionDetails: &UnaryExpressionDetails{
					Op:                 UNARY_SUB,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &UnaryExpression{
				UnaryExpressionDetails: &UnaryExpressionDetails{
					Op:                 UNARY_SUB,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(13),
			},
			expectEquivalent: true,
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
