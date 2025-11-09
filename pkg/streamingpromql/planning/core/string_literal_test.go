// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

func TestStringLiteral_Equivalence(t *testing.T) {
	testCases := map[string]struct {
		a                planning.Node
		b                planning.Node
		expectEquivalent bool
	}{
		"identical": {
			a: &StringLiteral{
				StringLiteralDetails: &StringLiteralDetails{
					Value:              "abc",
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &StringLiteral{
				StringLiteralDetails: &StringLiteralDetails{
					Value:              "abc",
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: true,
		},
		"different expression position": {
			a: &StringLiteral{
				StringLiteralDetails: &StringLiteralDetails{
					Value:              "abc",
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &StringLiteral{
				StringLiteralDetails: &StringLiteralDetails{
					Value:              "abc",
					ExpressionPosition: PositionRange{Start: 3, End: 4},
				},
			},
			expectEquivalent: true,
		},
		"different value": {
			a: &StringLiteral{
				StringLiteralDetails: &StringLiteralDetails{
					Value:              "abc",
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &StringLiteral{
				StringLiteralDetails: &StringLiteralDetails{
					Value:              "xyz",
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: false,
		},
		"different type": {
			a: &StringLiteral{
				StringLiteralDetails: &StringLiteralDetails{
					Value:              "abc",
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b:                numberLiteralOf(12),
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
