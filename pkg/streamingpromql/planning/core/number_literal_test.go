// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

func TestNumberLiteral_Equivalence(t *testing.T) {
	testCases := map[string]struct {
		a                planning.Node
		b                planning.Node
		expectEquivalent bool
	}{
		"identical": {
			a: &NumberLiteral{
				NumberLiteralDetails: &NumberLiteralDetails{
					Value:              12,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &NumberLiteral{
				NumberLiteralDetails: &NumberLiteralDetails{
					Value:              12,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: true,
		},
		"different expression position": {
			a: &NumberLiteral{
				NumberLiteralDetails: &NumberLiteralDetails{
					Value:              12,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &NumberLiteral{
				NumberLiteralDetails: &NumberLiteralDetails{
					Value:              12,
					ExpressionPosition: PositionRange{Start: 3, End: 4},
				},
			},
			expectEquivalent: true,
		},
		"different value": {
			a: &NumberLiteral{
				NumberLiteralDetails: &NumberLiteralDetails{
					Value:              12,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &NumberLiteral{
				NumberLiteralDetails: &NumberLiteralDetails{
					Value:              13,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: false,
		},
		"different type": {
			a: &NumberLiteral{
				NumberLiteralDetails: &NumberLiteralDetails{
					Value:              12,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &StringLiteral{
				StringLiteralDetails: &StringLiteralDetails{
					Value:              "abc",
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
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
