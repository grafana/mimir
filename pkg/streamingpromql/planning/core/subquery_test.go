// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

func TestSubquery_Describe(t *testing.T) {
	testCases := map[string]struct {
		node     *Subquery
		expected string
	}{
		"no timestamp and no offset": {
			node: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range: time.Minute,
					Step:  20 * time.Second,
				},
			},
			expected: "[1m0s:20s]",
		},
		"no timestamp, has offset": {
			node: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:  time.Minute,
					Step:   20 * time.Second,
					Offset: time.Hour,
				},
			},
			expected: "[1m0s:20s] offset 1h0m0s",
		},
		"has timestamp and no offset": {
			node: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:     time.Minute,
					Step:      20 * time.Second,
					Timestamp: timestampOf(123456),
				},
			},
			expected: "[1m0s:20s] @ 123456 (1970-01-01T00:02:03.456Z)",
		},
		"has timestamp and offset": {
			node: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:     time.Minute,
					Step:      20 * time.Second,
					Offset:    time.Hour,
					Timestamp: timestampOf(123456),
				},
			},
			expected: "[1m0s:20s] @ 123456 (1970-01-01T00:02:03.456Z) offset 1h0m0s",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.node.Describe()
			require.Equal(t, testCase.expected, actual)
		})
	}
}

func TestSubquery_Equivalence(t *testing.T) {
	testCases := map[string]struct {
		a                planning.Node
		b                planning.Node
		expectEquivalent bool
	}{
		"identical": {
			a: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              time.Minute,
					Step:               20 * time.Second,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              time.Minute,
					Step:               20 * time.Second,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			expectEquivalent: true,
		},
		"different expression position": {
			a: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              time.Minute,
					Step:               20 * time.Second,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              time.Minute,
					Step:               20 * time.Second,
					ExpressionPosition: PositionRange{Start: 3, End: 4},
				},
				Inner: numberLiteralOf(12),
			},
			expectEquivalent: true,
		},
		"different type": {
			a: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              time.Minute,
					Step:               20 * time.Second,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b:                numberLiteralOf(12),
			expectEquivalent: false,
		},
		"different child node": {
			a: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              time.Minute,
					Step:               20 * time.Second,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              time.Minute,
					Step:               20 * time.Second,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(13),
			},
			expectEquivalent: true,
		},
		"different range": {
			a: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              time.Minute,
					Step:               20 * time.Second,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              2 * time.Minute,
					Step:               20 * time.Second,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			expectEquivalent: false,
		},
		"different step": {
			a: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              time.Minute,
					Step:               20 * time.Second,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              time.Minute,
					Step:               10 * time.Second,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			expectEquivalent: false,
		},
		"different offset": {
			a: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              time.Minute,
					Step:               20 * time.Second,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              time.Minute,
					Step:               20 * time.Second,
					Offset:             time.Hour,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			expectEquivalent: false,
		},
		"one with timestamp, one without": {
			a: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              time.Minute,
					Step:               20 * time.Second,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              time.Minute,
					Step:               20 * time.Second,
					Timestamp:          timestampOf(123),
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			expectEquivalent: false,
		},
		"both with different timestamps": {
			a: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              time.Minute,
					Step:               20 * time.Second,
					Timestamp:          timestampOf(123),
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
				Inner: numberLiteralOf(12),
			},
			b: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:              time.Minute,
					Step:               20 * time.Second,
					Timestamp:          timestampOf(456),
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
