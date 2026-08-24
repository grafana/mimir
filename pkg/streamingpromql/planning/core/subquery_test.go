// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
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

func TestSubquery_ChildrenTimeRange(t *testing.T) {
	// Subquery: [60s:5s], no offset, no @ timestamp.
	subquery := func() *Subquery {
		return &Subquery{
			SubqueryDetails: &SubqueryDetails{
				Range: time.Minute,
				Step:  5 * time.Second,
			},
		}
	}

	testCases := map[string]struct {
		node     *Subquery
		input    types.QueryTimeRange
		expected types.QueryTimeRange
	}{
		"range query, end aligned to step": {
			node: subquery(),
			// Parent step grid: 200, 205, 210, 215, 220.
			input:    types.NewRangeQueryTimeRange(timestamp.Time(200_000), timestamp.Time(220_000), 5*time.Second),
			expected: types.NewRangeQueryTimeRange(timestamp.Time(145_000), timestamp.Time(220_000), 5*time.Second),
		},
		"range query, end not aligned to step": {
			// Regression test for https://github.com/prometheus/prometheus/pull/18598.
			// Parent step grid: 201, 206, 211, 216 (last actual step); 220 is 4s past the last
			// aligned step. The subquery's end should not extend past the parent's last actual step,
			// because those samples will never be used.
			node:     subquery(),
			input:    types.NewRangeQueryTimeRange(timestamp.Time(201_000), timestamp.Time(220_000), 5*time.Second),
			expected: types.NewRangeQueryTimeRange(timestamp.Time(145_000), timestamp.Time(216_000), 5*time.Second),
		},
		"range query with subquery offset, end aligned to step": {
			node: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:  time.Minute,
					Step:   5 * time.Second,
					Offset: time.Minute,
				},
			},
			input:    types.NewRangeQueryTimeRange(timestamp.Time(200_000), timestamp.Time(220_000), 5*time.Second),
			expected: types.NewRangeQueryTimeRange(timestamp.Time(85_000), timestamp.Time(160_000), 5*time.Second),
		},
		"range query with subquery offset, end not aligned to step": {
			node: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:  time.Minute,
					Step:   5 * time.Second,
					Offset: time.Minute,
				},
			},
			// Parent's last aligned step is 216s; with subquery offset 60s, the inner end is 156s.
			input:    types.NewRangeQueryTimeRange(timestamp.Time(201_000), timestamp.Time(220_000), 5*time.Second),
			expected: types.NewRangeQueryTimeRange(timestamp.Time(85_000), timestamp.Time(156_000), 5*time.Second),
		},
		"range query with subquery @ timestamp, ignores parent end": {
			node: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:     time.Minute,
					Step:      5 * time.Second,
					Timestamp: timestampOf(123_456),
				},
			},
			// The @ modifier pins the subquery to a fixed instant, so the parent's (unaligned) end
			// is irrelevant. start = end = 123_456ms; first step-aligned sample is at 65_000ms.
			input:    types.NewRangeQueryTimeRange(timestamp.Time(201_000), timestamp.Time(220_000), 5*time.Second),
			expected: types.NewRangeQueryTimeRange(timestamp.Time(65_000), timestamp.Time(123_456), 5*time.Second),
		},
		"range query with subquery @ timestamp and offset": {
			node: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:     time.Minute,
					Step:      5 * time.Second,
					Offset:    time.Minute,
					Timestamp: timestampOf(123_456),
				},
			},
			// Similar to above, the @ modifier pins the subquery to a fixed instance, so the parent's unaligned end
			// is not relevant.
			input:    types.NewRangeQueryTimeRange(timestamp.Time(201_000), timestamp.Time(220_000), 5*time.Second),
			expected: types.NewRangeQueryTimeRange(timestamp.Time(5_000), timestamp.Time(63_456), 5*time.Second),
		},
		"instant query": {
			node:     subquery(),
			input:    types.NewInstantQueryTimeRange(timestamp.Time(200_000)),
			expected: types.NewRangeQueryTimeRange(timestamp.Time(145_000), timestamp.Time(200_000), 5*time.Second),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.node.ChildrenTimeRange(testCase.input)
			require.Equal(t, testCase.expected, actual)
		})
	}
}
