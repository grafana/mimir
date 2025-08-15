// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestVectorSelector_Describe(t *testing.T) {
	singleMatcher := []*LabelMatcher{
		{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
	}

	testCases := map[string]struct {
		node     *VectorSelector
		expected string
	}{
		"one matcher, no timestamp and no offset": {
			node: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: singleMatcher,
				},
			},
			expected: `{__name__="foo"}`,
		},
		"many matchers, no timestamp and no offset": {
			node: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
						{Name: "env", Type: labels.MatchNotEqual, Value: "test"},
						{Name: "region", Type: labels.MatchRegexp, Value: "au-.*"},
						{Name: "node", Type: labels.MatchNotRegexp, Value: "node-1-.*"},
					},
				},
			},
			expected: `{__name__="foo", env!="test", region=~"au-.*", node!~"node-1-.*"}`,
		},
		"one matcher, no timestamp, has offset": {
			node: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: singleMatcher,
					Offset:   time.Hour,
				},
			},
			expected: `{__name__="foo"} offset 1h0m0s`,
		},
		"one matcher, has timestamp and no offset": {
			node: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers:  singleMatcher,
					Timestamp: timestampOf(123456),
				},
			},
			expected: `{__name__="foo"} @ 123456 (1970-01-01T00:02:03.456Z)`,
		},
		"one matcher, has timestamp and offset": {
			node: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers:  singleMatcher,
					Offset:    time.Hour,
					Timestamp: timestampOf(123456),
				},
			},
			expected: `{__name__="foo"} @ 123456 (1970-01-01T00:02:03.456Z) offset 1h0m0s`,
		},
		"one matcher, returning sample timestamps": {
			node: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers:               singleMatcher,
					ReturnSampleTimestamps: true,
				},
			},
			expected: `{__name__="foo"}, return sample timestamps`,
		},
		"one matcher, skip histogram buckets enabled": {
			node: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers:             singleMatcher,
					SkipHistogramBuckets: true,
				},
			},
			expected: `{__name__="foo"}, skip histogram buckets`,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.node.Describe()
			require.Equal(t, testCase.expected, actual)
		})
	}
}

func TestVectorSelector_Equivalence(t *testing.T) {
	testCases := map[string]struct {
		a                planning.Node
		b                planning.Node
		expectEquivalent bool
	}{
		"identical": {
			a: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: true,
		},
		"different expression position": {
			a: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					ExpressionPosition: PositionRange{Start: 3, End: 4},
				},
			},
			expectEquivalent: true,
		},
		"different type": {
			a: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b:                numberLiteralOf(12),
			expectEquivalent: false,
		},
		"different offset": {
			a: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Offset:             time.Hour,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: false,
		},
		"one with timestamp, one without": {
			a: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Timestamp:          timestampOf(123),
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: false,
		},
		"both with different timestamps": {
			a: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Timestamp:          timestampOf(123),
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Timestamp:          timestampOf(456),
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: false,
		},
		"selectors with different name": {
			a: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name_2__", Type: labels.MatchEqual, Value: "foo"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: false,
		},
		"selectors with different type": {
			a: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: false,
		},
		"selectors with different value": {
			a: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "bar"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: false,
		},
		"one returning sample timestamps, one not": {
			a: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					ExpressionPosition:     PositionRange{Start: 1, End: 2},
					ReturnSampleTimestamps: false,
				},
			},
			b: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					ExpressionPosition:     PositionRange{Start: 1, End: 2},
					ReturnSampleTimestamps: true,
				},
			},
			expectEquivalent: false,
		},

		"one with skipping histogram buckets enabled, one without": {
			a: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					SkipHistogramBuckets: true,
					ExpressionPosition:   PositionRange{Start: 1, End: 2},
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

func timestampOf(ts int64) *time.Time {
	return TimeFromTimestamp(&ts)
}

func TestVectorSelector_QueriedTimeRange(t *testing.T) {
	startT := timestamp.Time(0).Add(time.Hour)
	endT := startT.Add(time.Hour)
	queryTimeRange := types.NewRangeQueryTimeRange(startT, endT, time.Minute)
	lookbackDelta := 7 * time.Minute
	offset := 3 * time.Minute
	ts := timestamp.Time(0).Add(5 * time.Hour)
	excludeLowerBoundary := time.Millisecond // See selector.ComputeQueriedTimeRange for an explanation of this.

	testCases := map[string]struct {
		selector *VectorSelector
		expected planning.QueriedTimeRange
	}{
		"no timestamp or offset": {
			selector: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{},
			},
			expected: planning.NewQueriedTimeRange(startT.Add(-lookbackDelta).Add(excludeLowerBoundary), endT),
		},
		"timestamp set, no offset": {
			selector: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Timestamp: &ts,
				},
			},
			expected: planning.NewQueriedTimeRange(ts.Add(-lookbackDelta).Add(excludeLowerBoundary), ts),
		},
		"offset set, no timestamp": {
			selector: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Offset: offset,
				},
			},
			expected: planning.NewQueriedTimeRange(startT.Add(-lookbackDelta).Add(-offset).Add(excludeLowerBoundary), endT.Add(-offset)),
		},
		"both timestamp and offset set": {
			selector: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Offset:    offset,
					Timestamp: &ts,
				},
			},
			expected: planning.NewQueriedTimeRange(ts.Add(-lookbackDelta).Add(-offset).Add(excludeLowerBoundary), ts.Add(-offset)),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, testCase.expected, testCase.selector.QueriedTimeRange(queryTimeRange, lookbackDelta))
		})
	}
}
