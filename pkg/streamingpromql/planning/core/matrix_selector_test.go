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

func TestMatrixSelector_Describe(t *testing.T) {
	singleMatcher := []*LabelMatcher{
		{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
	}

	testCases := map[string]struct {
		node     *MatrixSelector
		expected string
	}{
		"one matcher, no timestamp and no offset": {
			node: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: singleMatcher,
					Range:    time.Minute,
				},
			},
			expected: `{__name__="foo"}[1m0s]`,
		},
		"many matchers, no timestamp and no offset": {
			node: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
						{Name: "env", Type: labels.MatchNotEqual, Value: "test"},
						{Name: "region", Type: labels.MatchRegexp, Value: "au-.*"},
						{Name: "node", Type: labels.MatchNotRegexp, Value: "node-1-.*"},
					},
					Range: time.Minute,
				},
			},
			expected: `{__name__="foo", env!="test", region=~"au-.*", node!~"node-1-.*"}[1m0s]`,
		},
		"one matcher, no timestamp, has offset": {
			node: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: singleMatcher,
					Range:    time.Minute,
					Offset:   time.Hour,
				},
			},
			expected: `{__name__="foo"}[1m0s] offset 1h0m0s`,
		},
		"one matcher, has timestamp and no offset": {
			node: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers:  singleMatcher,
					Range:     time.Minute,
					Timestamp: timestampOf(123456),
				},
			},
			expected: `{__name__="foo"}[1m0s] @ 123456 (1970-01-01T00:02:03.456Z)`,
		},
		"one matcher, has timestamp and offset": {
			node: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers:  singleMatcher,
					Range:     time.Minute,
					Offset:    time.Hour,
					Timestamp: timestampOf(123456),
				},
			},
			expected: `{__name__="foo"}[1m0s] @ 123456 (1970-01-01T00:02:03.456Z) offset 1h0m0s`,
		},
		"one matcher, skip histogram buckets enabled": {
			node: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers:             singleMatcher,
					Range:                time.Minute,
					SkipHistogramBuckets: true,
				},
			},
			expected: `{__name__="foo"}[1m0s], skip histogram buckets`,
		},
		"one matcher, skip histogram buckets and anchored enabled": {
			node: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers:             singleMatcher,
					Range:                time.Minute,
					SkipHistogramBuckets: true,
					Anchored:             true,
				},
			},
			expected: `{__name__="foo"}[1m0s] anchored, skip histogram buckets`,
		},
		"one matcher, anchored enabled": {
			node: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: singleMatcher,
					Range:    time.Minute,
					Anchored: true,
				},
			},
			expected: `{__name__="foo"}[1m0s] anchored`,
		},
		"one matcher, smoothed enabled - not counter aware": {
			node: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: singleMatcher,
					Range:    time.Minute,
					Smoothed: true,
				},
			},
			expected: `{__name__="foo"}[1m0s] smoothed`,
		},
		"one matcher, smoothed enabled - counter aware": {
			node: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers:     singleMatcher,
					Range:        time.Minute,
					Smoothed:     true,
					CounterAware: true,
				},
			},
			expected: `{__name__="foo"}[1m0s] smoothed counter aware`,
		},
		"one matcher, anchored enabled - counter aware has no effect": {
			node: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers:     singleMatcher,
					Range:        time.Minute,
					Anchored:     true,
					CounterAware: true,
				},
			},
			expected: `{__name__="foo"}[1m0s] anchored`,
		},
		"one matcher - counter aware has no effect": {
			node: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers:     singleMatcher,
					Range:        time.Minute,
					CounterAware: true,
				},
			},
			expected: `{__name__="foo"}[1m0s]`,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.node.Describe()
			require.Equal(t, testCase.expected, actual)
		})
	}
}

func TestMatrixSelector_Equivalence(t *testing.T) {
	testCases := map[string]struct {
		a                planning.Node
		b                planning.Node
		expectEquivalent bool
	}{
		"identical": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: true,
		},
		"different expression position": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 3, End: 4},
				},
			},
			expectEquivalent: true,
		},
		"different type": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b:                numberLiteralOf(12),
			expectEquivalent: false,
		},
		"different range": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              2 * time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: false,
		},
		"different offset": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              time.Minute,
					Offset:             time.Hour,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: false,
		},
		"one with timestamp, one without": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              time.Minute,
					Timestamp:          timestampOf(123),
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: false,
		},
		"both with different timestamps": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              time.Minute,
					Timestamp:          timestampOf(123),
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              time.Minute,
					Timestamp:          timestampOf(456),
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: false,
		},
		"selectors with different name": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name_2__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: false,
		},
		"selectors with different type": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: false,
		},
		"selectors with different value": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "bar"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: false,
		},
		"one with skipping histogram buckets enabled, one without": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:                time.Minute,
					SkipHistogramBuckets: false,
					ExpressionPosition:   PositionRange{Start: 1, End: 2},
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
					},
					Range:                time.Minute,
					SkipHistogramBuckets: true,
					ExpressionPosition:   PositionRange{Start: 1, End: 2},
				},
			},
			expectEquivalent: true,
		},
		"one with smoothed and one without": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
					Smoothed:           true,
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
					Smoothed:           false,
				},
			},
			expectEquivalent: false,
		},
		"both smoothed": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
					Smoothed:           true,
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
					Smoothed:           true,
				},
			},
			expectEquivalent: true,
		},
		"neither smoothed": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
					Smoothed:           false,
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
					Smoothed:           false,
				},
			},
			expectEquivalent: true,
		},
		"one with anchored and one without": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
					Anchored:           true,
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
					Anchored:           false,
				},
			},
			expectEquivalent: false,
		},
		"both anchored": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
					Anchored:           true,
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
					Anchored:           true,
				},
			},
			expectEquivalent: true,
		},
		"neither anchored": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
					Anchored:           false,
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
					Anchored:           false,
				},
			},
			expectEquivalent: true,
		},
		"same wrapping smoothed": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
					Smoothed:           true,
					CounterAware:       true,
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
					Smoothed:           true,
					CounterAware:       true,
				},
			},
			expectEquivalent: true,
		},
		"one function wrapping smoothed": {
			a: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
					Smoothed:           true,
					CounterAware:       true,
				},
			},
			b: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Matchers: []*LabelMatcher{
						{Name: "__name__", Type: labels.MatchNotEqual, Value: "foo"},
					},
					Range:              time.Minute,
					ExpressionPosition: PositionRange{Start: 1, End: 2},
					Smoothed:           true,
				},
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

func TestMatrixSelector_MergeHints_SkipHistogramBuckets(t *testing.T) {
	runTest := func(t *testing.T, skipFirst, skipSecond bool, expectSkip bool) {
		first := &MatrixSelector{
			MatrixSelectorDetails: &MatrixSelectorDetails{
				SkipHistogramBuckets: skipFirst,
			},
		}
		second := &MatrixSelector{
			MatrixSelectorDetails: &MatrixSelectorDetails{
				SkipHistogramBuckets: skipSecond,
			},
		}

		err := first.MergeHints(second)
		require.NoError(t, err)
		require.Equal(t, expectSkip, first.SkipHistogramBuckets)
	}

	t.Run("neither has skip histogram buckets enabled", func(t *testing.T) {
		runTest(t, false, false, false)
	})

	t.Run("first has skip histogram buckets enabled, other does not", func(t *testing.T) {
		runTest(t, true, false, false)
	})

	t.Run("first has skip histogram buckets disabled, other does not", func(t *testing.T) {
		runTest(t, false, true, false)
	})

	t.Run("both have skip histogram buckets enabled", func(t *testing.T) {
		runTest(t, true, true, true)
	})
}

func TestMatrixSelector_MergeHints_ProjectionLabels(t *testing.T) {
	runTest := func(t *testing.T, lblsFirst, lblsSecond []string, expectLbls []string) {
		first := &MatrixSelector{
			MatrixSelectorDetails: &MatrixSelectorDetails{
				ProjectionLabels: lblsFirst,
			},
		}
		second := &MatrixSelector{
			MatrixSelectorDetails: &MatrixSelectorDetails{
				ProjectionLabels: lblsSecond,
			},
		}

		err := first.MergeHints(second)
		require.NoError(t, err)
		require.Equal(t, expectLbls, first.ProjectionLabels)
	}

	t.Run("neither has projection labels set", func(t *testing.T) {
		runTest(t, nil, nil, []string{})
	})

	t.Run("first has projection labels set, other does not", func(t *testing.T) {
		runTest(t, []string{"__series_hash__", "job", "zone"}, nil, []string{"__series_hash__", "job", "zone"})
	})

	t.Run("first has no projection labels, other does", func(t *testing.T) {
		runTest(t, nil, []string{"__series_hash__", "instance", "pod"}, []string{"__series_hash__", "instance", "pod"})
	})

	t.Run("both have projection labels set", func(t *testing.T) {
		runTest(t, []string{"__series_hash__", "job", "cluster"}, []string{"__series_hash__", "job", "region"}, []string{"__series_hash__", "cluster", "job", "region"})
	})
}

func TestMatrixSelector_QueriedTimeRange(t *testing.T) {
	startT := timestamp.Time(0).Add(time.Hour)
	endT := startT.Add(time.Hour)
	queryTimeRange := types.NewRangeQueryTimeRange(startT, endT, time.Minute)
	rng := 7 * time.Minute
	offset := 3 * time.Minute
	ts := timestamp.Time(0).Add(5 * time.Hour)
	excludeLowerBoundary := time.Millisecond // See selector.ComputeQueriedTimeRange for an explanation of this.

	testCases := map[string]struct {
		selector *MatrixSelector
		expected planning.QueriedTimeRange
	}{
		"no timestamp or offset": {
			selector: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Range: rng,
				},
			},
			expected: planning.NewQueriedTimeRange(startT.Add(-rng).Add(excludeLowerBoundary), endT),
		},
		"timestamp set, no offset": {
			selector: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Range:     rng,
					Timestamp: &ts,
				},
			},
			expected: planning.NewQueriedTimeRange(ts.Add(-rng).Add(excludeLowerBoundary), ts),
		},
		"offset set, no timestamp": {
			selector: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Range:  rng,
					Offset: offset,
				},
			},
			expected: planning.NewQueriedTimeRange(startT.Add(-rng).Add(-offset).Add(excludeLowerBoundary), endT.Add(-offset)),
		},
		"both timestamp and offset set": {
			selector: &MatrixSelector{
				MatrixSelectorDetails: &MatrixSelectorDetails{
					Range:     rng,
					Offset:    offset,
					Timestamp: &ts,
				},
			},
			expected: planning.NewQueriedTimeRange(ts.Add(-rng).Add(-offset).Add(excludeLowerBoundary), ts.Add(-offset)),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			timeRange, err := testCase.selector.QueriedTimeRange(queryTimeRange, 100*time.Minute)
			require.NoError(t, err)
			require.Equal(t, testCase.expected, timeRange)
		})
	}
}
