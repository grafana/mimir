// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
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
