// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
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
