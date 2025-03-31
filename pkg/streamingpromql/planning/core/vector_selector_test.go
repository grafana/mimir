// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
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
					Timestamp: &Timestamp{Timestamp: 123456},
				},
			},
			expected: `{__name__="foo"} @ 123456 (1970-01-01T00:02:03.456Z)`,
		},
		"one matcher, has timestamp and offset": {
			node: &VectorSelector{
				VectorSelectorDetails: &VectorSelectorDetails{
					Matchers:  singleMatcher,
					Offset:    time.Hour,
					Timestamp: &Timestamp{Timestamp: 123456},
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
