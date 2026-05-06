// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

func TestDuplicate_SplittingCacheKey(t *testing.T) {
	testCases := map[string]struct {
		matchers []*core.LabelMatcher
		expected string
	}{
		"single matcher": {
			matchers: []*core.LabelMatcher{
				{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
			},
			expected: `{__name__="foo"}`,
		},
		"no matchers": {
			matchers: []*core.LabelMatcher{},
			expected: `{}`,
		},
		"multiple matchers": {
			matchers: []*core.LabelMatcher{
				{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
				{Name: "env", Type: labels.MatchNotEqual, Value: "test"},
			},
			expected: `{__name__="foo", env!="test"}`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			d := &Duplicate{
				DuplicateDetails: &DuplicateDetails{},
				Inner: &core.MatrixSelector{
					MatrixSelectorDetails: &core.MatrixSelectorDetails{
						Matchers: tc.matchers,
						Range:    2 * time.Hour,
					},
				},
			}
			require.Equal(t, tc.expected, d.SplittingCacheKey())
		})
	}
}

func TestDuplicateFilter_SplittingCacheKey(t *testing.T) {
	innerMatchers := []*core.LabelMatcher{
		{Name: "__name__", Type: labels.MatchEqual, Value: "foo"},
	}

	testCases := map[string]struct {
		filters     []*core.LabelMatcher
		subsetIndex int64
		expected    string
	}{
		"single filter": {
			filters: []*core.LabelMatcher{
				{Name: "a", Type: labels.MatchEqual, Value: "1"},
			},
			expected: `duplicate_filter({a="1"}, 0, ({__name__="foo"}))`,
		},
		"multiple filters": {
			filters: []*core.LabelMatcher{
				{Name: "a", Type: labels.MatchEqual, Value: "1"},
				{Name: "b", Type: labels.MatchNotEqual, Value: "2"},
			},
			expected: `duplicate_filter({a="1", b!="2"}, 0, ({__name__="foo"}))`,
		},
		"subset index is part of the cache key": {
			filters: []*core.LabelMatcher{
				{Name: "a", Type: labels.MatchEqual, Value: "1"},
			},
			subsetIndex: 7,
			expected:    `duplicate_filter({a="1"}, 7, ({__name__="foo"}))`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			f := &DuplicateFilter{
				DuplicateFilterDetails: &DuplicateFilterDetails{
					Filters:     tc.filters,
					SubsetIndex: tc.subsetIndex,
				},
				Inner: &Duplicate{
					DuplicateDetails: &DuplicateDetails{},
					Inner: &core.MatrixSelector{
						MatrixSelectorDetails: &core.MatrixSelectorDetails{
							Matchers: innerMatchers,
							Range:    2 * time.Hour,
						},
					},
				},
			}
			require.Equal(t, tc.expected, f.SplittingCacheKey())
		})
	}
}
