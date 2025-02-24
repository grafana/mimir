// SPDX-License-Identifier: AGPL-3.0-only

package activeseriesmodel

import (
	"fmt"
	"strconv"
	"testing"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatcher_MatchesSeries(t *testing.T) {
	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"bar_starts_with_1":             `{bar=~"1.*"}`,
		"does_not_have_foo_label":       `{foo=""}`,
		"has_foo_and_bar_starts_with_1": `{foo!="", bar=~"1.*"}`,
		"has_foo_label":                 `{foo!=""}`,
	}))

	for _, tc := range []struct {
		series   labels.Labels
		expected []int
	}{
		{
			series: labels.FromStrings("foo", "true", "baz", "unrelated"),
			expected: []int{
				3, // has_foo_label
			},
		},
		{
			series: labels.FromStrings("foo", "true", "bar", "100", "baz", "unrelated"),
			expected: []int{
				0, // bar_starts_with_1
				2, // has_foo_and_bar_starts_with_1
				3, // has_foo_label
			},
		},
		{
			series: labels.FromStrings("foo", "true", "bar", "200", "baz", "unrelated"),
			expected: []int{
				3, // has_foo_label
			},
		},
		{
			series: labels.FromStrings("bar", "200", "baz", "unrelated"),
			expected: []int{
				1, // does_not_have_foo_label
			},
		},
		{
			series: labels.FromStrings("bar", "100", "baz", "unrelated"),
			expected: []int{
				0, // bar_starts_with_1
				1, // does_not_have_foo_label
			},
		},
		{
			series: labels.FromStrings("baz", "unrelated"),
			expected: []int{
				1, // does_not_have_foo_label
			},
		},
	} {
		t.Run(tc.series.String(), func(t *testing.T) {
			got := asm.Matches(tc.series)
			assert.Equal(t, tc.expected, preAllocDynamicSliceToSlice(got))
		})
	}
}

func preAllocDynamicSliceToSlice(prealloc PreAllocDynamicSlice) []int {
	slice := make([]int, prealloc.Len())
	for i := 0; i < prealloc.Len(); i++ {
		slice[i] = int(prealloc.Get(i))
	}
	return slice
}

func BenchmarkMatchesSeries(b *testing.B) {
	trackerCounts := []int{10, 100, 1000}
	asms := make([]*Matchers, len(trackerCounts))

	for i, matcherCount := range trackerCounts {
		configMap := map[string]string{}
		for j := 0; j < matcherCount; j++ {
			configMap[strconv.Itoa(j)] = fmt.Sprintf(`{this_will_match_%d="true"}`, j)
		}
		config, err := NewCustomTrackersConfig(configMap)
		require.NoError(b, err)
		asms[i] = NewMatchers(config)
	}

	makeLabels := func(total, matching int) labels.Labels {
		if total < matching {
			b.Fatal("wrong test setup, total < matching")
		}
		builder := labels.NewScratchBuilder(total)
		for i := 0; i < matching; i++ {
			builder.Add(fmt.Sprintf("this_will_match_%d", i), "true")
		}
		for i := matching; i < total; i++ {
			builder.Add(fmt.Sprintf("something_else_%d", i), "true")
		}
		builder.Sort()
		return builder.Labels()
	}

	for i, trackerCount := range trackerCounts {
		for _, bc := range []struct {
			total, matching int
		}{
			{1, 0},
			{1, 1},
			{10, 1},
			{10, 2},
			{10, 5},
			{25, 1},
			{25, 2},
			{25, 5},
			{100, 1},
			{100, 2},
			{100, 5},
		} {
			series := makeLabels(bc.total, bc.matching)
			b.Run(fmt.Sprintf("TrackerCount: %d, Labels: %d, Matching: %d", trackerCount, bc.total, bc.matching), func(b *testing.B) {
				for x := 0; x < b.N; x++ {
					got := asms[i].Matches(series)
					require.Equal(b, bc.matching, got.Len())
				}
			})
		}
	}
}

func TestCustomTrackersConfigs_MalformedMatcher(t *testing.T) {
	for _, matcher := range []string{
		`{foo}`,
		`{foo=~"}`,
	} {
		t.Run(matcher, func(t *testing.T) {
			config := map[string]string{
				"malformed": matcher,
			}

			_, err := NewCustomTrackersConfig(config)
			assert.Error(t, err)
		})
	}
}

func TestAmlabelMatchersToProm_HappyCase(t *testing.T) {
	amMatcher, err := amlabels.NewMatcher(amlabels.MatchRegexp, "foo", "bar.*")
	require.NoError(t, err)

	expected := labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar.*")
	assert.Equal(t, expected.String(), amlabelMatcherToProm(amMatcher).String())
}

func TestAmlabelMatchersToProm_MatchTypeValues(t *testing.T) {
	lastType := amlabels.MatchNotRegexp
	// just checking that our assumption on that MatchType enums are the same is correct
	for mt := amlabels.MatchEqual; mt <= lastType; mt++ {
		assert.Equal(t, mt.String(), labels.MatchType(mt).String())
	}
	// and that nobody just added more match types in amlabels,
	assert.Panics(t, func() {
		_ = (lastType + 1).String()
	}, "amlabels.MatchNotRegexp is expected to be the last enum value, update the test and check mapping")
}
