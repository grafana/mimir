// SPDX-License-Identifier: AGPL-3.0-only

package activeseries

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
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
			series: labels.Labels{{Name: "foo", Value: "true"}, {Name: "baz", Value: "unrelated"}},
			expected: []int{
				3, // has_foo_label
			},
		},
		{
			series: labels.Labels{{Name: "foo", Value: "true"}, {Name: "bar", Value: "100"}, {Name: "baz", Value: "unrelated"}},
			expected: []int{
				0, // bar_starts_with_1
				2, // has_foo_and_bar_starts_with_1
				3, // has_foo_label
			},
		},
		{
			series: labels.Labels{{Name: "foo", Value: "true"}, {Name: "bar", Value: "200"}, {Name: "baz", Value: "unrelated"}},
			expected: []int{
				3, // has_foo_label
			},
		},
		{
			series: labels.Labels{{Name: "bar", Value: "200"}, {Name: "baz", Value: "unrelated"}},
			expected: []int{
				1, // does_not_have_foo_label
			},
		},
		{
			series: labels.Labels{{Name: "bar", Value: "100"}, {Name: "baz", Value: "unrelated"}},
			expected: []int{
				0, // bar_starts_with_1
				1, // does_not_have_foo_label
			},
		},
		{
			series: labels.Labels{{Name: "baz", Value: "unrelated"}},
			expected: []int{
				1, // does_not_have_foo_label
			},
		},
	} {
		t.Run(tc.series.String(), func(t *testing.T) {
			got := asm.Matches(tc.series)
			assert.Equal(t, tc.expected, fixedSliceToSlice(got))
		})
	}
}

func fixedSliceToSlice(fixed fixedSlice) []int {
	slice := make([]int, fixed.len())
	for i := 0; i < fixed.len(); i++ {
		slice[i] = fixed.get(i)
	}
	return slice
}

func BenchmarkMatchesSeries(b *testing.B) {

	trackerCounts := []int{10, 100, 1000, 10000}
	asms := make([]*Matchers, len(trackerCounts))

	for i, matcherCount := range trackerCounts {
		configMap := map[string]string{}
		for j := 0; j < matcherCount; j++ {
			configMap[strconv.Itoa(j)] = fmt.Sprintf("{grafanacloud_usage_group=~%d.*}", j)
		}
		config, _ := NewCustomTrackersConfig(configMap)
		asms[i] = NewMatchers(config)

	}

	labelCounts := []int{1, 10, 100}
	series := make([]labels.Labels, len(labelCounts))
	for i, labelCount := range labelCounts {
		l := labels.Labels{
			{Name: "grafanacloud_usage_group", Value: "1"}, // going to match exactly to one matcher
		}
		for j := 1; j < labelCount; j++ {
			labelEntry := labels.Label{Name: fmt.Sprintf("foo%d", j), Value: "true"}
			l = append(l, labelEntry)
		}
		series[i] = l
	}

	for i, trackerCount := range trackerCounts {
		for j, labelCount := range labelCounts {
			b.Run(fmt.Sprintf("TrackerCount: %d, LabelCount: %d", trackerCount, labelCount), func(b *testing.B) {
				for x := 0; x < b.N; x++ {
					got := asms[i].Matches(series[j])
					if got.len() > 2 {
						b.FailNow()
					}
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
	assert.Equal(t, expected, amlabelMatcherToProm(amMatcher))
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
