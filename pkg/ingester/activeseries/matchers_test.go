// SPDX-License-Identifier: AGPL-3.0-only

package activeseries

import (
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
		expected map[int]bool
	}{
		{
			series: labels.Labels{{Name: "foo", Value: "true"}, {Name: "baz", Value: "unrelated"}},
			expected: map[int]bool{
				3: true, // has_foo_label
			},
		},
		{
			series: labels.Labels{{Name: "foo", Value: "true"}, {Name: "bar", Value: "100"}, {Name: "baz", Value: "unrelated"}},
			expected: map[int]bool{
				0: true, // bar_starts_with_1
				2: true, // has_foo_and_bar_starts_with_1
				3: true, // has_foo_label
			},
		},
		{
			series: labels.Labels{{Name: "foo", Value: "true"}, {Name: "bar", Value: "200"}, {Name: "baz", Value: "unrelated"}},
			expected: map[int]bool{
				3: true, // has_foo_label
			},
		},
		{
			series: labels.Labels{{Name: "bar", Value: "200"}, {Name: "baz", Value: "unrelated"}},
			expected: map[int]bool{
				1: true, // does_not_have_foo_label
			},
		},
		{
			series: labels.Labels{{Name: "bar", Value: "100"}, {Name: "baz", Value: "unrelated"}},
			expected: map[int]bool{
				0: true, // bar_starts_with_1
				1: true, // does_not_have_foo_label
			},
		},
		{
			series: labels.Labels{{Name: "baz", Value: "unrelated"}},
			expected: map[int]bool{
				1: true, // does_not_have_foo_label
			},
		},
	} {
		t.Run(tc.series.String(), func(t *testing.T) {
			got := asm.Matches(tc.series)
			assert.Equal(t, tc.expected, got)
		})
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
