// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"flag"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
)

func TestActiveSeriesCustomTrackersConfigs(t *testing.T) {
	for _, tc := range []struct {
		name     string
		flags    []string
		expected ActiveSeriesCustomTrackersConfig
		error    error
	}{
		{
			name:     "empty flag value produces empty config",
			flags:    []string{`-ingester.active-series-custom-trackers=`},
			expected: nil,
		},
		{
			name:  "empty matcher fails",
			flags: []string{`-ingester.active-series-custom-trackers=foo:`},
			error: errors.New(`invalid value "foo:" for flag -ingester.active-series-custom-trackers: semicolon-separated values should be <name>:<matcher>, but one of the sides was empty in the value 0: "foo:"`),
		},
		{
			name:  "empty whitespace-only matcher fails",
			flags: []string{`-ingester.active-series-custom-trackers=foo: `},
			error: errors.New(`invalid value "foo: " for flag -ingester.active-series-custom-trackers: semicolon-separated values should be <name>:<matcher>, but one of the sides was empty in the value 0: "foo: "`),
		},
		{
			name:  "second empty whitespace-only matcher fails",
			flags: []string{`-ingester.active-series-custom-trackers=foo: ;bar:{}`},
			error: errors.New(`invalid value "foo: ;bar:{}" for flag -ingester.active-series-custom-trackers: semicolon-separated values should be <name>:<matcher>, but one of the sides was empty in the value 0: "foo: "`),
		},
		{
			name:  "empty name fails",
			flags: []string{`-ingester.active-series-custom-trackers=:{}`},
			error: errors.New(`invalid value ":{}" for flag -ingester.active-series-custom-trackers: semicolon-separated values should be <name>:<matcher>, but one of the sides was empty in the value 0: ":{}"`),
		},
		{
			name:  "empty whitespace-only name fails",
			flags: []string{`-ingester.active-series-custom-trackers= :{}`},
			error: errors.New(`invalid value " :{}" for flag -ingester.active-series-custom-trackers: semicolon-separated values should be <name>:<matcher>, but one of the sides was empty in the value 0: " :{}"`),
		},
		{
			name:     "one matcher",
			flags:    []string{`-ingester.active-series-custom-trackers=foo:{foo="bar"}`},
			expected: ActiveSeriesCustomTrackersConfig{`foo`: `{foo="bar"}`},
		},
		{
			name: "whitespaces are trimmed from name and matcher",
			flags: []string{`-ingester.active-series-custom-trackers= foo :	{foo="bar"}` + "\n "},
			expected: ActiveSeriesCustomTrackersConfig{`foo`: `{foo="bar"}`},
		},
		{
			name:     "two matchers in one flag value",
			flags:    []string{`-ingester.active-series-custom-trackers=foo:{foo="bar"};baz:{baz="bar"}`},
			expected: ActiveSeriesCustomTrackersConfig{`foo`: `{foo="bar"}`, `baz`: `{baz="bar"}`},
		},
		{
			name:     "two matchers in two flag values",
			flags:    []string{`-ingester.active-series-custom-trackers=foo:{foo="bar"}`, `-ingester.active-series-custom-trackers=baz:{baz="bar"}`},
			expected: ActiveSeriesCustomTrackersConfig{`foo`: `{foo="bar"}`, `baz`: `{baz="bar"}`},
		},
		{
			name:  "two matchers with same name in same flag",
			flags: []string{`-ingester.active-series-custom-trackers=foo:{foo="bar"};foo:{boo="bam"}`},
			error: errors.New(`invalid value "foo:{foo=\"bar\"};foo:{boo=\"bam\"}" for flag -ingester.active-series-custom-trackers: matcher "foo" for active series custom trackers is provided twice`),
		},
		{
			name:  "two matchers with same name in separate flags",
			flags: []string{`-ingester.active-series-custom-trackers=foo:{foo="bar"}`, `-ingester.active-series-custom-trackers=foo:{boo="bam"}`},
			error: errors.New(`invalid value "foo:{boo=\"bam\"}" for flag -ingester.active-series-custom-trackers: matcher "foo" for active series custom trackers is provided twice`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flagSet := flag.NewFlagSet("test", flag.ContinueOnError)

			var config ActiveSeriesCustomTrackersConfig
			flagSet.Var(&config, "ingester.active-series-custom-trackers", "...usage docs...")
			err := flagSet.Parse(tc.flags)

			if tc.error != nil {
				assert.EqualError(t, err, tc.error.Error())
			} else {
				assert.Equal(t, tc.expected, config)
			}
		})
	}
}

func TestActiveSeriesMatcher_MatchesSeries(t *testing.T) {
	config := ActiveSeriesCustomTrackersConfig{
		"bar_starts_with_1":             `{bar=~"1.*"}`,
		"does_not_have_foo_label":       `{foo=""}`,
		"has_foo_and_bar_starts_with_1": `{foo!="", bar=~"1.*"}`,
		"has_foo_label":                 `{foo!=""}`,
	}

	asm, err := NewActiveSeriesMatchers(config)
	require.NoError(t, err)

	for _, tc := range []struct {
		series   labels.Labels
		expected []bool
	}{
		{
			series: labels.Labels{{Name: "foo", Value: "true"}, {Name: "baz", Value: "unrelated"}},
			expected: []bool{
				false, // bar_starts_with_1
				false, // does_not_have_foo_label
				false, // has_foo_and_bar_starts_with_1
				true,  // has_foo_label
			},
		},
		{
			series: labels.Labels{{Name: "foo", Value: "true"}, {Name: "bar", Value: "100"}, {Name: "baz", Value: "unrelated"}},
			expected: []bool{
				true,  // bar_starts_with_1
				false, // does_not_have_foo_label
				true,  // has_foo_and_bar_starts_with_1
				true,  // has_foo_label
			},
		},
		{
			series: labels.Labels{{Name: "foo", Value: "true"}, {Name: "bar", Value: "200"}, {Name: "baz", Value: "unrelated"}},
			expected: []bool{
				false, // bar_starts_with_1
				false, // does_not_have_foo_label
				false, // has_foo_and_bar_starts_with_1
				true,  // has_foo_label
			},
		},
		{
			series: labels.Labels{{Name: "bar", Value: "200"}, {Name: "baz", Value: "unrelated"}},
			expected: []bool{
				false, // bar_starts_with_1
				true,  // does_not_have_foo_label
				false, // has_foo_and_bar_starts_with_1
				false, // has_foo_label
			},
		},
		{
			series: labels.Labels{{Name: "bar", Value: "100"}, {Name: "baz", Value: "unrelated"}},
			expected: []bool{
				true,  // bar_starts_with_1
				true,  // does_not_have_foo_label
				false, // has_foo_and_bar_starts_with_1
				false, // has_foo_label
			},
		},
		{
			series: labels.Labels{{Name: "baz", Value: "unrelated"}},
			expected: []bool{
				false, // bar_starts_with_1
				true,  // does_not_have_foo_label
				false, // has_foo_and_bar_starts_with_1
				false, // has_foo_label
			},
		},
	} {
		t.Run(tc.series.String(), func(t *testing.T) {
			got := asm.Matches(tc.series)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestActiveSeriesMatcher_MalformedMatcher(t *testing.T) {
	for _, matcher := range []string{
		`{foo}`,
		`{foo=~"}`,
	} {
		t.Run(matcher, func(t *testing.T) {
			config := ActiveSeriesCustomTrackersConfig{
				"malformed": matcher,
			}

			_, err := NewActiveSeriesMatchers(config)
			assert.Error(t, err)
		})
	}
}

func TestAmlabelMatchersToProm_HappyCase(t *testing.T) {
	t.Run("happy case", func(t *testing.T) {
		amMatcher, err := amlabels.NewMatcher(amlabels.MatchRegexp, "foo", "bar.*")
		require.NoError(t, err)

		expected := labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar.*")
		assert.Equal(t, expected, amlabelMatcherToProm(amMatcher))
	})
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
