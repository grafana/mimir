// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"flag"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

func mustNewActiveSeriesCustomTrackersConfig(t *testing.T, source map[string]string) *ActiveSeriesCustomTrackersConfig {
	m, err := newActiveSeriesCustomTrackersConfig(source)
	require.NoError(t, err)
	return &m
}

func TestActiveSeriesCustomTrackersConfigs(t *testing.T) {
	for _, tc := range []struct {
		name     string
		flags    []string
		expected *ActiveSeriesCustomTrackersConfig
		error    error
	}{
		{
			name:     "empty flag value produces empty config",
			flags:    []string{`-ingester.active-series-custom-trackers=`},
			expected: &ActiveSeriesCustomTrackersConfig{},
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
			expected: mustNewActiveSeriesCustomTrackersConfig(t, map[string]string{`foo`: `{foo="bar"}`}),
		},
		{
			name: "whitespaces are trimmed from name and matcher",
			flags: []string{`-ingester.active-series-custom-trackers= foo :	{foo="bar"}` + "\n "},
			expected: mustNewActiveSeriesCustomTrackersConfig(t, map[string]string{`foo`: `{foo="bar"}`}),
		},
		{
			name:     "two matchers in one flag value",
			flags:    []string{`-ingester.active-series-custom-trackers=foo:{foo="bar"};baz:{baz="bar"}`},
			expected: mustNewActiveSeriesCustomTrackersConfig(t, map[string]string{`foo`: `{foo="bar"}`, `baz`: `{baz="bar"}`}),
		},
		{
			name:     "two matchers in two flag values",
			flags:    []string{`-ingester.active-series-custom-trackers=foo:{foo="bar"}`, `-ingester.active-series-custom-trackers=baz:{baz="bar"}`},
			expected: mustNewActiveSeriesCustomTrackersConfig(t, map[string]string{`foo`: `{foo="bar"}`, `baz`: `{baz="bar"}`}),
		},
		{
			name:  "two matchers with same name in same flag",
			flags: []string{`-ingester.active-series-custom-trackers=foo:{foo="bar"};foo:{boo="bam"}`},
			error: errors.New(`invalid value "foo:{foo=\"bar\"};foo:{boo=\"bam\"}" for flag -ingester.active-series-custom-trackers: matcher "foo" for active series custom trackers is provided twice`),
		},
		{
			name:  "two matchers with same name in separate flags",
			flags: []string{`-ingester.active-series-custom-trackers=foo:{foo="bar"}`, `-ingester.active-series-custom-trackers=foo:{boo="bam"}`},
			error: errors.New(`invalid value "foo:{boo=\"bam\"}" for flag -ingester.active-series-custom-trackers: matcher "foo" for active series custom trackers is provided more than once`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flagSet := flag.NewFlagSet("test", flag.ContinueOnError)

			var config ActiveSeriesCustomTrackersConfig
			flagSet.Var(&config, "ingester.active-series-custom-trackers", "...usage docs...")
			err := flagSet.Parse(tc.flags)

			if tc.error != nil {
				assert.EqualError(t, err, tc.error.Error())
				return
			}

			require.Equal(t, tc.expected, &config)

			// Check that ActiveSeriesCustomTrackersConfig.String() value is a valid flag value.
			flagSetAgain := flag.NewFlagSet("test-string", flag.ContinueOnError)
			var configAgain ActiveSeriesCustomTrackersConfig
			flagSetAgain.Var(&configAgain, "ingester.active-series-custom-trackers", "...usage docs...")
			require.NoError(t, flagSetAgain.Parse([]string{"-ingester.active-series-custom-trackers=" + config.String()}))

			require.Equal(t, tc.expected, &configAgain)
		})
	}
}

func TestActiveSeriesMatcher_MatchesSeries(t *testing.T) {
	asm := NewActiveSeriesMatchers(mustNewActiveSeriesCustomTrackersConfig(t, map[string]string{
		"bar_starts_with_1":             `{bar=~"1.*"}`,
		"does_not_have_foo_label":       `{foo=""}`,
		"has_foo_and_bar_starts_with_1": `{foo!="", bar=~"1.*"}`,
		"has_foo_label":                 `{foo!=""}`,
	}))

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

func TestActiveSeriesCustomTrackersConfigs_MalformedMatcher(t *testing.T) {
	for _, matcher := range []string{
		`{foo}`,
		`{foo=~"}`,
	} {
		t.Run(matcher, func(t *testing.T) {
			config := map[string]string{
				"malformed": matcher,
			}

			_, err := newActiveSeriesCustomTrackersConfig(config)
			assert.Error(t, err)
		})
	}
}

func TestActiveSeriesMatcher_Equality(t *testing.T) {
	matcherSets := [][]string{
		{
			`foo:{foo="bar"};baz:{baz="bar"}`,
			`baz:{baz="bar"};foo:{foo="bar"}`,
			`  foo:{foo="bar"};baz:{baz="bar"} `,
		},
		{
			`test:{test="true"}`,
		},
		{
			`foo:{foo="bar"};baz:{baz="bar"};extra:{extra="extra"}`,
		},
	}

	for _, matcherSet := range matcherSets {
		t.Run("EqualityBetweenSet", func(t *testing.T) {
			var activeSeriesMatchers []*ActiveSeriesMatchers
			for _, matcherConfig := range matcherSet {
				config := &ActiveSeriesCustomTrackersConfig{}
				err := config.Set(matcherConfig)
				require.NoError(t, err)
				asm := NewActiveSeriesMatchers(config)
				activeSeriesMatchers = append(activeSeriesMatchers, asm)
			}
			for i := 0; i < len(activeSeriesMatchers); i++ {
				for j := i + 1; j < len(activeSeriesMatchers); j++ {
					assert.True(t, activeSeriesMatchers[i].Equals(activeSeriesMatchers[j]), "matcher configs should be equal")
				}
			}
		})
	}

	t.Run("NotEqualsAcrossSets", func(t *testing.T) {
		var activeSeriesMatchers []*ActiveSeriesMatchers
		for _, matcherConfigs := range matcherSets {
			exampleConfig := matcherConfigs[0]
			config := &ActiveSeriesCustomTrackersConfig{}
			err := config.Set(exampleConfig)
			require.NoError(t, err)
			asm := NewActiveSeriesMatchers(config)
			activeSeriesMatchers = append(activeSeriesMatchers, asm)
		}

		for i := 0; i < len(activeSeriesMatchers); i++ {
			for j := i + 1; j < len(activeSeriesMatchers); j++ {
				assert.False(t, activeSeriesMatchers[i].Equals(activeSeriesMatchers[j]), "matcher configs should NOT be equal")
			}
		}
	})

}

func TestActiveSeriesCustomTrackersConfigs_Deserialization(t *testing.T) {
	correctInput := `
        baz: "{baz='bar'}"
        foo: "{foo='bar'}"
    `
	malformedInput :=
		`
        baz: "123"
        foo: "{foo='bar'}"
    `
	t.Run("ShouldDeserializeCorrectInput", func(t *testing.T) {
		config := ActiveSeriesCustomTrackersConfig{}
		err := yaml.Unmarshal([]byte(correctInput), &config)
		assert.NoError(t, err, "failed do deserialize ActiveSeriesMatchers")
		expectedConfig, err := newActiveSeriesCustomTrackersConfig(map[string]string{
			"baz": "{baz='bar'}",
			"foo": "{foo='bar'}",
		})
		require.NoError(t, err)
		assert.Equal(t, expectedConfig.String(), config.String())
	})

	t.Run("ShouldErrorOnMalformedInput", func(t *testing.T) {
		config := ActiveSeriesCustomTrackersConfig{}
		err := yaml.Unmarshal([]byte(malformedInput), &config)
		assert.Error(t, err, "should not deserialize malformed input")
	})
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
