// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"flag"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

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

func TestRuntimeOverridesUnmarshal(t *testing.T) {
	r := ActiveSeriesCustomTrackersOverrides{}
	input := `
default:
  integrations/apolloserver: "{job='integrations/apollo-server'}"
  integrations/caddy: "{job='integrations/caddy'}"
tenant_specific:
  1:
    team_A: "{grafanacloud_team='team_a'}"
    team_B: "{grafanacloud_team='team_b'}"
`

	require.NoError(t, yaml.UnmarshalStrict([]byte(input), &r))
}

func TestActiveSeriesCustomTrackersOverridesProvider(t *testing.T) {
	overridesReference := &ActiveSeriesCustomTrackersOverrides{}
	tests := map[string]struct {
		provider *ActiveSeriesCustomTrackersOverridesProvider
		expected *ActiveSeriesCustomTrackersOverrides
	}{
		"nil provider returns nil": {
			provider: nil,
			expected: nil,
		},
		"nil getter returns nil": {
			provider: &ActiveSeriesCustomTrackersOverridesProvider{},
			expected: nil,
		},
		"getter is called": {
			provider: &ActiveSeriesCustomTrackersOverridesProvider{
				Getter: func() *ActiveSeriesCustomTrackersOverrides {
					return overridesReference
				},
			},
			expected: overridesReference,
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.provider.Get())
		})
	}
}

func TestMatchersForUser(t *testing.T) {
	defaultMatchers := mustNewActiveSeriesCustomTrackersConfig(t, map[string]string{
		"foo": `{foo="bar"}`,
		"bar": `{baz="bar"}`,
	})
	tenantSpecificMatchers := mustNewActiveSeriesCustomTrackersConfig(t, map[string]string{
		"team_a": `{team="team_a"}`,
		"team_b": `{team="team_b"}`,
	})

	activeSeriesCustomTrackersOverrides := &ActiveSeriesCustomTrackersOverrides{
		Default: defaultMatchers,
		TenantSpecific: map[string]*ActiveSeriesCustomTrackersConfig{
			"1": tenantSpecificMatchers,
		},
	}

	tests := map[string]struct {
		userID   string
		expected *ActiveSeriesCustomTrackersConfig
	}{
		"User with no override should return default": {
			userID:   "5",
			expected: defaultMatchers,
		},
		"User with override should return override": {
			userID:   "1",
			expected: tenantSpecificMatchers,
		},
	}
	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			matchersConfigForUser := activeSeriesCustomTrackersOverrides.MatchersConfigForUser(testData.userID)
			assert.True(t, testData.expected.String() == matchersConfigForUser.String())
		})
	}
}

func TestActiveSeriesCustomTrackerConfig_Equality(t *testing.T) {
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
			var activeSeriesCustomTrackersConfigs []*ActiveSeriesCustomTrackersConfig
			for _, matcherConfig := range matcherSet {
				config := &ActiveSeriesCustomTrackersConfig{}
				err := config.Set(matcherConfig)
				require.NoError(t, err)
				activeSeriesCustomTrackersConfigs = append(activeSeriesCustomTrackersConfigs, config)
			}
			for i := 0; i < len(activeSeriesCustomTrackersConfigs); i++ {
				for j := i + 1; j < len(activeSeriesCustomTrackersConfigs); j++ {
					assert.Equal(t, activeSeriesCustomTrackersConfigs[i].String(), activeSeriesCustomTrackersConfigs[j].String(), "matcher configs should be equal")
				}
			}
		})
	}

	t.Run("NotEqualsAcrossSets", func(t *testing.T) {
		var activeSeriesMatchers []*ActiveSeriesCustomTrackersConfig
		for _, matcherConfigs := range matcherSets {
			exampleConfig := matcherConfigs[0]
			config := &ActiveSeriesCustomTrackersConfig{}
			err := config.Set(exampleConfig)
			require.NoError(t, err)
			activeSeriesMatchers = append(activeSeriesMatchers, config)
		}

		for i := 0; i < len(activeSeriesMatchers); i++ {
			for j := i + 1; j < len(activeSeriesMatchers); j++ {
				assert.NotEqual(t, activeSeriesMatchers[i].String(), activeSeriesMatchers[j].String(), "matcher configs should NOT be equal")
			}
		}
	})

}
