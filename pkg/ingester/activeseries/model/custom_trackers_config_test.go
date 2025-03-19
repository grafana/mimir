// SPDX-License-Identifier: AGPL-3.0-only

package activeseriesmodel

import (
	"bytes"
	"flag"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func mustNewCustomTrackersConfigFromMap(t require.TestingT, source map[string]string) CustomTrackersConfig {
	m, err := NewCustomTrackersConfig(source)
	require.NoError(t, err)
	return m
}

func mustNewCustomTrackersConfigFromString(t *testing.T, source string) CustomTrackersConfig {
	m := CustomTrackersConfig{}
	err := m.Set(source)
	require.NoError(t, err)
	return m
}

func mustNewCustomTrackersConfigDeserializedFromYaml(t *testing.T, yamlString string) CustomTrackersConfig {
	m := CustomTrackersConfig{}
	err := yaml.Unmarshal([]byte(yamlString), &m)
	require.NoError(t, err)
	return m
}

func TestCustomTrackersConfigs(t *testing.T) {
	for _, tc := range []struct {
		name     string
		flags    []string
		expected CustomTrackersConfig
		error    error
	}{
		{
			name:     "empty flag value produces empty config",
			flags:    []string{`-ingester.active-series-custom-trackers=`},
			expected: CustomTrackersConfig{},
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
			expected: mustNewCustomTrackersConfigFromMap(t, map[string]string{`foo`: `{foo="bar"}`}),
		},
		{
			name:     "whitespaces are trimmed from name and matcher",
			flags:    []string{`-ingester.active-series-custom-trackers= foo :      {foo="bar"}` + "\n "},
			expected: mustNewCustomTrackersConfigFromMap(t, map[string]string{`foo`: `{foo="bar"}`}),
		},
		{
			name:     "two matchers in one flag value",
			flags:    []string{`-ingester.active-series-custom-trackers=foo:{foo="bar"};baz:{baz="bar"}`},
			expected: mustNewCustomTrackersConfigFromMap(t, map[string]string{`foo`: `{foo="bar"}`, `baz`: `{baz="bar"}`}),
		},
		{
			name:     "two matchers in two flag values",
			flags:    []string{`-ingester.active-series-custom-trackers=foo:{foo="bar"}`, `-ingester.active-series-custom-trackers=baz:{baz="bar"}`},
			expected: mustNewCustomTrackersConfigFromMap(t, map[string]string{`foo`: `{foo="bar"}`, `baz`: `{baz="bar"}`}),
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

			var config CustomTrackersConfig
			flagSet.Var(&config, "ingester.active-series-custom-trackers", "...usage docs...")
			err := flagSet.Parse(tc.flags)

			if tc.error != nil {
				assert.EqualError(t, err, tc.error.Error())
				return
			}

			require.Equal(t, tc.expected, config)

			// Check that CustomTrackersConfig.String() value is a valid flag value.
			flagSetAgain := flag.NewFlagSet("test-string", flag.ContinueOnError)
			var configAgain CustomTrackersConfig
			flagSetAgain.Var(&configAgain, "ingester.active-series-custom-trackers", "...usage docs...")
			require.NoError(t, flagSetAgain.Parse([]string{"-ingester.active-series-custom-trackers=" + config.String()}))

			require.Equal(t, tc.expected, configAgain)
		})
	}
}

func TestMaximumNumberOfTrackers(t *testing.T) {
	t.Run("Flag based setup", func(t *testing.T) {
		var flagToSet bytes.Buffer
		numberOfTrackers := maxNumberOfTrackers + 1
		for i := 0; i < numberOfTrackers; i++ {
			flagToSet.WriteString(fmt.Sprintf("name%d:{__name__=%d}", i, i))
			if i < numberOfTrackers-1 {
				flagToSet.WriteString(";")
			}
		}

		c := CustomTrackersConfig{}
		err := c.Set(flagToSet.String())
		require.Error(t, err, "custom tracker config should not accept more than %d trackers", maxNumberOfTrackers)
	})

	t.Run("Map based setup", func(t *testing.T) {
		configMap := map[string]string{}
		numberOfTrackers := maxNumberOfTrackers + 1
		for i := 0; i < numberOfTrackers; i++ {
			configMap[fmt.Sprintf("name%d", i)] = fmt.Sprintf("{__name__=%d}", i)
		}

		_, err := NewCustomTrackersConfig(configMap)
		require.Error(t, err, "custom tracker config should not accept more than %d trackers", maxNumberOfTrackers)
	})
}

func TestCustomTrackerConfig_Equality(t *testing.T) {
	configSets := [][]CustomTrackersConfig{
		{
			mustNewCustomTrackersConfigFromString(t, `foo:{foo='bar'};baz:{baz='bar'}`),
			mustNewCustomTrackersConfigFromMap(t, map[string]string{
				"baz": `{baz='bar'}`,
				"foo": `{foo='bar'}`,
			}),
			mustNewCustomTrackersConfigDeserializedFromYaml(t,
				`
                baz: "{baz='bar'}"
                foo: "{foo='bar'}"`),
		},
		{
			mustNewCustomTrackersConfigFromString(t, `test:{test='true'}`),
			mustNewCustomTrackersConfigFromMap(t, map[string]string{"test": `{test='true'}`}),
			mustNewCustomTrackersConfigDeserializedFromYaml(t, `test: "{test='true'}"`),
		},
		{
			mustNewCustomTrackersConfigDeserializedFromYaml(t,
				`
        baz: "{baz='bar'}"
        foo: "{foo='bar'}"
        extra: "{extra='extra'}"`),
		},
	}

	for _, configSet := range configSets {
		t.Run("EqualityBetweenSet", func(t *testing.T) {
			for i := 0; i < len(configSet); i++ {
				for j := i + 1; j < len(configSet); j++ {
					assert.Equal(t, configSet[i].String(), configSet[j].String(), "matcher configs should be equal")
				}
			}
		})
	}

	t.Run("NotEqualsAcrossSets", func(t *testing.T) {
		var activeSeriesMatchers []*CustomTrackersConfig
		for _, matcherConfigs := range configSets {
			activeSeriesMatchers = append(activeSeriesMatchers, &matcherConfigs[0])
		}

		for i := 0; i < len(activeSeriesMatchers); i++ {
			for j := i + 1; j < len(activeSeriesMatchers); j++ {
				assert.NotEqual(t, activeSeriesMatchers[i].String(), activeSeriesMatchers[j].String(), "matcher configs should NOT be equal")
			}
		}
	})

}

func TestTrackersConfigs_Deserialization(t *testing.T) {
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
		config := CustomTrackersConfig{}
		err := yaml.Unmarshal([]byte(correctInput), &config)
		assert.NoError(t, err, "failed do deserialize Matchers")
		expectedConfig, err := NewCustomTrackersConfig(map[string]string{
			"baz": "{baz='bar'}",
			"foo": "{foo='bar'}",
		})
		require.NoError(t, err)
		assert.Equal(t, expectedConfig.String(), config.String())
	})

	t.Run("ShouldErrorOnMalformedInput", func(t *testing.T) {
		config := CustomTrackersConfig{}
		err := yaml.Unmarshal([]byte(malformedInput), &config)
		assert.Error(t, err, "should not deserialize malformed input")
	})
}

func TestTrackersConfigs_SerializeDeserialize(t *testing.T) {
	sourceYAML := `
    baz: "{baz='bar'}"
    foo: "{foo='bar'}"
    `

	obj := mustNewCustomTrackersConfigDeserializedFromYaml(t, sourceYAML)

	t.Run("ShouldSerializeDeserializeResultsTheSame", func(t *testing.T) {
		out, err := yaml.Marshal(obj)
		require.NoError(t, err, "failed do serialize Custom trackers config")
		reSerialized := CustomTrackersConfig{}
		err = yaml.Unmarshal(out, &reSerialized)
		require.NoError(t, err, "Failed to deserialize serialized object")
		assert.Equal(t, obj, reSerialized)
	})
}

func TestCustomTrackersConfig_Equal(t *testing.T) {
	tc := map[string]struct {
		cfg1     CustomTrackersConfig
		cfg2     CustomTrackersConfig
		expected bool
	}{

		"Equal configurations": {
			cfg1:     mustNewCustomTrackersConfigFromString(t, `foo:{foo="bar"};baz:{baz="bar"}`),
			cfg2:     mustNewCustomTrackersConfigFromString(t, `foo:{foo="bar"};baz:{baz="bar"}`),
			expected: true,
		},
		"Different descriptions": {
			cfg1: func() CustomTrackersConfig {
				c := mustNewCustomTrackersConfigFromString(t, `foo:{foo="bar"};baz:{baz="bar"}`)
				c.string = "cfg1"
				return c
			}(),
			cfg2: func() CustomTrackersConfig {
				c := mustNewCustomTrackersConfigFromString(t, `foo:{foo="bar"};baz:{baz="bar"}`)
				c.string = "cfg2"
				return c
			}(),
			expected: false,
		},
		"Different source maps": {
			cfg1: func() CustomTrackersConfig {
				c := mustNewCustomTrackersConfigFromString(t, `foo:{foo="bar"};baz:{baz="bar"}`)
				c.source = map[string]string{"a": "source1"}
				return c
			}(),
			cfg2: func() CustomTrackersConfig {
				c := mustNewCustomTrackersConfigFromString(t, `foo:{foo="bar"};baz:{baz="bar"}`)
				c.source = map[string]string{"b": "source2"}
				return c
			}(),
			expected: false,
		},
		"Different config maps": {
			cfg1: func() CustomTrackersConfig {
				c := mustNewCustomTrackersConfigFromString(t, `foo:{foo="bar"};baz:{baz="bar"}`)
				c.config = map[string]labelsMatchers{"a": nil}
				return c
			}(),
			cfg2: func() CustomTrackersConfig {
				c := mustNewCustomTrackersConfigFromString(t, `foo:{foo="bar"};baz:{baz="bar"}`)
				c.config = map[string]labelsMatchers{"b": nil}
				return c
			}(),
			expected: false,
		},
		"Different keys in source maps": {
			cfg1: CustomTrackersConfig{
				source: map[string]string{"a": "source1"},
				config: map[string]labelsMatchers{"b": nil},
				string: "cfg1",
			},
			cfg2: CustomTrackersConfig{
				source: map[string]string{"c": "source1"},
				config: map[string]labelsMatchers{"b": nil},
				string: "cfg1",
			},
			expected: false,
		},
		"Different keys in config maps": {
			cfg1: CustomTrackersConfig{
				source: map[string]string{"a": "source1"},
				config: map[string]labelsMatchers{"b": nil},
				string: "cfg1",
			},
			cfg2: CustomTrackersConfig{
				source: map[string]string{"a": "source1"},
				config: map[string]labelsMatchers{"c": nil},
				string: "cfg1",
			},
			expected: false,
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.cfg1.Equal(tt.cfg2))
			assert.Equal(t, tt.expected, cmp.Equal(tt.cfg1, tt.cfg2))
		})
	}
}

func TestMergeCustomTrackersConfig(t *testing.T) {
	tests := map[string]struct {
		first    string
		second   string
		expected string
	}{
		"both configs are empty": {
			first:    "",
			second:   "",
			expected: "",
		},
		"the first config is empty": {
			first: "",
			second: `
                baz: "{baz='bar'}"
                foo: "{foo='bar'}"`,
			expected: `
                baz: "{baz='bar'}"
                foo: "{foo='bar'}"`,
		},
		"the second config is empty": {
			first: `
                baz: "{baz='bar'}"
                foo: "{foo='bar'}"`,
			second: "",
			expected: `
                baz: "{baz='bar'}"
                foo: "{foo='bar'}"`,
		},
		"both configs are non-empty and they both have the same key-value pairs in the same order": {
			first: `
                baz: "{baz='bar'}"
                foo: "{foo='bar'}"`,
			second: `
                baz: "{baz='bar'}"
                foo: "{foo='bar'}"`,
			expected: `
                baz: "{baz='bar'}"
                foo: "{foo='bar'}"`,
		},
		"both configs are non-empty and they both have the same key-value pairs but in different order": {
			first: `
                baz: "{baz='bar'}"
                foo: "{foo='bar'}"`,
			second: `
                foo: "{foo='bar'}"
                baz: "{baz='bar'}"`,
			expected: `
                baz: "{baz='bar'}"
                foo: "{foo='bar'}"`,
		},
		"both configs are non-empty and they some but not all overlapping keys": {
			first: `
                baz: "{baz='first'}"
                foo: "{foo='first'}"`,
			second: `
                foo: "{foo='second'}"
                bar: "{bar='second'}"`,
			expected: `
                bar: "{bar='second'}"
                baz: "{baz='first'}"
                foo: "{foo='second'}"`,
		},
		"both configs are non-empty and they no overlapping keys": {
			first: `
                baz: "{baz='first'}"
                foo: "{foo='first'}"`,
			second: `
                bar: "{bar='second'}"`,
			expected: `
                bar: "{bar='second'}"
                baz: "{baz='first'}"
                foo: "{foo='first'}"`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			first := mustNewCustomTrackersConfigDeserializedFromYaml(t, testData.first)
			second := mustNewCustomTrackersConfigDeserializedFromYaml(t, testData.second)
			expected := mustNewCustomTrackersConfigDeserializedFromYaml(t, testData.expected)

			merged := MergeCustomTrackersConfig(first, second)
			require.Equal(t, expected.source, merged.source)
			require.Equal(t, expected.config, merged.config)
			require.Equal(t, expected.string, merged.string)

			// The original configs should NOT have been changed.
			expectedFirst := mustNewCustomTrackersConfigDeserializedFromYaml(t, testData.first)
			require.Equal(t, expectedFirst.source, first.source)
			require.Equal(t, expectedFirst.config, first.config)
			require.Equal(t, expectedFirst.string, first.string)

			expectedSecond := mustNewCustomTrackersConfigDeserializedFromYaml(t, testData.second)
			require.Equal(t, expectedSecond.source, second.source)
			require.Equal(t, expectedSecond.config, second.config)
			require.Equal(t, expectedSecond.string, second.string)
		})
	}
}
