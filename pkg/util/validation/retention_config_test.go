package validation

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func mustNewRetentionConfigFromMap(t require.TestingT, source map[model.Duration][]string) RetentionCfg {
	m, err := NewRetentionConfig(source)
	require.NoError(t, err)
	return m
}

func mustNewRetentionConfigDeserializedFromYaml(t *testing.T, yamlString string) RetentionCfg {
	m := RetentionCfg{}
	err := yaml.Unmarshal([]byte(yamlString), &m)
	require.NoError(t, err)
	return m
}

func TestRetentionConfig_Equality(t *testing.T) {
	dr1, err := model.ParseDuration("1y")
	require.NoError(t, err)
	dr2, err := model.ParseDuration("2y")
	require.NoError(t, err)
	tests := []struct {
		name     string
		omap     RetentionCfg
		oyaml    RetentionCfg
		expected bool
	}{
		{
			name: "Two identical configs should be equal",
			omap: mustNewRetentionConfigFromMap(t, map[model.Duration][]string{
				dr1: {
					`{aaa='bar'}`,
					`{bbb='foo'}`},
				dr2: {
					`{ccc='bar'}`,
				},
			}),
			oyaml: mustNewRetentionConfigDeserializedFromYaml(t,
				`
                1y: 
                  - "{aaa='bar'}"
                  - "{bbb='foo'}"
                2y:
                  - "{ccc='bar'}"`),
			expected: true,
		},
		{
			name: "Two configs with different matchers should not be equal",
			omap: mustNewRetentionConfigFromMap(t, map[model.Duration][]string{
				dr1: {
					`{aaa='bar'}`,
					`{bbb='foo'}`},
				dr2: {
					`{ccc='bar'}`,
				},
			}),
			oyaml: mustNewRetentionConfigDeserializedFromYaml(t,
				`
            1y:
              - "{aaa='bar'}"
            2y:
              - "{bbb='bar'}"`),
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.expected {
				assert.EqualValues(t, test.omap, test.oyaml)
			} else {
				assert.NotEqualValues(t, test.omap, test.oyaml)
			}
		})
	}
}

func TestRetentionConfigs_Deserialization(t *testing.T) {
	correctInput := `
    1y:
      - "{baz='bar'}"
    2y:
      - "{foo='bar'}"`
	malformedInput := `
    1y:
      - "123"
    100z:
      - "{foo='bar'}"`

	t.Run("ShouldDeserializeCorrectInput", func(t *testing.T) {
		config := RetentionCfg{}
		err := yaml.Unmarshal([]byte(correctInput), &config)
		assert.NoError(t, err, "failed to deserialize Matchers")
		dr1, err := model.ParseDuration("1y")
		require.NoError(t, err)
		dr2, err := model.ParseDuration("2y")
		require.NoError(t, err)
		expectedConfig, err := NewRetentionConfig(map[model.Duration][]string{
			dr1: {"{baz='bar'}"},
			dr2: {"{foo='bar'}"},
		})
		require.NoError(t, err)
		assert.EqualValues(t, expectedConfig, config)
	})

	t.Run("ShouldErrorOnMalformedInput", func(t *testing.T) {
		config := RetentionCfg{}
		err := yaml.Unmarshal([]byte(malformedInput), &config)
		assert.Error(t, err, "should not deserialize malformed input")
	})
}

func TestRetentionConfigs_SerializeDeserialize(t *testing.T) {
	sourceYAML := `
    1y: 
      - "{baz='bar'}"
      - "{foo='fuz'}"
    80w: 
      - "{foo='bar'}"`

	obj := mustNewRetentionConfigDeserializedFromYaml(t, sourceYAML)

	t.Run("ShouldSerializeDeserializeResultsTheSame", func(t *testing.T) {
		out, err := yaml.Marshal(obj)
		require.NoError(t, err, "failed do serialize Custom trackers config")
		reSerialized := RetentionCfg{}
		err = yaml.Unmarshal(out, &reSerialized)
		require.NoError(t, err, "Failed to deserialize serialized object")
		assert.Equal(t, obj, reSerialized)
	})
}
