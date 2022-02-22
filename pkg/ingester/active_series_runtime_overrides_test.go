// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

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
	defaultMatchers, err := NewActiveSeriesMatchers(
		map[string]string{
			"foo": `{foo="bar"}`,
			"bar": `{baz="bar"}`,
		})

	require.NoError(t, err)
	tenantSpecificMatchers, err := NewActiveSeriesMatchers(
		map[string]string{
			"team_a": `{team="team_a"}`,
			"team_b": `{team="team_b"}`,
		},
	)
	require.NoError(t, err)
	activeSeriesCustomTrackersOverrides := &ActiveSeriesCustomTrackersOverrides{
		Default: defaultMatchers,
		TenantSpecific: map[string]*ActiveSeriesMatchers{
			"1": tenantSpecificMatchers,
		},
	}
	tests := map[string]struct {
		userID   string
		expected *ActiveSeriesMatchers
	}{
		"User with no override should return deafult": {
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
			matchersForUser := activeSeriesCustomTrackersOverrides.MatchersForUser(testData.userID)
			assert.True(t, testData.expected.Equals(matchersForUser))
		})
	}
}
