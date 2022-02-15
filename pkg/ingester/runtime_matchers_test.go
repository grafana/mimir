// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestRuntimeMatchersUnmarshal(t *testing.T) {
	r := RuntimeMatchersConfig{}
	input := `
default_matchers:
  integrations/apolloserver: "{job='integrations/apollo-server'}"
  integrations/caddy: "{job='integrations/caddy'}"
tenant_matchers:
  1:
    team_A: "{grafanacloud_team='team_a'}"
    team_B: "{grafanacloud_team='team_b'}"
`

	require.NoError(t, yaml.UnmarshalStrict([]byte(input), &r))
	require.Equal(t, "{job='integrations/apollo-server'}", r.DefaultMatchers["integrations/apolloserver"])
	require.Equal(t, "{job='integrations/caddy'}", r.DefaultMatchers["integrations/caddy"])
	require.Equal(t, "{grafanacloud_team='team_a'}", r.TenantSpecificMatchers["1"]["team_A"])
	require.Equal(t, "{grafanacloud_team='team_b'}", r.TenantSpecificMatchers["1"]["team_B"])
}
