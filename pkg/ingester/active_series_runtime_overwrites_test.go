// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestRuntimeMatchersUnmarshal(t *testing.T) {
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
