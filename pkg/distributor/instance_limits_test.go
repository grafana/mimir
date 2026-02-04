// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestInstanceLimitsUnmarshal(t *testing.T) {
	SetDefaultInstanceLimitsForYAMLUnmarshalling(InstanceLimits{
		MaxIngestionRate:             10,
		MaxInflightPushRequests:      40,
		MaxInflightPushRequestsBytes: 1024 * 1024,
	})

	l := InstanceLimits{}
	input := `
max_ingestion_rate: 125.678
max_inflight_push_requests: 50
`
	dec := yaml.NewDecoder(strings.NewReader(input))
	dec.KnownFields(true)
	require.NoError(t, dec.Decode(&l))
	require.Equal(t, 125.678, l.MaxIngestionRate)
	require.Equal(t, 50, l.MaxInflightPushRequests)
	require.Equal(t, 1024*1024, l.MaxInflightPushRequestsBytes) // default value
}
