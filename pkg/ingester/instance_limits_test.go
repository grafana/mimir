// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/instance_limits_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestInstanceLimitsUnmarshal(t *testing.T) {
	defaultInstanceLimits = &InstanceLimits{
		MaxIngestionRate:        10,
		MaxInMemoryTenants:      20,
		MaxInMemorySeries:       30,
		MaxInflightPushRequests: 40,
	}

	l := InstanceLimits{}
	input := `
max_ingestion_rate: 125.678
max_tenants: 50000
`
	dec := yaml.NewDecoder(strings.NewReader(input))
	dec.KnownFields(true)
	require.NoError(t, dec.Decode(&l))
	require.Equal(t, float64(125.678), l.MaxIngestionRate)
	require.Equal(t, int64(50000), l.MaxInMemoryTenants)
	require.Equal(t, int64(30), l.MaxInMemorySeries)       // default value
	require.Equal(t, int64(40), l.MaxInflightPushRequests) // default value
}
