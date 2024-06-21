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

	"github.com/grafana/mimir/pkg/ingester/ingestererr"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestInstanceLimitsUnmarshal(t *testing.T) {
	SetDefaultInstanceLimitsForYAMLUnmarshalling(InstanceLimits{
		MaxIngestionRate:        10,
		MaxInMemoryTenants:      20,
		MaxInMemorySeries:       30,
		MaxInflightPushRequests: 40,
	})

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

func TestInstanceLimitErr(t *testing.T) {
	userID := "1"
	limitErrors := []error{
		errMaxIngestionRateReached,
		errMaxTenantsReached,
		errMaxInMemorySeriesReached,
		errMaxInflightRequestsReached,
	}
	for _, limitError := range limitErrors {
		var instanceLimitErr ingestererr.InstanceLimitReachedError
		require.Error(t, instanceLimitErr)
		require.ErrorAs(t, limitError, &instanceLimitErr)
		checkIngesterError(t, limitError, mimirpb.INSTANCE_LIMIT, false)

		wrappedWithUserErr := ingestererr.WrapOrAnnotateWithUser(limitError, userID)
		require.Error(t, wrappedWithUserErr)
		require.ErrorIs(t, wrappedWithUserErr, limitError)
		checkIngesterError(t, wrappedWithUserErr, mimirpb.INSTANCE_LIMIT, false)
	}
}

func checkIngesterError(t *testing.T, err error, expectedCause mimirpb.ErrorCause, shouldBeSoft bool) {
	t.Helper()
	var ingesterErr ingestererr.IngesterError
	require.ErrorAs(t, err, &ingesterErr)
	require.Equal(t, expectedCause, ingesterErr.ErrorCause())

	if shouldBeSoft {
		var softErr ingestererr.SoftError
		require.ErrorAs(t, err, &softErr)
	}
}
