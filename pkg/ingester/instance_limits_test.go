// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/instance_limits_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/middleware"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
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
		wrapOrAnnotateWithUser(errMaxIngestionRateReached, userID),
		errMaxTenantsReached,
		wrapOrAnnotateWithUser(errMaxTenantsReached, userID),
		errMaxInMemorySeriesReached,
		wrapOrAnnotateWithUser(errMaxInMemorySeriesReached, userID),
		errMaxInflightRequestsReached,
		wrapOrAnnotateWithUser(errMaxInflightRequestsReached, userID),
	}
	for _, limitError := range limitErrors {
		var safe safeToWrap
		require.ErrorAs(t, limitError, &safe)

		var optional middleware.OptionalLogging
		require.ErrorAs(t, limitError, &optional)
		require.False(t, optional.ShouldLog(context.Background(), time.Duration(0)))

		stat, ok := status.FromError(limitError)
		require.True(t, ok, "expected to be able to convert to gRPC status")
		require.Equal(t, codes.Unavailable, stat.Code())
		require.ErrorContains(t, stat.Err(), limitError.Error())
	}
}
