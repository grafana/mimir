// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/instance_limits_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/middleware"
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
	t.Run("bare error implements ShouldLog()", func(t *testing.T) {
		var optional middleware.OptionalLogging
		require.ErrorAs(t, errMaxInflightRequestsReached, &optional)
		require.False(t, optional.ShouldLog(context.Background(), time.Duration(0)))
	})

	t.Run("wrapped error implements ShouldLog()", func(t *testing.T) {
		err := fmt.Errorf("%w: oh no", errMaxTenantsReached)
		var optional middleware.OptionalLogging
		require.ErrorAs(t, err, &optional)
		require.False(t, optional.ShouldLog(context.Background(), time.Duration(0)))
	})

	t.Run("bare error implements GRPCStatus()", func(t *testing.T) {
		s, ok := status.FromError(errMaxInMemorySeriesReached)
		require.True(t, ok, "expected to be able to convert to gRPC status")
		require.Equal(t, codes.Unavailable, s.Code())
	})

	t.Run("wrapped error implements GRPCStatus()", func(t *testing.T) {
		err := fmt.Errorf("%w: oh no", errMaxIngestionRateReached)
		s, ok := status.FromError(err)
		require.True(t, ok, "expected to be able to convert to gRPC status")
		require.Equal(t, codes.Unavailable, s.Code())
	})
}
