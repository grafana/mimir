// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
)

func TestIsStillWarming(t *testing.T) {
	t.Parallel()
	require.True(t, IsStillWarming(errStillWarming(7)))
	require.False(t, IsStillWarming(nil))
	require.False(t, IsStillWarming(errors.New("not a status")))
	require.False(t, IsStillWarming(status.Error(codes.Unavailable, "transport problem")))
	require.False(t, IsStillWarming(status.Error(codes.NotFound, stillWarmingDetail+" partition=1")))
	require.False(t, IsStillWarming(errPartitionEpochUnavailable(7)), "epoch-unavailable must not trigger still-warming fallback")
}

func TestListTSDBsForTenant_HintWithoutEpochErrors(t *testing.T) {
	t.Parallel()
	r := &Readcache{
		partitions: map[int32]*partitionState{},
		frozen:     map[int32][]*frozenEpoch{},
	}
	dbs, err := r.listTSDBsForTenant("tenant", &client.QueryAttributionHint{PartitionId: 42})
	require.Error(t, err)
	require.Contains(t, err.Error(), partitionEpochUnavailableDetail)
	require.Nil(t, dbs)
}
