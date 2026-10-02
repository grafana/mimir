// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/extprom"
)

func TestMirrorShuffleShardingStrategy(t *testing.T) {
	const (
		numZones            = 3
		numInstancesPerZone = 4
		mirroredInstanceID  = "instance-zone-b-2"
	)

	ctx := t.Context()
	store, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	tokenGen := ring.NewRandomTokenGeneratorWithSeed(1)
	require.NoError(t, store.CAS(ctx, RingKey, func(any) (any, bool, error) {
		d := ring.NewDesc()
		var takenTokens []uint32
		for z := range numZones {
			zone := fmt.Sprintf("zone-%c", 'a'+z)
			for i := range numInstancesPerZone {
				tokens := tokenGen.GenerateTokens(64, takenTokens)
				takenTokens = append(takenTokens, tokens...)
				d.AddIngester(fmt.Sprintf("instance-%s-%d", zone, i), fmt.Sprintf("127.0.%d.%d:9095", z, i), zone, tokens, ring.ACTIVE, time.Now(), false, time.Time{}, nil)
			}
		}
		return d, true, nil
	}))

	r, err := ring.NewWithStoreClientAndStrategy(ring.Config{
		ReplicationFactor:    3,
		ZoneAwarenessEnabled: true,
		HeartbeatTimeout:     time.Minute,
		SubringCacheDisabled: true,
	}, mirrorRingName, RingKey, store, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), r)) })
	require.NoError(t, ring.WaitInstanceState(ctx, r, mirroredInstanceID, ring.ACTIVE))

	userIDs := make([]string, 0, 50)
	for i := range 50 {
		userIDs = append(userIDs, fmt.Sprintf("user-%d", i))
	}
	limits := &shardingLimitsMock{storeGatewayTenantShardSize: 6}
	dynamicReplication := NewNopDynamicReplication(3)

	mirror := NewMirrorShuffleShardingStrategy(r, mirroredInstanceID, dynamicReplication, limits, log.NewNopLogger())

	// requireSameAsMirroredInstance makes sure that the mirror selects the same users and blocks as
	// the strategy of the mirrored instance at addr.
	requireSameAsMirroredInstance := func(t *testing.T, addr string) {
		mirrored := NewShuffleShardingStrategy(r, mirroredInstanceID, addr, dynamicReplication, limits, log.NewNopLogger())

		expectedUsers, err := mirrored.FilterUsers(ctx, userIDs)
		require.NoError(t, err)
		require.NotEmpty(t, expectedUsers)
		actualUsers, err := mirror.FilterUsers(ctx, userIDs)
		require.NoError(t, err)
		require.Equal(t, expectedUsers, actualUsers)

		for _, userID := range expectedUsers {
			expectedBlocks := filterTestBlocks(t, mirrored, userID)
			require.NotEmpty(t, expectedBlocks)
			require.Equal(t, expectedBlocks, filterTestBlocks(t, mirror, userID), "user %s", userID)
		}
	}

	t.Run("loads the blocks of the mirrored instance", func(t *testing.T) {
		requireSameAsMirroredInstance(t, "127.0.1.2:9095")
	})

	t.Run("follows the address of the mirrored instance", func(t *testing.T) {
		const newAddr = "127.0.1.99:9095"
		updateTestRing(t, store, func(d *ring.Desc) {
			inst := d.Ingesters[mirroredInstanceID]
			inst.Addr = newAddr
			d.Ingesters[mirroredInstanceID] = inst
		})
		test.Poll(t, 5*time.Second, newAddr, func() any {
			inst, err := r.GetInstance(mirroredInstanceID)
			if err != nil {
				return err
			}
			return inst.Addr
		})

		requireSameAsMirroredInstance(t, newAddr)
	})

	t.Run("is unhealthy if the mirrored instance is not in the ring", func(t *testing.T) {
		updateTestRing(t, store, func(d *ring.Desc) { d.RemoveIngester(mirroredInstanceID) })
		test.Poll(t, 5*time.Second, false, func() any { return r.HasInstance(mirroredInstanceID) })

		users, err := mirror.FilterUsers(ctx, userIDs)
		require.ErrorIs(t, err, errStoreGatewayUnhealthy)
		require.Nil(t, users)
	})
}

func TestMirrorConfig_Validate(t *testing.T) {
	shardingRing := RingConfig{}
	shardingRing.KVStore.Prefix = "secondary/"

	tests := map[string]struct {
		cfg         MirrorConfig
		expectedErr string
	}{
		"disabled": {
			cfg: MirrorConfig{Enabled: false, RingKVPrefix: "secondary/"},
		},
		"enabled with a different prefix": {
			cfg: MirrorConfig{Enabled: true, RingKVPrefix: "collectors/"},
		},
		"enabled with the same prefix as the sharding ring": {
			cfg:         MirrorConfig{Enabled: true, RingKVPrefix: "secondary/"},
			expectedErr: "the store-gateway mirror mode requires a mirrored ring KV store prefix that is different from the sharding ring KV store prefix",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := tc.cfg.Validate(shardingRing)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedErr)
			}
		})
	}
}

// filterTestBlocks returns the IDs of the test blocks that the strategy keeps for userID.
func filterTestBlocks(t *testing.T, s ShardingStrategy, userID string) map[ulid.ULID]struct{} {
	now := time.Now()
	metas := map[ulid.ULID]*block.Meta{}
	for i := range 100 {
		id := ulid.MustNew(uint64(i+1), nil)
		metas[id] = &block.Meta{BlockMeta: tsdb.BlockMeta{
			ULID:    id,
			MinTime: now.Add(-48 * time.Hour).UnixMilli(),
			MaxTime: now.Add(-24 * time.Hour).UnixMilli(),
		}}
	}

	synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"})
	require.NoError(t, s.FilterBlocks(t.Context(), userID, metas, nil, synced))

	kept := make(map[ulid.ULID]struct{}, len(metas))
	for id := range metas {
		kept[id] = struct{}{}
	}
	return kept
}

func updateTestRing(t *testing.T, store kv.Client, update func(*ring.Desc)) {
	require.NoError(t, store.CAS(t.Context(), RingKey, func(in any) (any, bool, error) {
		d := in.(*ring.Desc)
		update(d)
		return d, true, nil
	}))
}
