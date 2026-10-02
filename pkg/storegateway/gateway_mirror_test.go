// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"fmt"
	"math"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	dstest "github.com/grafana/dskit/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mimir_testutil "github.com/grafana/mimir/pkg/storage/tsdb/testutil"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestStoreGateway_MirrorModeLoadsTheBlocksOfTheMirroredInstance(t *testing.T) {
	test.VerifyNoLeak(t)

	const (
		numReferenceGateways = 3
		mirroredInstanceID   = "gateway-2"
	)
	userIDs := []string{"user-1", "user-2"}

	bucketClient, storageDir := mimir_testutil.PrepareFilesystemBucket(t)
	now := time.Now()
	for _, userID := range userIDs {
		mockTSDB(t, path.Join(storageDir, userID), 24, 12, now.Add(-24*time.Hour).UnixMilli(), now.UnixMilli())
		createBucketIndex(t, bucketClient, userID)
	}

	ctx := context.Background()
	referenceRingStore, referenceCloser := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, referenceCloser.Close()) })
	mirrorRingStore, mirrorCloser := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, mirrorCloser.Close()) })

	// The gateways get fixed tokens from a tokens file, so each reference gateway owns blocks of each tenant.
	tokenGen := ring.NewRandomTokenGeneratorWithSeed(1)
	var takenTokens []uint32
	tokensFiles := map[int]string{}
	for id := 1; id <= numReferenceGateways; id++ {
		tokens := tokenGen.GenerateTokens(ringNumTokensDefault, takenTokens)
		takenTokens = append(takenTokens, tokens...)
		tokensFiles[id] = path.Join(t.TempDir(), "tokens")
		require.NoError(t, tokens.StoreToFile(tokensFiles[id]))
	}

	newGatewayConfig := func(id int) Config {
		cfg := mockGatewayConfig()
		// With RF 1, each block is in one reference store-gateway only, so the test shows the sharding.
		cfg.ShardingRing.ReplicationFactor = 1
		cfg.ShardingRing.InstanceID = fmt.Sprintf("gateway-%d", id)
		cfg.ShardingRing.InstanceAddr = fmt.Sprintf("127.0.0.%d", id)
		cfg.ShardingRing.TokensFilePath = tokensFiles[id]
		return cfg
	}
	startGateway := func(cfg Config, ringStore, mirroredRingStore kv.Client) *StoreGateway {
		storageCfg := mockStorageConfig(t)
		storageCfg.BucketStore.SyncInterval = time.Hour
		g, err := newStoreGatewayWithMirror(cfg, storageCfg, bucketClient, ringStore, mirroredRingStore, validation.NewOverrides(defaultLimitsConfig(), nil), log.NewNopLogger(), prometheus.NewPedanticRegistry(), nil)
		require.NoError(t, err)
		require.NoError(t, g.StartAsync(ctx))
		t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(ctx, g)) })
		return g
	}

	referenceGateways := map[string]*StoreGateway{}
	for id := 1; id <= numReferenceGateways; id++ {
		cfg := newGatewayConfig(id)
		referenceGateways[cfg.ShardingRing.InstanceID] = startGateway(cfg, referenceRingStore, nil)
	}
	for _, g := range referenceGateways {
		require.NoError(t, g.AwaitRunning(ctx))
	}
	// Wait until the mirrored instance owns its final set of blocks.
	for _, g := range referenceGateways {
		dstest.Poll(t, 5*time.Second, numReferenceGateways, func() any { return g.ring.InstancesCount() })
	}
	for _, g := range referenceGateways {
		g.syncStores(ctx, syncReasonRingChange)
	}

	// The mirror joins its own ring with the instance ID of the mirrored instance, and a different address.
	mirrorCfg := newGatewayConfig(2)
	mirrorCfg.ShardingRing.InstanceAddr = "127.0.1.2"
	mirrorCfg.ShardingRing.TokensFilePath = ""
	mirrorCfg.Mirror = MirrorConfig{Enabled: true, RingKVPrefix: "reference/"}
	mirror := startGateway(mirrorCfg, mirrorRingStore, referenceRingStore)
	require.NoError(t, mirror.AwaitRunning(ctx))

	for _, userID := range userIDs {
		expected := queriedBlocks(t, referenceGateways[mirroredInstanceID], userID)
		require.NotEmpty(t, expected)
		require.Equal(t, expected, queriedBlocks(t, mirror, userID), "user %s", userID)

		// The other reference store-gateways hold other blocks, so the mirror is not the same as every instance.
		allBlocks := map[string]struct{}{}
		for _, g := range referenceGateways {
			for _, id := range queriedBlocks(t, g, userID) {
				allBlocks[id] = struct{}{}
			}
		}
		require.Less(t, len(expected), len(allBlocks), "user %s", userID)
	}

	// The mirror joins only its own ring.
	require.Equal(t, []string{"gateway-1", "gateway-2", "gateway-3"}, ringInstanceIDs(t, referenceRingStore))
	require.Equal(t, []string{mirroredInstanceID}, ringInstanceIDs(t, mirrorRingStore))
	inst, err := mirror.ring.GetInstance(mirroredInstanceID)
	require.NoError(t, err)
	require.Equal(t, "127.0.1.2:0", inst.Addr)
}

// queriedBlocks returns the sorted IDs of the blocks that the store-gateway queries for a Series request for all series of userID.
func queriedBlocks(t *testing.T, g *StoreGateway, userID string) []string {
	srv := newStoreGatewayTestServer(t, g)
	req := &storepb.SeriesRequest{
		MinTime:  math.MinInt64,
		MaxTime:  math.MaxInt64,
		Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: "series_id", Value: ".+"}},
	}
	_, _, hints, _, err := srv.Series(setUserIDToGRPCContext(context.Background(), userID), req)
	require.NoError(t, err)

	ids := make([]string, 0, len(hints.QueriedBlocks))
	for _, b := range hints.QueriedBlocks {
		ids = append(ids, b.Id)
	}
	sort.Strings(ids)
	return ids
}

func ringInstanceIDs(t *testing.T, store kv.Client) []string {
	desc, err := store.Get(context.Background(), RingKey)
	require.NoError(t, err)
	ids := make([]string, 0)
	for id := range desc.(*ring.Desc).Ingesters {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
