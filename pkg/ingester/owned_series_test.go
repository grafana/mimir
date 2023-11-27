// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
)

func TestOwnedSeriesService(t *testing.T) {
	const testUser = "test-user"
	const seriesCount = 10

	kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.KVStore.Mock = kvStore
	cfg.IngesterRing.InstanceID = "first-ingester"
	cfg.IngesterRing.NumTokens = seriesCount/2 + 1 // We will use token for half of the series + one token for user.
	cfg.IngesterRing.ZoneAwarenessEnabled = true
	cfg.IngesterRing.InstanceZone = "zone"
	cfg.IngesterRing.ReplicationFactor = 1 // Currently we require RF=number of zones, and we will only work with single zone.

	// Generate some series, and compute their hashes.
	var seriesToWrite []series
	var seriesTokens []uint32
	for seriesIdx := 0; seriesIdx < seriesCount; seriesIdx++ {
		s := series{
			lbls:      labels.FromStrings(labels.MetricName, "test", fmt.Sprintf("lbl_%05d", seriesIdx), "value"),
			value:     float64(0),
			timestamp: time.Now().UnixMilli(),
		}
		seriesToWrite = append(seriesToWrite, s)
		seriesTokens = append(seriesTokens, mimirpb.ShardByAllLabels(testUser, s.lbls))
	}

	// Verify that series tokens have some gaps between them.
	slices.Sort(seriesTokens)
	for i := 1; i < len(seriesTokens); i++ {
		require.Greater(t, seriesTokens[i]-seriesTokens[i-1], uint32(1))
	}

	// Insert our ingester into the ring. When lifecycler starts, it will find this entry, and keep the tokens.
	updateRing(t, kvStore, func(desc *ring.Desc) {
		var tokens []uint32
		for i := 0; i < seriesCount/2; i++ {
			tokens = append(tokens, seriesTokens[i]+1)
		}
		// This instance be the second ingester in the shuffle shard (skip=1).
		tokens = append(tokens, userToken(testUser, cfg.IngesterRing.InstanceZone, 1)+1)
		slices.Sort(tokens)

		desc.AddIngester(cfg.IngesterRing.InstanceID, cfg.IngesterRing.InstanceAddr, cfg.IngesterRing.InstanceZone, tokens, ring.ACTIVE, time.Now())
	})
	ing := setupIngester(t, cfg)

	// Start the ring watching.
	rng, err := ring.New(cfg.IngesterRing.ToRingConfig(), "ingester", IngesterRingKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), rng))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), rng))
	})

	tenantShards := map[string]int{
		testUser: 0, // use all ingesters
	}

	buf := concurrency.SyncBuffer{}

	ownedSeries := newOwnedSeriesService(10*time.Minute, cfg.IngesterRing.InstanceID, rng, log.NewLogfmtLogger(&buf), nil, func(user string) int { return tenantShards[user] }, ing.getTSDBUsers, ing.getTSDB)

	t.Run("no user is updated for empty ingester", func(t *testing.T) {
		require.Equal(t, 0, ownedSeries.updateAllTenants(context.Background(), false))
	})

	require.NoError(t, pushSeriesToIngester(user.InjectOrgID(context.Background(), testUser), t, ing, seriesToWrite))
	db := ing.getTSDB(testUser)
	require.NotNil(t, db)

	t.Run("update due to new user", func(t *testing.T) {
		require.Equal(t, recomputeOwnedSeriesReasonNewUser, db.requiresOwnedSeriesUpdate.Load())
		// First ingester owns all the series, even without any ownedSeries run. This is because each created series is automatically counted as "owned".
		c, _ := db.ownedSeriesAndShards()
		require.Equal(t, seriesCount, c)
		require.Equal(t, 1, ownedSeries.updateAllTenants(context.Background(), false), buf.String())
		require.Contains(t, buf.String(), recomputeOwnedSeriesReasonNewUser)
		require.Equal(t, "", db.requiresOwnedSeriesUpdate.Load())
		// First ingester still owns all the series.
		c, _ = db.ownedSeriesAndShards()
		require.Equal(t, seriesCount, c)
	})

	// Now we insert another ingester to the ring, with tokens that will make it second half of the series.
	// This ingester will also be first ingester in the user's shuffle shard (skip: 0).
	updateRing(t, kvStore, func(desc *ring.Desc) {
		var tokens []uint32
		for i := seriesCount / 2; i < seriesCount; i++ {
			tokens = append(tokens, seriesTokens[i]+1)
		}
		tokens = append(tokens, userToken(testUser, cfg.IngesterRing.InstanceZone, 0)+1)
		slices.Sort(tokens)

		// Must be in the same zone, because we use RF=1, and require RF=num of zones.
		desc.AddIngester("another-ingester", "localhost", cfg.IngesterRing.InstanceZone, tokens, ring.ACTIVE, time.Now())
	})

	t.Run("no update if we don't indicate ring change", func(t *testing.T) {
		// Since our user doesn't have any new reason set for recomputing owned series, and we pass ringChanged=false, no recompute will happen.
		require.Equal(t, "", db.requiresOwnedSeriesUpdate.Load())
		buf.Reset()
		require.Equal(t, 0, ownedSeries.updateAllTenants(context.Background(), false), buf.String())
		c, _ := db.ownedSeriesAndShards()
		require.Equal(t, seriesCount, c)
	})

	t.Run("update due to ring change", func(t *testing.T) {
		// If we try again with ringChanged=true, we should see recomputed owned series.
		buf.Reset()
		require.Equal(t, 1, ownedSeries.updateAllTenants(context.Background(), true), buf.String())
		require.Contains(t, buf.String(), recomputeOwnedSeriesReasonRingChanged)
		require.Equal(t, "", db.requiresOwnedSeriesUpdate.Load())
		c, _ := db.ownedSeriesAndShards()
		require.Equal(t, seriesCount/2, c)
	})

	t.Run("change shard size to 2, owned series are unchanged", func(t *testing.T) {
		// Now don't change the ring, but change shard size. First we set it to 2, which is our number of ingesters.
		// This will not change owned series (because we only have 2 ingesters, and both are already used), but will trigger recompute.
		tenantShards[testUser] = 2
		buf.Reset()
		require.Equal(t, 1, ownedSeries.updateAllTenants(context.Background(), false), buf.String())
		require.Contains(t, buf.String(), recomputeOwnedSeriesReasonShardSizeChanged)
		require.Equal(t, "", db.requiresOwnedSeriesUpdate.Load())
		c, _ := db.ownedSeriesAndShards()
		require.Equal(t, seriesCount/2, c)
	})

	t.Run("change shard size to 1, owned series are changed", func(t *testing.T) {
		// Next, change shard size to 1. This will cause that all series are owned by single ingester only, and it's not "our" ingester, but "another-ingester".
		tenantShards[testUser] = 1
		buf.Reset()
		require.Equal(t, 1, ownedSeries.updateAllTenants(context.Background(), false), buf.String())
		require.Contains(t, buf.String(), recomputeOwnedSeriesReasonShardSizeChanged)
		require.Equal(t, "", db.requiresOwnedSeriesUpdate.Load())
		c, _ := db.ownedSeriesAndShards()
		require.Equal(t, 0, c)
	})

	t.Run("change shard size back to 2, ingester will own the series again", func(t *testing.T) {
		tenantShards[testUser] = 2
		buf.Reset()
		require.Equal(t, 1, ownedSeries.updateAllTenants(context.Background(), false), buf.String())
		require.Contains(t, buf.String(), recomputeOwnedSeriesReasonShardSizeChanged)
		require.Equal(t, "", db.requiresOwnedSeriesUpdate.Load())
		c, _ := db.ownedSeriesAndShards()
		require.Equal(t, seriesCount/2, c)
	})

	// Now let's unregister another-ingester, and keep shard-size at 1. After recomputing ownership, all series should belong to first-ingester again.
	updateRing(t, kvStore, func(desc *ring.Desc) {
		desc.RemoveIngester("another-ingester")
	})

	t.Run("after removal of second ingester, first ingester owns all series", func(t *testing.T) {
		buf.Reset()
		require.Equal(t, 1, ownedSeries.updateAllTenants(context.Background(), true), buf.String()) // We need to signal that ring has changed, otherwise no recomputation is done.
		require.Contains(t, buf.String(), recomputeOwnedSeriesReasonRingChanged)
		require.Equal(t, "", db.requiresOwnedSeriesUpdate.Load())
		c, _ := db.ownedSeriesAndShards()
		require.Equal(t, seriesCount, c)
	})

	// Do early compaction. This should remove all series from memory.
	ing.compactBlocks(context.Background(), true, time.Now().Add(1*time.Minute).UnixMilli(), nil)

	t.Run("check owned series after early compaction", func(t *testing.T) {
		require.Equal(t, recomputeOwnedSeriesReasonEarlyCompaction, db.requiresOwnedSeriesUpdate.Load())
		buf.Reset()
		require.Equal(t, 1, ownedSeries.updateAllTenants(context.Background(), false), buf.String())
		require.Contains(t, buf.String(), recomputeOwnedSeriesReasonEarlyCompaction)
		require.Equal(t, "", db.requiresOwnedSeriesUpdate.Load())
		c, _ := db.ownedSeriesAndShards()
		require.Equal(t, 0, c)
	})
}

func TestOwnedSeriesRingChanged(t *testing.T) {
	kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	// Configure ring
	rc.KVStore.Mock = kvStore
	rc.HeartbeatTimeout = 1 * time.Minute
	rc.ReplicationFactor = 3
	rc.ZoneAwarenessEnabled = true

	// Start the ring watching.
	rng, err := ring.New(rc, "ingester", IngesterRingKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), rng))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), rng))
	})

	buf := concurrency.SyncBuffer{}

	const instanceID1 = "first ingester"
	const instanceID2 = "second instance"

	ownedSeries := newOwnedSeriesService(10*time.Minute, instanceID1, rng, log.NewLogfmtLogger(&buf), nil, nil, nil, nil)

	updateRing(t, kvStore, func(desc *ring.Desc) {
		desc.AddIngester(instanceID1, "localhost:11111", "zone", []uint32{1, 2, 3}, ring.ACTIVE, time.Now())
	})

	// First call should indicate ring change.
	t.Run("first call always reports change", func(t *testing.T) {
		changed, err := ownedSeries.checkRingForChanges()
		require.NoError(t, err)
		require.True(t, changed)
	})

	t.Run("second call (without ring change) reports no change", func(t *testing.T) {
		changed, err := ownedSeries.checkRingForChanges()
		require.NoError(t, err)
		require.False(t, changed)
	})

	t.Run("new instance added", func(t *testing.T) {
		updateRing(t, kvStore, func(desc *ring.Desc) {
			desc.AddIngester(instanceID2, "localhost:22222", "zone", []uint32{4, 5, 6}, ring.ACTIVE, time.Now())
		})

		changed, err := ownedSeries.checkRingForChanges()
		require.NoError(t, err)
		require.True(t, changed)
	})

	t.Run("change of state is not interesting", func(t *testing.T) {
		updateRing(t, kvStore, func(desc *ring.Desc) {
			desc.AddIngester(instanceID2, "localhost:22222", "zone", []uint32{4, 5, 6}, ring.LEAVING, time.Now())
		})

		// Change of state is not interesting.
		changed, err := ownedSeries.checkRingForChanges()
		require.NoError(t, err)
		require.False(t, changed)
	})

	t.Run("removal of instance", func(t *testing.T) {
		updateRing(t, kvStore, func(desc *ring.Desc) {
			desc.RemoveIngester(instanceID2)
		})

		changed, err := ownedSeries.checkRingForChanges()
		require.NoError(t, err)
		require.True(t, changed)
	})
}

func userToken(user, zone string, skip int) uint32 {
	r := rand.New(rand.NewSource(util.ShuffleShardSeed(user, zone)))

	for ; skip > 0; skip-- {
		_ = r.Uint32()
	}
	return r.Uint32()
}

func setupIngester(t *testing.T, cfg Config) *Ingester {
	ing, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
	})
	return ing
}

func updateRing(t *testing.T, kvStore *consul.Client, updateFn func(*ring.Desc)) {
	err := kvStore.CAS(context.Background(), IngesterRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		d, _ := in.(*ring.Desc)
		if d == nil {
			d = ring.NewDesc()
		}

		updateFn(d)

		return d, true, nil
	})
	require.NoError(t, err)

	// Wait a bit to make sure that ring has received the update.
	time.Sleep(100 * time.Millisecond)
}
