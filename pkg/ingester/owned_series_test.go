package ingester

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
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
	kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.KVStore.Mock = kvStore
	cfg.IngesterRing.InstanceID = "first-ingester"
	cfg.IngesterRing.NumTokens = 1
	cfg.IngesterRing.ZoneAwarenessEnabled = true
	cfg.IngesterRing.InstanceZone = "zone"
	cfg.IngesterRing.ReplicationFactor = 1 // Currently we require RF=number of zones, and we will only work with single zone.

	const testUser = "test-user"
	const seriesCount = 10

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

	// Verify that series tokens have big gaps between them, so that we can insert ingesters that own some of these.
	slices.Sort(seriesTokens)
	for i := 1; i < len(seriesTokens); i++ {
		require.Greater(t, seriesTokens[i]-seriesTokens[i-1], uint32(10000))
	}

	updateRing(t, kvStore, func(desc *ring.Desc) {
		var tokens []uint32
		for i := 0; i < seriesCount/2; i++ {
			tokens = append(tokens, seriesTokens[i]+1)
		}
		// This instance be second ingester in the shuffle shard (skip=1).
		tokens = append(tokens, userToken(testUser, cfg.IngesterRing.InstanceZone, 1)+1)
		slices.Sort(tokens)

		desc.AddIngester(cfg.IngesterRing.InstanceID, cfg.IngesterRing.InstanceAddr, cfg.IngesterRing.InstanceZone, tokens, ring.ACTIVE, time.Now())
	})
	ing := setupIngester(t, cfg)

	rng, err := ring.New(cfg.IngesterRing.ToRingConfig(), "ingester", IngesterRingKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), rng))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), rng))
	})

	tenantShards := map[string]int{
		"user": 0, // use all ingesters.
	}

	// 10 minutes = never.
	ownedSeries := newOwnedSeriesService(10*time.Minute, cfg.IngesterRing.InstanceID, rng, log.NewNopLogger(), func(user string) int { return tenantShards[user] }, ing.getTSDBUsers, ing.getTSDB)

	// Since there are no users, no user will be updated.
	require.Equal(t, 0, ownedSeries.updateAll(context.Background(), false))

	// Push all series
	userContext := user.InjectOrgID(context.Background(), testUser)
	require.NoError(t, pushSeriesToIngester(userContext, t, ing, seriesToWrite))

	// There is now one user, with fresh TSDB.
	db := ing.getTSDB(testUser)
	require.NotNil(t, db)
	require.Equal(t, recomputeOwnedSeriesReasonNewUser, db.requiresOwnedSeriesUpdate.Load())
	c, _ := db.OwnedSeriesAndShards()
	require.Equal(t, seriesCount, c)
	require.Equal(t, 1, ownedSeries.updateAll(context.Background(), false))
	require.Equal(t, "", db.requiresOwnedSeriesUpdate.Load())
	// We should still own all series.
	c, _ = db.OwnedSeriesAndShards()
	require.Equal(t, seriesCount, c)

	// Now we insert another ingester to the ring, with token that will make it own several series.
	updateRing(t, kvStore, func(desc *ring.Desc) {
		var tokens []uint32
		for i := seriesCount / 2; i < seriesCount; i++ {
			tokens = append(tokens, seriesTokens[i]+1)
		}
		// This instance be first ingester in the shuffle shard (skip=0).
		tokens = append(tokens, userToken(testUser, cfg.IngesterRing.InstanceZone, 0)+1)
		slices.Sort(tokens)

		// Must be in the same zone, because we use RF=1, and require RF=num of zones.
		desc.AddIngester("another-ingester", "localhost", cfg.IngesterRing.InstanceZone, tokens, ring.ACTIVE, time.Now())
	})

	// Since our user doesn't have any new reason for recomputing owned series, and we pass ringChanged=false, no recompute will happen.
	require.Equal(t, "", db.requiresOwnedSeriesUpdate.Load())
	require.Equal(t, 0, ownedSeries.updateAll(context.Background(), false))
	c, _ = db.OwnedSeriesAndShards()
	require.Equal(t, seriesCount, c)

	// If we try again with ringChanged=true, we should see recomputed owned series.
	require.Equal(t, 1, ownedSeries.updateAll(context.Background(), true))
	require.Equal(t, "", db.requiresOwnedSeriesUpdate.Load())
	c, _ = db.OwnedSeriesAndShards()
	require.Equal(t, seriesCount/2, c)

	// Now don't change ring, but change shard size. First we set it to 2, which is our number of ingesters.
	// This will not change owned series, but will trigger recompute.
	tenantShards[testUser] = 2
	require.Equal(t, 1, ownedSeries.updateAll(context.Background(), false))
	require.Equal(t, "", db.requiresOwnedSeriesUpdate.Load())
	c, _ = db.OwnedSeriesAndShards()
	require.Equal(t, seriesCount/2, c)

	// Next, change shard size to 1. This will cause that all series are owned by single ingester only, and it's not "our" ingester, but "another-ingester".
	tenantShards[testUser] = 1
	require.Equal(t, 1, ownedSeries.updateAll(context.Background(), false))
	require.Equal(t, "", db.requiresOwnedSeriesUpdate.Load())
	c, _ = db.OwnedSeriesAndShards()
	require.Equal(t, 0, c)

	// Now let's unregister another-ingester, and keep shard-size at 1. After recomputing ownership, all series should belong to first-ingester again.
	updateRing(t, kvStore, func(desc *ring.Desc) {
		desc.RemoveIngester("another-ingester")
	})

	require.Equal(t, 1, ownedSeries.updateAll(context.Background(), true)) // We need to signal that ring has changed, otherwise no recomputation is done.
	require.Equal(t, "", db.requiresOwnedSeriesUpdate.Load())
	c, _ = db.OwnedSeriesAndShards()
	require.Equal(t, seriesCount, c)
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
