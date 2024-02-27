// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
)

const ownedServiceTestUser = "test-user"
const ownedServiceSeriesCount = 10

type ownedSeriesTestContext struct {
	seriesToWrite []series
	seriesTokens  []uint32
	ingesterZone  string

	ownedSeries *ownedSeriesService
	ing         *Ingester
	db          *userTSDB
	kvStore     *watchingKV

	buf          *concurrency.SyncBuffer
	tenantShards map[string]int
}

func (c *ownedSeriesTestContext) pushUserSeries(t *testing.T) {
	require.NoError(t, pushSeriesToIngester(user.InjectOrgID(context.Background(), ownedServiceTestUser), t, c.ing, c.seriesToWrite))
	db := c.ing.getTSDB(ownedServiceTestUser)
	require.NotNil(t, db)
	c.db = db
}

func (c *ownedSeriesTestContext) checkUpdateReasonForUser(t *testing.T, expectedReason string) {
	require.Equal(t, expectedReason, c.db.requiresOwnedSeriesUpdate.Load())
}

func (c *ownedSeriesTestContext) checkUserSeriesOwnedAndShardsByTestedIngester(t *testing.T, series, shards int) {
	cnt, sh := c.db.ownedSeriesAndShards()
	require.Equal(t, series, cnt)
	require.Equal(t, shards, sh)
}

func (c *ownedSeriesTestContext) updateOwnedSeriesAndCheckResult(t *testing.T, ringChanged bool, expectedUpdatedTenants int, expectedReason string) {
	c.buf.Reset()
	require.Equal(t, expectedUpdatedTenants, c.ownedSeries.updateAllTenants(context.Background(), ringChanged), c.buf.String())
	require.Contains(t, c.buf.String(), expectedReason)
}

func (c *ownedSeriesTestContext) registerTestedIngesterIntoRing(t *testing.T, instanceID, instanceAddr, instanceZone string) {
	c.ingesterZone = instanceZone

	// Insert our ingester into the ring. When lifecycler starts, it will find this entry, and keep the tokens.
	updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
		var tokens []uint32
		for i := 0; i < ownedServiceSeriesCount/2; i++ {
			tokens = append(tokens, c.seriesTokens[i]+1)
		}
		// This instance be the second ingester in the shuffle shard (skip=1). "second" ingester will be first ingester in the shard.
		tokens = append(tokens, userToken(ownedServiceTestUser, instanceZone, 1)+1)
		slices.Sort(tokens)

		desc.AddIngester(instanceID, instanceAddr, instanceZone, tokens, ring.ACTIVE, time.Now())
	})
}

// Insert second ingester to the ring, with tokens that will make it second half of the series.
// This ingester will also be first ingester in the user's shuffle shard (skip: 0).
func (c *ownedSeriesTestContext) registerSecondIngesterOwningHalfOfTheTokens(t *testing.T) {
	updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
		var tokens []uint32
		for i := ownedServiceSeriesCount / 2; i < ownedServiceSeriesCount; i++ {
			tokens = append(tokens, c.seriesTokens[i]+1)
		}
		tokens = append(tokens, userToken(ownedServiceTestUser, c.ingesterZone, 0)+1)
		slices.Sort(tokens)

		// Must be in the same zone, because we use RF=1, and require RF=num of zones.
		desc.AddIngester("second-ingester", "localhost", c.ingesterZone, tokens, ring.ACTIVE, time.Now())
	})
}

func (c *ownedSeriesTestContext) removeSecondIngester(t *testing.T) {
	updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
		desc.RemoveIngester("second-ingester")
	})
}

func TestOwnedSeriesService(t *testing.T) {
	// Generate some series, and compute their hashes.
	var seriesToWrite []series
	var seriesTokens []uint32
	for seriesIdx := 0; seriesIdx < ownedServiceSeriesCount; seriesIdx++ {
		s := series{
			lbls:      labels.FromStrings(labels.MetricName, "test", fmt.Sprintf("lbl_%05d", seriesIdx), "value"),
			value:     float64(0),
			timestamp: time.Now().UnixMilli(),
		}
		seriesToWrite = append(seriesToWrite, s)
		seriesTokens = append(seriesTokens, mimirpb.ShardByAllLabels(ownedServiceTestUser, s.lbls))
	}

	// Verify that series tokens have some gaps between them.
	slices.Sort(seriesTokens)
	for i := 1; i < len(seriesTokens); i++ {
		require.Greater(t, seriesTokens[i], seriesTokens[i-1])
	}

	testCases := map[string]func(t *testing.T, c *ownedSeriesTestContext){
		"empty ingester": func(t *testing.T, c *ownedSeriesTestContext) {
			require.Equal(t, 0, c.ownedSeries.updateAllTenants(context.Background(), false))
		},

		"update due to new user": func(t *testing.T, c *ownedSeriesTestContext) {
			c.pushUserSeries(t)
			c.checkUpdateReasonForUser(t, recomputeOwnedSeriesReasonNewUser)
			// First ingester owns all the series, even without any ownedSeries run. This is because each created series is automatically counted as "owned".
			c.checkUserSeriesOwnedAndShardsByTestedIngester(t, ownedServiceSeriesCount, 0)
			c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
			c.checkUpdateReasonForUser(t, "")
			// First ingester still owns all the series.
			c.checkUserSeriesOwnedAndShardsByTestedIngester(t, ownedServiceSeriesCount, 0)
		},

		"no ring change after adding ingester": func(t *testing.T, c *ownedSeriesTestContext) {
			c.pushUserSeries(t)
			c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
			c.registerSecondIngesterOwningHalfOfTheTokens(t)

			// Since our user doesn't have any new reason set for recomputing owned series, and we pass ringChanged=false, no recompute will happen.
			c.checkUpdateReasonForUser(t, "")
			c.updateOwnedSeriesAndCheckResult(t, false, 0, "")
			c.checkUserSeriesOwnedAndShardsByTestedIngester(t, ownedServiceSeriesCount, 0)
		},

		"ring change after adding ingester": func(t *testing.T, c *ownedSeriesTestContext) {
			c.pushUserSeries(t)
			c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)

			c.registerSecondIngesterOwningHalfOfTheTokens(t)

			c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonRingChanged)
			c.checkUpdateReasonForUser(t, "")
			c.checkUserSeriesOwnedAndShardsByTestedIngester(t, ownedServiceSeriesCount/2, 0)
		},

		"unchanged ring, but tenant shard changed from 0 to 2": func(t *testing.T, c *ownedSeriesTestContext) {
			c.pushUserSeries(t)
			c.registerSecondIngesterOwningHalfOfTheTokens(t)
			c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonNewUser)

			c.checkUserSeriesOwnedAndShardsByTestedIngester(t, ownedServiceSeriesCount/2, 0)

			// Now don't change the ring, but change shard size from 0 to 2, which is also our number of ingesters.
			// This will not change owned series (because we only have 2 ingesters, and both are already used), but will trigger recompute.
			c.tenantShards[ownedServiceTestUser] = 2

			c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
			c.checkUpdateReasonForUser(t, "")
			c.checkUserSeriesOwnedAndShardsByTestedIngester(t, ownedServiceSeriesCount/2, 2)
		},

		"change tenant shard size from 2 to 1, removing our ingester from the shard": func(t *testing.T, c *ownedSeriesTestContext) {
			c.tenantShards[ownedServiceTestUser] = 2

			c.pushUserSeries(t)
			c.registerSecondIngesterOwningHalfOfTheTokens(t)
			c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonNewUser)
			c.checkUserSeriesOwnedAndShardsByTestedIngester(t, ownedServiceSeriesCount/2, 2)

			// now change to 1. This will only keep "second ingester" in the shard, so our tested ingester will own 0 series.
			c.tenantShards[ownedServiceTestUser] = 1

			c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonShardSizeChanged)
			c.checkUpdateReasonForUser(t, "")
			c.checkUserSeriesOwnedAndShardsByTestedIngester(t, 0, 1)
		},

		"change tenant shard size from 1 to 2, adding our ingester to the shard": func(t *testing.T, c *ownedSeriesTestContext) {
			c.tenantShards[ownedServiceTestUser] = 1

			c.pushUserSeries(t)
			c.registerSecondIngesterOwningHalfOfTheTokens(t)
			c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonNewUser)
			c.checkUserSeriesOwnedAndShardsByTestedIngester(t, 0, 1)

			// now change shard size to 2, which will add our tested ingester
			c.tenantShards[ownedServiceTestUser] = 2

			c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonShardSizeChanged)
			c.checkUpdateReasonForUser(t, "")
			c.checkUserSeriesOwnedAndShardsByTestedIngester(t, ownedServiceSeriesCount/2, 2)
		},

		"unregister second ingester owning all series": func(t *testing.T, c *ownedSeriesTestContext) {
			c.tenantShards[ownedServiceTestUser] = 1

			c.pushUserSeries(t)
			c.registerSecondIngesterOwningHalfOfTheTokens(t)
			c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonNewUser)
			c.checkUserSeriesOwnedAndShardsByTestedIngester(t, 0, 1)

			// Now unregister second ingester.
			c.removeSecondIngester(t)

			c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonRingChanged)
			c.checkUserSeriesOwnedAndShardsByTestedIngester(t, ownedServiceSeriesCount, 1)
		},

		"early compaction removes all series": func(t *testing.T, c *ownedSeriesTestContext) {
			c.pushUserSeries(t)

			c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonNewUser)
			c.checkUserSeriesOwnedAndShardsByTestedIngester(t, ownedServiceSeriesCount, 0)

			// Run early compaction. This removes all series from memory.
			c.ing.compactBlocks(context.Background(), true, time.Now().Add(1*time.Minute).UnixMilli(), nil)
			require.Equal(t, uint64(0), c.db.Head().NumSeries())

			c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonEarlyCompaction)
			c.checkUserSeriesOwnedAndShardsByTestedIngester(t, 0, 0)
		},

		"recompute after previous ring check failed": func(t *testing.T, c *ownedSeriesTestContext) {
			c.pushUserSeries(t)

			// This overwrites "new user" reason.
			c.db.requiresOwnedSeriesUpdate.Store(recomputeOwnedSeriesReasonRingChanged)
			c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonRingChanged)
			c.checkUserSeriesOwnedAndShardsByTestedIngester(t, ownedServiceSeriesCount, 0)
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			wkv := &watchingKV{Client: kvStore}

			cfg := defaultIngesterTestConfig(t)
			cfg.IngesterRing.KVStore.Mock = wkv // Use "watchingKV" so that we know when update was processed.
			cfg.IngesterRing.InstanceID = "first-ingester"
			cfg.IngesterRing.NumTokens = ownedServiceSeriesCount/2 + 1 // We will use token for half of the series + one token for user.
			cfg.IngesterRing.ZoneAwarenessEnabled = true
			cfg.IngesterRing.InstanceZone = "zone"
			cfg.IngesterRing.ReplicationFactor = 1 // Currently we require RF=number of zones, and we will only work with single zone.

			// Start the ring watching. We need watcher to be running when we're doing ring updates, otherwise our update-and-watch function will fail.
			rng, err := ring.New(cfg.IngesterRing.ToRingConfig(), "ingester", IngesterRingKey, log.NewNopLogger(), nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), rng))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), rng))
			})

			c := ownedSeriesTestContext{
				seriesToWrite: seriesToWrite,
				seriesTokens:  seriesTokens,
				kvStore:       wkv,
			}

			c.registerTestedIngesterIntoRing(t, cfg.IngesterRing.InstanceID, cfg.IngesterRing.InstanceAddr, cfg.IngesterRing.InstanceZone)

			c.ing = setupIngester(t, cfg)

			c.tenantShards = map[string]int{}
			c.buf = &concurrency.SyncBuffer{}

			c.ownedSeries = newOwnedSeriesService(10*time.Minute, cfg.IngesterRing.InstanceID, rng, log.NewLogfmtLogger(c.buf), nil, func(user string) int { return c.tenantShards[user] }, c.ing.getTSDBUsers, c.ing.getTSDB)

			tc(t, &c)
		})
	}
}

func TestOwnedSeriesRingChanged(t *testing.T) {
	kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	wkv := &watchingKV{Client: kvStore}

	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	// Configure ring
	rc.KVStore.Mock = wkv
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

	updateRingAndWaitForWatcherToReadUpdate(t, wkv, func(desc *ring.Desc) {
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
		updateRingAndWaitForWatcherToReadUpdate(t, wkv, func(desc *ring.Desc) {
			desc.AddIngester(instanceID2, "localhost:22222", "zone", []uint32{4, 5, 6}, ring.ACTIVE, time.Now())
		})

		changed, err := ownedSeries.checkRingForChanges()
		require.NoError(t, err)
		require.True(t, changed)
	})

	t.Run("change of state is not interesting", func(t *testing.T) {
		updateRingAndWaitForWatcherToReadUpdate(t, wkv, func(desc *ring.Desc) {
			desc.AddIngester(instanceID2, "localhost:22222", "zone", []uint32{4, 5, 6}, ring.LEAVING, time.Now())
		})

		// Change of state is not interesting.
		changed, err := ownedSeries.checkRingForChanges()
		require.NoError(t, err)
		require.False(t, changed)
	})

	t.Run("removal of instance", func(t *testing.T) {
		updateRingAndWaitForWatcherToReadUpdate(t, wkv, func(desc *ring.Desc) {
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

func updateRingAndWaitForWatcherToReadUpdate(t *testing.T, kvStore *watchingKV, updateFn func(*ring.Desc)) {
	// Clear existing updates, so that we can test if next update was processed.
	kvStore.getAndResetUpdatedKeys()

	err := kvStore.CAS(context.Background(), IngesterRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		d, _ := in.(*ring.Desc)
		if d == nil {
			d = ring.NewDesc()
		}

		updateFn(d)

		return d, true, nil
	})
	require.NoError(t, err)

	test.Poll(t, 1*time.Second, true, func() interface{} {
		v := kvStore.getAndResetUpdatedKeys()
		return slices.Contains(v, IngesterRingKey)
	})
}

type watchingKV struct {
	kv.Client

	updatedKeysMu sync.Mutex
	updatedKeys   []string
}

func (w *watchingKV) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	w.Client.WatchKey(ctx, key, func(i interface{}) bool {
		v := f(i)

		w.updatedKeysMu.Lock()
		defer w.updatedKeysMu.Unlock()
		w.updatedKeys = append(w.updatedKeys, key)

		return v
	})
}

func (w *watchingKV) getAndResetUpdatedKeys() []string {
	w.updatedKeysMu.Lock()
	defer w.updatedKeysMu.Unlock()

	r := w.updatedKeys
	w.updatedKeys = nil
	return r
}
