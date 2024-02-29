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
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

const ownedServiceTestUser = "test-user"
const ownedServiceSeriesCount = 10
const ownedServiceTestUserSeriesLimit = 10000

const defaultHeartbeatPeriod = 100 * time.Millisecond

type ownedSeriesTestContextBase struct {
	seriesToWrite []series

	ownedSeries *ownedSeriesService
	ing         *Ingester
	db          *userTSDB
	kvStore     *watchingKV

	buf *concurrency.SyncBuffer

	cfg       Config
	overrides *validation.Overrides
}

func (c *ownedSeriesTestContextBase) pushUserSeries(t *testing.T) {
	require.NoError(t, pushSeriesToIngester(user.InjectOrgID(context.Background(), ownedServiceTestUser), t, c.ing, c.seriesToWrite))
	db := c.ing.getTSDB(ownedServiceTestUser)
	require.NotNil(t, db)
	c.db = db
}

func (c *ownedSeriesTestContextBase) checkUpdateReasonForUser(t *testing.T, expectedReason string) {
	require.Equal(t, expectedReason, c.db.requiresOwnedSeriesUpdate.Load())
}

func (c *ownedSeriesTestContextBase) checkTestedIngesterOwnedSeriesState(t *testing.T, series, shards, limit int) {
	os := c.db.ownedSeriesState()
	require.Equal(t, series, os.ownedSeriesCount)
	require.Equal(t, shards, os.shardSize)
	require.Equal(t, limit, os.localSeriesLimit)
}

func (c *ownedSeriesTestContextBase) updateOwnedSeriesAndCheckResult(t *testing.T, ringChanged bool, expectedUpdatedTenants int, expectedReason string) {
	c.buf.Reset()
	require.Equal(t, expectedUpdatedTenants, c.ownedSeries.updateAllTenants(context.Background(), ringChanged), c.buf.String())
	require.Contains(t, c.buf.String(), expectedReason)
}

type ownedSeriesWithIngesterRingTestContext struct {
	ownedSeriesTestContextBase

	seriesTokens []uint32
	ingesterZone string

	ingesterRing ring.ReadRing

	// when true, the test ingester will be second in the shuffle shard, and the "second" ingester will be first
	swapShardOrder bool
}

func (c *ownedSeriesWithIngesterRingTestContext) registerTestedIngesterIntoRing(t *testing.T, instanceID, instanceAddr, instanceZone string) {
	c.ingesterZone = instanceZone

	skip := 0
	if c.swapShardOrder {
		skip = 1
	}

	// Insert our ingester into the ring. When lifecycler starts, it will find this entry, and keep the tokens.
	updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
		var tokens []uint32
		for i := 0; i < ownedServiceSeriesCount/2; i++ {
			tokens = append(tokens, c.seriesTokens[i]+1)
		}
		// This instance be the second ingester in the shuffle shard (skip=1). "second" ingester will be first ingester in the shard.
		tokens = append(tokens, userToken(ownedServiceTestUser, c.ingesterZone, skip)+1)
		slices.Sort(tokens)

		desc.AddIngester(instanceID, instanceAddr, instanceZone, tokens, ring.ACTIVE, time.Now())
	})
}

// Insert second ingester to the ring, with tokens that will make it second half of the series.
func (c *ownedSeriesWithIngesterRingTestContext) registerSecondIngesterOwningHalfOfTheTokens(t *testing.T) {
	skip := 1
	if c.swapShardOrder {
		skip = 0
	}

	updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
		var tokens []uint32
		for i := ownedServiceSeriesCount / 2; i < ownedServiceSeriesCount; i++ {
			tokens = append(tokens, c.seriesTokens[i]+1)
		}
		tokens = append(tokens, userToken(ownedServiceTestUser, c.ingesterZone, skip)+1)
		slices.Sort(tokens)

		// Must be in the same zone, because we use RF=1, and require RF=num of zones.
		desc.AddIngester("second-ingester", "localhost", c.ingesterZone, tokens, ring.ACTIVE, time.Now())
	})
}

func (c *ownedSeriesWithIngesterRingTestContext) removeSecondIngester(t *testing.T) {
	updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
		desc.RemoveIngester("second-ingester")
	})
}

func TestOwnedSeriesServiceWithIngesterRing(t *testing.T) {
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

	testCases := map[string]struct {
		limits         map[string]*validation.Limits
		swapShardOrder bool
		testFunc       func(t *testing.T, c *ownedSeriesWithIngesterRingTestContext, limits map[string]*validation.Limits)
	}{
		"empty ingester": {
			testFunc: func(t *testing.T, c *ownedSeriesWithIngesterRingTestContext, limits map[string]*validation.Limits) {
				require.Equal(t, 0, c.ownedSeries.updateAllTenants(context.Background(), false))
			},
		},
		"new user trigger": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
					IngestionTenantShardSize: 0,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithIngesterRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)

				// first ingester owns all the series, even without any ownedSeries run. this is because each created series is automatically counted as "owned".
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				// run initial owned series check
				c.checkUpdateReasonForUser(t, recomputeOwnedSeriesReasonNewUser)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// first ingester still owns all the series
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				// re-running shouldn't trigger a recompute, since no reason is set
				c.updateOwnedSeriesAndCheckResult(t, false, 0, "")

				// first ingester still owns all the series
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
			},
		},
		"new user trigger from WAL replay": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
					IngestionTenantShardSize: 0,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithIngesterRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				dataDir := c.ing.cfg.BlocksStorageConfig.TSDB.Dir

				// stop the ingester
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c.ing))

				// restart the ingester with the same TSDB directory to trigger WAL replay
				c.ing = setupIngesterWithOverrides(t, c.cfg, c.overrides, c.ingesterRing, dataDir)
				c.db = c.ing.getTSDB(ownedServiceTestUser)
				require.NotNil(t, c.db)

				// the owned series will be counted during WAL replay and reason set to "new user"
				// shard size and local limit are initialized to correct values
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, recomputeOwnedSeriesReasonNewUser)
			},
		},
		"shard size = 1, scale ingesters up and down": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
					IngestionTenantShardSize: 1,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithIngesterRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				// add an ingester
				c.registerSecondIngesterOwningHalfOfTheTokens(t)

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				// since no reason set, shard size and local limit are unchanged, and we pass ringChanged=false, no recompute will happen
				c.updateOwnedSeriesAndCheckResult(t, false, 0, "")
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")

				// passing ringChanged=true won't trigger a recompute either, because the user's subring hasn't changed
				c.updateOwnedSeriesAndCheckResult(t, true, 0, "")
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")

				// remove the second ingester
				c.removeSecondIngester(t)

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				// since no reason set, shard size and local limit are unchanged, and we pass ringChanged=false, no recompute will happen
				c.updateOwnedSeriesAndCheckResult(t, false, 0, "")
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")

				// passing ringChanged=true won't trigger a recompute either, because the user's subring hasn't changed
				c.updateOwnedSeriesAndCheckResult(t, true, 0, "")
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")
			},
		},
		"shard size = 1, scale ingesters up and down, series move to new ingster": {
			swapShardOrder: true,
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
					IngestionTenantShardSize: 1,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithIngesterRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				// add an ingester (it will become the first ingester in the shuffle shard)
				c.registerSecondIngesterOwningHalfOfTheTokens(t)

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				// since no reason set, shard size and local limit are unchanged, and we pass ringChanged=false, no recompute will happen
				c.updateOwnedSeriesAndCheckResult(t, false, 0, "")
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")

				// passing ringChanged=true will trigger recompute because the token ownership has changed
				c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonRingChanged)
				c.checkTestedIngesterOwnedSeriesState(t, 0, 1, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")

				// remove the second ingester, moving the series back to the original ingester
				c.removeSecondIngester(t)

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, 0, 1, ownedServiceTestUserSeriesLimit)

				// since no reason set, shard size and local limit are unchanged, and we pass ringChanged=false, no recompute will happen
				c.updateOwnedSeriesAndCheckResult(t, false, 0, "")
				c.checkTestedIngesterOwnedSeriesState(t, 0, 1, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")

				// passing ringChanged=true will trigger recompute because the token ownership has changed
				c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonRingChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")
			},
		},
		"shard size = 0, scale ingesters up and down": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
					IngestionTenantShardSize: 0,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithIngesterRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first ingester.
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				// add an ingester
				c.registerSecondIngesterOwningHalfOfTheTokens(t)

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				// will recompute because the local limit has changed (takes precedence over ring change)
				c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 0, ownedServiceTestUserSeriesLimit/2)
				c.checkUpdateReasonForUser(t, "")

				// remove the second ingester
				c.removeSecondIngester(t)

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 0, ownedServiceTestUserSeriesLimit/2)

				// will recompute because the local limit has changed (takes precedence over ring change)
				c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")
			},
		},
		"unchanged ring, shard size from 0 to ingester count": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
					IngestionTenantShardSize: 0,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithIngesterRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// add an ingester
				c.registerSecondIngesterOwningHalfOfTheTokens(t)

				// will recompute because the local limit has changed (takes precedence over ring change)
				c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 0, ownedServiceTestUserSeriesLimit/2)
				c.checkUpdateReasonForUser(t, "")

				// now don't change the ring, but change shard size from 0 to 2, which is also our number of ingesters
				// this will not change owned series (because we only have 2 ingesters, and both are already used), but will trigger recompute because the shard size has changed
				limits[ownedServiceTestUser].IngestionTenantShardSize = 2

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 0, ownedServiceTestUserSeriesLimit/2)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit/2)
				c.checkUpdateReasonForUser(t, "")
			},
		},
		"unchanged ring, shard size < ingesters, shard size up and down": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
					IngestionTenantShardSize: 1,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithIngesterRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// add an ingester
				c.registerSecondIngesterOwningHalfOfTheTokens(t)

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				// change shard size to 2, splitting the series between ingesters
				limits[ownedServiceTestUser].IngestionTenantShardSize = 2

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit/2)
				c.checkUpdateReasonForUser(t, "")

				// change shard size back to 1, moving the series back to the original ingester
				limits[ownedServiceTestUser].IngestionTenantShardSize = 1

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit/2)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")
			},
		},
		"unchanged ring, shard size < ingesters, shard size up and down, series start on other ingester": {
			swapShardOrder: true,
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
					IngestionTenantShardSize: 1,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithIngesterRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// add an ingester
				c.registerSecondIngesterOwningHalfOfTheTokens(t)

				// pass ringChanged=true to trigger recompute
				c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonRingChanged)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the second ingester
				c.checkTestedIngesterOwnedSeriesState(t, 0, 1, ownedServiceTestUserSeriesLimit)

				// change shard size to 2, splitting the series between ingesters
				limits[ownedServiceTestUser].IngestionTenantShardSize = 2

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, 0, 1, ownedServiceTestUserSeriesLimit)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit/2)
				c.checkUpdateReasonForUser(t, "")

				// change shard size back to 1, moving the series back to the original ingester
				limits[ownedServiceTestUser].IngestionTenantShardSize = 1

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit/2)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, 0, 1, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")
			},
		},
		"unchanged ring and shards, series limit up and down": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
					IngestionTenantShardSize: 0,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithIngesterRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				// increase series limit
				limits[ownedServiceTestUser].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit * 2

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit*2)
				c.checkUpdateReasonForUser(t, "")

				// decrease series limit
				limits[ownedServiceTestUser].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit*2)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")
			},
		},
		"unchanged ring, series limit and shard size up and down in tandem": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
					IngestionTenantShardSize: 1,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithIngesterRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// add an ingester
				c.registerSecondIngesterOwningHalfOfTheTokens(t)

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				// double series limit and shard size
				limits[ownedServiceTestUser].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit * 2
				limits[ownedServiceTestUser].IngestionTenantShardSize = 2

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")

				// halve series limit and shard size
				limits[ownedServiceTestUser].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit
				limits[ownedServiceTestUser].IngestionTenantShardSize = 1

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")
			},
		},
		"early compaction trigger": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
					IngestionTenantShardSize: 0,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithIngesterRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				// run early compaction removing all series from the head
				c.ing.compactBlocks(context.Background(), true, time.Now().Add(1*time.Minute).UnixMilli(), nil)
				require.Equal(t, uint64(0), c.db.Head().NumSeries())

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				c.checkUpdateReasonForUser(t, recomputeOwnedSeriesReasonEarlyCompaction)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonEarlyCompaction)
				c.checkTestedIngesterOwnedSeriesState(t, 0, 0, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")
			},
		},
		"previous ring check failed": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
					IngestionTenantShardSize: 0,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithIngesterRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				c.db.requiresOwnedSeriesUpdate.Store(recomputeOwnedSeriesReasonGetTokenRangesFailed)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonGetTokenRangesFailed)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			c := ownedSeriesWithIngesterRingTestContext{
				ownedSeriesTestContextBase: ownedSeriesTestContextBase{
					seriesToWrite: seriesToWrite,
				},
				seriesTokens:   seriesTokens,
				swapShardOrder: tc.swapShardOrder,
			}

			kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			c.kvStore = &watchingKV{Client: kvStore}

			c.cfg = defaultIngesterTestConfig(t)
			c.cfg.IngesterRing.KVStore.Mock = c.kvStore // Use "watchingKV" so that we know when update was processed.
			c.cfg.IngesterRing.InstanceID = "first-ingester"
			c.cfg.IngesterRing.NumTokens = ownedServiceSeriesCount/2 + 1 // We will use token for half of the series + one token for user.
			c.cfg.IngesterRing.ZoneAwarenessEnabled = true
			c.cfg.IngesterRing.InstanceZone = "zone"
			c.cfg.IngesterRing.ReplicationFactor = 1 // Currently we require RF=number of zones, and we will only work with single zone.
			c.cfg.IngesterRing.HeartbeatPeriod = defaultHeartbeatPeriod
			c.cfg.IngesterRing.UnregisterOnShutdown = false

			// Note that we don't start the ingester's owned series service (cfg.UseIngesterOwnedSeriesForLimits and cfg.UpdateIngesterOwnedSeries are false)
			// because we'll be testing a stand-alone service to better control when it runs.

			// Start the ring watching. We need watcher to be running when we're doing ring updates, otherwise our update-and-watch function will fail.
			c.ingesterRing = createAndStartRing(t, c.cfg.IngesterRing.ToRingConfig())

			c.registerTestedIngesterIntoRing(t, c.cfg.IngesterRing.InstanceID, c.cfg.IngesterRing.InstanceAddr, c.cfg.IngesterRing.InstanceZone)

			var err error
			c.overrides, err = validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(tc.limits))
			require.NoError(t, err)

			c.ing = setupIngesterWithOverrides(t, c.cfg, c.overrides, c.ingesterRing, "") // Pass ring so that limiter and oss see the same state

			c.buf = &concurrency.SyncBuffer{}

			c.ownedSeries = newOwnedSeriesService(
				10*time.Minute,
				newOwnedSeriesIngesterRingStrategy(c.cfg.IngesterRing.InstanceID, c.ingesterRing, c.ing.limits.IngestionTenantShardSize),
				log.NewLogfmtLogger(c.buf),
				nil,
				c.ing.limiter.maxSeriesPerUser,
				c.ing.getTSDBUsers,
				c.ing.getTSDB,
			)

			tc.testFunc(t, &c, tc.limits)
		})
	}
}

type ownedSeriesWithPartitionsRingTestContext struct {
	ownedSeriesTestContextBase

	partitionsRing *ring.PartitionRingWatcher
}

func (c *ownedSeriesWithPartitionsRingTestContext) createPartition(t *testing.T, partitionID int32, partitionActive ring.PartitionState) {
	updatePartitionRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(partitionRing *ring.PartitionRingDesc) {
		partitionRing.AddPartition(partitionID, partitionActive, time.Now())
	})
}

func TestOwnedSeriesServiceWithPartitionsRing(t *testing.T) {
	const kafkaTopic = "topic"

	var seriesToWrite []series
	for seriesIdx := 0; seriesIdx < ownedServiceSeriesCount; seriesIdx++ {
		s := series{
			lbls:      labels.FromStrings(labels.MetricName, "test", fmt.Sprintf("lbl_%05d", seriesIdx), "value"),
			value:     float64(0),
			timestamp: time.Now().UnixMilli(),
		}
		seriesToWrite = append(seriesToWrite, s)
	}

	testCases := map[string]struct {
		limits         map[string]*validation.Limits
		swapShardOrder bool
		testFunc       func(t *testing.T, c *ownedSeriesWithPartitionsRingTestContext, limits map[string]*validation.Limits)
	}{
		"empty ingester": {
			testFunc: func(t *testing.T, c *ownedSeriesWithPartitionsRingTestContext, limits map[string]*validation.Limits) {
				require.Equal(t, 0, c.ownedSeries.updateAllTenants(context.Background(), false))
			},
		},
		"new user trigger": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					MaxGlobalSeriesPerUser:             ownedServiceTestUserSeriesLimit,
					IngestionPartitionsTenantShardSize: 0,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithPartitionsRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)

				// first ingester owns all the series, even without any ownedSeries run. this is because each created series is automatically counted as "owned".
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				// run initial owned series check
				c.checkUpdateReasonForUser(t, recomputeOwnedSeriesReasonNewUser)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// first ingester still owns all the series
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				// re-running shouldn't trigger a recompute, since no reason is set
				c.updateOwnedSeriesAndCheckResult(t, false, 0, "")

				// first ingester still owns all the series
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
			},
		},
		//"new user trigger from WAL replay": {
		//	limits: map[string]*validation.Limits{
		//		ownedServiceTestUser: {
		//			MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
		//			IngestionTenantShardSize: 0,
		//		},
		//	},
		//	testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
		//		c.pushUserSeries(t)
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// initial state: all series are owned by the first ingester
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
		//
		//		dataDir := c.ing.cfg.BlocksStorageConfig.TSDB.Dir
		//
		//		// stop the ingester
		//		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c.ing))
		//
		//		// restart the ingester with the same TSDB directory to trigger WAL replay
		//		c.ing = setupIngesterWithOverrides(t, c.cfg, c.overrides, c.ingesterRing, dataDir)
		//		c.db = c.ing.getTSDB(ownedServiceTestUser)
		//		require.NotNil(t, c.db)
		//
		//		// the owned series will be counted during WAL replay and reason set to "new user"
		//		// shard size and local limit are initialized to correct values
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, recomputeOwnedSeriesReasonNewUser)
		//	},
		//},
		//"shard size = 1, scale ingesters up and down": {
		//	limits: map[string]*validation.Limits{
		//		ownedServiceTestUser: {
		//			MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
		//			IngestionTenantShardSize: 1,
		//		},
		//	},
		//	testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
		//		c.pushUserSeries(t)
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// initial state: all series are owned by the first ingester
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//
		//		// add an ingester
		//		c.registerSecondIngesterOwningHalfOfTheTokens(t)
		//
		//		// verify no change in state before owned series run
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//
		//		// since no reason set, shard size and local limit are unchanged, and we pass ringChanged=false, no recompute will happen
		//		c.updateOwnedSeriesAndCheckResult(t, false, 0, "")
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// passing ringChanged=true won't trigger a recompute either, because the user's subring hasn't changed
		//		c.updateOwnedSeriesAndCheckResult(t, true, 0, "")
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// remove the second ingester
		//		c.removeSecondIngester(t)
		//
		//		// verify no change in state before owned series run
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//
		//		// since no reason set, shard size and local limit are unchanged, and we pass ringChanged=false, no recompute will happen
		//		c.updateOwnedSeriesAndCheckResult(t, false, 0, "")
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// passing ringChanged=true won't trigger a recompute either, because the user's subring hasn't changed
		//		c.updateOwnedSeriesAndCheckResult(t, true, 0, "")
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, "")
		//	},
		//},
		//"shard size = 1, scale ingesters up and down, series move to new ingster": {
		//	swapShardOrder: true,
		//	limits: map[string]*validation.Limits{
		//		ownedServiceTestUser: {
		//			MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
		//			IngestionTenantShardSize: 1,
		//		},
		//	},
		//	testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
		//		c.pushUserSeries(t)
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// initial state: all series are owned by the first ingester
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//
		//		// add an ingester (it will become the first ingester in the shuffle shard)
		//		c.registerSecondIngesterOwningHalfOfTheTokens(t)
		//
		//		// verify no change in state before owned series run
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//
		//		// since no reason set, shard size and local limit are unchanged, and we pass ringChanged=false, no recompute will happen
		//		c.updateOwnedSeriesAndCheckResult(t, false, 0, "")
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// passing ringChanged=true will trigger recompute because the token ownership has changed
		//		c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonRingChanged)
		//		c.checkTestedIngesterOwnedSeriesState(t, 0, 1, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// remove the second ingester, moving the series back to the original ingester
		//		c.removeSecondIngester(t)
		//
		//		// verify no change in state before owned series run
		//		c.checkTestedIngesterOwnedSeriesState(t, 0, 1, ownedServiceTestUserSeriesLimit)
		//
		//		// since no reason set, shard size and local limit are unchanged, and we pass ringChanged=false, no recompute will happen
		//		c.updateOwnedSeriesAndCheckResult(t, false, 0, "")
		//		c.checkTestedIngesterOwnedSeriesState(t, 0, 1, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// passing ringChanged=true will trigger recompute because the token ownership has changed
		//		c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonRingChanged)
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, "")
		//	},
		//},
		//"shard size = 0, scale ingesters up and down": {
		//	limits: map[string]*validation.Limits{
		//		ownedServiceTestUser: {
		//			MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
		//			IngestionTenantShardSize: 0,
		//		},
		//	},
		//	testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
		//		c.pushUserSeries(t)
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// initial state: all series are owned by the first ingester.
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
		//
		//		// add an ingester
		//		c.registerSecondIngesterOwningHalfOfTheTokens(t)
		//
		//		// verify no change in state before owned series run
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
		//
		//		// will recompute because the local limit has changed (takes precedence over ring change)
		//		c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 0, ownedServiceTestUserSeriesLimit/2)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// remove the second ingester
		//		c.removeSecondIngester(t)
		//
		//		// verify no change in state before owned series run
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 0, ownedServiceTestUserSeriesLimit/2)
		//
		//		// will recompute because the local limit has changed (takes precedence over ring change)
		//		c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, "")
		//	},
		//},
		//"unchanged ring, shard size from 0 to ingester count": {
		//	limits: map[string]*validation.Limits{
		//		ownedServiceTestUser: {
		//			MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
		//			IngestionTenantShardSize: 0,
		//		},
		//	},
		//	testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
		//		c.pushUserSeries(t)
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// add an ingester
		//		c.registerSecondIngesterOwningHalfOfTheTokens(t)
		//
		//		// will recompute because the local limit has changed (takes precedence over ring change)
		//		c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 0, ownedServiceTestUserSeriesLimit/2)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// now don't change the ring, but change shard size from 0 to 2, which is also our number of ingesters
		//		// this will not change owned series (because we only have 2 ingesters, and both are already used), but will trigger recompute because the shard size has changed
		//		limits[ownedServiceTestUser].IngestionTenantShardSize = 2
		//
		//		// verify no change in state before owned series run
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 0, ownedServiceTestUserSeriesLimit/2)
		//
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit/2)
		//		c.checkUpdateReasonForUser(t, "")
		//	},
		//},
		//"unchanged ring, shard size < ingesters, shard size up and down": {
		//	limits: map[string]*validation.Limits{
		//		ownedServiceTestUser: {
		//			MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
		//			IngestionTenantShardSize: 1,
		//		},
		//	},
		//	testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
		//		c.pushUserSeries(t)
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// add an ingester
		//		c.registerSecondIngesterOwningHalfOfTheTokens(t)
		//
		//		// initial state: all series are owned by the first ingester
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//
		//		// change shard size to 2, splitting the series between ingesters
		//		limits[ownedServiceTestUser].IngestionTenantShardSize = 2
		//
		//		// verify no change in state before owned series run
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit/2)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// change shard size back to 1, moving the series back to the original ingester
		//		limits[ownedServiceTestUser].IngestionTenantShardSize = 1
		//
		//		// verify no change in state before owned series run
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit/2)
		//
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, "")
		//	},
		//},
		//"unchanged ring, shard size < ingesters, shard size up and down, series start on other ingester": {
		//	swapShardOrder: true,
		//	limits: map[string]*validation.Limits{
		//		ownedServiceTestUser: {
		//			MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
		//			IngestionTenantShardSize: 1,
		//		},
		//	},
		//	testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
		//		c.pushUserSeries(t)
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// add an ingester
		//		c.registerSecondIngesterOwningHalfOfTheTokens(t)
		//
		//		// pass ringChanged=true to trigger recompute
		//		c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonRingChanged)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// initial state: all series are owned by the second ingester
		//		c.checkTestedIngesterOwnedSeriesState(t, 0, 1, ownedServiceTestUserSeriesLimit)
		//
		//		// change shard size to 2, splitting the series between ingesters
		//		limits[ownedServiceTestUser].IngestionTenantShardSize = 2
		//
		//		// verify no change in state before owned series run
		//		c.checkTestedIngesterOwnedSeriesState(t, 0, 1, ownedServiceTestUserSeriesLimit)
		//
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit/2)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// change shard size back to 1, moving the series back to the original ingester
		//		limits[ownedServiceTestUser].IngestionTenantShardSize = 1
		//
		//		// verify no change in state before owned series run
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit/2)
		//
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
		//		c.checkTestedIngesterOwnedSeriesState(t, 0, 1, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, "")
		//	},
		//},
		//"unchanged ring and shards, series limit up and down": {
		//	limits: map[string]*validation.Limits{
		//		ownedServiceTestUser: {
		//			MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
		//			IngestionTenantShardSize: 0,
		//		},
		//	},
		//	testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
		//		c.pushUserSeries(t)
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// initial state: all series are owned by the first ingester
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
		//
		//		// increase series limit
		//		limits[ownedServiceTestUser].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit * 2
		//
		//		// verify no change in state before owned series run
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
		//
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit*2)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// decrease series limit
		//		limits[ownedServiceTestUser].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit
		//
		//		// verify no change in state before owned series run
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit*2)
		//
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, "")
		//	},
		//},
		//"unchanged ring, series limit and shard size up and down in tandem": {
		//	limits: map[string]*validation.Limits{
		//		ownedServiceTestUser: {
		//			MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
		//			IngestionTenantShardSize: 1,
		//		},
		//	},
		//	testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
		//		c.pushUserSeries(t)
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// add an ingester
		//		c.registerSecondIngesterOwningHalfOfTheTokens(t)
		//
		//		// initial state: all series are owned by the first ingester
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//
		//		// double series limit and shard size
		//		limits[ownedServiceTestUser].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit * 2
		//		limits[ownedServiceTestUser].IngestionTenantShardSize = 2
		//
		//		// verify no change in state before owned series run
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// halve series limit and shard size
		//		limits[ownedServiceTestUser].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit
		//		limits[ownedServiceTestUser].IngestionTenantShardSize = 1
		//
		//		// verify no change in state before owned series run
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit)
		//
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, "")
		//	},
		//},
		//"early compaction trigger": {
		//	limits: map[string]*validation.Limits{
		//		ownedServiceTestUser: {
		//			MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
		//			IngestionTenantShardSize: 0,
		//		},
		//	},
		//	testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
		//		c.pushUserSeries(t)
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// initial state: all series are owned by the first ingester
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
		//
		//		// run early compaction removing all series from the head
		//		c.ing.compactBlocks(context.Background(), true, time.Now().Add(1*time.Minute).UnixMilli(), nil)
		//		require.Equal(t, uint64(0), c.db.Head().NumSeries())
		//
		//		// verify no change in state before owned series run
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
		//
		//		c.checkUpdateReasonForUser(t, recomputeOwnedSeriesReasonEarlyCompaction)
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonEarlyCompaction)
		//		c.checkTestedIngesterOwnedSeriesState(t, 0, 0, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, "")
		//	},
		//},
		//"previous ring check failed": {
		//	limits: map[string]*validation.Limits{
		//		ownedServiceTestUser: {
		//			MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
		//			IngestionTenantShardSize: 0,
		//		},
		//	},
		//	testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
		//		c.pushUserSeries(t)
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
		//		c.checkUpdateReasonForUser(t, "")
		//
		//		// initial state: all series are owned by the first ingester
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
		//
		//		c.db.requiresOwnedSeriesUpdate.Store(recomputeOwnedSeriesReasonGetTokenRangesFailed)
		//		c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonGetTokenRangesFailed)
		//		c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
		//		c.checkUpdateReasonForUser(t, "")
		//	},
		//},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			c := ownedSeriesWithPartitionsRingTestContext{
				ownedSeriesTestContextBase: ownedSeriesTestContextBase{
					seriesToWrite: seriesToWrite,
				},
			}

			ingesterKVStore, ingesterKvStoreCloser := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, ingesterKvStoreCloser.Close()) })

			partitionsKVStore, partitionsKVStoreCloser := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, partitionsKVStoreCloser.Close()) })

			c.kvStore = &watchingKV{Client: partitionsKVStore}

			_, kafkaAddr := testkafka.CreateCluster(t, 10, kafkaTopic)

			// Ingester will not use this ring for anything, but it will register into it.
			c.cfg = defaultIngesterTestConfig(t)
			c.cfg.IngestStorageConfig.Enabled = true
			c.cfg.IngestStorageConfig.KafkaConfig.Topic = kafkaTopic
			c.cfg.IngestStorageConfig.KafkaConfig.Address = kafkaAddr
			c.cfg.IngesterRing.KVStore.Mock = ingesterKVStore // not using "watchingKV", we're not interested in ingester ring.
			c.cfg.IngesterRing.InstanceID = "instance-1"      // Ingester will use number 1 as partition ID.

			// We must start partition ring watcher, so that "watchingKV" will see updates.
			prw := ring.NewPartitionRingWatcher(PartitionRingName, PartitionRingKey, c.kvStore, log.NewNopLogger(), nil)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), prw))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), prw))
			})

			// Note that we don't start the ingester's owned series service (cfg.UseIngesterOwnedSeriesForLimits and cfg.UpdateIngesterOwnedSeries are false)
			// because we'll be testing a stand-alone service to better control when it runs.

			c.partitionsRing = prw
			c.createPartition(t, 1, ring.PartitionActive)

			var err error
			c.overrides, err = validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(tc.limits))
			require.NoError(t, err)

			c.ing = setupIngesterWithOverridesAndPartitionRing(t, c.cfg, c.overrides, c.partitionsRing, "")

			c.buf = &concurrency.SyncBuffer{}

			// We also don't start this service, and will only call its methods.
			c.ownedSeries = newOwnedSeriesService(
				10*time.Minute,
				newOwnedSeriesPartitionRingStrategy(c.ing.ingestPartitionID, c.partitionsRing, c.ing.limits.IngestionPartitionsTenantShardSize),
				log.NewLogfmtLogger(c.buf),
				nil,
				c.ing.limiter.maxSeriesPerUser,
				c.ing.getTSDBUsers,
				c.ing.getTSDB,
			)

			tc.testFunc(t, &c, tc.limits)
		})
	}
}

func TestOwnedSeriesIngesterRingStrategyRingChanged(t *testing.T) {
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

	rng := createAndStartRing(t, rc)

	const instanceID1 = "first ingester"
	const instanceID2 = "second instance"

	ringStrategy := newOwnedSeriesIngesterRingStrategy(instanceID1, rng, nil)

	updateRingAndWaitForWatcherToReadUpdate(t, wkv, func(desc *ring.Desc) {
		desc.AddIngester(instanceID1, "localhost:11111", "zone", []uint32{1, 2, 3}, ring.ACTIVE, time.Now())
	})

	// First call should indicate ring change.
	t.Run("first call always reports change", func(t *testing.T) {
		changed, err := ringStrategy.checkRingForChanges()
		require.NoError(t, err)
		require.True(t, changed)
	})

	t.Run("second call (without ring change) reports no change", func(t *testing.T) {
		changed, err := ringStrategy.checkRingForChanges()
		require.NoError(t, err)
		require.False(t, changed)
	})

	t.Run("new instance added", func(t *testing.T) {
		updateRingAndWaitForWatcherToReadUpdate(t, wkv, func(desc *ring.Desc) {
			desc.AddIngester(instanceID2, "localhost:22222", "zone", []uint32{4, 5, 6}, ring.ACTIVE, time.Now())
		})

		changed, err := ringStrategy.checkRingForChanges()
		require.NoError(t, err)
		require.True(t, changed)
	})

	t.Run("change of state is not interesting", func(t *testing.T) {
		updateRingAndWaitForWatcherToReadUpdate(t, wkv, func(desc *ring.Desc) {
			desc.AddIngester(instanceID2, "localhost:22222", "zone", []uint32{4, 5, 6}, ring.LEAVING, time.Now())
		})

		// Change of state is not interesting.
		changed, err := ringStrategy.checkRingForChanges()
		require.NoError(t, err)
		require.False(t, changed)
	})

	t.Run("removal of instance", func(t *testing.T) {
		updateRingAndWaitForWatcherToReadUpdate(t, wkv, func(desc *ring.Desc) {
			desc.RemoveIngester(instanceID2)
		})

		changed, err := ringStrategy.checkRingForChanges()
		require.NoError(t, err)
		require.True(t, changed)
	})
}

func TestOwnedSeriesPartitionsRingStrategyRingChanged(t *testing.T) {
	kvStore, closer := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	wkv := &watchingKV{Client: kvStore}
	prw := ring.NewPartitionRingWatcher(PartitionRingName, PartitionRingKey, wkv, log.NewNopLogger(), nil)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), prw))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), prw))
	})

	ringStrategy := newOwnedSeriesPartitionRingStrategy(1, prw, nil)

	checkExpectedRingChange := func(expected bool) {
		changed, err := ringStrategy.checkRingForChanges()
		require.NoError(t, err)
		require.Equal(t, expected, changed)
	}

	updatePartitionRingAndWaitForWatcherToReadUpdate(t, wkv, func(partitionRing *ring.PartitionRingDesc) {
		partitionRing.AddPartition(1, ring.PartitionActive, time.Now())
	})

	t.Run("first call with active partition in the ring reports change", func(t *testing.T) {
		// State of the ring: 1: Active
		checkExpectedRingChange(true)
		// second call reports no change
		checkExpectedRingChange(false)
	})

	t.Run("new active partition added, change reported", func(t *testing.T) {
		updatePartitionRingAndWaitForWatcherToReadUpdate(t, wkv, func(partitionRing *ring.PartitionRingDesc) {
			partitionRing.AddPartition(2, ring.PartitionActive, time.Now())
		})

		// State of the ring: 1: Active, 2: Active
		checkExpectedRingChange(true)
		// second call reports no change
		checkExpectedRingChange(false)
	})

	t.Run("new inactive partition added, no change reported", func(t *testing.T) {
		updatePartitionRingAndWaitForWatcherToReadUpdate(t, wkv, func(partitionRing *ring.PartitionRingDesc) {
			partitionRing.AddPartition(3, ring.PartitionInactive, time.Now())
		})
		// State of the ring: 1: Active, 2: Active, 3: Inactive
		checkExpectedRingChange(false)
	})

	t.Run("new pending partition added, no change reported", func(t *testing.T) {
		updatePartitionRingAndWaitForWatcherToReadUpdate(t, wkv, func(partitionRing *ring.PartitionRingDesc) {
			partitionRing.AddPartition(4, ring.PartitionPending, time.Now())
		})
		// State of the ring: 1: Active, 2: Active, 3: Inactive, 4: Pending
		checkExpectedRingChange(false)
	})

	t.Run("inactive partition changed to active, change reported", func(t *testing.T) {
		updatePartitionRingAndWaitForWatcherToReadUpdate(t, wkv, func(partitionRing *ring.PartitionRingDesc) {
			partitionRing.UpdatePartitionState(3, ring.PartitionActive, time.Now())
		})
		// State of the ring: 1: Active, 2: Active, 3: Active, 4: Pending
		checkExpectedRingChange(true)
		// second call reports no change
		checkExpectedRingChange(false)
	})

	t.Run("active partition changed to inactive, change reported", func(t *testing.T) {
		updatePartitionRingAndWaitForWatcherToReadUpdate(t, wkv, func(partitionRing *ring.PartitionRingDesc) {
			partitionRing.UpdatePartitionState(1, ring.PartitionInactive, time.Now())
		})
		// State of the ring: 1: Inactive, 2: Active, 3: Active, 4: Pending
		checkExpectedRingChange(true)
		// second call reports no change
		checkExpectedRingChange(false)
	})

	t.Run("inactive partition changed to pending, no change reported", func(t *testing.T) {
		updatePartitionRingAndWaitForWatcherToReadUpdate(t, wkv, func(partitionRing *ring.PartitionRingDesc) {
			partitionRing.UpdatePartitionState(1, ring.PartitionPending, time.Now())
		})
		// State of the ring: 1: Pending, 2: Active, 3: Active, 4: Pending
		checkExpectedRingChange(false)
	})

	t.Run("two partitions change at the same time", func(t *testing.T) {
		updatePartitionRingAndWaitForWatcherToReadUpdate(t, wkv, func(partitionRing *ring.PartitionRingDesc) {
			partitionRing.UpdatePartitionState(1, ring.PartitionActive, time.Now())
			partitionRing.UpdatePartitionState(2, ring.PartitionInactive, time.Now())
		})
		// State of the ring: 1: Active, 2: Inactive, 3: Active, 4: Pending
		checkExpectedRingChange(true)
		// second call reports no change
		checkExpectedRingChange(false)
	})

	t.Run("active partition removed", func(t *testing.T) {
		updatePartitionRingAndWaitForWatcherToReadUpdate(t, wkv, func(partitionRing *ring.PartitionRingDesc) {
			partitionRing.RemovePartition(3)
		})
		// State of the ring: 1: Active, 2: Inactive, 4: Pending
		checkExpectedRingChange(true)
		// second call reports no change
		checkExpectedRingChange(false)
	})

	t.Run("inactive partition removed", func(t *testing.T) {
		updatePartitionRingAndWaitForWatcherToReadUpdate(t, wkv, func(partitionRing *ring.PartitionRingDesc) {
			partitionRing.RemovePartition(2)
		})
		// State of the ring: 1: Active, 4: Pending
		checkExpectedRingChange(false)
	})
}

func userToken(user, zone string, skip int) uint32 {
	r := rand.New(rand.NewSource(util.ShuffleShardSeed(user, zone)))

	for ; skip > 0; skip-- {
		_ = r.Uint32()
	}
	return r.Uint32()
}

func setupIngesterWithOverrides(t *testing.T, cfg Config, overrides *validation.Overrides, ingesterRing ring.ReadRing, dataDir string) *Ingester {
	ing, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, overrides, ingesterRing, dataDir, "", nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
	})
	return ing
}

func setupIngesterWithOverridesAndPartitionRing(t *testing.T, cfg Config, overrides *validation.Overrides, partitionRing *ring.PartitionRingWatcher, dataDir string) *Ingester {
	// ingestersRing will be created automatically.
	ing, err := prepareIngesterWithBlockStorageAndOverridesAndPartitionRing(t, cfg, overrides, nil, partitionRing, dataDir, "", nil)
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
		d := ring.GetOrCreateRingDesc(in)
		updateFn(d)
		return d, true, nil
	})
	require.NoError(t, err)

	test.Poll(t, 1*time.Second, true, func() interface{} {
		v := kvStore.getAndResetUpdatedKeys()
		return slices.Contains(v, IngesterRingKey)
	})
}

func updatePartitionRingAndWaitForWatcherToReadUpdate(t *testing.T, kvStore *watchingKV, updateFn func(partitionRing *ring.PartitionRingDesc)) {
	// Clear existing updates, so that we can test if next update was processed.
	kvStore.getAndResetUpdatedKeys()

	err := kvStore.CAS(context.Background(), PartitionRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
		d := ring.GetOrCreatePartitionRingDesc(in)
		updateFn(d)
		return d, true, nil
	})
	require.NoError(t, err)

	test.Poll(t, 1*time.Second, true, func() interface{} {
		v := kvStore.getAndResetUpdatedKeys()
		return slices.Contains(v, PartitionRingKey)
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
