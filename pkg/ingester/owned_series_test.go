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
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

const ownedServiceSeriesCount = 10
const ownedServiceTestUserSeriesLimit = 10000

const defaultHeartbeatPeriod = 100 * time.Millisecond

type ownedSeriesTestContextBase struct {
	seriesToWrite []series

	user        string
	ownedSeries *ownedSeriesService
	ing         *Ingester
	db          *userTSDB
	kvStore     *watchingKV

	buf *concurrency.SyncBuffer

	cfg       Config
	overrides *validation.Overrides
}

func (c *ownedSeriesTestContextBase) pushUserSeries(t *testing.T) {
	require.NoError(t, pushSeriesToIngester(user.InjectOrgID(context.Background(), c.user), t, c.ing, c.seriesToWrite))
	db := c.ing.getTSDB(c.user)
	require.NotNil(t, db)
	c.db = db
}

func (c *ownedSeriesTestContextBase) checkUpdateReasonForUser(t *testing.T, expectedReason string) {
	require.Equal(t, expectedReason, c.db.requiresOwnedSeriesUpdate.Load())
}

func (c *ownedSeriesTestContextBase) checkTestedIngesterOwnedSeriesState(t *testing.T, series, shards, limit int) {
	os := c.db.ownedSeriesState()
	require.Equal(t, series, os.ownedSeriesCount, "owned series")
	require.Equal(t, shards, os.shardSize, "shard size")
	require.Equal(t, limit, os.localSeriesLimit, "local series limit")
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
		tokens = append(tokens, userToken(c.user, c.ingesterZone, skip)+1)
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
		tokens = append(tokens, userToken(c.user, c.ingesterZone, skip)+1)
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
	const ownedServiceTestUser = "test-user"

	// Generate some series, and compute their hashes.
	seriesToWrite, seriesTokens := generateSeriesWithTokens(ownedServiceTestUser)

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
					user:          ownedServiceTestUser,
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

func generateSeriesWithTokens(testUser string) ([]series, []uint32) {
	var seriesToWrite []series
	var seriesTokens []uint32
	for seriesIdx := 0; seriesIdx < ownedServiceSeriesCount; seriesIdx++ {
		s := series{
			lbls:      labels.FromStrings(labels.MetricName, "test", fmt.Sprintf("lbl_%05d", seriesIdx), "value"),
			value:     float64(0),
			timestamp: time.Now().UnixMilli(),
		}
		seriesToWrite = append(seriesToWrite, s)
		seriesTokens = append(seriesTokens, mimirpb.ShardByAllLabels(testUser, s.lbls))
	}
	return seriesToWrite, seriesTokens
}

type ownedSeriesWithPartitionsRingTestContext struct {
	ownedSeriesTestContextBase

	partitionID    int32 // Partition used by tested ingester.
	partitionsRing *ring.PartitionRingWatcher
}

// Push series to Kafka, and wait for ingester to ingest them.
func (c *ownedSeriesWithPartitionsRingTestContext) pushUserSeries(t *testing.T) {
	// Create a Kafka writer and then write all series.
	writer := ingest.NewWriter(c.cfg.IngestStorageConfig.KafkaConfig, log.NewNopLogger(), nil)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), writer))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), writer))
	})

	for _, s := range c.seriesToWrite {
		req, _, _, _ := mockWriteRequest(t, s.lbls, s.value, s.timestamp)
		require.NoError(t, writer.WriteSync(context.Background(), c.partitionID, c.user, req))
	}

	// Wait until the ingester ingested all series from Kafka.
	test.Poll(t, 1*time.Second, len(c.seriesToWrite), func() interface{} {
		return int(c.ing.getTSDB(c.user).Head().NumSeries())
	})

	// After pushing series, set db in test context.
	db := c.ing.getTSDB(c.user)
	require.NotNil(t, db)
	c.db = db
}

func (c *ownedSeriesWithPartitionsRingTestContext) addPartition(t *testing.T, partitionID int32, partitionActive ring.PartitionState) {
	updatePartitionRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(partitionRing *ring.PartitionRingDesc) {
		partitionRing.AddPartition(partitionID, partitionActive, time.Now())
	})
}

func (c *ownedSeriesWithPartitionsRingTestContext) removePartition(t *testing.T, partitionID int32) {
	updatePartitionRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(partitionRing *ring.PartitionRingDesc) {
		partitionRing.RemovePartition(partitionID)
	})
}

func (c *ownedSeriesWithPartitionsRingTestContext) createIngesterAndPartitionRing(t *testing.T) {
	if c.ing != nil {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c.ing))
	}
	if c.partitionsRing != nil {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c.partitionsRing))
	}

	ing, _, prw := createTestIngesterWithIngestStorage(t, &c.cfg, c.overrides, nil)
	c.ing = ing
	c.partitionsRing = prw

	// Ingester and partitions ring watcher are not started yet.
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c.partitionsRing))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c.partitionsRing))
	})

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c.ing))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c.ing))
	})

	// Make sure that partition is now registered in the ring, and is active. This takes at least 1 sec.
	test.Poll(t, 2*time.Second, true, func() interface{} {
		p := prw.PartitionRing().ActivePartitionIDs()
		return slices.Contains(p, c.partitionID)
	})

	c.buf = &concurrency.SyncBuffer{}

	// Create owned series service tied to the new ingester. We don't start this service, and will only call its methods.
	c.ownedSeries = newOwnedSeriesService(
		10*time.Minute,
		newOwnedSeriesPartitionRingStrategy(c.ing.ingestPartitionID, c.partitionsRing, c.ing.limits.IngestionPartitionsTenantShardSize),
		log.NewLogfmtLogger(c.buf),
		nil,
		c.ing.limiter.maxSeriesPerUser,
		c.ing.getTSDBUsers,
		c.ing.getTSDB,
	)
}

const ownedServiceTestUserPartitionsRing = "test-user-123"

// How many series from ownedServiceSeriesCount end up in given partition when tenant is using both of them.
var seriesSplitForPartitions0And1 = map[int32]int{0: 3, 1: 7}
var seriesSplitForPartitions1And2 = map[int32]int{1: 2, 2: 8}

// This test shows which partitions are part of test user shuffle-shard. These properties are used in TestOwnedSeriesServiceWithPartitionsRing test.
// Note that shuffle shard for given tenant and number of partitions is completely deterministic, because partitions use deterministic tokens.
func TestOwnedSeriesPartitionsTestUserShuffleSharding(t *testing.T) {
	_, tokens := generateSeriesWithTokens(ownedServiceTestUserPartitionsRing)

	type testCase struct {
		partitions                   []int32
		shardSize                    int
		expectedPartitionsInTheShard []int32
		expectedSeriesPerPartition   map[int32]int
	}

	for _, tc := range []testCase{
		{partitions: []int32{1}, shardSize: 1, expectedPartitionsInTheShard: []int32{1}, expectedSeriesPerPartition: map[int32]int{1: 10}},
		{partitions: []int32{1, 2}, shardSize: 1, expectedPartitionsInTheShard: []int32{2}, expectedSeriesPerPartition: map[int32]int{2: 10}},
		{partitions: []int32{1, 2}, shardSize: 2, expectedPartitionsInTheShard: []int32{1, 2}, expectedSeriesPerPartition: seriesSplitForPartitions1And2},
		{partitions: []int32{0, 1}, shardSize: 1, expectedPartitionsInTheShard: []int32{1}, expectedSeriesPerPartition: map[int32]int{1: 10}},
		{partitions: []int32{0, 1}, shardSize: 2, expectedPartitionsInTheShard: []int32{0, 1}, expectedSeriesPerPartition: seriesSplitForPartitions0And1},
	} {
		t.Run(fmt.Sprintf("%d/%d", tc.partitions, tc.shardSize), func(t *testing.T) {
			rd := ring.NewPartitionRingDesc()
			for _, pid := range tc.partitions {
				rd.AddPartition(pid, ring.PartitionActive, time.Time{})
			}

			r := ring.NewPartitionRing(*rd)
			nr, err := r.ShuffleShard(ownedServiceTestUserPartitionsRing, tc.shardSize)
			require.NoError(t, err)
			require.Equal(t, tc.expectedPartitionsInTheShard, nr.PartitionIDs())

			seriesPerPartition := map[int32]int{}

			for _, key := range tokens {
				pid, err := nr.ActivePartitionForKey(key)
				require.NoError(t, err)
				seriesPerPartition[pid]++
			}

			require.Equal(t, tc.expectedSeriesPerPartition, seriesPerPartition)
		})
	}
}

func TestOwnedSeriesServiceWithPartitionsRing(t *testing.T) {
	seriesToWrite, _ := generateSeriesWithTokens(ownedServiceTestUserPartitionsRing)

	testCases := map[string]struct {
		limits              map[string]*validation.Limits
		registerPartitionID int32 // Partition that's added to the ring before test. Tested ingester will own this partition.
		testFunc            func(t *testing.T, c *ownedSeriesWithPartitionsRingTestContext, limits map[string]*validation.Limits)
	}{
		"empty ingester": {
			testFunc: func(t *testing.T, c *ownedSeriesWithPartitionsRingTestContext, limits map[string]*validation.Limits) {
				require.Equal(t, 0, c.ownedSeries.updateAllTenants(context.Background(), false))
			},
		},
		"new user trigger": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUserPartitionsRing: {
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
		"new user trigger from WAL replay": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUserPartitionsRing: {
					MaxGlobalSeriesPerUser:             ownedServiceTestUserSeriesLimit,
					IngestionPartitionsTenantShardSize: 0,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithPartitionsRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				// stop the ingester
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c.ing))

				// restart the ingester with the same TSDB directory to trigger WAL replay
				c.createIngesterAndPartitionRing(t)
				c.db = c.ing.getTSDB(ownedServiceTestUserPartitionsRing)
				require.NotNil(t, c.db)

				// the owned series will be counted during WAL replay and reason set to "new user"
				// shard size and local limit are initialized to correct values
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, recomputeOwnedSeriesReasonNewUser)
			},
		},
		"shard size = 1, scale ingesters up and down, partition shard is unaffected": {
			registerPartitionID: 2, // initially only partition 2 will be in the ring.

			limits: map[string]*validation.Limits{
				ownedServiceTestUserPartitionsRing: {
					MaxGlobalSeriesPerUser:             ownedServiceTestUserSeriesLimit,
					IngestionPartitionsTenantShardSize: 1,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithPartitionsRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				// Partition 2 was added at first, now we add partition 1. However partition 2 will still "own" the test user (see TestOwnedSeriesPartitionsTestUserShuffleSharding).
				c.addPartition(t, 1, ring.PartitionActive)

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				// since no reason set, shard size and local limit are unchanged, and we pass ringChanged=false, no recompute will happen
				c.updateOwnedSeriesAndCheckResult(t, false, 0, "")
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")

				// passing ringChanged=true won't trigger a recompute either, because the user's partition subring hasn't changed
				c.updateOwnedSeriesAndCheckResult(t, true, 0, "")
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")

				// remove partition 1 -- there will be only partition 2 left, used by our tested ingester.
				c.removePartition(t, 1)

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
			registerPartitionID: 1,

			limits: map[string]*validation.Limits{
				ownedServiceTestUserPartitionsRing: {
					MaxGlobalSeriesPerUser:             ownedServiceTestUserSeriesLimit,
					IngestionPartitionsTenantShardSize: 1,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithPartitionsRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				// Add new partition. This will become the only partition in the shuffle shard for the test user. See TestOwnedSeriesPartitionsTestUserShuffleSharding.
				c.addPartition(t, 2, ring.PartitionActive)

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

				// remove partition 2, which changes user's shuffle shard back to partition 1 only.
				c.removePartition(t, 2)

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
		"shard size = 0, scale partitions up and down": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUserPartitionsRing: {
					MaxGlobalSeriesPerUser:             ownedServiceTestUserSeriesLimit,
					IngestionPartitionsTenantShardSize: 0,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithPartitionsRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first partition.
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				// add new partition. Since shard size = 0, tenant will use both partitions.
				c.addPartition(t, 1, ring.PartitionActive)

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				// will recompute because the local limit has changed (takes precedence over ring change)
				c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
				c.checkTestedIngesterOwnedSeriesState(t, seriesSplitForPartitions0And1[0], 0, ownedServiceTestUserSeriesLimit/2)
				c.checkUpdateReasonForUser(t, "")

				// remove the second ingester
				c.removePartition(t, 1)

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, seriesSplitForPartitions0And1[0], 0, ownedServiceTestUserSeriesLimit/2)

				// will recompute because the local limit has changed (takes precedence over ring change)
				c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")
			},
		},
		"unchanged ring, shard size from 0 to partition count": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUserPartitionsRing: {
					MaxGlobalSeriesPerUser:             ownedServiceTestUserSeriesLimit,
					IngestionPartitionsTenantShardSize: 0,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithPartitionsRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// add new partition. Since shard size = 0, tenant will use both partitions.
				c.addPartition(t, 1, ring.PartitionActive)

				// will recompute because the local limit has changed (takes precedence over ring change)
				c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
				c.checkTestedIngesterOwnedSeriesState(t, seriesSplitForPartitions0And1[0], 0, ownedServiceTestUserSeriesLimit/2)
				c.checkUpdateReasonForUser(t, "")

				// now don't change the ring, but change shard size from 0 to 2, which is also our number of partitions.
				// this will not change owned series (because we only have 2 partitions, and both are already used), but will trigger recompute because the shard size has changed.
				limits[ownedServiceTestUserPartitionsRing].IngestionPartitionsTenantShardSize = 2

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, seriesSplitForPartitions0And1[0], 0, ownedServiceTestUserSeriesLimit/2)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, seriesSplitForPartitions0And1[0], 2, ownedServiceTestUserSeriesLimit/2)
				c.checkUpdateReasonForUser(t, "")
			},
		},
		"unchanged ring, shard size < partitions, shard size up and down": {
			registerPartitionID: 2, // This is partition in the shard when using partitions 1, 2 and shard size=1.

			limits: map[string]*validation.Limits{
				ownedServiceTestUserPartitionsRing: {
					MaxGlobalSeriesPerUser:             ownedServiceTestUserSeriesLimit,
					IngestionPartitionsTenantShardSize: 1,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithPartitionsRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// add partition 1, that will later (after changing shard size) own some series.
				c.addPartition(t, 1, ring.PartitionActive)

				// initial state: all series are owned by the first ingester (partition 0)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				// change shard size to 2, splitting the series between two partitions.
				limits[ownedServiceTestUserPartitionsRing].IngestionPartitionsTenantShardSize = 2

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, seriesSplitForPartitions1And2[2], 2, ownedServiceTestUserSeriesLimit/2)
				c.checkUpdateReasonForUser(t, "")

				// change shard size back to 1, moving the series back to the original ingester
				limits[ownedServiceTestUserPartitionsRing].IngestionPartitionsTenantShardSize = 1

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, seriesSplitForPartitions1And2[2], 2, ownedServiceTestUserSeriesLimit/2)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")
			},
		},
		"unchanged ring, shard size < ingesters, shard size up and down, series start on other ingester": {
			registerPartitionID: 1, // This partition is NOT in the shard when using partitions 1, 2 and shard size=1.

			limits: map[string]*validation.Limits{
				ownedServiceTestUserPartitionsRing: {
					MaxGlobalSeriesPerUser:             ownedServiceTestUserSeriesLimit,
					IngestionPartitionsTenantShardSize: 1,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithPartitionsRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// add partition 2, that owns all series at first (when shard size=1).
				c.addPartition(t, 2, ring.PartitionActive)

				// pass ringChanged=true to trigger recompute
				c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonRingChanged)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by partition 2.
				c.checkTestedIngesterOwnedSeriesState(t, 0, 1, ownedServiceTestUserSeriesLimit)

				// change shard size to 2, splitting the series between ingesters
				limits[ownedServiceTestUserPartitionsRing].IngestionPartitionsTenantShardSize = 2

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, 0, 1, ownedServiceTestUserSeriesLimit)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, seriesSplitForPartitions1And2[1], 2, ownedServiceTestUserSeriesLimit/2)
				c.checkUpdateReasonForUser(t, "")

				// change shard size back to 1, moving the series back to the original partition.
				limits[ownedServiceTestUserPartitionsRing].IngestionPartitionsTenantShardSize = 1

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, seriesSplitForPartitions1And2[1], 2, ownedServiceTestUserSeriesLimit/2)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, 0, 1, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")
			},
		},
		"unchanged ring and shards, series limit up and down": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUserPartitionsRing: {
					MaxGlobalSeriesPerUser:             ownedServiceTestUserSeriesLimit,
					IngestionPartitionsTenantShardSize: 0,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithPartitionsRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the partition 0.
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				// increase series limit
				limits[ownedServiceTestUserPartitionsRing].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit * 2

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit*2)
				c.checkUpdateReasonForUser(t, "")

				// decrease series limit
				limits[ownedServiceTestUserPartitionsRing].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit*2)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")
			},
		},
		"unchanged ring, series limit and shard size up and down in tandem": {
			registerPartitionID: 1, // This is partition in shard when using partitions 0, 1 and shardSize=1.

			limits: map[string]*validation.Limits{
				ownedServiceTestUserPartitionsRing: {
					MaxGlobalSeriesPerUser:             ownedServiceTestUserSeriesLimit,
					IngestionPartitionsTenantShardSize: 1,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithPartitionsRingTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// Add partition 0.
				c.addPartition(t, 0, ring.PartitionActive)

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				// double series limit and shard size
				limits[ownedServiceTestUserPartitionsRing].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit * 2
				limits[ownedServiceTestUserPartitionsRing].IngestionPartitionsTenantShardSize = 2

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, seriesSplitForPartitions0And1[1], 2, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")

				// halve series limit and shard size
				limits[ownedServiceTestUserPartitionsRing].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit
				limits[ownedServiceTestUserPartitionsRing].IngestionPartitionsTenantShardSize = 1

				// verify no change in state before owned series run
				c.checkTestedIngesterOwnedSeriesState(t, seriesSplitForPartitions0And1[1], 2, ownedServiceTestUserSeriesLimit)

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")
			},
		},
		"early compaction trigger": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUserPartitionsRing: {
					MaxGlobalSeriesPerUser:             ownedServiceTestUserSeriesLimit,
					IngestionPartitionsTenantShardSize: 0,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithPartitionsRingTestContext, limits map[string]*validation.Limits) {
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
				ownedServiceTestUserPartitionsRing: {
					MaxGlobalSeriesPerUser:             ownedServiceTestUserSeriesLimit,
					IngestionPartitionsTenantShardSize: 0,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesWithPartitionsRingTestContext, limits map[string]*validation.Limits) {
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
			c := ownedSeriesWithPartitionsRingTestContext{
				ownedSeriesTestContextBase: ownedSeriesTestContextBase{
					user:          ownedServiceTestUserPartitionsRing,
					seriesToWrite: seriesToWrite,
				},
				partitionID: tc.registerPartitionID,
			}

			partitionsKVStore, partitionsKVStoreCloser := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, partitionsKVStoreCloser.Close()) })

			c.kvStore = &watchingKV{Client: partitionsKVStore}

			c.cfg = defaultIngesterTestConfig(t)
			c.cfg.IngesterRing.InstanceID = fmt.Sprintf("ingester-%d", tc.registerPartitionID) // Ingester owns partition based on instance ID.
			c.cfg.IngesterPartitionRing.kvMock = c.kvStore                                     // Set ring with our in-memory KV, that we will use for watching.
			c.cfg.BlocksStorageConfig.TSDB.Dir = ""                                            // Don't use default value, otherwise

			var err error
			c.overrides, err = validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(tc.limits))
			require.NoError(t, err)

			// createTestIngesterWithIngestStorage will register partition and ingester into the partition ring.
			c.createIngesterAndPartitionRing(t)

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
