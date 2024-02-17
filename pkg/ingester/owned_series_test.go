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
	"github.com/grafana/mimir/pkg/util/validation"
)

const ownedServiceTestUser = "test-user"
const ownedServiceSeriesCount = 10
const ownedServiceTestUserSeriesLimit = 10000

const defaultHeartbeatPeriod = 100 * time.Millisecond

type ownedSeriesTestContext struct {
	seriesToWrite []series
	seriesTokens  []uint32
	ingesterZone  string

	ownedSeries *ownedSeriesService
	ing         *Ingester
	db          *userTSDB
	kvStore     *watchingKV

	buf *concurrency.SyncBuffer

	// when true, the test ingester will be second in the shuffle shard, and the "second" ingester will be first
	swapShardOrder bool

	ringHeartbeatPeriod time.Duration
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

func (c *ownedSeriesTestContext) checkTestedIngesterOwnedSeriesState(t *testing.T, series, shards, limit int) {
	os := c.db.ownedSeriesState()
	require.Equal(t, series, os.count)
	require.Equal(t, shards, os.shardSize)
	require.Equal(t, limit, os.localLimit)
}

func (c *ownedSeriesTestContext) checkCalculatedLocalLimit(t *testing.T, expectedLimit int, msg string) {
	_, minLimit := c.db.getSeriesAndMinForSeriesLimit()
	localLimit := c.ing.limiter.maxSeriesPerUser(ownedServiceTestUser, minLimit)
	require.Equal(t, expectedLimit, localLimit, msg)
}

func (c *ownedSeriesTestContext) updateOwnedSeriesAndCheckResult(t *testing.T, ringChanged bool, expectedUpdatedTenants int, expectedReason string) {
	c.buf.Reset()
	require.Equal(t, expectedUpdatedTenants, c.ownedSeries.updateAllTenants(context.Background(), ringChanged), c.buf.String())
	require.Contains(t, c.buf.String(), expectedReason)
}

func (c *ownedSeriesTestContext) registerTestedIngesterIntoRing(t *testing.T, instanceID, instanceAddr, instanceZone string) {
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
func (c *ownedSeriesTestContext) registerSecondIngesterOwningHalfOfTheTokens(t *testing.T) {
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

	testCases := map[string]struct {
		limits              map[string]*validation.Limits
		swapShardOrder      bool
		ringHeartbeatPeriod time.Duration
		skipMsg             string
		testFunc            func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits)
	}{
		"empty ingester": {
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
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
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
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
		"shard size = 1, scale ingesters up and down": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
					IngestionTenantShardSize: 1,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				// add an ingester
				c.registerSecondIngesterOwningHalfOfTheTokens(t)

				// wait for the ingester to see the updated ring
				test.Poll(t, 2*c.ringHeartbeatPeriod, 2, func() interface{} {
					return c.ing.lifecycler.InstancesCount()
				})

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

				// wait for the ingester to see the updated ring
				test.Poll(t, 2*c.ringHeartbeatPeriod, 1, func() interface{} {
					return c.ing.lifecycler.InstancesCount()
				})

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
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				// add an ingester (it will become the first ingester in the shuffle shard)
				c.registerSecondIngesterOwningHalfOfTheTokens(t)

				// wait for the ingester to see the updated ring
				test.Poll(t, 2*c.ringHeartbeatPeriod, 2, func() interface{} {
					return c.ing.lifecycler.InstancesCount()
				})

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

				// wait for the ingester to see the updated ring
				test.Poll(t, 2*c.ringHeartbeatPeriod, 1, func() interface{} {
					return c.ing.lifecycler.InstancesCount()
				})

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
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first ingester.
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				// add an ingester
				c.registerSecondIngesterOwningHalfOfTheTokens(t)

				// wait for the ingester to see the updated ring
				test.Poll(t, 2*c.ringHeartbeatPeriod, 2, func() interface{} {
					return c.ing.lifecycler.InstancesCount()
				})

				// will recompute because the local limit has changed (takes precedence over ring change)
				c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 0, ownedServiceTestUserSeriesLimit/2)
				c.checkUpdateReasonForUser(t, "")

				// remove the second ingester
				c.removeSecondIngester(t)

				// wait for the ingester to see the updated ring
				test.Poll(t, 2*c.ringHeartbeatPeriod, 1, func() interface{} {
					return c.ing.lifecycler.InstancesCount()
				})

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
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// add an ingester
				c.registerSecondIngesterOwningHalfOfTheTokens(t)

				// wait for the ingester to see the updated ring
				test.Poll(t, 2*c.ringHeartbeatPeriod, 2, func() interface{} {
					return c.ing.lifecycler.InstancesCount()
				})

				// will recompute because the local limit has changed (takes precedence over ring change)
				c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 0, ownedServiceTestUserSeriesLimit/2)
				c.checkUpdateReasonForUser(t, "")

				// now don't change the ring, but change shard size from 0 to 2, which is also our number of ingesters
				// this will not change owned series (because we only have 2 ingesters, and both are already used), but will trigger recompute because the shard size has changed
				limits[ownedServiceTestUser].IngestionTenantShardSize = 2

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
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// add an ingester
				c.registerSecondIngesterOwningHalfOfTheTokens(t)

				// wait for the ingester to see the updated ring
				test.Poll(t, 2*c.ringHeartbeatPeriod, 2, func() interface{} {
					return c.ing.lifecycler.InstancesCount()
				})

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				// change shard size to 2, splitting the series between ingesters
				limits[ownedServiceTestUser].IngestionTenantShardSize = 2

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit/2)
				c.checkUpdateReasonForUser(t, "")

				// change shard size back to 1, moving the series back to the original ingester
				limits[ownedServiceTestUser].IngestionTenantShardSize = 1

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
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// add an ingester
				c.registerSecondIngesterOwningHalfOfTheTokens(t)

				// wait for the ingester to see the updated ring
				test.Poll(t, 2*c.ringHeartbeatPeriod, 2, func() interface{} {
					return c.ing.lifecycler.InstancesCount()
				})

				// pass ringChanged=true to trigger recompute
				c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonRingChanged)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the second ingester
				c.checkTestedIngesterOwnedSeriesState(t, 0, 1, ownedServiceTestUserSeriesLimit)

				// change shard size to 2, splitting the series between ingesters
				limits[ownedServiceTestUser].IngestionTenantShardSize = 2

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit/2)
				c.checkUpdateReasonForUser(t, "")

				// change shard size back to 1, moving the series back to the original ingester
				limits[ownedServiceTestUser].IngestionTenantShardSize = 1

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
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				// increase series limit
				limits[ownedServiceTestUser].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit * 2

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonLocalLimitChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit*2)
				c.checkUpdateReasonForUser(t, "")

				// decrease series limit
				limits[ownedServiceTestUser].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit

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
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// add an ingester
				c.registerSecondIngesterOwningHalfOfTheTokens(t)

				// wait for the ingester to see the updated ring
				test.Poll(t, 2*c.ringHeartbeatPeriod, 2, func() interface{} {
					return c.ing.lifecycler.InstancesCount()
				})

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 1, ownedServiceTestUserSeriesLimit)

				// double series limit and shard size
				limits[ownedServiceTestUser].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit * 2
				limits[ownedServiceTestUser].IngestionTenantShardSize = 2

				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonShardSizeChanged)
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 2, ownedServiceTestUserSeriesLimit)
				c.checkUpdateReasonForUser(t, "")

				// halve series limit and shard size
				limits[ownedServiceTestUser].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit
				limits[ownedServiceTestUser].IngestionTenantShardSize = 1

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
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first ingester
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				// run early compaction removing all series from the head
				c.ing.compactBlocks(context.Background(), true, time.Now().Add(1*time.Minute).UnixMilli(), nil)
				require.Equal(t, uint64(0), c.db.Head().NumSeries())

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
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
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
		"shard size = 0, scale ingesters up, don't wait for lifecycler heartbeat": {
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
					IngestionTenantShardSize: 0,
				},
			},
			ringHeartbeatPeriod: 5 * time.Second,
			skipMsg:             "This test illustrates a known issue with running the owned series recompute before the lifecycler has a chance to heartbeat.",
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {

				// The owned series service talks to the ring directly, but the ingester limiter gets info about the ring from the lifecycler. The lifecycler
				// only updates when it heartbeats the ring, which is off-cycle with the owned series service cycle. This means that if the number of ingesters
				// or shards changes and the owned series service loop runs BEFORE the lifecycler has had a chance to heartbeat and update its view of the ring,
				// the owned series count will be correctly recomputed, but we will continue to cache the local limit from BEFORE the ring/shard change.
				// This will be corrected on the subsequent run of the owned series service (it will see that the local limit has changed and update the cached value)
				// but for the time in between we will be running with the old local limit.

				c.pushUserSeries(t)
				c.updateOwnedSeriesAndCheckResult(t, false, 1, recomputeOwnedSeriesReasonNewUser)
				c.checkUpdateReasonForUser(t, "")

				// initial state: all series are owned by the first ingester.
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount, 0, ownedServiceTestUserSeriesLimit)

				// add an ingester
				c.registerSecondIngesterOwningHalfOfTheTokens(t)

				// run an owned series update before the lifecycler has a chance to heartbeat
				c.updateOwnedSeriesAndCheckResult(t, true, 1, recomputeOwnedSeriesReasonLocalLimitChanged)                // this will fail, because the reason will be RingChanged
				c.checkTestedIngesterOwnedSeriesState(t, ownedServiceSeriesCount/2, 0, ownedServiceTestUserSeriesLimit/2) // this will fail, because the limiter still returns the old local limit
				c.checkUpdateReasonForUser(t, "")
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if tc.skipMsg != "" {
				t.Skip(tc.skipMsg)
			}

			heartbeatPeriod := defaultHeartbeatPeriod
			if tc.ringHeartbeatPeriod != 0 {
				heartbeatPeriod = tc.ringHeartbeatPeriod
			}

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
			cfg.IngesterRing.HeartbeatPeriod = heartbeatPeriod

			// Start the ring watching. We need watcher to be running when we're doing ring updates, otherwise our update-and-watch function will fail.
			rng, err := ring.New(cfg.IngesterRing.ToRingConfig(), "ingester", IngesterRingKey, log.NewNopLogger(), nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), rng))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), rng))
			})

			c := ownedSeriesTestContext{
				seriesToWrite:       seriesToWrite,
				seriesTokens:        seriesTokens,
				kvStore:             wkv,
				swapShardOrder:      tc.swapShardOrder,
				ringHeartbeatPeriod: heartbeatPeriod,
			}

			c.registerTestedIngesterIntoRing(t, cfg.IngesterRing.InstanceID, cfg.IngesterRing.InstanceAddr, cfg.IngesterRing.InstanceZone)

			overrides, err := validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(tc.limits))
			require.NoError(t, err)

			c.ing = setupIngesterWithOverrides(t, cfg, overrides, nil) // no need to pass the ring -- we'll test a completely separate OSS

			// wait for the ingester lifecycler to sync the initial ring
			test.Poll(t, 2*c.ringHeartbeatPeriod, 1, func() interface{} {
				return c.ing.lifecycler.InstancesCount()
			})

			c.buf = &concurrency.SyncBuffer{}

			c.ownedSeries = newOwnedSeriesService(
				10*time.Minute,
				cfg.IngesterRing.InstanceID,
				rng,
				log.NewLogfmtLogger(c.buf),
				nil,
				c.ing.limits.IngestionTenantShardSize,
				c.ing.limiter.maxSeriesPerUser,
				c.ing.getTSDBUsers,
				c.ing.getTSDB,
			)

			tc.testFunc(t, &c, tc.limits)
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

	ownedSeries := newOwnedSeriesService(10*time.Minute, instanceID1, rng, log.NewLogfmtLogger(&buf), nil, nil, nil, nil, nil)

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

func TestOwnedSeriesLimiting(t *testing.T) {
	testCases := map[string]struct {
		numZones                 int
		startingIngestersPerZone int
		limits                   map[string]*validation.Limits
		ringHeartbeatPeriod      time.Duration
		skipMsg                  string
		testFunc                 func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits)
	}{
		"single zone, shard size = 1, scale ingesters up and down": {
			numZones:                 1,
			startingIngestersPerZone: 1,
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					IngestionTenantShardSize: 1,
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				// initial limit
				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "")

				// add other ingesters
				updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
					desc.AddIngester("ingester-1-1", "localhost", "zone-1", []uint32{101}, ring.ACTIVE, time.Now())
				})

				// wait for the ingester to see the updated ring
				test.Poll(t, 2*c.ringHeartbeatPeriod, 2, func() interface{} {
					return c.ing.lifecycler.InstancesCount()
				})

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")

				// run owned series update (ringChanged=true, but should be a no-op since token ranges didn't change)
				require.Equal(t, 0, c.ownedSeries.updateAllTenants(context.Background(), true))

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")

				// remove an ingester
				updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
					desc.RemoveIngester("ingester-1-1")
				})

				// wait for the ingester to see the updated ring
				test.Poll(t, 2*c.ringHeartbeatPeriod, 1, func() interface{} {
					return c.ing.lifecycler.InstancesCount()
				})

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")

				// run owned series update (ringChanged=true, but should be a no-op since token ranges didn't change)
				require.Equal(t, 0, c.ownedSeries.updateAllTenants(context.Background(), true))

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")
			},
		},
		"single zone, shard size = 0, scale ingesters up and down": {
			numZones:                 1,
			startingIngestersPerZone: 1,
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					IngestionTenantShardSize: 0,
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				// initial limit
				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "")

				// add other ingesters
				updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
					desc.AddIngester("ingester-1-1", "localhost", "zone-1", []uint32{101}, ring.ACTIVE, time.Now())
				})

				// wait for the ingester to see the updated ring
				test.Poll(t, 2*c.ringHeartbeatPeriod, 2, func() interface{} {
					return c.ing.lifecycler.InstancesCount()
				})

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit shouldn't be reduced before owned series update")

				// run owned series update (ringChanged=true, expect recalculation)
				require.Equal(t, 1, c.ownedSeries.updateAllTenants(context.Background(), true))

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit/2, "limit should be updated")

				// remove an ingester
				updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
					desc.RemoveIngester("ingester-1-1")
				})

				// wait for the ingester to see the updated ring
				test.Poll(t, 2*c.ringHeartbeatPeriod, 1, func() interface{} {
					return c.ing.lifecycler.InstancesCount()
				})

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "higher limit should take effect immediately")

				// run owned series update (ringChanged=true, expect recalculation)
				require.Equal(t, 1, c.ownedSeries.updateAllTenants(context.Background(), true))

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")
			},
		},
		"single zone, unchanged ring, shard size from 0 to ingester count": {
			numZones:                 1,
			startingIngestersPerZone: 2,
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					IngestionTenantShardSize: 0,
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				// initial limit
				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit/2, "")

				// change tenant shard size to equal the number of ingesters
				limits[ownedServiceTestUser].IngestionTenantShardSize = 2

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit/2, "limit should be unchanged")

				// run owned series update (ringChanged=false, expect recalculation due to shard size change)
				require.Equal(t, 1, c.ownedSeries.updateAllTenants(context.Background(), false))

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit/2, "limit should be unchanged")
			},
		},
		"single zone, unchanged ring, shard size < ingesters, shard size up and down": {
			numZones:                 1,
			startingIngestersPerZone: 2,
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					IngestionTenantShardSize: 1,
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				// initial limit
				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "")

				// increase shard size
				limits[ownedServiceTestUser].IngestionTenantShardSize = 2

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit shouldn't be reduced before owned series update")

				// run owned series update (ringChanged=false, expect recalculation due to shard size change)
				require.Equal(t, 1, c.ownedSeries.updateAllTenants(context.Background(), false))

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit/2, "limit should be updated")

				// decrease shard size
				limits[ownedServiceTestUser].IngestionTenantShardSize = 1

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "higher limit should take effect immediately")

				// run owned series update (ringChanged=false, expect recalculation due to shard size change)
				require.Equal(t, 1, c.ownedSeries.updateAllTenants(context.Background(), false))

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")
			},
		},
		"single zone, unchanged ring and shards, series limit up and down": {
			numZones:                 1,
			startingIngestersPerZone: 1,
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					IngestionTenantShardSize: 0,
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				// initial limit
				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "")

				// increase series limit
				limits[ownedServiceTestUser].MaxGlobalSeriesPerUser = 2 * ownedServiceTestUserSeriesLimit

				c.checkCalculatedLocalLimit(t, 2*ownedServiceTestUserSeriesLimit, "higher limit should take effect immediately")

				// run owned series update (ringChanged=false, expect recalculation due to limit change)
				require.Equal(t, 1, c.ownedSeries.updateAllTenants(context.Background(), false))

				c.checkCalculatedLocalLimit(t, 2*ownedServiceTestUserSeriesLimit, "limit should be unchanged")

				// decrease series limit
				limits[ownedServiceTestUser].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit

				c.checkCalculatedLocalLimit(t, 2*ownedServiceTestUserSeriesLimit, "limit shouldn't be reduced before owned series update")

				// run owned series update (ringChanged=false, expect recalculation due to limit change)
				require.Equal(t, 1, c.ownedSeries.updateAllTenants(context.Background(), false))

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be updated")
			},
		},
		"single zone, unchanged ring, series limit and shard size up and down in tandem": {
			numZones:                 1,
			startingIngestersPerZone: 2,
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					IngestionTenantShardSize: 1,
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				// initial limit
				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "")

				// double series limit and shard size
				limits[ownedServiceTestUser].MaxGlobalSeriesPerUser = 2 * ownedServiceTestUserSeriesLimit
				limits[ownedServiceTestUser].IngestionTenantShardSize = 2

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")

				// run owned series update (ringChanged=false, expect recalculation due to shard size change)
				require.Equal(t, 1, c.ownedSeries.updateAllTenants(context.Background(), false))

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")

				// halve series limit and shard size
				limits[ownedServiceTestUser].MaxGlobalSeriesPerUser = ownedServiceTestUserSeriesLimit
				limits[ownedServiceTestUser].IngestionTenantShardSize = 1

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")

				// run owned series update (ringChanged=false, expect recalculation due to shard size change)
				require.Equal(t, 1, c.ownedSeries.updateAllTenants(context.Background(), false))

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")
			},
		},
		"multi zone, shard size = num zones, scale ingesters up and down": {
			numZones:                 3,
			startingIngestersPerZone: 1,
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					IngestionTenantShardSize: 3, // one ingester per zone
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				// initial limit
				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "")

				// we'd normally scale up all zones at once, but doing it one by one lets us
				// put the test ingester in a variety of scenarios (e.g.: what if it's in the only
				// zone that's scaled up? the only zone scaled down? etc.)

				// scale up zone by zone
				ingesterCount := 3
				for i := 1; i <= 3; i++ {
					updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
						desc.AddIngester(fmt.Sprintf("ingester-%d-1", i), "localhost", fmt.Sprintf("zone-%d", i), []uint32{uint32(100*i + 1)}, ring.ACTIVE, time.Now())
						desc.AddIngester(fmt.Sprintf("ingester-%d-2", i), "localhost", fmt.Sprintf("zone-%d", i), []uint32{uint32(100*i + 2)}, ring.ACTIVE, time.Now())
						desc.AddIngester(fmt.Sprintf("ingester-%d-3", i), "localhost", fmt.Sprintf("zone-%d", i), []uint32{uint32(100*i + 3)}, ring.ACTIVE, time.Now())
						desc.AddIngester(fmt.Sprintf("ingester-%d-4", i), "localhost", fmt.Sprintf("zone-%d", i), []uint32{uint32(100*i + 4)}, ring.ACTIVE, time.Now())
					})
					ingesterCount += 4

					// wait for the ingester to see the updated ring
					test.Poll(t, 2*c.ringHeartbeatPeriod, ingesterCount, func() interface{} {
						return c.ing.lifecycler.InstancesCount()
					})

					c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")

					// run owned series update (ringChanged=true, but should be a no-op since token ranges didn't change)
					require.Equal(t, 0, c.ownedSeries.updateAllTenants(context.Background(), true))

					c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")
				}

				// scale down zone by zone
				for i := 1; i <= 3; i++ {
					updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
						desc.RemoveIngester(fmt.Sprintf("ingester-%d-4", i))
						desc.RemoveIngester(fmt.Sprintf("ingester-%d-3", i))
						desc.RemoveIngester(fmt.Sprintf("ingester-%d-2", i))
						desc.RemoveIngester(fmt.Sprintf("ingester-%d-1", i))
					})
					ingesterCount -= 4

					// wait for the ingester to see the updated ring
					test.Poll(t, 2*c.ringHeartbeatPeriod, ingesterCount, func() interface{} {
						return c.ing.lifecycler.InstancesCount()
					})

					c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")

					// run owned series update (ringChanged=true, but should be a no-op since token ranges didn't change)
					require.Equal(t, 0, c.ownedSeries.updateAllTenants(context.Background(), true))

					c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")
				}
			},
		},
		"multi zone, shard size > ingesters, scale ingesters up and down": {
			numZones:                 3,
			startingIngestersPerZone: 1,
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					IngestionTenantShardSize: 15, // 5 ingesters per zone (don't use 0 so we exercise shuffle shard logic in limiter)
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
				},
			},
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {
				// initial limit
				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "")

				// we'd normally scale up all zones at once, but doing it one by one lets us
				// put the test ingester in a variety of scenarios (e.g.: what if it's in the only
				// zone that's scaled up? the only zone scaled down? etc.)

				// scale up zone 1
				ingesterCount := 3
				updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
					desc.AddIngester("ingester-1-1", "localhost", "zone-1", []uint32{101}, ring.ACTIVE, time.Now())
					desc.AddIngester("ingester-1-2", "localhost", "zone-1", []uint32{102}, ring.ACTIVE, time.Now())
					desc.AddIngester("ingester-1-3", "localhost", "zone-1", []uint32{103}, ring.ACTIVE, time.Now())
					desc.AddIngester("ingester-1-4", "localhost", "zone-1", []uint32{104}, ring.ACTIVE, time.Now())
				})
				ingesterCount += 4

				// wait for the ingester to see the updated ring
				test.Poll(t, 2*c.ringHeartbeatPeriod, ingesterCount, func() interface{} {
					return c.ing.lifecycler.InstancesCount()
				})

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit shouldn't be reduced before owned series update")

				// run owned series update (ringChanged=true, recalculation expected)
				require.Equal(t, 1, c.ownedSeries.updateAllTenants(context.Background(), true))

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit/5, "limit should be updated")

				// scale up other zones
				for i := 2; i <= 3; i++ {
					updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
						desc.AddIngester(fmt.Sprintf("ingester-%d-1", i), "localhost", fmt.Sprintf("zone-%d", i), []uint32{uint32(100*i + 1)}, ring.ACTIVE, time.Now())
						desc.AddIngester(fmt.Sprintf("ingester-%d-2", i), "localhost", fmt.Sprintf("zone-%d", i), []uint32{uint32(100*i + 2)}, ring.ACTIVE, time.Now())
						desc.AddIngester(fmt.Sprintf("ingester-%d-3", i), "localhost", fmt.Sprintf("zone-%d", i), []uint32{uint32(100*i + 3)}, ring.ACTIVE, time.Now())
						desc.AddIngester(fmt.Sprintf("ingester-%d-4", i), "localhost", fmt.Sprintf("zone-%d", i), []uint32{uint32(100*i + 4)}, ring.ACTIVE, time.Now())
					})
					ingesterCount += 4

					// wait for the ingester to see the updated ring
					test.Poll(t, 2*c.ringHeartbeatPeriod, ingesterCount, func() interface{} {
						return c.ing.lifecycler.InstancesCount()
					})

					c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit/5, "limit should be unchanged")

					// run owned series update (ringChanged=true, but should be a no-op since token ranges didn't change)
					require.Equal(t, 0, c.ownedSeries.updateAllTenants(context.Background(), true))

					c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit/5, "limit should be unchanged")
				}

				// scale down zone 1
				updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
					desc.RemoveIngester("ingester-1-4")
					desc.RemoveIngester("ingester-1-3")
					desc.RemoveIngester("ingester-1-2")
					desc.RemoveIngester("ingester-1-1")
				})
				ingesterCount -= 4

				// wait for the ingester to see the updated ring
				test.Poll(t, 2*c.ringHeartbeatPeriod, ingesterCount, func() interface{} {
					return c.ing.lifecycler.InstancesCount()
				})

				// higher limit should take effect immediately
				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "higher limit should take effect immediately")

				// run owned series update (ringChanged=true, recalculation expected)
				require.Equal(t, 1, c.ownedSeries.updateAllTenants(context.Background(), true))

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")

				// scale down other zones
				for i := 2; i <= 3; i++ {
					updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
						desc.RemoveIngester(fmt.Sprintf("ingester-%d-4", i))
						desc.RemoveIngester(fmt.Sprintf("ingester-%d-3", i))
						desc.RemoveIngester(fmt.Sprintf("ingester-%d-2", i))
						desc.RemoveIngester(fmt.Sprintf("ingester-%d-1", i))
					})
					ingesterCount -= 4

					// wait for the ingester to see the updated ring
					test.Poll(t, 2*c.ringHeartbeatPeriod, ingesterCount, func() interface{} {
						return c.ing.lifecycler.InstancesCount()
					})

					c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")

					// run owned series update (ringChanged=true, but should be a no-op since token ranges didn't change)
					require.Equal(t, 0, c.ownedSeries.updateAllTenants(context.Background(), true))

					c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "limit should be unchanged")
				}
			},
		},
		"single zone, shard size = 0, scale ingesters up, don't wait for lifecycler heartbeat": {
			numZones:                 1,
			startingIngestersPerZone: 1,
			limits: map[string]*validation.Limits{
				ownedServiceTestUser: {
					IngestionTenantShardSize: 0,
					MaxGlobalSeriesPerUser:   ownedServiceTestUserSeriesLimit,
				},
			},
			ringHeartbeatPeriod: 5 * time.Second,
			skipMsg:             "This test illustrates a known issue with running the owned series recompute before the lifecycler has a chance to heartbeat.",
			testFunc: func(t *testing.T, c *ownedSeriesTestContext, limits map[string]*validation.Limits) {

				// The owned series service talks to the ring directly, but the ingester limiter gets info about the ring from the lifecycler. The lifecycler
				// only updates when it heartbeats the ring, which is off-cycle with the owned series service cycle. This means that if the number of ingesters
				// or shards changes and the owned series service loop runs BEFORE the lifecycler has had a chance to heartbeat and update its view of the ring,
				// the owned series count will be correctly recomputed, but we will continue to cache the local limit from BEFORE the ring/shard change.
				// This will be corrected on the subsequent run of the owned series service (it will see that the local limit has changed and update the cached value)
				// but for the time in between we will be running with the old local limit.

				// initial limit
				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit, "")

				// add other ingesters
				updateRingAndWaitForWatcherToReadUpdate(t, c.kvStore, func(desc *ring.Desc) {
					desc.AddIngester("ingester-1-1", "localhost", "zone-1", []uint32{101}, ring.ACTIVE, time.Now())
				})

				// run an owned series update before the lifecycler has a chance to heartbeat (ringChanged=true, expect recalculation)
				require.Equal(t, 1, c.ownedSeries.updateAllTenants(context.Background(), true))

				c.checkCalculatedLocalLimit(t, ownedServiceTestUserSeriesLimit/2, "limit should be updated") // this will fail, because the limiter still returns the old local limit to be cached
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if tc.skipMsg != "" {
				t.Skip(tc.skipMsg)
			}

			heartbeatPeriod := defaultHeartbeatPeriod
			if tc.ringHeartbeatPeriod != 0 {
				heartbeatPeriod = tc.ringHeartbeatPeriod
			}

			cfg := defaultIngesterTestConfig(t)
			cfg.IngesterRing.InstanceID = "ingester-1-0"
			cfg.IngesterRing.ZoneAwarenessEnabled = true
			cfg.IngesterRing.InstanceZone = "zone-1"
			cfg.IngesterRing.ReplicationFactor = tc.numZones // RF needs to equal number of zones
			cfg.IngesterRing.HeartbeatPeriod = heartbeatPeriod
			cfg.UpdateIngesterOwnedSeries = true
			cfg.UseIngesterOwnedSeriesForLimits = true

			wkv := &watchingKV{Client: cfg.IngesterRing.KVStore.Mock}
			cfg.IngesterRing.KVStore.Mock = wkv

			// start the ring
			rng, err := ring.New(cfg.IngesterRing.ToRingConfig(), "ingester", IngesterRingKey, log.NewNopLogger(), nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), rng))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), rng))
			})

			c := ownedSeriesTestContext{
				kvStore:             wkv,
				ringHeartbeatPeriod: heartbeatPeriod,
			}

			// add initial ingesters to ring
			updateRingAndWaitForWatcherToReadUpdate(t, wkv, func(desc *ring.Desc) {
				for i := 1; i <= tc.numZones; i++ {
					for j := 0; j < tc.startingIngestersPerZone; j++ {
						desc.AddIngester(fmt.Sprintf("ingester-%d-%d", i, j), "localhost", fmt.Sprintf("zone-%d", i), []uint32{uint32(100*i + j)}, ring.ACTIVE, time.Now())
					}
				}
			})

			// start the ingester under test
			overrides, err := validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(tc.limits))
			require.NoError(t, err)

			c.ing = setupIngesterWithOverrides(t, cfg, overrides, rng)

			// wait for the ingester lifecycler to sync the initial ring
			test.Poll(t, 2*c.ringHeartbeatPeriod, tc.numZones*tc.startingIngestersPerZone, func() interface{} {
				return c.ing.lifecycler.InstancesCount()
			})

			// write series to create TSDB
			c.seriesToWrite = []series{
				{
					lbls:      labels.FromStrings(labels.MetricName, "test", "label", "value"),
					value:     float64(0),
					timestamp: time.Now().UnixMilli(),
				},
			}
			c.pushUserSeries(t)

			// populate test context
			c.ownedSeries = c.ing.ownedSeriesService
			require.NotNil(t, c.ownedSeries)
			require.NotNil(t, c.ownedSeries.ingestersRing)

			// run an initial owned series update to clear any pending updates due to e.g. new user
			c.ownedSeries.updateAllTenants(context.Background(), true)
			c.checkUpdateReasonForUser(t, "")

			// run test
			tc.testFunc(t, &c, tc.limits)
		})
	}
}

func userToken(user, zone string, skip int) uint32 {
	r := rand.New(rand.NewSource(util.ShuffleShardSeed(user, zone)))

	for ; skip > 0; skip-- {
		_ = r.Uint32()
	}
	return r.Uint32()
}

func setupIngesterWithOverrides(t *testing.T, cfg Config, overrides *validation.Overrides, ingesterRing ring.ReadRing) *Ingester {
	ing, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, overrides, ingesterRing, "", "", nil)
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
