// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
	utiltest "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestMain(m *testing.M) {
	utiltest.VerifyNoLeakTestMain(m)
}

// testPartitionsCount is lower in test to make debugging easier.
const testPartitionsCount = 16

func TestUsageTracker_Tracking(t *testing.T) {
	t.Run("happy-case series tracking", func(t *testing.T) {
		t.Parallel()

		tracker := newReadyTestUsageTracker(t, map[string]*validation.Limits{
			"tenant": {
				MaxActiveSeriesPerUser: testPartitionsCount, // one series per partition.
				MaxGlobalSeriesPerUser: testPartitionsCount * 100,
			},
		})

		resp, err := tracker.TrackSeries(t.Context(), &usagetrackerpb.TrackSeriesRequest{
			UserID:       "tenant",
			Partition:    0,
			SeriesHashes: []uint64{0, 1},
		})
		require.NoError(t, err)
		require.Len(t, resp.RejectedSeriesHashes, 1)
	})

	t.Run("no series hashes", func(t *testing.T) {
		t.Parallel()

		tracker := newReadyTestUsageTracker(t, map[string]*validation.Limits{
			"tenant": {
				MaxActiveSeriesPerUser: testPartitionsCount, // one series per partition.
				MaxGlobalSeriesPerUser: testPartitionsCount * 100,
			},
		})

		resp, err := tracker.TrackSeries(t.Context(), &usagetrackerpb.TrackSeriesRequest{
			UserID:       "tenant",
			Partition:    0,
			SeriesHashes: []uint64{},
		})
		require.NoError(t, err)
		require.Empty(t, resp.RejectedSeriesHashes)
	})

	t.Run("should not use partitions that are not in running state", func(t *testing.T) {
		t.Parallel()

		tracker := newReadyTestUsageTracker(t, map[string]*validation.Limits{
			"tenant": {
				MaxActiveSeriesPerUser: testPartitionsCount, // one series per partition.
				MaxGlobalSeriesPerUser: testPartitionsCount * 100,
			},
		})
		withRLock(&tracker.partitionsMtx, func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), tracker.partitions[0]))
		})

		_, err := tracker.TrackSeries(t.Context(), &usagetrackerpb.TrackSeriesRequest{
			UserID:       "tenant",
			Partition:    0,
			SeriesHashes: []uint64{0, 1},
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "partition handler 0 is not running (state: Terminated)")
	})

	t.Run("applies global series limit when active series limit is not configured", func(t *testing.T) {
		t.Parallel()

		tracker := newReadyTestUsageTracker(t, map[string]*validation.Limits{
			"tenant": {
				MaxActiveSeriesPerUser: 0,                       // unset
				MaxGlobalSeriesPerUser: testPartitionsCount * 2, // two series per partition
			},
		})

		resp, err := tracker.TrackSeries(t.Context(), &usagetrackerpb.TrackSeriesRequest{
			UserID:       "tenant",
			Partition:    0,
			SeriesHashes: []uint64{0, 1, 2, 3},
		})
		require.NoError(t, err)
		require.Len(t, resp.RejectedSeriesHashes, 2)
	})
}

func TestUsageTracker_BatchTracking(t *testing.T) {
	limits := map[string]*validation.Limits{
		"tenant1": {
			MaxActiveSeriesPerUser: testPartitionsCount, // one series per partition.
			MaxGlobalSeriesPerUser: testPartitionsCount * 100,
		},
		"tenant2": {
			MaxActiveSeriesPerUser: testPartitionsCount, // one series per partition.
			MaxGlobalSeriesPerUser: testPartitionsCount * 100,
		},
	}

	t.Run("batch tracking empty", func(t *testing.T) {
		t.Parallel()

		tracker := newReadyTestUsageTracker(t, limits)
		resp, err := tracker.TrackSeriesBatch(t.Context(), &usagetrackerpb.TrackSeriesBatchRequest{
			Partitions: []*usagetrackerpb.TrackSeriesBatchPartition{},
		})

		require.NoError(t, err)
		require.Empty(t, resp.Rejections)
	})

	t.Run("batch tracking happy-case series", func(t *testing.T) {
		t.Parallel()

		tracker := newReadyTestUsageTracker(t, limits)
		resp, err := tracker.TrackSeriesBatch(t.Context(), &usagetrackerpb.TrackSeriesBatchRequest{
			Partitions: []*usagetrackerpb.TrackSeriesBatchPartition{
				{Partition: 0, Users: []*usagetrackerpb.TrackSeriesBatchUser{
					{UserID: "tenant1", SeriesHashes: []uint64{0, 1}},
					{UserID: "tenant2", SeriesHashes: []uint64{2, 3}},
				}},
			},
		})

		require.NoError(t, err)
		require.EqualValues(t, resp.Rejections, []*usagetrackerpb.TrackSeriesBatchRejection{
			{Partition: 0, Users: []*usagetrackerpb.TrackSeriesBatchRejectionUser{
				{UserID: "tenant1", RejectedSeriesHashes: []uint64{1}},
				{UserID: "tenant2", RejectedSeriesHashes: []uint64{3}},
			}},
		})
	})

	t.Run("batch tracking redundant user", func(t *testing.T) {
		t.Parallel()

		tracker := newReadyTestUsageTracker(t, limits)

		resp, err := tracker.TrackSeriesBatch(t.Context(), &usagetrackerpb.TrackSeriesBatchRequest{
			Partitions: []*usagetrackerpb.TrackSeriesBatchPartition{
				{Partition: 0, Users: []*usagetrackerpb.TrackSeriesBatchUser{
					{UserID: "tenant1", SeriesHashes: []uint64{0}},
					{UserID: "tenant1", SeriesHashes: []uint64{1}},
					{UserID: "tenant1", SeriesHashes: []uint64{2}},
					{UserID: "tenant1", SeriesHashes: []uint64{3}},
				}},
			},
		})

		require.NoError(t, err)
		require.EqualValues(t, resp.Rejections, []*usagetrackerpb.TrackSeriesBatchRejection{
			{Partition: 0, Users: []*usagetrackerpb.TrackSeriesBatchRejectionUser{
				{UserID: "tenant1", RejectedSeriesHashes: []uint64{1}},
				{UserID: "tenant1", RejectedSeriesHashes: []uint64{2}},
				{UserID: "tenant1", RejectedSeriesHashes: []uint64{3}},
			}},
		})
	})
}

func TestUsageTracker_PartitionAssignment(t *testing.T) {
	t.Run("happy-case initial assignment of all partitions", func(t *testing.T) {
		t.Parallel()

		ikv, pkv, cluster := prepareKVStoreAndKafkaMocks(t)
		trackers := map[string]*UsageTracker{
			"a0": newTestUsageTracker(t, 0, "zone-a", ikv, pkv, cluster),
			"b0": newTestUsageTracker(t, 0, "zone-b", ikv, pkv, cluster),
		}
		t.Cleanup(func() { stopAllTrackers(t, trackers) })
		waitUntilAllTrackersSeeAllInstancesInTheirZones(t, trackers)

		// First checks, we haven't done any kind of reconciliation yet, so no partitions should be active yet, no partitions have owners.
		requireAllTrackersNotReady(t, trackers)

		{
			partitionRing := getPartitionRing(t, pkv)
			require.Empty(t, partitionRing.ActivePartitionIDs(), "No partitions should be active yet.")
			for partitionID := int32(0); partitionID < testPartitionsCount; partitionID++ {
				require.Empty(t, partitionRing.MultiPartitionOwnerIDs(partitionID, nil), "Partition %d should have no owners yet.")
			}
		}

		// All usage-trackers have started now, run partition handler reconciliations.
		reconcileAllTrackersPartitionCountTimes(t, trackers)

		// Check that every partition has two owners.
		{
			ownedPartitions := map[string]int{}

			partitionRing := getPartitionRing(t, pkv)
			for partitionID := int32(0); partitionID < testPartitionsCount; partitionID++ {
				owners := partitionRing.MultiPartitionOwnerIDs(0, nil)
				require.Len(t, owners, 2, "Partition %d should have 2 owners", partitionID)
				for _, o := range owners {
					ownedPartitions[o]++
				}
			}

			// Check that each owner owns testParitionsCount / (len(trackers) / 2 zones) partitions.
			for owner, partitions := range ownedPartitions {
				require.Equal(t, testPartitionsCount/(len(trackers)/2), partitions, "Usage-Tracker %q should own exactly %d partitions.", owner, partitions)
			}
		}

		// We should become active now.
		// This happens asynchronously in the partition lifecycler.
		require.EventuallyWithT(t, func(t *assert.CollectT) {
			require.Len(t, getPartitionRing(t, pkv).ActivePartitionIDs(), testPartitionsCount)
		}, 5*time.Second, 100*time.Millisecond, "All partitions should be active now.")

		// Usage-Trackers should be ready now.
		requireAllTrackersReady(t, trackers)
	})

	t.Run("partitions are instantiated even if previous instances are not running yet", func(t *testing.T) {
		t.Parallel()

		ikv, pkv, cluster := prepareKVStoreAndKafkaMocks(t)
		trackers := map[string]*UsageTracker{
			"a1": newTestUsageTracker(t, 1, "zone-a", ikv, pkv, cluster),
			"b1": newTestUsageTracker(t, 1, "zone-b", ikv, pkv, cluster),
		}
		waitUntilAllTrackersSeeAllInstancesInTheirZones(t, trackers)
		reconcileAllTrackersPartitionCountTimes(t, trackers)

		// They should not bre ready already.
		requireAllTrackersReady(t, trackers)

		// start zone-a-0.
		trackers["a0"] = newTestUsageTracker(t, 0, "zone-a", ikv, pkv, cluster)
		waitUntilAllTrackersSeeAllInstancesInTheirZones(t, trackers)

		// Reconcile them again.
		for partitionID := int32(0); partitionID < testPartitionsCount; partitionID++ {
			for key, ut := range trackers {
				require.NoError(t, ut.reconcilePartitions(context.Background()), "Tracker %q could not reconcile", key)
			}
		}

		// zone-a-0 and zone-a-1 should be ready now, but zone-b-1 is not ready yet because zone-b-0 didn't join the instance ring yet.
		require.NoError(t, trackers["a0"].CheckReady(context.Background()), "Tracker a0 should be ready now")
		require.NoError(t, trackers["a1"].CheckReady(context.Background()), "Tracker a1 should be ready now")
		require.NoError(t, trackers["b1"].CheckReady(context.Background()), "Tracker b1 should not be ready yet")

		for _, tracker := range trackers {
			withRLock(&tracker.partitionsMtx, func() {
				require.Len(t, tracker.partitions, testPartitionsCount/2, "Tracker %q should have %d partitions", tracker.cfg.InstanceRing.InstanceID, testPartitionsCount/2)
			})
		}
	})

	t.Run("new instance takes partitions, old shut downs them", func(t *testing.T) {
		t.Parallel()

		// lostPartitionsShutdownGracePeriod should be long enough for this test not to be flaky,
		// but also short enough to keep the test fast
		const lostPartitionsShutdownGracePeriod = time.Second
		configure := func(cfg *Config) {
			cfg.LostPartitionsShutdownGracePeriod = lostPartitionsShutdownGracePeriod
			cfg.MaxPartitionsToCreatePerReconcile = testPartitionsCount
		}

		ikv, pkv, cluster := prepareKVStoreAndKafkaMocks(t)
		a0 := newTestUsageTracker(t, 0, "zone-a", ikv, pkv, cluster, configure)
		b0 := newTestUsageTracker(t, 0, "zone-b", ikv, pkv, cluster, configure)
		trackers := map[string]*UsageTracker{
			"a0": a0,
			"b0": b0,
		}
		waitUntilAllTrackersSeeAllInstancesInTheirZones(t, trackers)

		// Create trackers and check that they're ready.
		require.NoError(t, a0.reconcilePartitions(context.Background()))
		require.NoError(t, b0.reconcilePartitions(context.Background()))
		requireAllTrackersReady(t, trackers)

		// Start zone-a-1.
		a1 := newTestUsageTracker(t, 1, "zone-a", ikv, pkv, cluster, configure)
		trackers["a1"] = a1

		waitUntilAllTrackersSeeAllInstancesInTheirZones(t, trackers)

		// Reconcile both, they will create all the needed partitions.
		require.NoError(t, a0.reconcilePartitions(context.Background()))
		require.NoError(t, a1.reconcilePartitions(context.Background()))

		// All trackers should be ready.
		requireAllTrackersReady(t, trackers)

		// zone-a-0 should have 16 partitions, zone-a-1 should have 8, this is deterministic at this point.
		withRLock(&a0.partitionsMtx, func() { require.Len(t, a0.partitions, 16) })
		withRLock(&a1.partitionsMtx, func() { require.Len(t, a1.partitions, 8) })

		partitionRing := getPartitionRing(t, pkv)
		// First half still has 2 owners.
		for partitionID := int32(0); partitionID < testPartitionsCount/2; partitionID++ {
			require.Len(t, partitionRing.MultiPartitionOwnerIDs(partitionID, nil), 2, "Partition %d should have 2 owners.", partitionID)
		}
		// Second half of partitions should have 3 owners now (zone-a-0, zone-a-1, zone-b-1)
		for partitionID := int32(testPartitionsCount / 2); partitionID < testPartitionsCount; partitionID++ {
			require.Len(t, partitionRing.MultiPartitionOwnerIDs(partitionID, nil), 3, "Partition %d should have 2 owners.", partitionID)
		}

		// Wait until ring changes propagate so a0 sees that a1 took partitions.
		time.Sleep(time.Second)

		// Reconcile zone-a-0 once more after ring is updated to make sure that it notes all the lost partitions with their times.
		require.NoError(t, a0.reconcilePartitions(context.Background()))
		// We should've lost 8 partitions, because zone-a-1 took them.
		withRLock(&a0.partitionsMtx, func() { require.Len(t, a0.lostPartitions, 8) })

		// Wait until old partitions are lost.
		time.Sleep(lostPartitionsShutdownGracePeriod)

		// Now zone-a-0 will shutdown partitions, zone-a-1 does a noop reconciliation.
		require.NoError(t, a0.reconcilePartitions(context.Background()))
		require.NoError(t, a1.reconcilePartitions(context.Background()))

		// zone-a-0 should have 8 partitions, zone-a-1 should have 8, this is deterministic at this point.
		withRLock(&a0.partitionsMtx, func() { require.Len(t, a0.partitions, 8) })
		withRLock(&a1.partitionsMtx, func() { require.Len(t, a1.partitions, 8) })

		// Check that owners are updated in the ring.
		partitionRing = getPartitionRing(t, pkv)
		for partitionID := int32(0); partitionID < testPartitionsCount; partitionID++ {
			require.Len(t, partitionRing.MultiPartitionOwnerIDs(partitionID, nil), 2, "Partition %d should have 2 owners.", partitionID)
		}
	})

	t.Run("new instance took partitions, restarted old instance should remove stale ownership", func(t *testing.T) {
		// This test simulates a scenario where an instance is supposed to lose partitions because a higher one started and now owns them
		// but in the meantime it crashed.
		// In this case the new instance should check that it's not owning the partitions it's not handling anymore.
		t.Parallel()

		// lostPartitionsShutdownGracePeriod should be long enough for this test not to be flaky,
		// but also short enough to keep the test fast
		const lostPartitionsShutdownGracePeriod = time.Second
		configure := func(cfg *Config) {
			cfg.LostPartitionsShutdownGracePeriod = lostPartitionsShutdownGracePeriod
			cfg.MaxPartitionsToCreatePerReconcile = testPartitionsCount
		}

		ikv, pkv, cluster := prepareKVStoreAndKafkaMocks(t)
		a0 := newTestUsageTracker(t, 0, "zone-a", ikv, pkv, cluster, configure)
		trackers := map[string]*UsageTracker{
			"a0": a0,
		}
		waitUntilAllTrackersSeeAllInstancesInTheirZones(t, trackers)

		// Create trackers and check that they're ready.
		require.NoError(t, a0.reconcilePartitions(context.Background()))
		requireAllTrackersReady(t, trackers)

		// Start zone-a-1.
		a1 := newTestUsageTracker(t, 1, "zone-a", ikv, pkv, cluster, configure)
		trackers["a1"] = a1

		waitUntilAllTrackersSeeAllInstancesInTheirZones(t, trackers)

		// Reconcile both, they will create all the needed partitions.
		require.NoError(t, a0.reconcilePartitions(context.Background()))
		require.NoError(t, a1.reconcilePartitions(context.Background()))

		// All trackers should be ready.
		requireAllTrackersReady(t, trackers)

		// zone-a-0 should have 16 partitions, zone-a-1 should have 8, this is deterministic at this point.
		withRLock(&a0.partitionsMtx, func() { require.Len(t, a0.partitions, 16) })
		withRLock(&a1.partitionsMtx, func() { require.Len(t, a1.partitions, 8) })

		// At this point last partition is still owned by both zone-a-0 (who's prepared to lose them) and zone-a-1 (who just took them).
		// Ring should say that last partition is owned by zone-a-1.
		require.Equal(t, []string{"usage-tracker-zone-a-0", "usage-tracker-zone-a-1"}, getPartitionRing(t, pkv).MultiPartitionOwnerIDs(testPartitionsCount-1, nil))

		// Let's forget about a0 for a second, let's say it crashed (even though we didn't stop it, but it's easier for sake of tests).
		// a0 starts again.
		newA0 := newTestUsageTracker(t, 0, "zone-a", ikv, pkv, cluster, configure)
		trackers["a0"] = newA0 // Overwrite it so waitUntilAllTrackersSeeAllInstancesInTheirZones works correctly.

		waitUntilAllTrackersSeeAllInstancesInTheirZones(t, trackers)
		require.NoError(t, newA0.reconcilePartitions(context.Background()))

		// New zone-a-0 should have 8 partitions.
		withRLock(&newA0.partitionsMtx, func() { require.Len(t, newA0.partitions, 8) })

		// Ring should say that last partition is owned by zone-a-1.
		require.Equal(t, []string{"usage-tracker-zone-a-1"}, getPartitionRing(t, pkv).MultiPartitionOwnerIDs(testPartitionsCount-1, nil))
	})

	prepareTwoUsageTrackersWithPartitionsReconciled := func(t *testing.T) (map[string]*UsageTracker, kv.Client) {
		// lostPartitionsShutdownGracePeriod should be long enough for this test not to be flaky,
		// but also short enough to keep the test fast
		const lostPartitionsShutdownGracePeriod = time.Second
		configure := func(cfg *Config) {
			cfg.LostPartitionsShutdownGracePeriod = lostPartitionsShutdownGracePeriod
			cfg.MaxPartitionsToCreatePerReconcile = testPartitionsCount
		}

		ikv, pkv, cluster := prepareKVStoreAndKafkaMocks(t)
		a0 := newTestUsageTracker(t, 0, "zone-a", ikv, pkv, cluster, configure)
		a1 := newTestUsageTracker(t, 1, "zone-a", ikv, pkv, cluster, configure)
		trackers := map[string]*UsageTracker{"a0": a0, "a1": a1}
		waitUntilAllTrackersSeeAllInstancesInTheirZones(t, trackers)

		// Create trackers and check that they're ready.
		require.NoError(t, a0.reconcilePartitions(context.Background()))
		require.NoError(t, a1.reconcilePartitions(context.Background()))
		requireAllTrackersReady(t, trackers)

		require.Equal(t, []string{"usage-tracker-zone-a-0"}, getPartitionRing(t, pkv).MultiPartitionOwnerIDs(0, nil))
		require.Equal(t, []string{"usage-tracker-zone-a-1"}, getPartitionRing(t, pkv).MultiPartitionOwnerIDs(testPartitionsCount-1, nil))
		return trackers, pkv
	}

	t.Run("partitions ownership should be kept during restarts", func(t *testing.T) {
		// This test simulates a scenario where an instance is supposed to lose partitions because a higher one started and now owns them
		// but in the meantime it crashed.
		// In this case the new instance should check that it's not owning the partitions it's not handling anymore.
		t.Parallel()

		trackers, pkv := prepareTwoUsageTrackersWithPartitionsReconciled(t)

		// We "restart" a0, which is just a graceful shutdown and start another one.
		// We don't even start it again.
		stopService(t, trackers["a0"])

		// Wait until the partition ring is updated.
		time.Sleep(time.Second)

		// Ownership shouldn't have changed.
		partitionRing := getPartitionRing(t, pkv)
		require.ElementsMatch(t, []string{"usage-tracker-zone-a-0"}, partitionRing.MultiPartitionOwnerIDs(0, nil))
		require.ElementsMatch(t, []string{"usage-tracker-zone-a-1"}, partitionRing.MultiPartitionOwnerIDs(testPartitionsCount-1, nil))
	})

	t.Run("reconcile after scale down should not panic or fail", func(t *testing.T) {
		t.Parallel()

		trackers, _ := prepareTwoUsageTrackersWithPartitionsReconciled(t)

		// We're scaling down, so we need to call the prepare downscale endpoint first.
		callPrepareDownscaleEndpoint(t, trackers["a1"], http.MethodPost)
		require.NoError(t, trackers["a1"].reconcilePartitions(t.Context()))
		stopService(t, trackers["a1"])
	})

	t.Run("scale down should lose partitions", func(t *testing.T) {
		t.Parallel()

		trackers, pkv := prepareTwoUsageTrackersWithPartitionsReconciled(t)

		// We're scaling down, so we need to call the prepare downscale endpoint first.
		callPrepareDownscaleEndpoint(t, trackers["a1"], http.MethodPost)
		stopService(t, trackers["a1"])

		// We don't even reconcile a0, ownership should be lost anyway.

		// a1 shouldn't own the partitions anymore.
		partitionRing := getPartitionRing(t, pkv)
		require.ElementsMatch(t, []string{"usage-tracker-zone-a-0"}, partitionRing.MultiPartitionOwnerIDs(0, nil))
		// Since we didn't reconcile, last partition has no owners anymore.
		lastPartitionOwners := partitionRing.MultiPartitionOwnerIDs(testPartitionsCount-1, nil)
		require.Empty(t, lastPartitionOwners)
	})

	t.Run("aborted scale down should not lose partitions", func(t *testing.T) {
		t.Parallel()

		trackers, pkv := prepareTwoUsageTrackersWithPartitionsReconciled(t)

		// Call the prepare downscale endpoint, but don't stop the service.
		callPrepareDownscaleEndpoint(t, trackers["a1"], http.MethodPost)

		// Call the prepare downscale endpoint with DELETE method, i.e., cancel the downscale.
		callPrepareDownscaleEndpoint(t, trackers["a1"], http.MethodDelete)

		// Now do a graceful shutdown because a1 is restarting.
		stopService(t, trackers["a1"])

		// Ownership shouldn't have changed.
		partitionRing := getPartitionRing(t, pkv)
		require.ElementsMatch(t, []string{"usage-tracker-zone-a-0"}, partitionRing.MultiPartitionOwnerIDs(0, nil))
		require.ElementsMatch(t, []string{"usage-tracker-zone-a-1"}, partitionRing.MultiPartitionOwnerIDs(testPartitionsCount-1, nil))
	})

	t.Run("respects MaxPartitionsToCreatePerReconcile", func(t *testing.T) {
		t.Parallel()

		ikv, pkv, cluster := prepareKVStoreAndKafkaMocks(t)
		const (
			expectedZoneAReconcilesToCreateAllPartitions = 4
			expectedZoneBReconcilesToCreateAllPartitions = 2
		)
		trackers := map[string]*UsageTracker{
			"a0": newTestUsageTracker(t, 0, "zone-a", ikv, pkv, cluster, func(cfg *Config) {
				cfg.MaxPartitionsToCreatePerReconcile = testPartitionsCount / expectedZoneAReconcilesToCreateAllPartitions
			}),
			"b0": newTestUsageTracker(t, 0, "zone-b", ikv, pkv, cluster, func(cfg *Config) {
				cfg.MaxPartitionsToCreatePerReconcile = testPartitionsCount / expectedZoneBReconcilesToCreateAllPartitions
			}),
		}
		waitUntilAllTrackersSeeAllInstancesInTheirZones(t, trackers)

		// Reconcile once:
		for key, ut := range trackers {
			require.NoError(t, ut.reconcilePartitions(context.Background()), "Tracker %q could not reconcile", key)
			withRLock(&ut.partitionsMtx, func() {
				require.Equal(t, ut.cfg.MaxPartitionsToCreatePerReconcile, len(ut.partitions))
			})
		}

		// Should not be ready yet:
		requireAllTrackersNotReady(t, trackers)

		// Reconcile rest expected times.
		for reconcile := 1; reconcile < expectedZoneAReconcilesToCreateAllPartitions; reconcile++ {
			require.NoError(t, trackers["a0"].reconcilePartitions(context.Background()), "Tracker a0 could not reconcile")
		}
		for reconcile := 1; reconcile < expectedZoneBReconcilesToCreateAllPartitions; reconcile++ {
			require.NoError(t, trackers["b0"].reconcilePartitions(context.Background()), "Tracker b0 could not reconcile")
		}

		// Should be ready now.
		for key, ut := range trackers {
			require.NoError(t, ut.CheckReady(context.Background()), "Tracker %q should be ready now", key)
		}
	})

	t.Run("instance should be LEAVING when restarting, JOINING while not ready", func(t *testing.T) {
		t.Parallel()

		ikv, pkv, cluster := prepareKVStoreAndKafkaMocks(t)

		// Instances start in JOINING state.
		a0 := newTestUsageTracker(t, 0, "zone-a", ikv, pkv, cluster, func(cfg *Config) {
			cfg.MaxPartitionsToCreatePerReconcile = testPartitionsCount
		})
		assertInstanceStateInTheRing(t, ikv, a0.cfg.InstanceRing.InstanceID, ring.JOINING)
		waitUntilAllTrackersSeeAllInstancesInTheirZones(t, map[string]*UsageTracker{"a0": a0})

		// When all partitions are reconciled, instance should be ACTIVE.
		require.NoError(t, a0.reconcilePartitions(t.Context()))
		assertInstanceStateInTheRing(t, ikv, a0.cfg.InstanceRing.InstanceID, ring.ACTIVE)

		// By default instance doesn't leave the ring on graceful shutdown, so it should be LEAVING.
		stopService(t, a0)
		assertInstanceStateInTheRing(t, ikv, a0.cfg.InstanceRing.InstanceID, ring.LEAVING)

		// Restarting the instance should put it back to JOINING state.
		a0 = newTestUsageTracker(t, 0, "zone-a", ikv, pkv, cluster)
		assertInstanceStateInTheRing(t, ikv, a0.cfg.InstanceRing.InstanceID, ring.JOINING)
	})

	t.Run("instance should remove itself on shutdown from the ring if downscale was prepared", func(t *testing.T) {
		t.Parallel()

		ikv, pkv, cluster := prepareKVStoreAndKafkaMocks(t)
		a0 := newTestUsageTracker(t, 0, "zone-a", ikv, pkv, cluster)
		assertInstanceStateInTheRing(t, ikv, a0.cfg.InstanceRing.InstanceID, ring.JOINING)
		waitUntilAllTrackersSeeAllInstancesInTheirZones(t, map[string]*UsageTracker{"a0": a0})

		// Prepare downscale.
		callPrepareDownscaleEndpoint(t, a0, http.MethodPost)
		stopService(t, a0)
		assert.Len(t, getInstanceRingDesc(t, ikv).Ingesters, 0, "Instance should be removed from the ring after prepare downscale endpoint is called")
	})

	t.Run("instance should NOT remove itself on shutdown from the ring after an aborted downscaling", func(t *testing.T) {
		t.Parallel()

		ikv, pkv, cluster := prepareKVStoreAndKafkaMocks(t)
		a0 := newTestUsageTracker(t, 0, "zone-a", ikv, pkv, cluster)
		assertInstanceStateInTheRing(t, ikv, a0.cfg.InstanceRing.InstanceID, ring.JOINING)
		waitUntilAllTrackersSeeAllInstancesInTheirZones(t, map[string]*UsageTracker{"a0": a0})

		// Prepare downscale.
		callPrepareDownscaleEndpoint(t, a0, http.MethodPost)
		// Abort downscale.
		callPrepareDownscaleEndpoint(t, a0, http.MethodDelete)
		stopService(t, a0)
		assertInstanceStateInTheRing(t, ikv, a0.cfg.InstanceRing.InstanceID, ring.LEAVING)
	})
}

func TestUsageTracker_GetUsersCloseToLimit(t *testing.T) {
	t.Run("happy case", func(t *testing.T) {
		makeSeries := func(n int) []uint64 {
			series := make([]uint64, n)
			for i := range series {
				series[i] = uint64(i)
			}
			return series
		}

		tracker := newReadyTestUsageTracker(t, map[string]*validation.Limits{
			"a": {MaxActiveSeriesPerUser: 1000 * testPartitionsCount},
			"b": {MaxActiveSeriesPerUser: 2000 * testPartitionsCount},
			"c": {MaxActiveSeriesPerUser: 1000 * testPartitionsCount},
			"d": {MaxActiveSeriesPerUser: 2000 * testPartitionsCount},
			"e": {MaxActiveSeriesPerUser: 1000 * testPartitionsCount},
		})

		for _, tenant := range []string{"a", "b", "c", "d", "e"} {
			resp, err := tracker.TrackSeries(t.Context(), &usagetrackerpb.TrackSeriesRequest{
				UserID:       tenant,
				Partition:    0,
				SeriesHashes: makeSeries(900),
			})
			require.NoError(t, err)
			require.Empty(t, resp.RejectedSeriesHashes)
		}

		// Call updateLimits (on all partitions, although we only need partition 0.
		withRLock(&tracker.partitionsMtx, func() {
			for _, p := range tracker.partitions {
				done := make(chan struct{})
				p.forceUpdateLimitsForTests <- done
				<-done
			}
		})

		resp, err := tracker.GetUsersCloseToLimit(t.Context(), &usagetrackerpb.GetUsersCloseToLimitRequest{Partition: 0})
		require.NoError(t, err)
		require.Equal(t, []string{"a", "c", "e"}, resp.SortedUserIds, "List of users close to the limit should be sorted lexicographically")
	})

	t.Run("partition handler is not running", func(t *testing.T) {
		tracker := newReadyTestUsageTracker(t, map[string]*validation.Limits{
			"a": {MaxActiveSeriesPerUser: 1000 * testPartitionsCount},
		})

		// Call updateLimits (on all partitions, although we only need partition 0.
		withRLock(&tracker.partitionsMtx, func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), tracker.partitions[0]))
		})

		_, err := tracker.GetUsersCloseToLimit(t.Context(), &usagetrackerpb.GetUsersCloseToLimitRequest{Partition: 0})
		require.Error(t, err)
		require.ErrorContains(t, err, "partition handler 0 is not running (state: Terminated)")
	})
}

func callPrepareDownscaleEndpoint(t *testing.T, ut *UsageTracker, method string) {
	t.Helper()
	req, err := http.NewRequest(method, "/usage-tracker/prepare-instance-ring-downscale", nil)
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	ut.PrepareInstanceRingDownscaleHandler(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "Unexpected response code from prepare downscale endpoint: %s", rec.Body.String())
}

func assertInstanceStateInTheRing(t *testing.T, kvStore kv.Client, instance string, expectedState ring.InstanceState) {
	ringDesc := getInstanceRingDesc(t, kvStore)
	instanceDesc, ok := ringDesc.Ingesters[instance]
	require.True(t, ok, "Instance %q should be in the ring", instance)
	require.Equal(t, expectedState.String(), instanceDesc.State.String(), "Instance %q should be in state %s but was in state %s", instance, expectedState.String(), instanceDesc.State.String())

}

func getInstanceRingDesc(t *testing.T, kvStore kv.Client) *ring.Desc {
	val, err := kvStore.Get(context.Background(), InstanceRingKey)
	require.NoError(t, err)
	return ring.GetOrCreateRingDesc(val)
}

func getPartitionRing(t require.TestingT, kvStore kv.Client) *ring.PartitionRing {
	val, err := kvStore.Get(context.Background(), PartitionRingKey)
	require.NoError(t, err)
	desc := ring.GetOrCreatePartitionRingDesc(val)
	partitionRing, err := ring.NewPartitionRing(*desc)
	require.NoError(t, err)
	return partitionRing
}

func requireAllTrackersReady(t *testing.T, trackers map[string]*UsageTracker) {
	t.Helper()
	for key, ut := range trackers {
		require.NoError(t, ut.CheckReady(context.Background()), "Tracker %q should be ready now", key)
	}
}

func requireAllTrackersNotReady(t *testing.T, trackers map[string]*UsageTracker) {
	for key, ut := range trackers {
		require.Error(t, ut.CheckReady(context.Background()), "Tracker %q should not be ready yet", key)
	}
}

func stopAllTrackers(t *testing.T, trackers map[string]*UsageTracker) {
	t.Helper()
	for key, ut := range trackers {
		if err := services.StopAndAwaitTerminated(t.Context(), ut); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Unexpected error stopping tracker %q: %s", key, err)
		}
	}
}

func waitUntilAllTrackersSeeAllInstancesInTheirZones(tb testing.TB, trackers map[string]*UsageTracker) {
	require.EventuallyWithT(tb, func(t *assert.CollectT) {
		trackersPerZone := map[string]int{}
		for _, ut := range trackers {
			trackersPerZone[ut.cfg.InstanceRing.InstanceZone]++
		}
		for _, ut := range trackers {
			require.Equal(t, trackersPerZone[ut.cfg.InstanceRing.InstanceZone], ut.instanceRing.WritableInstancesWithTokensInZoneCount(ut.cfg.InstanceRing.InstanceZone))
		}
	}, time.Second, 50*time.Millisecond)
}

// reconcileAllTrackersPartitionCountTimes calls reoncilePartition on each tracker testPartitionsCount times.
// Since we only allow one partition handler creation per reconcile by default, this should allow for creation of all partitions.
func reconcileAllTrackersPartitionCountTimes(t require.TestingT, trackers map[string]*UsageTracker) {
	for partitionID := int32(0); partitionID < testPartitionsCount; partitionID++ {
		for key, ut := range trackers {
			require.NoError(t, ut.reconcilePartitions(context.Background()), "Tracker %q could not reconcile", key)
		}
	}
}

func prepareKVStoreAndKafkaMocks(tb testing.TB) (*consul.Client, *consul.Client, *kfake.Cluster) {
	consulConfig := consul.Config{
		MaxCasRetries: 100,
		CasRetryDelay: 50 * time.Millisecond,
	}
	ikv, instanceKVCloser := consul.NewInMemoryClientWithConfig(ring.GetCodec(), consulConfig, log.NewNopLogger(), nil)
	tb.Cleanup(func() { assert.NoError(tb, instanceKVCloser.Close()) })
	pkv, partitionKVCloser := consul.NewInMemoryClientWithConfig(ring.GetPartitionRingCodec(), consulConfig, log.NewNopLogger(), nil)
	tb.Cleanup(func() { assert.NoError(tb, partitionKVCloser.Close()) })
	cluster := fakeKafkaCluster(tb)
	return ikv, pkv, cluster
}

func newReadyTestUsageTracker(t *testing.T, limits map[string]*validation.Limits, options ...func(cfg *Config)) *UsageTracker {
	options = append(options, func(cfg *Config) {
		cfg.MaxPartitionsToCreatePerReconcile = testPartitionsCount // create all partitions in one reconcile.
	})
	ikv, pkv, cluster := prepareKVStoreAndKafkaMocks(t)
	tracker := newTestUsageTrackerWithDeps(t, 0, "zone-a", ikv, pkv, cluster, newUsageTrackerDeps{tenantLimits: validation.NewMockTenantLimits(limits)}, options...)
	waitUntilAllTrackersSeeAllInstancesInTheirZones(t, map[string]*UsageTracker{"a0": tracker})
	require.NoError(t, tracker.reconcilePartitions(t.Context()))

	require.EventuallyWithT(t, func(t *assert.CollectT) {
		require.Equal(t, testPartitionsCount, tracker.partitionRing.PartitionRing().ActivePartitionsCount())
	}, 5*time.Second, 100*time.Millisecond, "All partitions should be active now.")

	return tracker
}

func newTestUsageTracker(t *testing.T, index int, zone string, ikv, pkv kv.Client, cluster *kfake.Cluster, options ...func(cfg *Config)) *UsageTracker {
	return newTestUsageTrackerWithDeps(t, index, zone, ikv, pkv, cluster, newUsageTrackerDeps{}, options...)
}

type newUsageTrackerDeps struct {
	registry     *prometheus.Registry
	logger       log.Logger
	tenantLimits validation.TenantLimits
}

func newTestUsageTrackerWithDeps(tb testing.TB, index int, zone string, ikv, pkv kv.Client, cluster *kfake.Cluster, deps newUsageTrackerDeps, options ...func(cfg *Config)) *UsageTracker {
	instanceID := fmt.Sprintf("usage-tracker-%s-%d", zone, index)
	cfg := newTestUsageTrackerConfig(tb, instanceID, zone, ikv, pkv, cluster)
	for _, option := range options {
		option(&cfg)
	}
	logger := deps.logger
	if logger == nil {
		logger = utiltest.NewTestingLogger(tb)
	}
	reg := deps.registry
	if reg == nil {
		reg = prometheus.NewPedanticRegistry()
	}
	overrides := validation.NewOverrides(validation.Limits{}, deps.tenantLimits)

	instanceRing, err := NewInstanceRingClient(cfg.InstanceRing, logger, reg)
	require.NoError(tb, err)
	startServiceAndStopOnCleanup(tb, instanceRing)

	partitionRingWatcher := NewPartitionRingWatcher(pkv, logger, reg)
	partitionRing := ring.NewMultiPartitionInstanceRing(partitionRingWatcher, instanceRing, cfg.InstanceRing.HeartbeatTimeout)
	startServiceAndStopOnCleanup(tb, partitionRingWatcher)

	ut, err := NewUsageTracker(cfg, instanceRing, partitionRing, overrides, logger, reg)
	require.NoError(tb, err)

	startServiceAndStopOnCleanup(tb, ut)
	return ut
}

func startServiceAndStopOnCleanup(tb testing.TB, svc services.Service) {
	tb.Helper()
	require.NoError(tb, services.StartAndAwaitRunning(context.Background(), svc))
	tb.Cleanup(func() { stopService(tb, svc) })
}

func stopService(tb testing.TB, svc services.Service) {
	err := services.StopAndAwaitTerminated(context.Background(), svc)
	if err != nil && !errors.Is(err, context.Canceled) {
		tb.Errorf("Unexpected error stopping service %T: %s", svc, err)
	}
}

func newTestUsageTrackerConfig(tb testing.TB, instanceID, zone string, ikv, pkv kv.Client, cluster *kfake.Cluster) Config {
	var cfg Config
	fs := flag.NewFlagSet("usage-tracker", flag.PanicOnError)
	cfg.RegisterFlags(fs, log.NewNopLogger())
	require.NoError(tb, fs.Parse(nil))
	cfg.Enabled = true

	cfg.Partitions = testPartitionsCount
	cfg.InstanceRing.InstanceID = instanceID
	cfg.InstanceRing.InstanceZone = zone
	cfg.InstanceRing.InstanceAddr = "localhost"
	// KVStore mocks.
	cfg.InstanceRing.KVStore.Mock = ikv
	cfg.PartitionRing.KVStore.Mock = pkv
	// Test lower test-related configs to avoid long waiting times.
	cfg.PartitionRing.waitOwnersDurationOnPending = 100 * time.Millisecond
	cfg.PartitionRing.lifecyclerPollingInterval = 50 * time.Millisecond
	// Fake kafka cluster address.
	cfg.EventsStorageWriter.Topic = eventsTopic
	cfg.EventsStorageReader.Topic = eventsTopic
	cfg.EventsStorageReader.Address = cluster.ListenAddrs()
	cfg.EventsStorageWriter.Address = cluster.ListenAddrs()
	cfg.EventsStorageWriter.AutoCreateTopicDefaultPartitions = testPartitionsCount

	cfg.SnapshotsMetadataWriter.Topic = snapshotsMetadataTopic
	cfg.SnapshotsMetadataReader.Topic = snapshotsMetadataTopic
	cfg.SnapshotsMetadataReader.Address = cluster.ListenAddrs()
	cfg.SnapshotsMetadataWriter.Address = cluster.ListenAddrs()
	cfg.SnapshotsMetadataWriter.AutoCreateTopicDefaultPartitions = testPartitionsCount

	cfg.PartitionReconcileInterval = time.Hour // we do reconciliation manually

	cfg.SnapshotsStorage.Filesystem.Directory = tb.TempDir()

	require.NoError(tb, cfg.ValidateForUsageTracker())
	return cfg
}

func fakeKafkaCluster(tb testing.TB, topicsToSeed ...string) *kfake.Cluster {
	tb.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(testPartitionsCount, topicsToSeed...))
	require.NoError(tb, err)
	tb.Cleanup(cluster.Close)
	return cluster
}

func withRLock(m *sync.RWMutex, fn func()) {
	m.RLock()
	defer m.RUnlock()
	fn()
}

func TestInstancePartitions(t *testing.T) {
	for partitions := int32(2); partitions <= 128; partitions *= 2 {
		t.Run(fmt.Sprintf("partitions=%d", partitions), func(t *testing.T) {
			for instances := int32(1); instances <= partitions; instances++ {
				t.Run(fmt.Sprintf("instances=%d", instances), func(t *testing.T) {
					type rng struct {
						start, end int32
					}
					var ranges []rng
					l := int32(0)
					minSize := int32(math.MaxInt32)
					maxSize := int32(0)
					for i := int32(0); i < instances; i++ {
						start, end := instancePartitions(i, instances, partitions)
						ranges = append(ranges, rng{start, end})
						require.True(t, start <= end, "instance=%d, start=%d end=%d", i, start, end)
						require.True(t, start >= 0, "instance=%d, start=%d end=%d", i, start, end)
						require.True(t, end <= partitions, "instance=%d, start=%d end=%d", i, start, end)
						size := end - start
						minSize = min(minSize, size)
						maxSize = max(maxSize, size)
						l += size
					}
					// Sort the ranges we got by starting partition.
					slices.SortFunc(ranges, func(a, b rng) int { return int(a.start - b.start) })
					// First range should start at 0.
					require.Equal(t, int32(0), ranges[0].start, "ranges=%v", ranges)
					// Last range should end at partitions.
					require.Equal(t, partitions, ranges[len(ranges)-1].end, "ranges=%v", ranges)
					for i := 1; i < len(ranges); i++ {
						// Each range should end where the next one starts.
						require.Equal(t, ranges[i-1].end, ranges[i].start, "ranges=%v", ranges)
					}
					// We should cover the total of partitions.
					require.Equal(t, l, partitions)
				})
			}
		})
	}
}

func TestParseInstanceID(t *testing.T) {
	t.Run("with zones", func(t *testing.T) {
		actual, err := parseInstanceID("usage-tracker-zone-a-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = parseInstanceID("usage-tracker-zone-b-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = parseInstanceID("usage-tracker-zone-a-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = parseInstanceID("usage-tracker-zone-b-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = parseInstanceID("mimir-backend-zone-c-2")
		require.NoError(t, err)
		assert.EqualValues(t, 2, actual)
	})

	t.Run("without zones", func(t *testing.T) {
		actual, err := parseInstanceID("usage-tracker-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = parseInstanceID("usage-tracker-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = parseInstanceID("mimir-backend-2")
		require.NoError(t, err)
		assert.EqualValues(t, 2, actual)
	})

	t.Run("should return error if the instance ID has a non supported format", func(t *testing.T) {
		_, err := parseInstanceID("unknown")
		require.Error(t, err)

		_, err = parseInstanceID("usage-tracker-zone-a-")
		require.Error(t, err)

		_, err = parseInstanceID("usage-tracker-zone-a")
		require.Error(t, err)
	})
}

func TestUsageTracker_CleanupSnapshots(t *testing.T) {
	t.Run("instance 0 performing cleanup", func(t *testing.T) {
		t.Parallel()

		testDir := t.TempDir()

		const idleTimeout = 15 * time.Minute
		logger := utiltest.NewTestingLogger(t)
		reg := prometheus.NewRegistry()

		// Prepare bucket config.
		var bucketConfig bucket.Config
		fs := flag.NewFlagSet("", flag.ExitOnError)
		bucketConfig.RegisterFlags(fs)
		require.Nil(t, fs.Parse(nil))
		bucketConfig.Filesystem.Directory = testDir

		snapshotsBucket, err := bucket.NewClient(context.Background(), bucketConfig, "usage-tracker-snapshots", logger, reg)
		require.NoError(t, err)

		now := time.Now()
		filenamesToDelete := []string{
			snapshotFilename(now.Add(-idleTimeout*(snapshotCleanupIdleTimeoutFactor*10)), "usage-tracker-zone-a-0", 0),
			snapshotFilename(now.Add(-idleTimeout*(snapshotCleanupIdleTimeoutFactor*10)), "usage-tracker-zone-a-0", 1),
			snapshotFilename(now.Add(-idleTimeout*(snapshotCleanupIdleTimeoutFactor+1)), "usage-tracker-zone-a-1", 0),
			snapshotFilename(now.Add(-idleTimeout*(snapshotCleanupIdleTimeoutFactor+1)), "usage-tracker-zone-a-1", 1),
			snapshotFilename(now.Add(-idleTimeout*snapshotCleanupIdleTimeoutFactor-time.Minute), "usage-tracker-zone-a-1", 0),
			snapshotFilename(now.Add(-idleTimeout*snapshotCleanupIdleTimeoutFactor-time.Minute), "usage-tracker-zone-a-1", 1),
		}
		for _, filename := range filenamesToDelete {
			require.NoError(t, snapshotsBucket.Upload(t.Context(), filename, strings.NewReader(`snapshot`), nil), "failed to upload snapshot %s", filename)
		}
		filenamesToKeep := []string{
			snapshotFilename(now.Add(-idleTimeout*snapshotCleanupIdleTimeoutFactor+time.Minute), "usage-tracker-zone-a-1", 0),
			snapshotFilename(now.Add(-idleTimeout*snapshotCleanupIdleTimeoutFactor+time.Minute), "usage-tracker-zone-a-1", 1),
			snapshotFilename(now.Add(-idleTimeout*5), "usage-tracker-zone-a-1", 0),
			snapshotFilename(now.Add(-idleTimeout*5), "usage-tracker-zone-a-1", 1),
			snapshotFilename(now.Add(-idleTimeout), "usage-tracker-zone-b-2", 0),
			snapshotFilename(now.Add(-idleTimeout), "usage-tracker-zone-b-3", 5),
			snapshotFilename(now.Add(-idleTimeout/10), "usage-tracker-zone-a-4", 2),
			snapshotFilename(now.Add(-idleTimeout/10), "usage-tracker-zone-a-5", 3),
		}
		for _, filename := range filenamesToKeep {
			require.NoError(t, snapshotsBucket.Upload(t.Context(), filename, strings.NewReader(`snapshot`), nil), "failed to upload snapshot %s", filename)
		}
		unknownFilenames := []string{
			"hello-world.bin",
			"linkin-park-hybrid-theory.mp3",
		}
		for _, filename := range unknownFilenames {
			require.NoError(t, snapshotsBucket.Upload(t.Context(), filename, strings.NewReader(`snapshot`), nil), "failed to upload snapshot %s", filename)
		}

		ikv, pkv, cluster := prepareKVStoreAndKafkaMocks(t)
		ut := newTestUsageTracker(t, 0, "zone-a", ikv, pkv, cluster, func(cfg *Config) {
			cfg.SnapshotsStorage.Filesystem.Directory = testDir
			cfg.IdleTimeout = idleTimeout
			cfg.SnapshotCleanupInterval = 500 * time.Millisecond
			cfg.SnapshotCleanupIntervalJitter = 0.1
		})
		require.NotNil(t, ut.snapshotCleanupsTotal)
		require.Equal(t, float64(-1), testutil.ToFloat64(ut.snapshotsRemainingInTheBucket))

		// Wait until cleanup is performed.
		require.Eventually(t, func() bool {
			return testutil.ToFloat64(ut.snapshotCleanupsTotal) > 0
		}, 10*time.Second, 100*time.Millisecond, "A cleanup should have been performed")

		// List the remaining snapshots in the bucket.
		var remaining []string
		require.NoError(t, snapshotsBucket.Iter(t.Context(), "", func(name string) error {
			remaining = append(remaining, name)
			return nil
		}))
		require.ElementsMatch(t, remaining, append(filenamesToKeep, unknownFilenames...), "Remaining snapshots should match the expected ones after cleanup")

		require.Equal(t, float64(len(filenamesToDelete)), testutil.ToFloat64(ut.snapshotCleanupsDeletedFiles))
		require.Equal(t, float64(len(unknownFilenames)), testutil.ToFloat64(ut.snapshotCleanupsFailedFiles))
		require.Equal(t, float64(len(filenamesToKeep)+len(unknownFilenames)), testutil.ToFloat64(ut.snapshotsRemainingInTheBucket))
	})

	t.Run("non-0 instance should not perform cleanups", func(t *testing.T) {
		t.Parallel()

		ikv, pkv, cluster := prepareKVStoreAndKafkaMocks(t)
		ut := newTestUsageTracker(t, 1, "zone-a", ikv, pkv, cluster, func(cfg *Config) {
			cfg.SnapshotCleanupInterval = 100 * time.Millisecond
			cfg.SnapshotCleanupIntervalJitter = 0.1
		})
		// Metric should not be exported.
		require.Nil(t, ut.snapshotCleanupsTotal)

		// Upload a file.
		require.NoError(t, ut.snapshotsBucket.Upload(t.Context(), snapshotFilename(time.Now().Add(-10*snapshotCleanupIdleTimeoutFactor*ut.cfg.IdleTimeout), "usage-tracker-zone-a-1", 0), strings.NewReader("snapshot"), nil))

		// Wait for a second, we run cleanups every 100ms.
		time.Sleep(time.Second)

		// File is still there.
		var remaining []string
		require.NoError(t, ut.snapshotsBucket.Iter(t.Context(), "", func(name string) error {
			remaining = append(remaining, name)
			return nil
		}))
		require.Len(t, remaining, 1)
	})
}

// TestMetricsGatheringIsNotConcurrent makes sure that metrics collection from usage-tracker is not made concurrently.
// Sometimes we run all 64 partitions on a single instance, serving thousands of tenants.
// When that happens, the default behaviour of the prometheus.Registry.Gather() is to launch a goroutine for each one of the collectors registered.
// Since each partition is a collector, it launches 64 quite-heavy goroutines, overwhelming the scheduler and increasing the tail latency.
func TestMetricsGatheringIsNotConcurrent(t *testing.T) {
	counter := 0
	trackerStoreCollectTestHook = func() {
		// If metrics gathering is concurrent, race detector should complain here.
		counter++
		require.Equal(t, 1, counter)
		counter--
	}
	t.Cleanup(func() { trackerStoreCollectTestHook = func() {} })

	const partitions = 64
	reg := prometheus.NewRegistry()
	logger := log.NewNopLogger()

	ikv, pkv, cluster := prepareKVStoreAndKafkaMocks(t)
	tracker := newTestUsageTrackerWithDeps(t, 0, "zone-a", ikv, pkv, cluster, newUsageTrackerDeps{registry: reg, logger: logger}, func(cfg *Config) {
		cfg.Partitions = partitions
		cfg.MaxPartitionsToCreatePerReconcile = partitions
		cfg.EventsStorageWriter.AutoCreateTopicDefaultPartitions = partitions
		cfg.SnapshotsMetadataWriter.AutoCreateTopicDefaultPartitions = partitions
	})
	waitUntilAllTrackersSeeAllInstancesInTheirZones(t, map[string]*UsageTracker{"a0": tracker})
	require.NoError(t, tracker.reconcilePartitions(t.Context()))

	require.EventuallyWithT(t, func(t *assert.CollectT) {
		require.Equal(t, partitions, tracker.partitionRing.PartitionRing().ActivePartitionsCount())
	}, 5*time.Second, 100*time.Millisecond, "All partitions should be active now.")

	_, err := reg.Gather()
	require.NoError(t, err)
}

func BenchmarkMetricsGathering(b *testing.B) {
	const partitions = 64

	for _, tenants := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("tenants=%d", tenants), func(b *testing.B) {
			reg := prometheus.NewRegistry()
			logger := log.NewNopLogger()

			ikv, pkv, cluster := prepareKVStoreAndKafkaMocks(b)
			tracker := newTestUsageTrackerWithDeps(b, 0, "zone-a", ikv, pkv, cluster, newUsageTrackerDeps{registry: reg, logger: logger}, func(cfg *Config) {
				cfg.Partitions = partitions
				cfg.MaxPartitionsToCreatePerReconcile = partitions
				cfg.EventsStorageWriter.AutoCreateTopicDefaultPartitions = partitions
				cfg.SnapshotsMetadataWriter.AutoCreateTopicDefaultPartitions = partitions
			})
			waitUntilAllTrackersSeeAllInstancesInTheirZones(b, map[string]*UsageTracker{"a0": tracker})
			require.NoError(b, tracker.reconcilePartitions(b.Context()))

			require.EventuallyWithT(b, func(t *assert.CollectT) {
				require.Equal(t, partitions, tracker.partitionRing.PartitionRing().ActivePartitionsCount())
			}, 5*time.Second, 100*time.Millisecond, "All partitions should be active now.")

			for tenant := 0; tenant < tenants; tenant++ {
				userID := strconv.Itoa(tenant)
				for partition := int32(0); partition < partitions; partition++ {
					resp, err := tracker.TrackSeries(b.Context(), &usagetrackerpb.TrackSeriesRequest{
						UserID:       userID,
						Partition:    partition,
						SeriesHashes: []uint64{0, 1},
					})
					require.NoError(b, err)
					require.Empty(b, resp.RejectedSeriesHashes)
				}
			}

			b.Run("gather", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					metrics, err := reg.Gather()
					require.NoError(b, err)
					require.NotEmpty(b, metrics)
				}
			})
		})
	}
}
