// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"flag"
	"fmt"
	"math"
	"slices"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"

	utiltest "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestMain(m *testing.M) {
	utiltest.VerifyNoLeakTestMain(m)
}

// testPartitionsCount is lower in test to make debugging easier.
const testPartitionsCount = 16

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

		// Take partitionRing from one of the usage-trackers for convenience.
		partitionRing := trackers["a0"].partitionRing
		require.Empty(t, partitionRing.PartitionRing().ActivePartitionIDs(), "No partitions should be active yet.")
		for partitionID := int32(0); partitionID < testPartitionsCount; partitionID++ {
			require.Empty(t, partitionRing.PartitionRing().MultiPartitionOwnerIDs(partitionID, nil), "Partition %d should have no owners yet.")
		}

		// All usage-trackers have started now, run partition reconciliations.
		reconcileAllTrackersPartitionCountTimes(t, trackers)

		// Check that every partition has two owners.
		ownedPartitions := map[string]int{}
		for partitionID := int32(0); partitionID < testPartitionsCount; partitionID++ {
			owners := partitionRing.PartitionRing().MultiPartitionOwnerIDs(0, nil)
			require.Len(t, owners, 2, "Partition %d should have 2 owners", partitionID)
			for _, o := range owners {
				ownedPartitions[o]++
			}
		}

		// Check that each owner owns testParitionsCount / (len(trackers) / 2 zones) partitions.
		for owner, partitions := range ownedPartitions {
			require.Equal(t, testPartitionsCount/(len(trackers)/2), partitions, "Usage-Tracker %q should own exactly %d partitions.", owner, partitions)
		}

		// We should have all partitions active by this point.
		require.EventuallyWithT(t, func(t *assert.CollectT) {
			require.Len(t, partitionRing.PartitionRing().ActivePartitionIDs(), testPartitionsCount)
		}, 5*time.Second, 100*time.Millisecond)

		// Usage-Trackers should be ready now.
		requireAllTrackersReady(t, trackers)
	})

	t.Run("partitions are not instantiated if previous instances are not running", func(t *testing.T) {
		t.Parallel()

		ikv, pkv, cluster := prepareKVStoreAndKafkaMocks(t)
		trackers := map[string]*UsageTracker{
			"a1": newTestUsageTracker(t, 1, "zone-a", ikv, pkv, cluster),
			"b1": newTestUsageTracker(t, 1, "zone-b", ikv, pkv, cluster),
		}
		waitUntilAllTrackersSeeAllInstancesInTheirZones(t, trackers)

		// Usage-trackers have started now, we run reconciliations but nothing should happen yet
		// because previous instances (-0) are not running yet.
		reconcileAllTrackersPartitionCountTimes(t, trackers)

		// They should not be ready yet.
		requireAllTrackersNotReady(t, trackers)

		// start zone-a-0.
		trackers["a0"] = newTestUsageTracker(t, 0, "zone-a", ikv, pkv, cluster)

		// Reconcile them again.
		for partitionID := int32(0); partitionID < testPartitionsCount; partitionID++ {
			for key, ut := range trackers {
				require.NoError(t, ut.reconcilePartitions(context.Background()), "Tracker %q could not reconcile", key)
			}
		}

		// zone-a-0 and zone-a-1 should be ready now, but zone-b-1 is not ready yet because zone-b-0 didn't join the instance ring yet.
		require.NoError(t, trackers["a0"].CheckReady(context.Background()), "Tracker a0 should be ready now")
		require.NoError(t, trackers["a1"].CheckReady(context.Background()), "Tracker a1 should be ready now")
		require.Error(t, trackers["b1"].CheckReady(context.Background()), "Tracker b1 should not be ready yet")
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

		// Check eventually because partitionRing update might be delayed.
		partitionRing := a0.partitionRing
		require.EventuallyWithT(t, func(t *assert.CollectT) {
			// First half still has 2 owners.
			for partitionID := int32(0); partitionID < testPartitionsCount/2; partitionID++ {
				require.Len(t, partitionRing.PartitionRing().MultiPartitionOwnerIDs(partitionID, nil), 2, "Partition %d should have 2 owners.", partitionID)
			}
			// Second half of partitions should have 3 owners now (zone-a-0, zone-a-1, zone-b-1)
			for partitionID := int32(testPartitionsCount / 2); partitionID < testPartitionsCount; partitionID++ {
				require.Len(t, partitionRing.PartitionRing().MultiPartitionOwnerIDs(partitionID, nil), 3, "Partition %d should have 2 owners.", partitionID)
			}
		}, time.Second, 100*time.Millisecond)

		// Reconcile zone-a-0 once more after ring is updated to make sure that it notes all the lost partitions with their times.
		require.NoError(t, a0.reconcilePartitions(context.Background()))

		// Wait until old partitions are lost.
		time.Sleep(lostPartitionsShutdownGracePeriod)

		// Now zone-a-0 will shutdown partitions, zone-a-1 does a noop reconciliation.
		require.NoError(t, a0.reconcilePartitions(context.Background()))
		require.NoError(t, a1.reconcilePartitions(context.Background()))

		// zone-a-0 should have 8 partitions, zone-a-1 should have 8, this is deterministic at this point.
		withRLock(&a0.partitionsMtx, func() { require.Len(t, a0.partitions, 8) })
		withRLock(&a1.partitionsMtx, func() { require.Len(t, a1.partitions, 8) })

		// Check eventually that owners are updated in the ring.
		require.EventuallyWithT(t, func(t *assert.CollectT) {
			for partitionID := int32(0); partitionID < testPartitionsCount; partitionID++ {
				require.Len(t, partitionRing.PartitionRing().MultiPartitionOwnerIDs(partitionID, nil), 2, "Partition %d should have 2 owners.", partitionID)
			}
		}, 5*lostPartitionsShutdownGracePeriod, 100*time.Millisecond)
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

func waitUntilAllTrackersSeeAllInstancesInTheirZones(t *testing.T, trackers map[string]*UsageTracker) {
	require.EventuallyWithT(t, func(t *assert.CollectT) {
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
// Since we only allow one partition creation per reconcile by default, this should allow for creation of all partitions.
func reconcileAllTrackersPartitionCountTimes(t require.TestingT, trackers map[string]*UsageTracker) {
	for partitionID := int32(0); partitionID < testPartitionsCount; partitionID++ {
		for key, ut := range trackers {
			require.NoError(t, ut.reconcilePartitions(context.Background()), "Tracker %q could not reconcile", key)
		}
	}
}

func prepareKVStoreAndKafkaMocks(t *testing.T) (*consul.Client, *consul.Client, *kfake.Cluster) {
	ikv, instanceKVCloser := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, instanceKVCloser.Close()) })
	pkv, partitionKVCloser := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, partitionKVCloser.Close()) })
	cluster := fakeKafkaCluster(t)
	return ikv, pkv, cluster
}

func newTestUsageTracker(t *testing.T, index int, zone string, ikv, pkv kv.Client, cluster *kfake.Cluster, options ...func(cfg *Config)) *UsageTracker {
	instanceID := fmt.Sprintf("usage-tracker-%s-%d", zone, index)
	cfg := newTestUsageTrackerConfig(t, instanceID, zone, ikv, pkv, cluster)
	for _, option := range options {
		option(&cfg)
	}
	reg := prometheus.NewPedanticRegistry()
	logger := log.NewNopLogger()
	instanceRing, err := NewInstanceRingClient(cfg.InstanceRing, logger, reg)
	require.NoError(t, err)
	startServiceAndStopOnCleanup(t, instanceRing)

	partitionRingWatcher := NewPartitionRingWatcher(pkv, logger, reg)
	partitionRing := ring.NewMultiPartitionInstanceRing(partitionRingWatcher, instanceRing, cfg.InstanceRing.HeartbeatTimeout)
	startServiceAndStopOnCleanup(t, partitionRingWatcher)

	overrides := validation.NewOverrides(validation.Limits{}, nil)

	ut, err := NewUsageTracker(cfg, instanceRing, partitionRing, overrides, logger, reg)
	require.NoError(t, err)

	startServiceAndStopOnCleanup(t, ut)
	return ut
}

func startServiceAndStopOnCleanup(t *testing.T, svc services.Service) {
	t.Helper()
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), svc))
	t.Cleanup(func() {
		err := services.StopAndAwaitTerminated(context.Background(), svc)
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Unexpected error stopping service %T: %s", svc, err)
		}
	})
}

func newTestUsageTrackerConfig(t *testing.T, instanceID, zone string, ikv, pkv kv.Client, cluster *kfake.Cluster) Config {
	var cfg Config
	fs := flag.NewFlagSet("usage-tracker", flag.PanicOnError)
	cfg.RegisterFlags(fs, log.NewNopLogger())
	require.NoError(t, fs.Parse(nil))
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
	cfg.EventsStorageReader.Address = cluster.ListenAddrs()[0]
	cfg.EventsStorageWriter.Address = cluster.ListenAddrs()[0]
	cfg.EventsStorageWriter.AutoCreateTopicDefaultPartitions = testPartitionsCount

	cfg.SnapshotsMetadataWriter.Topic = snapshotsMetadataTopic
	cfg.SnapshotsMetadataReader.Topic = snapshotsMetadataTopic
	cfg.SnapshotsMetadataReader.Address = cluster.ListenAddrs()[0]
	cfg.SnapshotsMetadataWriter.Address = cluster.ListenAddrs()[0]
	cfg.SnapshotsMetadataWriter.AutoCreateTopicDefaultPartitions = testPartitionsCount

	cfg.PartitionReconcileInterval = time.Hour // we do reconciliation manually

	cfg.SnapshotsStorage.Filesystem.Directory = t.TempDir()

	require.NoError(t, cfg.Validate())
	return cfg
}

func fakeKafkaCluster(t *testing.T, topicsToSeed ...string) *kfake.Cluster {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topicsToSeed...))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
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
						start, end, err := instancePartitions(i, instances, partitions)
						require.NoError(t, err)
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
