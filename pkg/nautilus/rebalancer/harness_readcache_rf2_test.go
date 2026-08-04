// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// rf2HarnessOpts builds the RF=2 shape used by the tests below: four
// partitions over two logical slots, each mirrored in two zones.
func rf2HarnessOpts() harnessOpts {
	return harnessOpts{cfg: Config{
		PartitionCount: 4,
		ReadcacheSlicer: ReadcacheSlicerConfig{
			Enabled:         true,
			Alpha:           1.0,
			MovementBudget:  0.5,
			DesiredReplicas: 2,
			LogicalIDPrefix: "readcache",
		},
	}}
}

// newRF2Harness returns a harness with both zone mirrors of two
// logical slots in the fleet and the tier-2 log pre-seeded with one
// lease per partition naming a logical slot. Seeding sidesteps the
// cold-start round, whose zero-load slicer pass deliberately collapses
// onto one instance and would obscure what these tests assert.
func newRF2Harness(t *testing.T) (*harness, map[string]*fakeReadcache) {
	t.Helper()
	h := newHarness(t, rf2HarnessOpts())
	pods := map[string]*fakeReadcache{}
	for _, id := range []string{"readcache-zone-a-0", "readcache-zone-b-0", "readcache-zone-a-1", "readcache-zone-b-1"} {
		pods[id] = h.addReadcache(id)
	}
	seedBalancedTierAssignments(t, h, []string{"readcache-0", "readcache-1"})
	return h, pods
}

// TestReadcacheRF2_PushFansOutToEveryZoneMirror is the core of the
// write-side wiring: the log holds one lease per partition naming a
// logical slot, and SetHashRanges reaches every concrete mirror of
// that slot so either can serve the partition.
func TestReadcacheRF2_PushFansOutToEveryZoneMirror(t *testing.T) {
	h, pods := newRF2Harness(t)

	require.NoError(t, h.runRound())

	assert.Equal(t, map[string]int{"readcache-0": 2, "readcache-1": 2}, h.ownersByInstance(),
		"leases must name logical slots, not concrete zone pods")

	assert.True(t, readcacheassignment.ReplicaMap{
		"readcache-0": {
			{InstanceID: "readcache-zone-a-0", Zone: "zone-a"},
			{InstanceID: "readcache-zone-b-0", Zone: "zone-b"},
		},
		"readcache-1": {
			{InstanceID: "readcache-zone-a-1", Zone: "zone-a"},
			{InstanceID: "readcache-zone-b-1", Zone: "zone-b"},
		},
	}.Equal(h.r.readcacheStore.getReplicaMap()))

	slot0 := pods["readcache-zone-a-0"].ownedPartitions()
	slot1 := pods["readcache-zone-a-1"].ownedPartitions()
	require.Len(t, slot0, 2)
	require.Len(t, slot1, 2)
	assert.Equal(t, slot0, pods["readcache-zone-b-0"].ownedPartitions(),
		"both mirrors of a logical slot must receive the same hash ranges")
	assert.Equal(t, slot1, pods["readcache-zone-b-1"].ownedPartitions())
	assert.NotEqual(t, slot0, slot1, "the two slots must hold different partitions")
}

// TestReadcacheRF2_ReconstructResolvesLogicalOwnersToMirrors guards the
// cold-start path: the reconstructor dials the pods named by the
// tier-2 log, and a logical slot ID is not a dialable instance. Without
// the expansion every ownership would be dropped as "not in the ring"
// and a rebalancer restart would reset the hash assignment to an even
// split, discarding the fleet's rebalanced state.
func TestReadcacheRF2_ReconstructResolvesLogicalOwnersToMirrors(t *testing.T) {
	h, _ := newRF2Harness(t)

	// One round to push the current tiling out to the mirrors, so they
	// have something to report back.
	require.NoError(t, h.runRound())

	activePartitions := []int32{0, 1, 2, 3}
	got := h.r.reconstructAssignmentFromReadcache(h.ctx, activePartitions)
	require.NotNil(t, got, "the readcache fleet's own view must be recoverable")
	require.NoError(t, got.Validate())

	covered := map[int32]struct{}{}
	for _, e := range got.Entries {
		covered[e.PartitionID] = struct{}{}
	}
	assert.Len(t, covered, len(activePartitions), "every partition must be reconstructed exactly once per range")

	// Without the expansion the logical owners resolve to nothing.
	h.r.readcacheStore.setReplicaMap(nil)
	assert.Nil(t, h.r.reconstructAssignmentFromReadcache(h.ctx, activePartitions))
}

// TestReadcacheRF2_PlacementSurvivesZoneMirrorLoss is the point of
// sticky logical placement: losing one zone pod is not a membership
// change, so no partition moves and the surviving mirror keeps
// serving. Under RF=1 the same event evacuates the pod's partitions.
func TestReadcacheRF2_PlacementSurvivesZoneMirrorLoss(t *testing.T) {
	h, pods := newRF2Harness(t)

	require.NoError(t, h.runRound())
	before := h.ownersByInstance()
	slot0Partitions := pods["readcache-zone-a-0"].ownedPartitions()
	require.NotEmpty(t, slot0Partitions)

	// zone-b of slot 0 goes away for good. Several rounds so the
	// membership tracker's hysteresis would have had time to drop a
	// concrete member under RF=1.
	h.removeReadcache("readcache-zone-b-0")
	for i := 0; i < 3; i++ {
		h.advance(30 * time.Second)
		require.NoError(t, h.runRound())
	}

	assert.Equal(t, before, h.ownersByInstance(),
		"losing one zone mirror must not move any partition")
	assert.Equal(t, []readcacheassignment.Replica{
		{InstanceID: "readcache-zone-a-0", Zone: "zone-a"},
	}, h.r.readcacheStore.getReplicaMap()["readcache-0"],
		"the replica map must drop the departed mirror so clients stop dialing it")
	assert.Equal(t, slot0Partitions, pods["readcache-zone-a-0"].ownedPartitions(),
		"the surviving mirror keeps the slot's ranges")
}

// TestReadcacheRF2_SingleZoneStatsFailureKeepsSlotEligible checks that
// the concrete->logical translation of the failed-stats exclusion set
// is wired into the round: one mirror failing its HashRangeStats RPC
// leaves the slot a valid placement target because the other mirror
// still reports.
func TestReadcacheRF2_SingleZoneStatsFailureKeepsSlotEligible(t *testing.T) {
	h, pods := newRF2Harness(t)

	require.NoError(t, h.runRound())
	before := h.ownersByInstance()
	require.Contains(t, before, "readcache-0")

	pods["readcache-zone-b-0"].hashRangeStatsErr = errors.New("simulated stats RPC failure")
	h.advance(30 * time.Second)
	require.NoError(t, h.runRound())

	assert.Equal(t, before, h.ownersByInstance(),
		"a single-zone stats failure must not take the logical slot out of placement")
}

// TestReadcacheRF2_ResetSpreadsOverLogicalSlots covers the admin
// escape hatch: a manual reset must write logical slot IDs, not the
// concrete pods it can see in the ring, or the readcaches would stop
// matching their own leases.
func TestReadcacheRF2_ResetSpreadsOverLogicalSlots(t *testing.T) {
	h, _ := newRF2Harness(t)

	res, err := h.r.ResetReadcacheAssignment(h.clock.Now())
	require.NoError(t, err)
	assert.Equal(t, 2, res.NumInstances)
	assert.Equal(t, map[string]int{"readcache-0": 2, "readcache-1": 2}, res.PerInstance)
	assert.Equal(t, map[string]int{"readcache-0": 2, "readcache-1": 2}, h.ownersByInstance())
	assert.NotEmpty(t, h.r.readcacheStore.getReplicaMap(),
		"the reset must publish the expansion for the logical IDs it just wrote")
}

// TestReadcacheRF1_PushAndPlacementUnchanged is the regression guard
// for the default configuration: with DesiredReplicas unset the leases
// name concrete pods, the replica map stays empty, and each pod gets
// only its own ranges.
func TestReadcacheRF1_PushAndPlacementUnchanged(t *testing.T) {
	h := newHarness(t, harnessOpts{cfg: Config{
		PartitionCount: 4,
		ReadcacheSlicer: ReadcacheSlicerConfig{
			Enabled:        true,
			Alpha:          1.0,
			MovementBudget: 0.5,
		},
	}})
	rc0 := h.addReadcache("readcache-0")
	rc1 := h.addReadcache("readcache-1")
	seedBalancedTierAssignments(t, h, []string{"readcache-0", "readcache-1"})

	require.NoError(t, h.runRound())
	assert.Equal(t, map[string]int{"readcache-0": 2, "readcache-1": 2}, h.ownersByInstance())
	assert.Empty(t, h.r.readcacheStore.getReplicaMap(), "RF=1 must publish no replica map")

	require.NotEmpty(t, rc0.ownedPartitions())
	require.NotEmpty(t, rc1.ownedPartitions())
	for _, pid := range rc0.ownedPartitions() {
		assert.NotContains(t, rc1.ownedPartitions(), pid,
			"under RF=1 a partition's ranges go to exactly one pod")
	}
}

// TestReadcacheRF2_MapChangeReachesSubscribersWithoutLeaseChange pins
// the reason the store rebroadcasts on a map change: a zone pod
// joining changes no lease, so a subscriber that only reacts to lease
// deltas would keep dialing a stale replica set.
func TestReadcacheRF2_MapChangeReachesSubscribersWithoutLeaseChange(t *testing.T) {
	h := newHarness(t, rf2HarnessOpts())
	h.addReadcache("readcache-zone-a-0")
	h.addReadcache("readcache-zone-a-1")
	seedBalancedTierAssignments(t, h, []string{"readcache-0", "readcache-1"})

	require.NoError(t, h.runRound())
	require.Len(t, h.r.readcacheStore.getReplicaMap()["readcache-0"], 1)
	ownersBefore := h.ownersByInstance()

	_, updates, unsubscribe := h.r.readcacheStore.subscribe(true)
	defer unsubscribe()
	select {
	case <-updates: // drain the priming snapshot
	default:
	}

	// zone-b scales up. Ownership is untouched: partition leases still
	// name the same logical slots.
	h.addReadcache("readcache-zone-b-0")
	h.addReadcache("readcache-zone-b-1")
	h.advance(30 * time.Second)
	require.NoError(t, h.runRound())
	require.Equal(t, ownersBefore, h.ownersByInstance(), "scale-up of a mirror must not move partitions")

	select {
	case u := <-updates:
		assert.Equal(t, []readcacheassignment.Replica{
			{InstanceID: "readcache-zone-a-0", Zone: "zone-a"},
			{InstanceID: "readcache-zone-b-0", Zone: "zone-b"},
		}, u.replicaMap["readcache-0"])
	default:
		t.Fatal("a new zone mirror must be broadcast to subscribers even without a lease change")
	}
}
