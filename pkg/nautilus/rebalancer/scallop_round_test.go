// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"flag"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

func TestScallopRoundPlansOncePublishesBothLogsThenPushes(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			Planner:        plannerScallop,
			PartitionCount: 2,
		},
	})
	rc0 := h.addReadcache("rc-0")
	rc1 := h.addReadcache("rc-1")
	now := h.clock.Now()
	current := assignment.EvenSplitForTenant("tenant-a", []int32{0, 1})
	require.True(t, h.r.store.apply(now, current, h.cfg.LeaseDuration, h.r.hashLeaseLookahead(), h.cfg.EntryRetention))
	require.True(t, h.r.readcacheStore.apply(now, &readcacheassignment.Assignment{Entries: []readcacheassignment.AssignmentEntry{
		{PartitionID: 0, InstanceID: "rc-0"},
		{PartitionID: 1, InstanceID: "rc-1"},
	}}, h.cfg.LeaseDuration, h.r.readcacheLeaseLookahead(), h.cfg.EntryRetention, 0))
	h.r.pushRanges(h.ctx, current, now)
	for _, entry := range current.Entries {
		if entry.PartitionID == 0 {
			rc0.setTenantLoad(entry.TenantID, entry.PartitionID, entry.Range, 100, 100)
		} else {
			rc1.setTenantLoad(entry.TenantID, entry.PartitionID, entry.Range, 1, 1)
		}
	}

	var calls atomic.Int32
	var plannedSnapshot scallop.Snapshot
	h.r.scallopPlan = func(snapshot scallop.Snapshot, _ scallop.Policy) (scallop.PlanResult, error) {
		calls.Add(1)
		plannedSnapshot = snapshot
		assert.Equal(t, map[int32]string{0: "rc-0", 1: "rc-1"}, snapshot.PartitionOwners)
		return scallop.PlanResult{
			Assignment:      snapshot.Assignment,
			PartitionOwners: map[int32]string{0: "rc-1", 1: "rc-0"},
			Actions: []scallop.Action{{
				Kind: scallop.ActionMovePartition, PartitionID: 0, FromReplica: "rc-0", ToReplica: "rc-1",
			}},
		}, nil
	}

	var pushesBeforeBothLogs atomic.Bool
	checkLogs := func() {
		owners := latestDesiredOwners(h.r.readcacheStore.snapshot(), now)
		hash := h.r.store.latestActiveAssignment(now)
		if owners[0] != "rc-1" || owners[1] != "rc-0" || hash == nil || len(hash.Entries) != len(current.Entries) {
			pushesBeforeBothLogs.Store(true)
		}
	}
	rc0.onSetHashRanges = checkLogs
	rc1.onSetHashRanges = checkLogs

	require.NoError(t, h.runRound())
	assert.Equal(t, int32(1), calls.Load(), "one round must invoke the joint planner exactly once")
	assert.False(t, pushesBeforeBothLogs.Load(), "both assignment logs must be published before any range push")
	assert.Equal(t, map[int32]string{0: "rc-1", 1: "rc-0"}, latestDesiredOwners(h.r.readcacheStore.snapshot(), now))
	assert.True(t, h.r.lastTier2RoundAt.IsZero(), "Scallop partition moves must not run legacy tier 2")
	assert.Zero(t, h.r.predictions.len(), "Scallop must branch before legacy prediction bookkeeping")

	traces := h.r.admin.traceSnapshot()
	require.NotEmpty(t, traces)
	last := traces[len(traces)-1]
	assert.Equal(t, plannerScallop, last.Planner)
	require.NotNil(t, last.Scallop)
	assert.Empty(t, last.Round.Actions)
	require.Len(t, last.Round.ScallopActions, 1)
	envelope, err := last.scallopReplayEnvelope()
	require.NoError(t, err)
	assert.Equal(t, plannedSnapshot, envelope.Snapshot)
	assert.Equal(t, scallop.DefaultPolicy(), envelope.Policy)
}

func TestScallopRoundIncompleteSnapshotPreservesAssignmentsAndSkipsPlan(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{Planner: plannerScallop, PartitionCount: 1},
	})
	rc := h.addReadcache("rc-0")
	now := h.clock.Now()
	current := assignment.EvenSplitForTenant("tenant-a", []int32{0})
	require.True(t, h.r.store.apply(now, current, h.cfg.LeaseDuration, h.r.hashLeaseLookahead(), h.cfg.EntryRetention))
	require.True(t, h.r.readcacheStore.apply(now, &readcacheassignment.Assignment{Entries: []readcacheassignment.AssignmentEntry{
		{PartitionID: 0, InstanceID: "rc-0"},
	}}, h.cfg.LeaseDuration, h.r.readcacheLeaseLookahead(), h.cfg.EntryRetention, 0))
	h.r.pushRanges(h.ctx, current, now)
	rc.setWarming(0)
	h.advance(4 * time.Minute)
	now = h.clock.Now()
	beforeHash := h.r.store.latestActiveAssignment(now)
	beforeOwners := latestDesiredOwners(h.r.readcacheStore.snapshot(), now)
	beforeHashHorizon := h.r.store.leaseHorizon(now)
	beforeReadcacheHorizon := h.r.readcacheStore.leaseHorizon(now)
	var calls atomic.Int32
	h.r.scallopPlan = func(snapshot scallop.Snapshot, policy scallop.Policy) (scallop.PlanResult, error) {
		calls.Add(1)
		return scallop.Plan(snapshot, policy)
	}

	require.NoError(t, h.runRound())
	assert.Zero(t, calls.Load(), "an incomplete snapshot must never reach Plan")
	assert.Equal(t, beforeHash, h.r.store.latestActiveAssignment(now))
	assert.Equal(t, beforeOwners, latestDesiredOwners(h.r.readcacheStore.snapshot(), now))
	assert.True(t, h.r.store.leaseHorizon(now).After(beforeHashHorizon), "skips still refresh hash leases")
	assert.True(t, h.r.readcacheStore.leaseHorizon(now).After(beforeReadcacheHorizon), "skips still refresh readcache leases")
}

func TestScallopColdStartEstablishesReadcacheCoverageWithoutLegacyTier2(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{Planner: plannerScallop, PartitionCount: 4},
	})
	h.addReadcache("rc-0")
	h.addReadcache("rc-1")

	require.NoError(t, h.runRound())
	assert.Len(t, h.tier2Active(), 4)
	assert.Equal(t, map[string]int{"rc-0": 2, "rc-1": 2}, h.ownersByInstance())
	assert.True(t, h.r.lastTier2RoundAt.IsZero())
}

func TestScallopPushExpandsLogicalOwnersToConcreteMirrors(t *testing.T) {
	h, pods := newRF2Harness(t)
	h.r.cfg.Planner = plannerScallop
	replicaMap := h.r.refreshReplicaMap()
	current := assignment.EvenSplitForTenant("tenant-a", []int32{0, 1, 2, 3})
	result := scallop.PlanResult{
		Assignment: current,
		PartitionOwners: map[int32]string{
			0: "readcache-0", 1: "readcache-1", 2: "readcache-0", 3: "readcache-1",
		},
	}
	translation, err := translateScallopPlan(scallop.Snapshot{Assignment: current}, result, []int32{0, 1, 2, 3})
	require.NoError(t, err)
	h.r.pushScallopRanges(h.ctx, translation.pushIntents, replicaMap)

	assert.Equal(t, []int32{0, 2}, pods["readcache-zone-a-0"].ownedPartitions())
	assert.Equal(t, pods["readcache-zone-a-0"].ownedPartitions(), pods["readcache-zone-b-0"].ownedPartitions())
	assert.Equal(t, []int32{1, 3}, pods["readcache-zone-a-1"].ownedPartitions())
	assert.Equal(t, pods["readcache-zone-a-1"].ownedPartitions(), pods["readcache-zone-b-1"].ownedPartitions())
}

func TestExplicitLegacySelectorMatchesDefaultLegacyPath(t *testing.T) {
	run := func(planner string) (map[string]int, bool) {
		h := newHarness(t, harnessOpts{cfg: Config{
			Planner:        planner,
			PartitionCount: 4,
			ReadcacheSlicer: ReadcacheSlicerConfig{
				Enabled:        true,
				Alpha:          1,
				MovementBudget: 1,
			},
		}})
		h.addReadcache("rc-0")
		h.addReadcache("rc-1")
		require.NoError(t, h.runRound())
		return h.ownersByInstance(), !h.r.lastTier2RoundAt.IsZero()
	}
	defaultOwners, defaultTier2 := run("")
	explicitOwners, explicitTier2 := run(plannerLegacy)
	assert.Equal(t, defaultOwners, explicitOwners)
	assert.True(t, defaultTier2)
	assert.Equal(t, defaultTier2, explicitTier2)
}

func TestConfigPlannerValidationAndDefault(t *testing.T) {
	assert.Equal(t, plannerLegacy, (&Config{}).planner())
	var defaults Config
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	defaults.RegisterFlagsWithPrefix("nautilus-rebalancer.", flags)
	require.NoError(t, flags.Parse(nil))
	assert.Equal(t, plannerLegacy, defaults.Planner)
	for _, planner := range []string{plannerLegacy, plannerScallop} {
		cfg := Config{Planner: planner, PartitionCount: 1}
		assert.NoError(t, cfg.Validate())
	}
	require.Error(t, (&Config{PartitionCount: 1}).Validate())
	cfg := Config{Planner: "other", PartitionCount: 1}
	require.ErrorContains(t, cfg.Validate(), "must be one of")
}

func latestDesiredOwners(entries []readcacheassignment.LogEntry, now time.Time) map[int32]string {
	type selected struct {
		id   string
		from time.Time
	}
	owners := map[int32]selected{}
	for _, entry := range entries {
		if !entry.ActiveAt(now) {
			continue
		}
		if previous, ok := owners[entry.PartitionID]; !ok || entry.From.After(previous.from) {
			owners[entry.PartitionID] = selected{id: entry.InstanceID, from: entry.From}
		}
	}
	out := make(map[int32]string, len(owners))
	for partitionID, owner := range owners {
		out[partitionID] = owner.id
	}
	return out
}
