// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

func TestBuildScallopSnapshotUsesAuthoritativeLoadsAndNewestOwner(t *testing.T) {
	now := time.Unix(10_000, 0)
	mid := uint32(math.MaxUint32 / 2)
	current := &assignment.Assignment{Entries: []assignment.Entry{
		{TenantID: "tenant-a", Range: assignment.HashRange{Lo: mid + 1, Hi: math.MaxUint32}, PartitionID: 1},
		{TenantID: "tenant-a", Range: assignment.HashRange{Lo: 0, Hi: mid}, PartitionID: 0},
	}}
	policy := scallop.DefaultPolicy()
	input := scallopSnapshotInput{
		now:              now,
		current:          current,
		activePartitions: []int32{1, 0},
		activeReplicas:   []string{"rc-new", "rc-1", "rc-old"},
		rates: []rangeRate{{
			tenantID: "tenant-a", hr: assignment.HashRange{Lo: 0, Hi: mid}, partitionID: 0, sampleRate: 42,
		}},
		readcacheLog: []readcacheassignment.LogEntry{
			{PartitionID: 0, InstanceID: "rc-new", From: now.Add(-time.Minute), To: now.Add(time.Hour)},
			{PartitionID: 1, InstanceID: "rc-1", From: now.Add(-time.Hour), To: now.Add(time.Hour)},
			{PartitionID: 0, InstanceID: "rc-old", From: now.Add(-time.Hour), To: now.Add(time.Minute)},
		},
		hashLog: []assignment.LogEntry{
			{TenantID: "tenant-a", Range: assignment.HashRange{Lo: 0, Hi: mid}, PartitionID: 0, From: now.Add(-time.Hour), To: now.Add(time.Hour)},
			{TenantID: "tenant-a", Range: assignment.HashRange{Lo: mid + 1, Hi: math.MaxUint32}, PartitionID: 1, From: now.Add(-time.Hour), To: now.Add(time.Hour)},
		},
	}
	snapshot, err := buildScallopSnapshot(input, policy)
	require.NoError(t, err)
	assert.Equal(t, []int32{0, 1}, snapshot.ActivePartitions)
	assert.Equal(t, []string{"rc-1", "rc-new", "rc-old"}, snapshot.ActiveReplicas)
	assert.Equal(t, "rc-new", snapshot.PartitionOwners[0], "the latest-From owner wins a safety overlap")
	assert.Equal(t, 42.0, snapshot.RangeLoads[scallop.RangeKey{TenantID: "tenant-a", Range: assignment.HashRange{Lo: 0, Hi: mid}}])
	assert.Equal(t, 0.0, snapshot.RangeLoads[scallop.RangeKey{TenantID: "tenant-a", Range: assignment.HashRange{Lo: mid + 1, Hi: math.MaxUint32}}])
	assert.Equal(t, now, snapshot.LastHostedAt["tenant-a"]["rc-old"], "all active overlap contributes locality, not just the desired owner")
	assert.Equal(t, now, snapshot.LastHostedAt["tenant-a"]["rc-new"])
	assert.Equal(t, current.Entries[0].Range.Lo, mid+1, "adapter must not sort the caller's assignment in place")

	reordered := input
	reordered.activePartitions = []int32{0, 1}
	reordered.activeReplicas = []string{"rc-old", "rc-1", "rc-new"}
	reordered.current = &assignment.Assignment{Entries: []assignment.Entry{current.Entries[1], current.Entries[0]}}
	reordered.readcacheLog = reverseCopy(input.readcacheLog)
	reordered.hashLog = reverseCopy(input.hashLog)
	reorderedSnapshot, err := buildScallopSnapshot(reordered, policy)
	require.NoError(t, err)
	firstJSON, err := json.Marshal(snapshot)
	require.NoError(t, err)
	secondJSON, err := json.Marshal(reorderedSnapshot)
	require.NoError(t, err)
	assert.Equal(t, string(firstJSON), string(secondJSON))
	firstPlan, err := scallop.Plan(snapshot, policy)
	require.NoError(t, err)
	secondPlan, err := scallop.Plan(reorderedSnapshot, policy)
	require.NoError(t, err)
	assert.Equal(t, firstPlan, secondPlan)
}

func reverseCopy[T any](input []T) []T {
	out := append([]T(nil), input...)
	for left, right := 0, len(out)-1; left < right; left, right = left+1, right-1 {
		out[left], out[right] = out[right], out[left]
	}
	return out
}

func TestBuildScallopSnapshotRejectsUnknownOrInvalidProductionState(t *testing.T) {
	now := time.Unix(10_000, 0)
	base := func() scallopSnapshotInput {
		return scallopSnapshotInput{
			now:                 now,
			current:             assignment.EvenSplitForTenant("tenant-a", []int32{0}),
			activePartitions:    []int32{0},
			activeReplicas:      []string{"rc-0"},
			readcacheLog:        []readcacheassignment.LogEntry{{PartitionID: 0, InstanceID: "rc-0", From: now.Add(-time.Hour), To: now.Add(time.Hour)}},
			unreadyPartitions:   map[int32]bool{},
			unavailableReplicas: map[string]struct{}{},
		}
	}
	assertFailure := func(t *testing.T, input scallopSnapshotInput, wantKind scallopSnapshotErrorKind, wantReason string) {
		t.Helper()
		_, err := buildScallopSnapshot(input, scallop.DefaultPolicy())
		require.Error(t, err)
		kind, reason := scallopSnapshotFailure(err)
		assert.Equal(t, wantKind, kind)
		assert.Equal(t, wantReason, reason)
	}

	t.Run("missing observation on unready partition", func(t *testing.T) {
		input := base()
		input.unreadyPartitions[0] = true
		assertFailure(t, input, scallopSnapshotNotReady, "load_unknown")
	})
	t.Run("unavailable owner", func(t *testing.T) {
		input := base()
		input.unavailableReplicas["rc-0"] = struct{}{}
		assertFailure(t, input, scallopSnapshotNotReady, "owner_unready")
	})
	t.Run("missing owner", func(t *testing.T) {
		input := base()
		input.readcacheLog = nil
		assertFailure(t, input, scallopSnapshotNotReady, "owner_missing")
	})
	t.Run("inactive owner", func(t *testing.T) {
		input := base()
		input.activeReplicas = []string{"rc-other"}
		assertFailure(t, input, scallopSnapshotNotReady, "owner_inactive")
	})
	t.Run("same-time owner conflict", func(t *testing.T) {
		input := base()
		input.activeReplicas = append(input.activeReplicas, "rc-other")
		input.readcacheLog = append(input.readcacheLog, readcacheassignment.LogEntry{
			PartitionID: 0, InstanceID: "rc-other", From: input.readcacheLog[0].From, To: now.Add(time.Hour),
		})
		assertFailure(t, input, scallopSnapshotInvalid, "owner_conflict")
	})
	t.Run("duplicate topology", func(t *testing.T) {
		input := base()
		input.activePartitions = []int32{0, 0}
		assertFailure(t, input, scallopSnapshotInvalid, "partition_duplicate")
		input = base()
		input.activeReplicas = []string{"rc-0", "rc-0"}
		assertFailure(t, input, scallopSnapshotInvalid, "replica_duplicate")
	})
	t.Run("invalid load", func(t *testing.T) {
		input := base()
		input.rates = []rangeRate{{
			tenantID: "tenant-a", hr: assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, partitionID: 0, sampleRate: math.NaN(),
		}}
		assertFailure(t, input, scallopSnapshotInvalid, "load_invalid")
	})
	t.Run("duplicate load", func(t *testing.T) {
		input := base()
		rate := rangeRate{tenantID: "tenant-a", hr: assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, partitionID: 0, sampleRate: 1}
		input.rates = []rangeRate{rate, rate}
		assertFailure(t, input, scallopSnapshotInvalid, "load_duplicate")
	})
}

func TestDeriveScallopLocalityUsesExclusiveIntervalIntersections(t *testing.T) {
	now := time.Unix(20_000, 0)
	window := time.Hour
	hashEntries := []assignment.LogEntry{
		{TenantID: "historical", PartitionID: 0, From: now.Add(-50 * time.Minute), To: now.Add(-30 * time.Minute)},
		{TenantID: "active", PartitionID: 0, From: now.Add(-20 * time.Minute), To: now.Add(time.Hour)},
		{TenantID: "touch-only", PartitionID: 1, From: now.Add(-40 * time.Minute), To: now.Add(-30 * time.Minute)},
		{TenantID: "expired", PartitionID: 2, From: now.Add(-2 * time.Hour), To: now.Add(-time.Hour)},
	}
	readcacheEntries := []readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "old", From: now.Add(-45 * time.Minute), To: now.Add(5 * time.Minute)},
		{PartitionID: 0, InstanceID: "new", From: now.Add(-5 * time.Minute), To: now.Add(time.Hour)},
		{PartitionID: 1, InstanceID: "boundary", From: now.Add(-30 * time.Minute), To: now},
		{PartitionID: 2, InstanceID: "expired", From: now.Add(-2 * time.Hour), To: now.Add(-time.Hour)},
	}
	got := deriveScallopLocality(now, window, hashEntries, readcacheEntries)
	assert.Equal(t, now.Add(-30*time.Minute), got["historical"]["old"])
	assert.Equal(t, now, got["active"]["old"], "the safety-window owner is still warm")
	assert.Equal(t, now, got["active"]["new"])
	assert.NotContains(t, got, "touch-only", "touching half-open endpoints do not overlap")
	assert.NotContains(t, got, "expired", "an interval ending at the window cutoff is expired")
}

func TestTranslateScallopPlanIsolatedApplyAndReplicaExpansion(t *testing.T) {
	now := time.Unix(30_000, 0)
	current := assignment.EvenSplitForTenant("tenant-a", []int32{0, 1})
	result := scallop.PlanResult{
		Assignment: current,
		PartitionOwners: map[int32]string{
			0: "logical-a",
			1: "logical-b",
		},
		Actions: []scallop.Action{{Kind: scallop.ActionMovePartition, PartitionID: 0, FromReplica: "logical-b", ToReplica: "logical-a"}},
	}
	translation, err := translateScallopPlan(scallop.Snapshot{Assignment: current}, result, []int32{1, 0})
	require.NoError(t, err)
	require.Equal(t, current.Entries, translation.hashAssignment.Entries, "partition moves must not alter range placement")
	require.Equal(t, result.Actions, translation.actions)

	originalReadcache := []readcacheassignment.LogEntry{{PartitionID: 0, InstanceID: "old", From: now.Add(-time.Hour), To: now.Add(time.Hour)}}
	originalHash := []assignment.LogEntry{{TenantID: "tenant-a", Range: current.Entries[0].Range, PartitionID: 0, From: now.Add(-time.Hour), To: now.Add(time.Hour)}}
	readcacheLog := readcacheassignment.NewLogFromEntries(originalReadcache)
	hashLog := assignment.NewLogFromEntries(originalHash)
	require.True(t, readcacheLog.Apply(now, translation.readcacheAssignment, time.Hour, time.Minute, time.Minute))
	require.True(t, hashLog.Apply(now, translation.hashAssignment, time.Hour, time.Minute))
	assert.Contains(t, readcacheLog.Lookup(now, 0), "logical-a")
	require.NoError(t, hashLog.LatestActiveAssignments(now).Validate())
	assert.Equal(t, "old", originalReadcache[0].InstanceID, "cloned log input remains untouched")
	assert.Len(t, originalHash, 1)

	replicaMap := readcacheassignment.ReplicaMap{
		"logical-a": {{InstanceID: "zone-a"}, {InstanceID: "zone-b"}},
		"logical-b": {{InstanceID: "zone-c"}},
	}
	resolved := resolveScallopPushIntents(translation.pushIntents, replicaMap)
	assert.Len(t, resolved["zone-a"], 1)
	assert.Equal(t, resolved["zone-a"], resolved["zone-b"])
	assert.Len(t, resolved["zone-c"], 1)
}
