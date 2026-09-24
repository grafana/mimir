// SPDX-License-Identifier: AGPL-3.0-only

package scallop

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

func TestPlanMovesRangeToReducePeakImbalance(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 1, 1},
		[]float64{4, 2, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)
	policy := zeroCostPolicy()
	policy.MaxActions = 1
	policy.Weights.TransitionEvents = 1
	policy.ActionMultipliers.Move = 0

	result, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Len(t, result.Actions, 1)
	require.Equal(t, ActionMove, result.Actions[0].Kind)
	require.Equal(t, int32(0), result.Actions[0].FromPartition)
	require.Equal(t, int32(1), result.Actions[0].ToPartition)
	require.Equal(t, 2.0, result.Actions[0].MovedLoad)
	require.Less(t, result.FinalCost.PartitionBalance, result.InitialCost.PartitionBalance)
	require.NoError(t, result.Assignment.Validate())
}

func TestPlanSplitsHotRangeWithoutMovingChildren(t *testing.T) {
	snapshot := Snapshot{
		At:               time.Unix(100, 0),
		Assignment:       assignment.EvenSplitForTenant("tenant-a", []int32{0}),
		RangeLoads:       map[RangeKey]float64{{TenantID: "tenant-a", Range: assignment.HashRange{Lo: 0, Hi: math.MaxUint32}}: 10},
		ActivePartitions: []int32{0, 1},
		PartitionOwners:  map[int32]string{0: "rc-a", 1: "rc-b"},
		ActiveReplicas:   []string{"rc-a", "rc-b"},
	}
	policy := zeroCostPolicy()
	policy.Weights.Resolution = 1
	policy.MaxActions = 10

	result, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Len(t, result.Actions, 1, "split children must not be acted on again before observation")
	require.Equal(t, ActionSplit, result.Actions[0].Kind)
	require.Len(t, result.Assignment.Entries, 2)
	require.Equal(t, int32(0), result.Assignment.Entries[0].PartitionID)
	require.Equal(t, int32(0), result.Assignment.Entries[1].PartitionID)
	require.NoError(t, result.Assignment.Validate())
}

func TestPlanMergesAdjacentRangesToReduceFragmentation(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 0, 0},
		[]float64{1, 1, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)
	policy := zeroCostPolicy()
	policy.Weights.Fragmentation = 1
	policy.MaxActions = 1

	result, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Len(t, result.Actions, 1)
	require.Equal(t, ActionMerge, result.Actions[0].Kind)
	require.Len(t, result.Assignment.Entries, 3)
	require.NoError(t, result.Assignment.Validate())
}

func TestPlanPrefersWarmDestination(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 1, 2},
		[]float64{4, 2, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b", 2: "rc-c"},
	)
	snapshot.LastHostedAt = map[string]map[string]time.Time{
		"tenant-a": {"rc-c": snapshot.At.Add(-time.Minute)},
	}
	policy := zeroCostPolicy()
	policy.Weights.LocalityMiss = 1
	policy.LocalityWindow = 5 * time.Minute
	policy.MaxActions = 1

	result, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Len(t, result.Actions, 1)
	require.Equal(t, ActionMove, result.Actions[0].Kind)
	require.Equal(t, int32(2), result.Actions[0].ToPartition)
	require.Zero(t, result.Actions[0].Transition.LocalityMiss)
}

func TestPlanRejectsMissingObservation(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 1},
		[]float64{1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)
	delete(snapshot.RangeLoads, RangeKey{TenantID: "tenant-a", Range: snapshot.Assignment.Entries[0].Range})

	_, err := Plan(snapshot, zeroCostPolicy())
	require.ErrorContains(t, err, "has no observed load")
}

func TestPlanIsDeterministic(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 1, 2},
		[]float64{4, 2, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b", 2: "rc-c"},
	)
	policy := zeroCostPolicy()
	policy.MaxActions = 3

	first, err := Plan(snapshot, policy)
	require.NoError(t, err)
	second, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestPlanChoosesNoOpWhenNoCandidateLowersCost(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 1},
		[]float64{1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)

	result, err := Plan(snapshot, zeroCostPolicy())
	require.NoError(t, err)
	require.Empty(t, result.Actions)
	require.Equal(t, snapshot.Assignment, result.Assignment)
	require.Equal(t, snapshot.PartitionOwners, result.PartitionOwners)
}

func TestPlanHonorsActionLimit(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 0, 0},
		[]float64{1, 1, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)
	policy := zeroCostPolicy()
	policy.Weights.Resolution = 1
	policy.MaxActions = 2

	result, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Len(t, result.Actions, policy.MaxActions)
}

func TestOnlySplitChildrenAreIneligibleForLaterActions(t *testing.T) {
	state := stateFromSnapshot(testSnapshot(
		[]int32{0, 1},
		[]float64{3, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	))

	moved := project(state, candidate{kind: ActionMove, index: 0, toPartition: 1})
	require.True(t, moved.ranges[0].observed)
	require.NotEmpty(t, generateCandidates(moved, []int32{0, 1}))

	merged := project(state, candidate{kind: ActionMerge, index: 0, otherIndex: 1, toPartition: 0})
	require.True(t, merged.ranges[0].observed)
	require.NotEmpty(t, generateCandidates(merged, []int32{0, 1}))

	split := project(state, candidate{kind: ActionSplit, index: 0})
	require.False(t, split.ranges[0].observed)
	require.False(t, split.ranges[1].observed)
	for _, candidate := range generateCandidates(split, []int32{0, 1}) {
		require.NotEqual(t, split.ranges[0].entry.Range, candidate.r)
		require.NotEqual(t, split.ranges[1].entry.Range, candidate.r)
	}
}

func TestCrossPartitionMergeCanChooseEitherNeighbor(t *testing.T) {
	state := stateFromSnapshot(testSnapshot(
		[]int32{0, 1},
		[]float64{3, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	))
	var merges []candidate
	for _, candidate := range generateCandidates(state, []int32{0, 1}) {
		if candidate.kind == ActionMerge {
			merges = append(merges, candidate)
		}
	}
	require.Len(t, merges, 2)
	require.Equal(t, int32(0), merges[0].toPartition)
	require.Equal(t, 1.0, merges[0].movedLoad)
	require.InDelta(t, 0.5, merges[0].movedHashFraction, 1e-12)
	require.Equal(t, int32(1), merges[1].toPartition)
	require.Equal(t, 3.0, merges[1].movedLoad)
	require.InDelta(t, 0.5, merges[1].movedHashFraction, 1e-12)
}

func zeroCostPolicy() Policy {
	return Policy{
		ActionMultipliers: ActionMultipliers{Move: 1, Split: 1, Merge: 1},
		MaxActions:        4,
	}
}

func testSnapshot(partitions []int32, loads []float64, owners map[int32]string) Snapshot {
	destinations := append([]int32(nil), partitions...)
	a := assignment.EvenSplitForTenant("tenant-a", destinations)
	rangeLoads := make(map[RangeKey]float64, len(a.Entries))
	for i, entry := range a.Entries {
		rangeLoads[RangeKey{TenantID: entry.TenantID, Range: entry.Range}] = loads[i]
	}
	activePartitions := make([]int32, 0, len(owners))
	replicaSet := map[string]struct{}{}
	for partition, owner := range owners {
		activePartitions = append(activePartitions, partition)
		replicaSet[owner] = struct{}{}
	}
	activeReplicas := make([]string, 0, len(replicaSet))
	for replica := range replicaSet {
		activeReplicas = append(activeReplicas, replica)
	}
	return Snapshot{
		At:               time.Unix(100, 0),
		Assignment:       a,
		RangeLoads:       rangeLoads,
		ActivePartitions: activePartitions,
		PartitionOwners:  owners,
		ActiveReplicas:   activeReplicas,
	}
}
