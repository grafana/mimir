// SPDX-License-Identifier: AGPL-3.0-only

package scallop

import (
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// TestPlanMovesRangeToReducePeakImbalance proves ordinary relocation competes on the primary balance cost.
func TestPlanMovesRangeToReducePeakImbalance(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 1, 1},
		[]float64{4, 2, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)
	policy := zeroCostPolicy()
	policy.ActionLimits = testActionLimits(1)
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

// TestPlanMovesPartitionWithoutChangingRangePlacement proves joint planning can correct only replica skew.
func TestPlanMovesPartitionWithoutChangingRangePlacement(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 1, 2, 3},
		[]float64{8, 8, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-a", 2: "rc-b", 3: "rc-b"},
	)
	policy := zeroCostPolicy()
	policy.Weights.ReplicaBalance = 1
	policy.ActionLimits = ActionLimits{Total: 1, MovePartition: 1}

	result, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Len(t, result.Actions, 1)
	require.Equal(t, ActionMovePartition, result.Actions[0].Kind)
	require.Equal(t, snapshot.Assignment, result.Assignment)
	require.Equal(t, result.Actions[0].FromReplica, snapshot.PartitionOwners[result.Actions[0].PartitionID])
	require.Equal(t, result.Actions[0].ToReplica, result.PartitionOwners[result.Actions[0].PartitionID])
	require.NotEqual(t, result.Actions[0].FromReplica, result.Actions[0].ToReplica)
	require.Equal(t, result.InitialCost.PartitionBalance, result.FinalCost.PartitionBalance)
	require.Less(t, result.FinalCost.ReplicaBalance, result.InitialCost.ReplicaBalance)
	require.Zero(t, result.Actions[0].MovedHashFraction)
}

// TestDefaultWeightsAllowPartitionMoveReversal reproduces partition ping-pong after a small observed-load shift.
func TestDefaultWeightsAllowPartitionMoveReversal(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 1, 2, 3, 4, 5},
		[]float64{1, 10, 10, 9, 10, 0},
		map[int32]string{0: "rc-a", 1: "rc-a", 2: "rc-a", 3: "rc-b", 4: "rc-b", 5: "rc-b"},
	)
	snapshot.LastHostedAt = map[string]map[string]time.Time{
		"tenant-a": {"rc-a": snapshot.At, "rc-b": snapshot.At},
	}
	policy := DefaultPolicy()
	policy.ActionLimits = ActionLimits{Total: 1, MovePartition: 1}

	first, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Len(t, first.Actions, 1)
	require.Equal(t, ActionMovePartition, first.Actions[0].Kind)
	require.Equal(t, int32(0), first.Actions[0].PartitionID)
	require.Equal(t, "rc-a", first.Actions[0].FromReplica)
	require.Equal(t, "rc-b", first.Actions[0].ToReplica)

	snapshot.At = snapshot.At.Add(time.Minute)
	snapshot.PartitionOwners = first.PartitionOwners
	for i, entry := range snapshot.Assignment.Entries {
		snapshot.RangeLoads[RangeKey{TenantID: entry.TenantID, Range: entry.Range}] =
			[]float64{1, 8, 10, 11, 10, 0}[i]
	}

	second, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Len(t, second.Actions, 1)
	require.Equal(t, ActionMovePartition, second.Actions[0].Kind)
	require.Equal(t, first.Actions[0].PartitionID, second.Actions[0].PartitionID)
	require.Equal(t, first.Actions[0].ToReplica, second.Actions[0].FromReplica)
	require.Equal(t, first.Actions[0].FromReplica, second.Actions[0].ToReplica)
}

// TestJointPlannerChangesActionTypeWithObservedLoad proves range and partition moves compete each round.
func TestJointPlannerChangesActionTypeWithObservedLoad(t *testing.T) {
	firstSnapshot := testSnapshot(
		[]int32{0, 0, 1, 2, 3, 3},
		[]float64{1, 1, 2, 2, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-a", 2: "rc-a", 3: "rc-b"},
	)
	policy := zeroCostPolicy()
	policy.Weights.ReplicaBalance = 1
	policy.ActionLimits = ActionLimits{Total: 1, Move: 1, MovePartition: 1}

	first, err := Plan(firstSnapshot, policy)
	require.NoError(t, err)
	require.Len(t, first.Actions, 1)
	require.Equal(t, ActionMovePartition, first.Actions[0].Kind)

	secondSnapshot := firstSnapshot
	secondSnapshot.Assignment = first.Assignment
	secondSnapshot.PartitionOwners = first.PartitionOwners
	secondSnapshot.RangeLoads = make(map[RangeKey]float64, len(first.Assignment.Entries))
	loads := []float64{6, 4, 1, 1, 4, 4}
	for i, entry := range first.Assignment.Entries {
		secondSnapshot.RangeLoads[RangeKey{TenantID: entry.TenantID, Range: entry.Range}] = loads[i]
	}

	second, err := Plan(secondSnapshot, policy)
	require.NoError(t, err)
	require.Len(t, second.Actions, 1)
	require.Equal(t, ActionMove, second.Actions[0].Kind)
}

// TestPartitionMoveCandidateSearchHonorsBudgets keeps two-tier search bounded and inspectable.
func TestPartitionMoveCandidateSearchHonorsBudgets(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 1, 2, 3},
		[]float64{8, 8, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-a", 2: "rc-b", 3: "rc-c"},
	)
	policy := zeroCostPolicy()
	policy.CandidateSearch.MaxPartitionMoveSources = 1
	policy.CandidateSearch.MaxDestinationsPerPartition = 1
	policy.CandidateSearch.MaxPartitionMoveCandidates = 1

	candidates, diagnostics := generateCandidates(stateFromSnapshot(snapshot), snapshot, policy)
	partitionMoves := 0
	for _, candidate := range candidates {
		if candidate.kind == ActionMovePartition {
			partitionMoves++
		}
	}
	require.LessOrEqual(t, partitionMoves, 1)
	require.Greater(t, diagnostics.Legal.MovePartition, diagnostics.Admitted.MovePartition)
	require.Greater(t, diagnostics.DiscardedByBudget.PartitionMoveSources, 0)
	require.Greater(t, diagnostics.DiscardedByBudget.PartitionMoveDestinations, 0)
}

// TestMergeShortlistReservesFairTenantCapacity prevents priority ties from starving later tenant IDs.
func TestMergeShortlistReservesFairTenantCapacity(t *testing.T) {
	var candidates []candidate
	for i := range 10 {
		candidates = append(candidates,
			candidate{kind: ActionMerge, tenantID: "tenant-a", r: assignment.HashRange{Lo: uint32(i * 2), Hi: uint32(i * 2)}, priority: 2},
			candidate{kind: ActionMerge, tenantID: "tenant-b", r: assignment.HashRange{Lo: uint32(i * 2), Hi: uint32(i * 2)}, priority: 1},
		)
	}

	selected := limitMergesFair(candidates, map[string]int{"tenant-a": 64, "tenant-b": 64}, 4)
	counts := map[string]int{}
	for _, candidate := range selected {
		counts[candidate.tenantID]++
	}
	require.Equal(t, map[string]int{"tenant-a": 2, "tenant-b": 2}, counts)
}

// TestPlanSplitsHotRangeWithoutMovingChildren protects the observe-before-moving split semantics.
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
	policy.ActionLimits = testActionLimits(10)

	result, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Len(t, result.Actions, 1, "split children must not be acted on again before observation")
	require.Equal(t, ActionSplit, result.Actions[0].Kind)
	require.Len(t, result.Assignment.Entries, 2)
	require.Equal(t, int32(0), result.Assignment.Entries[0].PartitionID)
	require.Equal(t, int32(0), result.Assignment.Entries[1].PartitionID)
	require.NoError(t, result.Assignment.Validate())
}

// TestPlanMergesAdjacentRangesToReduceFragmentation proves structural cost can select a merge without load skew.
func TestPlanMergesAdjacentRangesToReduceFragmentation(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 0, 0},
		[]float64{1, 1, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)
	policy := zeroCostPolicy()
	policy.Weights.Fragmentation = 1
	policy.ActionLimits = testActionLimits(1)

	result, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Len(t, result.Actions, 1)
	require.Equal(t, ActionMerge, result.Actions[0].Kind)
	require.Len(t, result.Assignment.Entries, 3)
	require.NoError(t, result.Assignment.Validate())
}

// TestMergeCostsCanPreventHarmfulConsolidation preserves no-op as a real structural alternative.
func TestMergeCostsCanPreventHarmfulConsolidation(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 0, 0},
		[]float64{1, 1, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)
	policy := zeroCostPolicy()
	policy.Weights.Fragmentation = 1
	policy.Weights.Resolution = 100
	policy.ActionLimits = ActionLimits{Total: 4, Merge: 4, MergePerTenant: 4}

	result, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Empty(t, result.Actions)
}

// TestPlanCapsMergesPerTenantWithoutSacrificingFairProgress limits structural change per observation.
func TestPlanCapsMergesPerTenantWithoutSacrificingFairProgress(t *testing.T) {
	first := assignment.EvenSplitForTenant("tenant-a", []int32{0, 0, 0, 0, 0, 0, 0, 0})
	second := assignment.EvenSplitForTenant("tenant-b", []int32{0, 0, 0, 0, 0, 0, 0, 0})
	entries := append(append([]assignment.Entry(nil), first.Entries...), second.Entries...)
	snapshot := Snapshot{
		At:               time.Unix(100, 0),
		Assignment:       &assignment.Assignment{Entries: entries},
		RangeLoads:       make(map[RangeKey]float64, len(entries)),
		ActivePartitions: []int32{0},
		PartitionOwners:  map[int32]string{0: "rc-a"},
		ActiveReplicas:   []string{"rc-a"},
	}
	for _, entry := range entries {
		snapshot.RangeLoads[RangeKey{TenantID: entry.TenantID, Range: entry.Range}] = 1
	}
	policy := zeroCostPolicy()
	policy.Weights.Fragmentation = 1
	policy.ActionLimits = ActionLimits{Total: 100, Merge: 100, MergePerTenant: 2}

	result, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Len(t, result.Actions, 4)
	mergesByTenant := map[string]int{}
	for _, action := range result.Actions {
		require.Equal(t, ActionMerge, action.Kind)
		mergesByTenant[action.TenantID]++
	}
	require.Equal(t, map[string]int{"tenant-a": 2, "tenant-b": 2}, mergesByTenant)
}

// TestPlanPrefersWarmDestination proves recent replica history influences shortlist and exact locality cost.
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
	policy.ActionLimits = testActionLimits(1)
	policy.CandidateSearch.MaxDestinationsPerRange = 1

	result, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Len(t, result.Actions, 1)
	require.Equal(t, ActionMove, result.Actions[0].Kind)
	require.Equal(t, int32(2), result.Actions[0].ToPartition)
	require.Zero(t, result.Actions[0].Transition.LocalityMiss)
}

// TestPlanRejectsMissingObservation prevents absent range load from being silently interpreted as zero.
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

// TestPlanIsDeterministic protects replayability for identical snapshots and policies.
func TestPlanIsDeterministic(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 1, 2},
		[]float64{4, 2, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b", 2: "rc-c"},
	)
	policy := zeroCostPolicy()
	policy.ActionLimits = testActionLimits(3)

	first, err := Plan(snapshot, policy)
	require.NoError(t, err)
	second, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

// TestPlanChoosesNoOpWhenNoCandidateLowersCost prevents neutral or harmful churn.
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

// TestPlanHonorsActionLimit ensures greedy planning terminates at the caller's control-plane budget.
func TestPlanHonorsActionLimit(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 0, 0},
		[]float64{1, 1, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)
	policy := zeroCostPolicy()
	policy.Weights.Resolution = 1
	policy.ActionLimits = testActionLimits(2)

	result, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Len(t, result.Actions, policy.ActionLimits.Total)
}

// TestPlanHonorsPerKindActionLimits keeps one action type from consuming another type's budget.
func TestPlanHonorsPerKindActionLimits(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 0, 0},
		[]float64{1, 1, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)
	policy := zeroCostPolicy()
	policy.Weights.Resolution = 1
	policy.ActionLimits = ActionLimits{Total: 10, Split: 1}

	result, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.Len(t, result.Actions, 1)
	require.Equal(t, ActionSplit, result.Actions[0].Kind)
}

// TestOnlySplitChildrenAreIneligibleForLaterActions distinguishes unknown split loads from known move and merge loads.
func TestOnlySplitChildrenAreIneligibleForLaterActions(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 1},
		[]float64{3, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)
	state := stateFromSnapshot(snapshot)
	policy := zeroCostPolicy()

	moved := project(state, candidate{kind: ActionMove, index: 0, toPartition: 1})
	require.True(t, moved.ranges[0].observed)
	movedCandidates, _ := generateCandidates(moved, snapshot, policy)
	require.NotEmpty(t, movedCandidates)

	merged := project(state, candidate{kind: ActionMerge, index: 0, otherIndex: 1, toPartition: 0})
	require.True(t, merged.ranges[0].observed)
	mergedCandidates, _ := generateCandidates(merged, snapshot, policy)
	require.NotEmpty(t, mergedCandidates)

	split := project(state, candidate{kind: ActionSplit, index: 0})
	require.False(t, split.ranges[0].observed)
	require.False(t, split.ranges[1].observed)
	splitCandidates, _ := generateCandidates(split, snapshot, policy)
	for _, candidate := range splitCandidates {
		require.NotEqual(t, split.ranges[0].entry.Range, candidate.r)
		require.NotEqual(t, split.ranges[1].entry.Range, candidate.r)
	}
}

// TestCrossPartitionMergeCanChooseEitherNeighbor keeps both legal relocation directions available to the cost model.
func TestCrossPartitionMergeCanChooseEitherNeighbor(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 1},
		[]float64{3, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)
	state := stateFromSnapshot(snapshot)
	candidates, _ := generateCandidates(state, snapshot, zeroCostPolicy())
	var merges []candidate
	for _, candidate := range candidates {
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

// TestCandidateSearchAdmitsEveryLegalCandidateWithinLimits proves small snapshots receive complete coverage.
func TestCandidateSearchAdmitsEveryLegalCandidateWithinLimits(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 1, 2},
		[]float64{4, 2, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b", 2: "rc-c"},
	)

	candidates, diagnostics := generateCandidates(stateFromSnapshot(snapshot), snapshot, zeroCostPolicy())
	require.Len(t, candidates, diagnostics.Legal.Total)
	require.Equal(t, diagnostics.Legal, diagnostics.Admitted)
	require.Equal(t, diagnostics.Legal, diagnostics.FullyScored)
	require.Zero(t, diagnostics.Discarded.Total)
	require.False(t, diagnostics.Truncated)
}

// TestCandidateSearchNeverExceedsConfiguredBudgets bounds expensive projection and accounts for every discarded candidate.
func TestCandidateSearchNeverExceedsConfiguredBudgets(t *testing.T) {
	owners := make(map[int32]string, 20)
	partitions := make([]int32, 40)
	loads := make([]float64, 40)
	for i := 0; i < 20; i++ {
		owners[int32(i)] = fmt.Sprintf("rc-%02d", i%4)
	}
	for i := range partitions {
		partitions[i] = int32(i % len(owners))
		loads[i] = float64(i + 1)
	}
	snapshot := testSnapshot(partitions, loads, owners)
	policy := zeroCostPolicy()
	policy.CandidateSearch = CandidateSearchLimits{
		MaxMoveSources:              3,
		MaxDestinationsPerRange:     2,
		MaxSplitCandidates:          2,
		MaxMergeCandidates:          2,
		MaxPartitionMoveSources:     2,
		MaxDestinationsPerPartition: 2,
		MaxPartitionMoveCandidates:  2,
		MaxFullyScored:              5,
	}

	candidates, diagnostics := generateCandidates(stateFromSnapshot(snapshot), snapshot, policy)
	require.LessOrEqual(t, len(candidates), policy.CandidateSearch.MaxFullyScored)
	require.LessOrEqual(t, diagnostics.Admitted.Move, 3*2)
	require.LessOrEqual(t, diagnostics.Admitted.Split, 2)
	require.LessOrEqual(t, diagnostics.Admitted.Merge, 2)
	require.True(t, diagnostics.Truncated)
	require.Greater(t, diagnostics.DiscardedByBudget.MoveSources, 0)
	require.Greater(t, diagnostics.DiscardedByBudget.Destinations, 0)
	require.Greater(t, diagnostics.DiscardedByBudget.Splits, 0)
	require.Greater(t, diagnostics.DiscardedByBudget.Merges, 0)
	require.Greater(t, diagnostics.DiscardedByBudget.FullyScored, 0)
	require.Equal(t, diagnostics.Discarded.Total,
		diagnostics.DiscardedByBudget.MoveSources+
			diagnostics.DiscardedByBudget.Destinations+
			diagnostics.DiscardedByBudget.Splits+
			diagnostics.DiscardedByBudget.Merges+
			diagnostics.DiscardedByBudget.PartitionMoveSources+
			diagnostics.DiscardedByBudget.PartitionMoveDestinations+
			diagnostics.DiscardedByBudget.PartitionMoves+
			diagnostics.DiscardedByBudget.FullyScored,
	)

	result, err := Plan(snapshot, policy)
	require.NoError(t, err)
	require.LessOrEqual(t,
		result.CandidateSearch.FullyScored.Total,
		result.CandidateSearch.Iterations*policy.CandidateSearch.MaxFullyScored,
	)
}

// TestCandidateSearchOmitsLeastLoadedNonOverloadedSourceWhenBounded protects the primary source-pruning heuristic.
func TestCandidateSearchOmitsLeastLoadedNonOverloadedSourceWhenBounded(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 0, 1},
		[]float64{50, 30, 20, 1},
		map[int32]string{0: "rc-a", 1: "rc-b", 2: "rc-c"},
	)
	policy := zeroCostPolicy()
	policy.CandidateSearch.MaxMoveSources = 1

	candidates, diagnostics := generateCandidates(stateFromSnapshot(snapshot), snapshot, policy)
	require.True(t, diagnostics.Truncated)
	for _, candidate := range candidates {
		if candidate.kind == ActionMove {
			require.Equal(t, int32(0), candidate.fromPartition)
		}
	}
}

// TestReplicaSurplusCanAdmitRangeFromUnderloadedPartition prevents partition-only pruning from hiding replica relief.
func TestReplicaSurplusCanAdmitRangeFromUnderloadedPartition(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 1, 2, 3},
		[]float64{100, 1, 0, 0},
		map[int32]string{0: "rc-a", 1: "rc-a", 2: "rc-b", 3: "rc-b"},
	)
	state := stateFromSnapshot(snapshot)
	index := buildCandidateIndex(state, snapshot)
	policy := zeroCostPolicy()
	policy.Weights.ReplicaBalance = 1

	require.Less(t, index.partitionLoads[1], index.partitionMean)
	require.Greater(t, index.replicaLoads["rc-a"], index.replicaMean)
	require.Greater(t, moveSourcePriority(state.ranges[1], index, policy), 0.0)
}

// TestPlanIsDeterministicUnderTopologyInputReordering prevents caller slice order from affecting ties or shortlists.
func TestPlanIsDeterministicUnderTopologyInputReordering(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 1, 2, 3, 3},
		[]float64{1, 1, 2, 2, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-a", 2: "rc-a", 3: "rc-b"},
	)
	reordered := snapshot
	reordered.ActivePartitions = append([]int32(nil), snapshot.ActivePartitions...)
	reordered.ActiveReplicas = append([]string(nil), snapshot.ActiveReplicas...)
	slices.Reverse(reordered.ActivePartitions)
	slices.Reverse(reordered.ActiveReplicas)
	policy := zeroCostPolicy()
	policy.Weights.ReplicaBalance = 1

	first, err := Plan(snapshot, policy)
	require.NoError(t, err)
	second, err := Plan(reordered, policy)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, ActionMovePartition, first.Actions[0].Kind)
}

func zeroCostPolicy() Policy {
	return Policy{
		ActionMultipliers: ActionMultipliers{Move: 1, Split: 1, Merge: 1, MovePartition: 1},
		CandidateSearch:   DefaultCandidateSearchLimits(),
		ActionLimits:      testActionLimits(4),
	}
}

func testActionLimits(limit int) ActionLimits {
	return ActionLimits{
		Total:          limit,
		Move:           limit,
		Split:          limit,
		Merge:          limit,
		MergePerTenant: limit,
		MovePartition:  limit,
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
