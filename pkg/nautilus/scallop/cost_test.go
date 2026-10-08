// SPDX-License-Identifier: AGPL-3.0-only

package scallop

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStateCostTerms(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 1, 1},
		[]float64{4, 2, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)
	policy := zeroCostPolicy()
	policy.Weights.ReplicaBalance = 2
	policy.Weights.Fragmentation = 3
	policy.Weights.Resolution = 4

	cost := stateCost(stateFromSnapshot(snapshot), totalsOf(stateFromSnapshot(snapshot)), snapshot, policy)
	require.InDelta(t, 0.5, cost.PartitionBalance, 1e-12)
	require.InDelta(t, 0.5, cost.ReplicaBalance, 1e-12)
	require.InDelta(t, 4, cost.Fragmentation, 1e-12)
	require.InDelta(t, 2, cost.Resolution, 1e-9)
	require.InDelta(t, 0.5, cost.WeightedPartitionBalance, 1e-12)
	require.InDelta(t, 1, cost.WeightedReplicaBalance, 1e-12)
	require.InDelta(t, 12, cost.WeightedFragmentation, 1e-12)
	require.InDelta(t, 8, cost.WeightedResolution, 1e-9)
	require.InDelta(t, 21.5, cost.WeightedTotal, 1e-9)
}

func TestTransitionCostTermsAndLocality(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 1, 1},
		[]float64{4, 2, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)
	state := stateFromSnapshot(snapshot)
	policy := zeroCostPolicy()
	policy.LocalityWindow = 5 * time.Minute
	policy.ActionMultipliers.Move = 2
	policy.Weights.TransitionEvents = 1
	policy.Weights.TransitionLoad = 2
	policy.Weights.TransitionHashSpace = 3
	policy.Weights.LocalityMiss = 4
	action := candidate{
		kind:              ActionMove,
		tenantID:          "tenant-a",
		toPartition:       1,
		movedLoad:         2,
		movedHashFraction: 0.25,
	}

	cost := transitionCost(action, state, totalsOf(state), snapshot, policy)
	require.InDelta(t, 1, cost.TransitionEvents, 1e-12)
	require.InDelta(t, 0.25, cost.TransitionLoad, 1e-12)
	require.InDelta(t, 0.25, cost.TransitionHashSpace, 1e-12)
	require.InDelta(t, 0.25, cost.LocalityMiss, 1e-12)
	require.InDelta(t, 1, cost.WeightedTransitionEvents, 1e-12)
	require.InDelta(t, 0.5, cost.WeightedTransitionLoad, 1e-12)
	require.InDelta(t, 0.75, cost.WeightedTransitionHashSpace, 1e-12)
	require.InDelta(t, 1, cost.WeightedLocalityMiss, 1e-12)
	require.InDelta(t, 3.25, cost.WeightedTotal, 1e-12)
	policy.ActionLimits = ActionLimits{Total: 1000, Move: 1000}
	require.Equal(t, cost.TransitionEvents, transitionCost(action, state, totalsOf(state), snapshot, policy).TransitionEvents,
		"execution capacity must not change per-event cost")
	coarserSnapshot := testSnapshot(
		[]int32{0, 1},
		[]float64{6, 2},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)
	require.Equal(t, cost.TransitionEvents,
		transitionCost(action, stateFromSnapshot(coarserSnapshot), totalsOf(stateFromSnapshot(coarserSnapshot)), coarserSnapshot, policy).TransitionEvents,
		"assignment granularity must not change per-event cost")

	snapshot.LastHostedAt = map[string]map[string]time.Time{
		"tenant-a": {"rc-b": snapshot.At.Add(-time.Minute)},
	}
	cost = transitionCost(action, state, totalsOf(state), snapshot, policy)
	require.Zero(t, cost.LocalityMiss)
	require.InDelta(t, 2.25, cost.WeightedTotal, 1e-12)
}

func TestPartitionMoveTransitionCostUsesTenantWeightedLocality(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 1, 1},
		[]float64{4, 2, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)
	snapshot.LastHostedAt = map[string]map[string]time.Time{
		"tenant-a": {"rc-b": snapshot.At},
	}
	state := stateFromSnapshot(snapshot)
	policy := zeroCostPolicy()
	policy.LocalityWindow = time.Hour
	policy.Weights.LocalityMiss = 1
	action := candidate{
		kind:        ActionMovePartition,
		partitionID: 0,
		toReplica:   "rc-b",
		movedLoad:   6,
		tenantLoads: map[string]float64{"tenant-a": 6},
	}

	cost := transitionCost(action, state, totalsOf(state), snapshot, policy)
	require.Zero(t, cost.LocalityMiss)
	require.Zero(t, cost.TransitionHashSpace)

	delete(snapshot.LastHostedAt, "tenant-a")
	cost = transitionCost(action, state, totalsOf(state), snapshot, policy)
	require.InDelta(t, 0.75, cost.LocalityMiss, 1e-12)
}

func TestRangeMoveUsesProjectedPartitionOwnerForLocality(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 1},
		[]float64{3, 1},
		map[int32]string{0: "rc-a", 1: "rc-b"},
	)
	snapshot.ActiveReplicas = append(snapshot.ActiveReplicas, "rc-c")
	snapshot.LastHostedAt = map[string]map[string]time.Time{
		"tenant-a": {"rc-c": snapshot.At},
	}
	state := stateFromSnapshot(snapshot)
	state = project(state, candidate{
		kind:        ActionMovePartition,
		partitionID: 1,
		toReplica:   "rc-c",
	})
	policy := zeroCostPolicy()
	policy.LocalityWindow = time.Hour
	policy.Weights.LocalityMiss = 1
	move := candidate{
		kind:        ActionMove,
		tenantID:    "tenant-a",
		toPartition: 1,
		movedLoad:   3,
	}

	require.Zero(t, transitionCost(move, state, totalsOf(state), snapshot, policy).LocalityMiss)
}

func TestPeakExcessZeroLoad(t *testing.T) {
	require.Zero(t, peakExcess(map[int32]float64{0: 0, 1: 0}))
}

func TestExactCandidateDeltasMatchCompleteProjection(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 1, 2, 3, 3},
		[]float64{12, 4, 7, 2, 1, 1},
		map[int32]string{0: "rc-a", 1: "rc-a", 2: "rc-b", 3: "rc-c"},
	)
	snapshot.LastHostedAt = map[string]map[string]time.Time{
		"tenant-a": {"rc-b": snapshot.At},
	}
	state := stateFromSnapshot(snapshot)
	totals := totalsOf(state)
	index := buildCandidateIndex(state, totals, snapshot)
	policy := DefaultPolicy()
	policy.CandidateSearch.MaxMoveSources = len(state.ranges)
	policy.CandidateSearch.MaxDestinationPartitions = len(snapshot.ActivePartitions)
	policy.CandidateSearch.MaxDestinationReplicas = len(snapshot.ActiveReplicas)
	before := stateCost(state, totals, snapshot, policy)
	candidates, _ := generateCandidates(state, snapshot, policy)
	require.NotEmpty(t, candidates)

	for _, candidate := range candidates {
		t.Run(candidate.key(), func(t *testing.T) {
			after, transition, total := scoreCandidate(candidate, before, index, state, snapshot, policy)
			projected := project(state, candidate)
			wantAfter := stateCost(projected, totals, snapshot, policy)
			wantTransition := transitionCost(candidate, state, totals, snapshot, policy)
			requireCostBreakdownInDelta(t, wantAfter, after)
			requireCostBreakdownInDelta(t, wantTransition, transition)
			require.InDelta(t, wantAfter.WeightedTotal+wantTransition.WeightedTotal, total, 1e-9)
		})
	}
}

func TestCommittedIndexMatchesRebuild(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 0, 1, 2},
		[]float64{9, 3, 2, 1},
		map[int32]string{0: "rc-a", 1: "rc-a", 2: "rc-b"},
	)
	state := stateFromSnapshot(snapshot)
	totals := totalsOf(state)
	index := buildCandidateIndex(state, totals, snapshot)
	policy := zeroCostPolicy()
	candidates, _ := generateCandidates(state, snapshot, policy)

	for _, candidate := range candidates {
		if candidate.kind != ActionMove && candidate.kind != ActionMovePartition {
			continue
		}
		trial := index.cloneLoads()
		if candidate.kind == ActionMove {
			updateMoveStructures(&trial, candidate, state)
		} else {
			updatePartitionMoveStructures(&trial, candidate, state)
		}
		applyCandidateToIndex(&trial, candidate, state)
		projected := project(state, candidate)
		rebuilt := buildCandidateIndex(projected, totals, snapshot)
		require.Equal(t, rebuilt.partitionOwners, trial.partitionOwners)
		require.Equal(t, rebuilt.partitionLoads, trial.partitionLoads)
		require.Equal(t, rebuilt.replicaLoads, trial.replicaLoads)
		requireCostBreakdownInDelta(t, stateCostFromIndex(rebuilt, policy), stateCostFromIndex(trial, policy))
	}
}

func TestMergeWaveDeltaMatchesCompleteProjection(t *testing.T) {
	snapshot := testSnapshot(
		[]int32{0, 1, 0, 1, 2, 2, 3, 3},
		[]float64{9, 2, 8, 1, 4, 3, 2, 1},
		map[int32]string{0: "rc-a", 1: "rc-b", 2: "rc-c", 3: "rc-d"},
	)
	state := stateFromSnapshot(snapshot)
	totals := totalsOf(state)
	index := buildCandidateIndex(state, totals, snapshot)
	policy := zeroCostPolicy()
	policy.Weights.Fragmentation = 10
	candidates, _ := generateCandidates(state, snapshot, policy)
	wave := selectMergeWave(candidates, state, 4, map[string]int{}, 4)
	require.NotEmpty(t, wave)

	before := stateCost(state, totals, snapshot, policy)
	after, transition, resolved, total, err := scoreMergeWave(
		state, index, before, wave, snapshot, policy,
	)
	require.NoError(t, err)
	require.NotEmpty(t, resolved)
	projected := projectMergeBatch(state, resolved)
	wantAfter := stateCost(projected, totals, snapshot, policy)
	var wantTransition CostBreakdown
	for _, candidate := range resolved {
		accumulateTransition(&wantTransition, transitionCost(candidate, state, totals, snapshot, policy))
	}
	requireCostBreakdownInDelta(t, wantAfter, after)
	requireCostBreakdownInDelta(t, wantTransition, transition)
	require.InDelta(t, wantAfter.WeightedTotal+wantTransition.WeightedTotal, total, 1e-9)
}

func requireCostBreakdownInDelta(t *testing.T, expected, actual CostBreakdown) {
	t.Helper()
	require.InDelta(t, expected.PartitionBalance, actual.PartitionBalance, 1e-9)
	require.InDelta(t, expected.ReplicaBalance, actual.ReplicaBalance, 1e-9)
	require.InDelta(t, expected.TransitionEvents, actual.TransitionEvents, 1e-9)
	require.InDelta(t, expected.TransitionLoad, actual.TransitionLoad, 1e-9)
	require.InDelta(t, expected.TransitionHashSpace, actual.TransitionHashSpace, 1e-9)
	require.InDelta(t, expected.LocalityMiss, actual.LocalityMiss, 1e-9)
	require.InDelta(t, expected.Fragmentation, actual.Fragmentation, 1e-9)
	require.InDelta(t, expected.Resolution, actual.Resolution, 1e-9)
	require.InDelta(t, expected.WeightedTotal, actual.WeightedTotal, 1e-9)
}
