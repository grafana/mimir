// SPDX-License-Identifier: AGPL-3.0-only

package scallop

// This file orchestrates greedy candidate selection into one replayable plan.

import (
	"fmt"
	"math"
	"sort"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

const costEpsilon = 1e-12

// Plan greedily selects lower-cost actions and fair merge waves within explicit execution limits.
func Plan(snapshot Snapshot, policy Policy) (PlanResult, error) {
	if err := snapshot.validate(); err != nil {
		return PlanResult{}, err
	}
	if err := policy.validate(); err != nil {
		return PlanResult{}, err
	}

	state := stateFromSnapshot(snapshot)
	initialCost := stateCost(state, snapshot, policy)
	currentCost := initialCost
	var actions []Action
	var selected CandidateCounts
	selectedMergesByTenant := map[string]int{}
	var cumulativeTransition CostBreakdown
	searchDiagnostics := CandidateSearchDiagnostics{Limits: policy.CandidateSearch}

	for selected.Total < policy.ActionLimits.Total {
		var best *evaluatedCandidate
		candidates, iterationDiagnostics := generateCandidatesFor(state, snapshot, policy, candidateGeneration{
			move:                   canSelect(ActionMove, selected, policy.ActionLimits),
			split:                  canSelect(ActionSplit, selected, policy.ActionLimits),
			merge:                  canSelect(ActionMerge, selected, policy.ActionLimits),
			movePartition:          canSelect(ActionMovePartition, selected, policy.ActionLimits),
			selectedMergesByTenant: selectedMergesByTenant,
			mergePerTenant:         policy.ActionLimits.MergePerTenant,
		})
		accumulateSearchDiagnostics(&searchDiagnostics, iterationDiagnostics)

		remainingMerges := min(
			policy.ActionLimits.Merge-selected.Merge,
			policy.ActionLimits.Total-selected.Total,
		)
		mergeWave := selectMergeWave(
			candidates,
			state,
			remainingMerges,
			selectedMergesByTenant,
			policy.ActionLimits.MergePerTenant,
		)
		if len(mergeWave) > 0 {
			projected, transition, resolvedWave, err := projectMergeWave(state, mergeWave, snapshot, policy)
			if err != nil {
				return PlanResult{}, err
			}
			after := stateCost(projected, snapshot, policy)
			total := after.WeightedTotal + transition.WeightedTotal
			if total < currentCost.WeightedTotal-costEpsilon {
				best = &evaluatedCandidate{
					state:      projected,
					after:      after,
					transition: transition,
					total:      total,
					wave:       resolvedWave,
				}
			}
		}

		for _, candidate := range candidates {
			if candidate.kind == ActionMerge || !canSelect(candidate.kind, selected, policy.ActionLimits) {
				continue
			}
			projected := project(state, candidate)
			projectedAssignment := projected.assignment()
			if err := projectedAssignment.Validate(); err != nil {
				return PlanResult{}, fmt.Errorf("candidate %s produced invalid assignment: %w", candidate.key(), err)
			}

			after := stateCost(projected, snapshot, policy)
			transition := transitionCost(candidate, state, snapshot, policy)
			total := after.WeightedTotal + transition.WeightedTotal
			if total >= currentCost.WeightedTotal-costEpsilon {
				continue
			}
			evaluated := evaluatedCandidate{
				candidate:  candidate,
				state:      projected,
				after:      after,
				transition: transition,
				total:      total,
			}
			if best == nil ||
				evaluated.total < best.total-costEpsilon ||
				(math.Abs(evaluated.total-best.total) <= costEpsilon && evaluated.candidate.key() < best.candidate.key()) {
				best = &evaluated
			}
		}

		if best == nil {
			break
		}

		if len(best.wave) > 0 {
			waveActions, next, waveTransition, err := materializeMergeWave(
				state, currentCost, best.wave, snapshot, policy, best.total,
			)
			if err != nil {
				return PlanResult{}, err
			}
			actions = append(actions, waveActions...)
			for _, action := range waveActions {
				selectedMergesByTenant[action.TenantID]++
			}
			selected.Merge += len(waveActions)
			selected.Total += len(waveActions)
			accumulateTransition(&cumulativeTransition, waveTransition)
			state = next
			currentCost = best.after
			continue
		}

		action := actionFromCandidate(best.candidate, currentCost, best.after, best.transition, best.total)
		actions = append(actions, action)
		incrementCandidateCount(&selected, best.candidate.kind)
		accumulateTransition(&cumulativeTransition, best.transition)
		state = best.state
		currentCost = best.after
	}

	finalCost := currentCost
	finalCost.TransitionEvents = cumulativeTransition.TransitionEvents
	finalCost.TransitionLoad = cumulativeTransition.TransitionLoad
	finalCost.TransitionHashSpace = cumulativeTransition.TransitionHashSpace
	finalCost.LocalityMiss = cumulativeTransition.LocalityMiss
	finalCost.WeightedTransitionEvents = cumulativeTransition.WeightedTransitionEvents
	finalCost.WeightedTransitionLoad = cumulativeTransition.WeightedTransitionLoad
	finalCost.WeightedTransitionHashSpace = cumulativeTransition.WeightedTransitionHashSpace
	finalCost.WeightedLocalityMiss = cumulativeTransition.WeightedLocalityMiss
	finalCost.WeightedTotal += cumulativeTransition.WeightedTotal

	return PlanResult{
		Assignment:      state.assignment(),
		PartitionOwners: clonePartitionOwners(state.partitionOwners),
		Actions:         actions,
		InitialCost:     initialCost,
		FinalCost:       finalCost,
		CandidateSearch: searchDiagnostics,
	}, nil
}

type evaluatedCandidate struct {
	candidate  candidate
	state      planningState
	after      CostBreakdown
	transition CostBreakdown
	total      float64
	wave       []candidate
}

// actionFromCandidate converts an internal winner and its cost evidence into the public action record.
func actionFromCandidate(c candidate, before, after, transition CostBreakdown, total float64) Action {
	action := Action{
		Kind:              c.kind,
		TenantID:          c.tenantID,
		Range:             c.r,
		FromPartition:     c.fromPartition,
		ToPartition:       c.toPartition,
		PartitionID:       c.partitionID,
		FromReplica:       c.fromReplica,
		ToReplica:         c.toReplica,
		Load:              c.load,
		MovedLoad:         c.movedLoad,
		MovedHashFraction: c.movedHashFraction,
		Before:            before,
		After:             after,
		Transition:        transition,
		CandidateTotal:    total,
	}
	if c.kind == ActionMerge {
		other := c.other
		action.Other = &other
	}
	action.Explanation = fmt.Sprintf(
		"%s lowers immediate weighted cost from %.6f to %.6f (state %.6f + transition %.6f)",
		c.kind, before.WeightedTotal, total, after.WeightedTotal, transition.WeightedTotal,
	)
	return action
}

// canSelect checks both total and action-kind execution limits.
func canSelect(kind ActionKind, selected CandidateCounts, limits ActionLimits) bool {
	if selected.Total >= limits.Total {
		return false
	}
	switch kind {
	case ActionMove:
		return selected.Move < limits.Move
	case ActionSplit:
		return selected.Split < limits.Split
	case ActionMerge:
		return selected.Merge < limits.Merge
	case ActionMovePartition:
		return selected.MovePartition < limits.MovePartition
	default:
		return false
	}
}

// incrementCandidateCount records one selected primitive action.
func incrementCandidateCount(counts *CandidateCounts, kind ActionKind) {
	switch kind {
	case ActionMove:
		counts.Move++
	case ActionSplit:
		counts.Split++
	case ActionMerge:
		counts.Merge++
	case ActionMovePartition:
		counts.MovePartition++
	}
	counts.Total++
}

// selectMergeWave chooses pairwise non-overlapping merges round-robin within each tenant's remaining allowance.
func selectMergeWave(
	candidates []candidate,
	state planningState,
	limit int,
	selectedByTenant map[string]int,
	perTenantLimit int,
) []candidate {
	if limit <= 0 {
		return nil
	}
	byTenant := map[string][]candidate{}
	rangeCounts := map[string]int{}
	for _, r := range state.ranges {
		rangeCounts[r.entry.TenantID]++
	}
	for _, candidate := range candidates {
		if candidate.kind == ActionMerge &&
			candidate.priority > costEpsilon &&
			selectedByTenant[candidate.tenantID] < perTenantLimit {
			byTenant[candidate.tenantID] = append(byTenant[candidate.tenantID], candidate)
		}
	}
	tenants := make([]string, 0, len(byTenant))
	for tenantID := range byTenant {
		tenants = append(tenants, tenantID)
		sortCandidatesByPriority(byTenant[tenantID])
	}
	sort.Slice(tenants, func(i, j int) bool {
		if rangeCounts[tenants[i]] != rangeCounts[tenants[j]] {
			return rangeCounts[tenants[i]] > rangeCounts[tenants[j]]
		}
		return tenants[i] < tenants[j]
	})
	used := map[string]struct{}{}
	offsets := map[string]int{}
	selectedInWave := map[string]int{}
	wave := make([]candidate, 0, limit)
	for len(wave) < limit {
		added := false
		for _, tenantID := range tenants {
			if selectedByTenant[tenantID]+selectedInWave[tenantID] >= perTenantLimit {
				continue
			}
			tenantCandidates := byTenant[tenantID]
			for offsets[tenantID] < len(tenantCandidates) {
				candidate := tenantCandidates[offsets[tenantID]]
				offsets[tenantID]++
				leftKey := fmt.Sprintf("%s/%d/%d", tenantID, candidate.r.Lo, candidate.r.Hi)
				rightKey := fmt.Sprintf("%s/%d/%d", tenantID, candidate.other.Lo, candidate.other.Hi)
				if _, exists := used[leftKey]; exists {
					continue
				}
				if _, exists := used[rightKey]; exists {
					continue
				}
				used[leftKey] = struct{}{}
				used[rightKey] = struct{}{}
				wave = append(wave, candidate)
				selectedInWave[tenantID]++
				added = true
				break
			}
			if len(wave) == limit {
				break
			}
		}
		if !added {
			break
		}
	}
	return wave
}

// projectMergeWave cumulatively projects and costs a complete primitive merge wave.
func projectMergeWave(
	state planningState,
	wave []candidate,
	snapshot Snapshot,
	policy Policy,
) (planningState, CostBreakdown, []candidate, error) {
	index := buildCandidateIndex(state, snapshot)
	resolved := make([]candidate, 0, len(wave))
	for _, original := range wave {
		candidate, ok := resolveMergeCandidate(state, original, index, snapshot, policy)
		if !ok {
			return planningState{}, CostBreakdown{}, nil, fmt.Errorf("merge wave candidate %s is no longer legal", original.key())
		}
		updateCandidateIndexForMerge(&index, candidate, state)
		resolved = append(resolved, candidate)
	}

	initialTotal := stateCost(state, snapshot, policy).WeightedTotal
	bestTotal := initialTotal
	bestLength := 0
	var bestState planningState
	var bestTransition CostBreakdown
	step := max(1, len(resolved)/64)
	var cumulative CostBreakdown
	for length, candidate := range resolved {
		accumulateTransition(&cumulative, transitionCost(candidate, state, snapshot, policy))
		prefixLength := length + 1
		if prefixLength%step != 0 && prefixLength != len(resolved) {
			continue
		}
		projected := projectMergeBatch(state, resolved[:prefixLength])
		after := stateCost(projected, snapshot, policy)
		total := after.WeightedTotal + cumulative.WeightedTotal
		if total < bestTotal-costEpsilon {
			bestTotal = total
			bestLength = prefixLength
			bestState = projected
			bestTransition = cumulative
		}
	}
	if bestLength == 0 {
		return state, CostBreakdown{}, nil, nil
	}
	resolved = resolved[:bestLength]
	if err := bestState.assignment().Validate(); err != nil {
		return planningState{}, CostBreakdown{}, nil, fmt.Errorf("merge wave produced invalid assignment: %w", err)
	}
	return bestState, bestTransition, resolved, nil
}

// projectMergeBatch applies pairwise non-overlapping merges in one linear pass.
func projectMergeBatch(state planningState, wave []candidate) planningState {
	byIndex := make(map[int]candidate, len(wave))
	for _, candidate := range wave {
		byIndex[candidate.index] = candidate
	}
	ranges := make([]rangeState, 0, len(state.ranges)-len(wave))
	for i := 0; i < len(state.ranges); i++ {
		candidate, ok := byIndex[i]
		if !ok {
			ranges = append(ranges, state.ranges[i])
			continue
		}
		left, right := state.ranges[i], state.ranges[i+1]
		ranges = append(ranges, rangeState{
			entry: assignment.Entry{
				TenantID:    left.entry.TenantID,
				Range:       assignment.HashRange{Lo: left.entry.Range.Lo, Hi: right.entry.Range.Hi},
				PartitionID: candidate.toPartition,
			},
			load:     left.load + right.load,
			observed: true,
		})
		i++
	}
	return planningState{ranges: ranges, partitionOwners: clonePartitionOwners(state.partitionOwners)}
}

// materializeMergeWave emits every selected merge with sequential cost evidence.
func materializeMergeWave(
	state planningState,
	before CostBreakdown,
	wave []candidate,
	snapshot Snapshot,
	policy Policy,
	waveTotal float64,
) ([]Action, planningState, CostBreakdown, error) {
	next := projectMergeBatch(state, wave)
	after := stateCost(next, snapshot, policy)
	actions := make([]Action, 0, len(wave))
	var cumulative CostBreakdown
	for _, candidate := range wave {
		transition := transitionCost(candidate, state, snapshot, policy)
		action := actionFromCandidate(candidate, before, after, transition, waveTotal)
		action.Explanation = fmt.Sprintf(
			"merge wave cumulatively lowers weighted cost from %.6f to %.6f across %d primitive merges",
			before.WeightedTotal, waveTotal, len(wave),
		)
		actions = append(actions, action)
		accumulateTransition(&cumulative, transition)
	}
	return actions, next, cumulative, nil
}

// resolveMergeCandidate chooses the lower-cost destination against cumulative wave state.
func resolveMergeCandidate(
	state planningState,
	original candidate,
	index candidateIndex,
	snapshot Snapshot,
	policy Policy,
) (candidate, bool) {
	if original.index < 0 || original.otherIndex != original.index+1 ||
		original.otherIndex >= len(state.ranges) {
		return candidate{}, false
	}
	left, right := state.ranges[original.index], state.ranges[original.otherIndex]
	if left.entry.TenantID != original.tenantID ||
		left.entry.Range != original.r ||
		right.entry.Range != original.other {
		return candidate{}, false
	}
	targets := []int32{left.entry.PartitionID}
	if right.entry.PartitionID != left.entry.PartitionID {
		targets = append(targets, right.entry.PartitionID)
	}
	var best *candidate
	bestPressure := math.Inf(1)
	for _, target := range targets {
		option := mergeCandidateForTarget(original.index, left, right, target)
		option.priority = mergePriority(option, left, right, state, index, snapshot, policy)
		pressure := mergePlacementPressure(option, index, state, policy)
		if best == nil ||
			option.priority > best.priority+costEpsilon ||
			(math.Abs(option.priority-best.priority) <= costEpsilon &&
				(pressure < bestPressure-costEpsilon ||
					(math.Abs(pressure-bestPressure) <= costEpsilon && option.key() < best.key()))) {
			copy := option
			best = &copy
			bestPressure = pressure
		}
	}
	return *best, true
}

// mergePlacementPressure breaks peak-cost ties toward less-loaded partitions and replicas.
func mergePlacementPressure(
	candidate candidate,
	index candidateIndex,
	state planningState,
	policy Policy,
) float64 {
	destinationLoad := index.partitionLoads[candidate.toPartition] + candidate.movedLoad
	destinationReplica := state.partitionOwners[candidate.toPartition]
	replicaLoad := index.replicaLoads[destinationReplica]
	if state.partitionOwners[candidate.fromPartition] != destinationReplica {
		replicaLoad += candidate.movedLoad
	}
	partitionPressure := destinationLoad / math.Max(index.partitionMean, costEpsilon)
	replicaPressure := replicaLoad / math.Max(index.replicaMean, costEpsilon)
	return partitionPressure + policy.Weights.ReplicaBalance*replicaPressure
}

// updateCandidateIndexForMerge keeps affected load aggregates current within one wave.
func updateCandidateIndexForMerge(index *candidateIndex, candidate candidate, state planningState) {
	if candidate.fromPartition != candidate.toPartition && candidate.movedLoad > 0 {
		index.partitionLoads[candidate.fromPartition] -= candidate.movedLoad
		index.partitionLoads[candidate.toPartition] += candidate.movedLoad
		fromReplica := state.partitionOwners[candidate.fromPartition]
		toReplica := state.partitionOwners[candidate.toPartition]
		if fromReplica != toReplica {
			index.replicaLoads[fromReplica] -= candidate.movedLoad
			index.replicaLoads[toReplica] += candidate.movedLoad
		}
	}
	index.partitionPeak = peakExcessWithMean(index.partitionLoads, index.partitionMean)
	index.replicaPeak = peakExcessWithMean(index.replicaLoads, index.replicaMean)
	index.tenantRangeCounts[candidate.tenantID]--
}

func clonePartitionOwners(owners map[int32]string) map[int32]string {
	cloned := make(map[int32]string, len(owners))
	for partitionID, owner := range owners {
		cloned[partitionID] = owner
	}
	return cloned
}

// accumulateSearchDiagnostics combines one greedy iteration's pruning evidence into the plan result.
func accumulateSearchDiagnostics(dst *CandidateSearchDiagnostics, src CandidateSearchDiagnostics) {
	dst.Iterations++
	dst.LegalMoveSources += src.LegalMoveSources
	dst.LegalMoveDestinations += src.LegalMoveDestinations
	dst.LegalPartitionMoveSources += src.LegalPartitionMoveSources
	dst.LegalPartitionMoveDestinations += src.LegalPartitionMoveDestinations
	addCandidateCounts(&dst.Legal, src.Legal)
	addCandidateCounts(&dst.Admitted, src.Admitted)
	addCandidateCounts(&dst.FullyScored, src.FullyScored)
	addCandidateCounts(&dst.Discarded, src.Discarded)
	dst.DiscardedByBudget.MoveSources += src.DiscardedByBudget.MoveSources
	dst.DiscardedByBudget.Destinations += src.DiscardedByBudget.Destinations
	dst.DiscardedByBudget.Splits += src.DiscardedByBudget.Splits
	dst.DiscardedByBudget.Merges += src.DiscardedByBudget.Merges
	dst.DiscardedByBudget.PartitionMoveSources += src.DiscardedByBudget.PartitionMoveSources
	dst.DiscardedByBudget.PartitionMoveDestinations += src.DiscardedByBudget.PartitionMoveDestinations
	dst.DiscardedByBudget.PartitionMoves += src.DiscardedByBudget.PartitionMoves
	dst.DiscardedByBudget.FullyScored += src.DiscardedByBudget.FullyScored
	dst.Truncated = dst.Truncated || src.Truncated
}

func addCandidateCounts(dst *CandidateCounts, src CandidateCounts) {
	dst.Move += src.Move
	dst.Split += src.Split
	dst.Merge += src.Merge
	dst.MovePartition += src.MovePartition
	dst.Total += src.Total
}
