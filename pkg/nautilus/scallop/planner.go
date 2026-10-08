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

// Plan greedily selects lower-cost moves, partition moves, and fair merge
// waves, then applies one split wave, all within explicit execution limits.
//
// Splits run last because they never change balance and their children cannot
// be acted on again before observation: deferring them lets observed ranges be
// moved first, and lets every worthwhile split be selected in one pass.
func Plan(snapshot Snapshot, policy Policy) (PlanResult, error) {
	if err := snapshot.validate(); err != nil {
		return PlanResult{}, err
	}
	if err := policy.validate(); err != nil {
		return PlanResult{}, err
	}

	state := stateFromSnapshot(snapshot)
	totals := totalsOf(state)
	index := buildCandidateIndex(state, totals, snapshot)
	initialCost := stateCostFromIndex(index, policy)
	currentCost := initialCost
	var actions []Action
	var selected CandidateCounts
	selectedMovesByTenant := map[string]int{}
	selectedMergesByTenant := map[string]int{}
	var cumulativeTransition CostBreakdown
	searchDiagnostics := CandidateSearchDiagnostics{Limits: policy.CandidateSearch}

	for selected.Total < policy.ActionLimits.Total {
		var best *evaluatedCandidate
		candidates, iterationDiagnostics := generateCandidatesFromIndex(state, index, snapshot, policy, candidateGeneration{
			move:                   canSelect(ActionMove, selected, policy.ActionLimits),
			merge:                  canSelect(ActionMerge, selected, policy.ActionLimits),
			movePartition:          canSelect(ActionMovePartition, selected, policy.ActionLimits),
			selectedMovesByTenant:  selectedMovesByTenant,
			movePerTenant:          policy.ActionLimits.MovePerTenant,
			selectedMergesByTenant: selectedMergesByTenant,
			mergePerTenant:         policy.ActionLimits.MergePerTenant,
		})

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
			after, transition, resolvedWave, total, err := scoreMergeWave(
				state, index, currentCost, mergeWave, snapshot, policy,
			)
			if err != nil {
				return PlanResult{}, err
			}
			iterationDiagnostics.Work.ExactDeltaScores += len(mergeWave)
			if len(resolvedWave) > 0 && total < currentCost.WeightedTotal-costEpsilon {
				best = &evaluatedCandidate{
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
			after, transition, total := scoreCandidate(candidate, currentCost, index, state, snapshot, policy)
			iterationDiagnostics.Work.ExactDeltaScores++
			if total >= currentCost.WeightedTotal-costEpsilon {
				continue
			}
			evaluated := evaluatedCandidate{
				candidate:  candidate,
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
			accumulateSearchDiagnostics(&searchDiagnostics, iterationDiagnostics)
			break
		}

		if len(best.wave) > 0 {
			waveActions, next, waveTransition, err := materializeMergeWave(
				state, totals, currentCost, best.wave, snapshot, policy, best.total,
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
			index = buildCandidateIndex(state, totals, snapshot)
			currentCost = best.after
			iterationDiagnostics.Work.CompleteProjections++
			accumulateSearchDiagnostics(&searchDiagnostics, iterationDiagnostics)
			continue
		}

		next := project(state, best.candidate)
		if err := next.assignment().Validate(); err != nil {
			return PlanResult{}, fmt.Errorf("selected candidate %s produced invalid assignment: %w", best.candidate.key(), err)
		}
		action := actionFromCandidate(best.candidate, currentCost, best.after, best.transition, best.total)
		actions = append(actions, action)
		incrementCandidateCount(&selected, best.candidate.kind)
		if best.candidate.kind == ActionMove {
			selectedMovesByTenant[best.candidate.tenantID]++
			updateMoveStructures(&index, best.candidate, state)
		} else if best.candidate.kind == ActionMovePartition {
			updatePartitionMoveStructures(&index, best.candidate, state)
		}
		applyCandidateToIndex(&index, best.candidate, state)
		accumulateTransition(&cumulativeTransition, best.transition)
		state = next
		currentCost = best.after
		iterationDiagnostics.Work.CompleteProjections++
		accumulateSearchDiagnostics(&searchDiagnostics, iterationDiagnostics)
	}

	remainingSplits := min(
		policy.ActionLimits.Split-selected.Split,
		policy.ActionLimits.Total-selected.Total,
	)
	if remainingSplits > 0 {
		wave, waveDiagnostics, err := planSplitWave(state, index, currentCost, remainingSplits, snapshot, policy)
		if err != nil {
			return PlanResult{}, err
		}
		accumulateSearchDiagnostics(&searchDiagnostics, waveDiagnostics)
		if len(wave.actions) > 0 {
			actions = append(actions, wave.actions...)
			selected.Split += len(wave.actions)
			selected.Total += len(wave.actions)
			accumulateTransition(&cumulativeTransition, wave.transition)
			state = wave.state
			currentCost = wave.after
		}
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

// scoreMergeWave resolves and exactly delta-scores a merge-wave prefix without
// materializing the full assignment. Balance interactions are preserved by
// applying each candidate to a private aggregate index before scoring the next.
func scoreMergeWave(
	state planningState,
	index candidateIndex,
	before CostBreakdown,
	wave []candidate,
	snapshot Snapshot,
	policy Policy,
) (CostBreakdown, CostBreakdown, []candidate, float64, error) {
	working := index.cloneLoads()
	resolved := make([]candidate, 0, len(wave))
	bestTotal := before.WeightedTotal
	bestLength := 0
	bestAfter := before
	var bestTransition CostBreakdown
	step := max(1, len(wave)/64)
	var cumulative CostBreakdown
	current := before
	for length, original := range wave {
		candidate, ok := resolveMergeCandidate(state, original, working, snapshot, policy)
		if !ok {
			return CostBreakdown{}, CostBreakdown{}, nil, 0,
				fmt.Errorf("merge wave candidate %s is no longer legal", original.key())
		}
		after, transition, _ := scoreCandidate(candidate, current, working, state, snapshot, policy)
		accumulateTransition(&cumulative, transition)
		applyCandidateToIndex(&working, candidate, state)
		resolved = append(resolved, candidate)
		current = after
		prefixLength := length + 1
		if prefixLength%step != 0 && prefixLength != len(wave) {
			continue
		}
		total := after.WeightedTotal + cumulative.WeightedTotal
		if total < bestTotal-costEpsilon {
			bestTotal = total
			bestLength = prefixLength
			bestAfter = after
			bestTransition = cumulative
		}
	}
	if bestLength == 0 {
		return before, CostBreakdown{}, nil, before.WeightedTotal, nil
	}
	resolved = resolved[:bestLength]
	return bestAfter, bestTransition, resolved, bestTotal, nil
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
	totals planTotals,
	before CostBreakdown,
	wave []candidate,
	snapshot Snapshot,
	policy Policy,
	waveTotal float64,
) ([]Action, planningState, CostBreakdown, error) {
	next := projectMergeBatch(state, wave)
	after := stateCost(next, totals, snapshot, policy)
	actions := make([]Action, 0, len(wave))
	var cumulative CostBreakdown
	for _, candidate := range wave {
		transition := transitionCost(candidate, state, totals, snapshot, policy)
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

type splitWave struct {
	actions    []Action
	state      planningState
	after      CostBreakdown
	transition CostBreakdown
}

// planSplitWave applies every cost-reducing split within per-tenant and
// cluster-wide limits in one step. Splits leave balance unchanged and their
// children are ineligible for further action, so each split's cost change is
// independent of the others and the wave's cost change is their sum.
func planSplitWave(
	state planningState,
	index candidateIndex,
	before CostBreakdown,
	limit int,
	snapshot Snapshot,
	policy Policy,
) (splitWave, CandidateSearchDiagnostics, error) {
	wave, diagnostics := selectSplitWave(state, index, snapshot, policy, limit)
	if len(wave) == 0 {
		return splitWave{}, diagnostics, nil
	}
	next := projectSplitBatch(state, wave)
	if err := next.assignment().Validate(); err != nil {
		return splitWave{}, diagnostics, fmt.Errorf("split wave produced invalid assignment: %w", err)
	}
	after := before
	transitions := make([]CostBreakdown, len(wave))
	var cumulative CostBreakdown
	for i, candidate := range wave {
		r := state.ranges[candidate.index]
		after.Fragmentation += 1 / float64(max(1, index.totals.tenants))
		after.Resolution -= r.load * rangeFraction(r.entry.Range) / 2
		transitions[i] = transitionCost(candidate, state, index.totals, snapshot, policy)
		accumulateTransition(&cumulative, transitions[i])
	}
	weightStateCost(&after, policy)
	total := after.WeightedTotal + cumulative.WeightedTotal
	if total >= before.WeightedTotal-costEpsilon {
		return splitWave{}, diagnostics, nil
	}
	actions := make([]Action, len(wave))
	for i, candidate := range wave {
		actions[i] = actionFromCandidate(candidate, before, after, transitions[i], total)
		actions[i].Explanation = fmt.Sprintf(
			"split wave lowers weighted cost from %.6f to %.6f across %d primitive splits",
			before.WeightedTotal, total, len(wave),
		)
	}
	diagnostics.Work.CompleteProjections++
	return splitWave{actions: actions, state: next, after: after, transition: cumulative}, diagnostics, nil
}

// selectSplitWave exactly costs every legal split, keeps each tenant's best
// cost-reducing splits up to its allowance, and returns the best of those up to
// limit in range order.
func selectSplitWave(
	state planningState,
	index candidateIndex,
	snapshot Snapshot,
	policy Policy,
	limit int,
) ([]candidate, CandidateSearchDiagnostics) {
	diagnostics := CandidateSearchDiagnostics{Limits: policy.CandidateSearch}
	transition := transitionCost(candidate{kind: ActionSplit}, state, index.totals, snapshot, policy).WeightedTotal
	byTenant := map[string][]candidate{}
	for _, i := range index.splittableRangeIndexes {
		r := state.ranges[i]
		diagnostics.Work.RangesInspected++
		diagnostics.Legal.Split++
		priority := splitPriority(r, index.totals, policy, transition)
		diagnostics.Work.ExactDeltaScores++
		if priority <= costEpsilon {
			continue
		}
		byTenant[r.entry.TenantID] = append(byTenant[r.entry.TenantID], candidate{
			kind:          ActionSplit,
			index:         i,
			otherIndex:    -1,
			tenantID:      r.entry.TenantID,
			r:             r.entry.Range,
			fromPartition: r.entry.PartitionID,
			toPartition:   r.entry.PartitionID,
			load:          r.load,
			priority:      priority,
		})
	}
	diagnostics.Legal.Total = diagnostics.Legal.Split
	diagnostics.Admitted = diagnostics.Legal
	diagnostics.FullyScored = diagnostics.Legal
	diagnostics.Work.Yielded = diagnostics.Legal

	var wave []candidate
	for _, tenantCandidates := range byTenant {
		sortCandidatesByPriority(tenantCandidates)
		wave = append(wave, tenantCandidates[:min(policy.ActionLimits.SplitPerTenant, len(tenantCandidates))]...)
	}
	sortCandidatesByPriority(wave)
	wave = wave[:min(limit, len(wave))]
	sort.Slice(wave, func(i, j int) bool { return wave[i].index < wave[j].index })
	return wave, diagnostics
}

// projectSplitBatch applies index-ordered, non-overlapping splits in one linear pass.
func projectSplitBatch(state planningState, wave []candidate) planningState {
	ranges := make([]rangeState, 0, len(state.ranges)+len(wave))
	next := 0
	for i, r := range state.ranges {
		if next < len(wave) && wave[next].index == i {
			left, right := splitChildren(r)
			ranges = append(ranges, left, right)
			next++
			continue
		}
		ranges = append(ranges, r)
	}
	return planningState{ranges: ranges, partitionOwners: clonePartitionOwners(state.partitionOwners)}
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
	dst.DiscardedByBudget.Merges += src.DiscardedByBudget.Merges
	dst.DiscardedByBudget.PartitionMoveSources += src.DiscardedByBudget.PartitionMoveSources
	dst.DiscardedByBudget.PartitionMoveDestinations += src.DiscardedByBudget.PartitionMoveDestinations
	dst.DiscardedByBudget.PartitionMoves += src.DiscardedByBudget.PartitionMoves
	dst.DiscardedByBudget.FullyScored += src.DiscardedByBudget.FullyScored
	dst.Work.OffendersExpanded.Partitions += src.Work.OffendersExpanded.Partitions
	dst.Work.OffendersExpanded.Replicas += src.Work.OffendersExpanded.Replicas
	dst.Work.OffendersExpanded.Tenants += src.Work.OffendersExpanded.Tenants
	dst.Work.RangesInspected += src.Work.RangesInspected
	dst.Work.AdjacencyEdgesInspected += src.Work.AdjacencyEdgesInspected
	dst.Work.DestinationsInspected += src.Work.DestinationsInspected
	addCandidateCounts(&dst.Work.Yielded, src.Work.Yielded)
	dst.Work.ExactDeltaScores += src.Work.ExactDeltaScores
	dst.Work.CompleteProjections += src.Work.CompleteProjections
	dst.Pruned.PartitionOffenders += src.Pruned.PartitionOffenders
	dst.Pruned.ReplicaOffenders += src.Pruned.ReplicaOffenders
	dst.Pruned.TenantOffenders += src.Pruned.TenantOffenders
	dst.Pruned.Ranges += src.Pruned.Ranges
	dst.Pruned.AdjacencyEdges += src.Pruned.AdjacencyEdges
	dst.Pruned.DestinationPartitions += src.Pruned.DestinationPartitions
	dst.Pruned.DestinationReplicas += src.Pruned.DestinationReplicas
	dst.Truncated = dst.Truncated || src.Truncated
}

func addCandidateCounts(dst *CandidateCounts, src CandidateCounts) {
	dst.Move += src.Move
	dst.Split += src.Split
	dst.Merge += src.Merge
	dst.MovePartition += src.MovePartition
	dst.Total += src.Total
}
