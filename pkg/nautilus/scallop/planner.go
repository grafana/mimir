// SPDX-License-Identifier: AGPL-3.0-only

package scallop

// This file orchestrates greedy candidate selection into one replayable plan.

import (
	"fmt"
	"math"
)

const costEpsilon = 1e-12

// Plan greedily selects the lowest-cost legal hash-range action until no
// candidate beats no-op or Policy.MaxActions is reached.
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
	var cumulativeTransition CostBreakdown
	searchDiagnostics := CandidateSearchDiagnostics{Limits: policy.CandidateSearch}

	for len(actions) < policy.MaxActions {
		var best *evaluatedCandidate
		candidates, iterationDiagnostics := generateCandidates(state, snapshot, policy)
		accumulateSearchDiagnostics(&searchDiagnostics, iterationDiagnostics)
		for _, candidate := range candidates {
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

		action := actionFromCandidate(best.candidate, currentCost, best.after, best.transition, best.total)
		actions = append(actions, action)
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
		PartitionOwners: clonePartitionOwners(snapshot.PartitionOwners),
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
}

// actionFromCandidate converts an internal winner and its cost evidence into the public action record.
func actionFromCandidate(c candidate, before, after, transition CostBreakdown, total float64) Action {
	action := Action{
		Kind:              c.kind,
		TenantID:          c.tenantID,
		Range:             c.r,
		FromPartition:     c.fromPartition,
		ToPartition:       c.toPartition,
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
	addCandidateCounts(&dst.Legal, src.Legal)
	addCandidateCounts(&dst.Admitted, src.Admitted)
	addCandidateCounts(&dst.FullyScored, src.FullyScored)
	addCandidateCounts(&dst.Discarded, src.Discarded)
	dst.DiscardedByBudget.MoveSources += src.DiscardedByBudget.MoveSources
	dst.DiscardedByBudget.Destinations += src.DiscardedByBudget.Destinations
	dst.DiscardedByBudget.Splits += src.DiscardedByBudget.Splits
	dst.DiscardedByBudget.Merges += src.DiscardedByBudget.Merges
	dst.DiscardedByBudget.FullyScored += src.DiscardedByBudget.FullyScored
	dst.Truncated = dst.Truncated || src.Truncated
}

func addCandidateCounts(dst *CandidateCounts, src CandidateCounts) {
	dst.Move += src.Move
	dst.Split += src.Split
	dst.Merge += src.Merge
	dst.Total += src.Total
}
