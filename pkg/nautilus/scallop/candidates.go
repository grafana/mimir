// SPDX-License-Identifier: AGPL-3.0-only

package scallop

// This file enumerates deterministic range actions and projects their assignment effects.

import (
	"fmt"
	"math"
	"sort"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

type candidate struct {
	kind ActionKind

	index      int
	otherIndex int

	tenantID string
	r        assignment.HashRange
	other    assignment.HashRange

	fromPartition int32
	toPartition   int32

	load              float64
	movedLoad         float64
	movedHashFraction float64
	priority          float64
}

// candidateIndex is the cheap first-stage view used to avoid fully projecting
// every legal action. Candidate generation first aggregates current partition
// and replica load here, uses those aggregates to rank likely move, split, and
// merge actions, and then sends only the bounded shortlist to Plan for exact
// projection, validation, and cost comparison. The index is rebuilt after
// every selected action, so priorities always describe the current projected
// state rather than the original snapshot.
type candidateIndex struct {
	partitionLoads map[int32]float64
	replicaLoads   map[string]float64
	partitionMean  float64
	replicaMean    float64
	totalLoad      float64
	tenantCount    int
}

type rankedRange struct {
	index    int
	priority float64
	key      string
}

type rankedPartition struct {
	id       int32
	priority float64
}

// generateCandidates returns a deterministic bounded shortlist of legal range actions.
func generateCandidates(state planningState, snapshot Snapshot, policy Policy) ([]candidate, CandidateSearchDiagnostics) {
	partitions := append([]int32(nil), snapshot.ActivePartitions...)
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	limits := policy.CandidateSearch
	diagnostics := CandidateSearchDiagnostics{Limits: limits}
	index := buildCandidateIndex(state, snapshot)

	moveSources := make([]rankedRange, 0, len(state.ranges))
	splits := make([]candidate, 0, min(len(state.ranges), limits.MaxSplitCandidates))
	for i, r := range state.ranges {
		if !r.observed {
			continue
		}
		if len(partitions) > 1 {
			diagnostics.LegalMoveSources++
			diagnostics.LegalMoveDestinations += len(partitions) - 1
			diagnostics.Legal.Move += len(partitions) - 1
			moveSources = append(moveSources, rankedRange{
				index:    i,
				priority: moveSourcePriority(r, index, snapshot, policy),
				key:      rangeStateKey(r),
			})
		}
		if r.entry.Range.Size() > 1 {
			diagnostics.Legal.Split++
			splits = append(splits, candidate{
				kind:          ActionSplit,
				index:         i,
				otherIndex:    -1,
				tenantID:      r.entry.TenantID,
				r:             r.entry.Range,
				fromPartition: r.entry.PartitionID,
				toPartition:   r.entry.PartitionID,
				load:          r.load,
				priority:      splitPriority(r, index, snapshot, policy),
			})
		}
	}

	selectedSources := selectMoveSources(moveSources, limits.MaxMoveSources)
	diagnostics.DiscardedByBudget.MoveSources =
		(len(moveSources) - len(selectedSources)) * max(0, len(partitions)-1)
	moves := make([]candidate, 0, len(selectedSources)*limits.MaxDestinationsPerRange)
	for _, source := range selectedSources {
		r := state.ranges[source.index]
		destinations := selectDestinations(r, partitions, index, snapshot, policy, limits.MaxDestinationsPerRange)
		diagnostics.DiscardedByBudget.Destinations += len(partitions) - 1 - len(destinations)
		for _, destination := range destinations {
			moves = append(moves, candidate{
				kind:              ActionMove,
				index:             source.index,
				otherIndex:        -1,
				tenantID:          r.entry.TenantID,
				r:                 r.entry.Range,
				fromPartition:     r.entry.PartitionID,
				toPartition:       destination.id,
				load:              r.load,
				movedLoad:         r.load,
				movedHashFraction: rangeFraction(r.entry.Range),
				priority:          source.priority + destination.priority,
			})
		}
	}

	merges := make([]candidate, 0, min(len(state.ranges), limits.MaxMergeCandidates))
	for i := 0; i+1 < len(state.ranges); i++ {
		left, right := state.ranges[i], state.ranges[i+1]
		if !left.observed || !right.observed {
			continue
		}
		if left.entry.TenantID != right.entry.TenantID ||
			left.entry.Range.Hi == ^uint32(0) ||
			left.entry.Range.Hi+1 != right.entry.Range.Lo {
			continue
		}

		targets := []int32{left.entry.PartitionID}
		if right.entry.PartitionID != left.entry.PartitionID {
			targets = append(targets, right.entry.PartitionID)
		}
		for _, target := range targets {
			movedLoad := 0.0
			movedRange := assignment.HashRange{}
			if left.entry.PartitionID != right.entry.PartitionID {
				if target == left.entry.PartitionID {
					movedLoad = right.load
					movedRange = right.entry.Range
				} else {
					movedLoad = left.load
					movedRange = left.entry.Range
				}
			}
			movedHashFraction := 0.0
			if left.entry.PartitionID != right.entry.PartitionID {
				movedHashFraction = rangeFraction(movedRange)
			}
			merge := candidate{
				kind:              ActionMerge,
				index:             i,
				otherIndex:        i + 1,
				tenantID:          left.entry.TenantID,
				r:                 left.entry.Range,
				other:             right.entry.Range,
				fromPartition:     partitionMovedFrom(left, right, target),
				toPartition:       target,
				load:              left.load + right.load,
				movedLoad:         movedLoad,
				movedHashFraction: movedHashFraction,
			}
			merge.priority = mergePriority(merge, left, right, state, index, snapshot, policy)
			merges = append(merges, merge)
			diagnostics.Legal.Merge++
		}
	}

	moves = limitCandidates(moves, len(moves))
	diagnostics.DiscardedByBudget.Splits = max(0, len(splits)-limits.MaxSplitCandidates)
	diagnostics.DiscardedByBudget.Merges = max(0, len(merges)-limits.MaxMergeCandidates)
	splits = limitCandidates(splits, limits.MaxSplitCandidates)
	merges = limitCandidates(merges, limits.MaxMergeCandidates)
	diagnostics.Admitted = candidateCounts(moves, splits, merges)

	candidates := make([]candidate, 0, diagnostics.Admitted.Total)
	candidates = append(candidates, moves...)
	candidates = append(candidates, splits...)
	candidates = append(candidates, merges...)
	diagnostics.DiscardedByBudget.FullyScored = max(0, len(candidates)-limits.MaxFullyScored)
	candidates = limitCandidates(candidates, limits.MaxFullyScored)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].key() < candidates[j].key()
	})
	diagnostics.FullyScored = countCandidates(candidates)
	diagnostics.Legal.Total = diagnostics.Legal.Move + diagnostics.Legal.Split + diagnostics.Legal.Merge
	diagnostics.Discarded = subtractCounts(diagnostics.Legal, diagnostics.FullyScored)
	diagnostics.Truncated = diagnostics.Discarded.Total > 0
	return candidates, diagnostics
}

// buildCandidateIndex caches aggregate loads and tenant count used to rank candidates cheaply.
func buildCandidateIndex(state planningState, snapshot Snapshot) candidateIndex {
	index := candidateIndex{
		partitionLoads: make(map[int32]float64, len(snapshot.ActivePartitions)),
		replicaLoads:   make(map[string]float64, len(snapshot.ActiveReplicas)),
	}
	tenants := map[string]struct{}{}
	for _, partitionID := range snapshot.ActivePartitions {
		index.partitionLoads[partitionID] = 0
	}
	for _, replica := range snapshot.ActiveReplicas {
		index.replicaLoads[replica] = 0
	}
	for _, r := range state.ranges {
		index.partitionLoads[r.entry.PartitionID] += r.load
		index.totalLoad += r.load
		tenants[r.entry.TenantID] = struct{}{}
	}
	for partitionID, load := range index.partitionLoads {
		index.replicaLoads[snapshot.PartitionOwners[partitionID]] += load
	}
	index.partitionMean = index.totalLoad / float64(len(snapshot.ActivePartitions))
	index.replicaMean = index.totalLoad / float64(len(snapshot.ActiveReplicas))
	index.tenantCount = len(tenants)
	return index
}

// moveSourcePriority estimates the weighted balance relief available from relocating one range.
func moveSourcePriority(r rangeState, index candidateIndex, snapshot Snapshot, policy Policy) float64 {
	partitionSurplus := math.Max(0, index.partitionLoads[r.entry.PartitionID]-index.partitionMean)
	replica := snapshot.PartitionOwners[r.entry.PartitionID]
	replicaSurplus := math.Max(0, index.replicaLoads[replica]-index.replicaMean)
	return math.Min(r.load, partitionSurplus) +
		policy.Weights.ReplicaBalance*math.Min(r.load, replicaSurplus)
}

// selectMoveSources keeps all small source sets or the highest-surplus bounded subset.
func selectMoveSources(sources []rankedRange, limit int) []rankedRange {
	if len(sources) <= limit {
		sort.Slice(sources, func(i, j int) bool { return sources[i].key < sources[j].key })
		return sources
	}
	sort.Slice(sources, func(i, j int) bool {
		if math.Abs(sources[i].priority-sources[j].priority) > costEpsilon {
			return sources[i].priority > sources[j].priority
		}
		return sources[i].key < sources[j].key
	})
	selected := sources[:0]
	for _, source := range sources {
		if source.priority <= costEpsilon || len(selected) >= limit {
			break
		}
		selected = append(selected, source)
	}
	return selected
}

// selectDestinations ranks destinations by partition deficit, replica deficit,
// and "warmth": whether locality history says the tenant was hosted by the
// destination's logical replica within Policy.LocalityWindow. Warmth is a
// caller-maintained proxy for reusable cache state, not measured cache content.
func selectDestinations(
	r rangeState,
	partitions []int32,
	index candidateIndex,
	snapshot Snapshot,
	policy Policy,
	limit int,
) []rankedPartition {
	destinations := make([]rankedPartition, 0, len(partitions)-1)
	for _, partitionID := range partitions {
		if partitionID == r.entry.PartitionID {
			continue
		}
		partitionDeficit := math.Max(0, index.partitionMean-index.partitionLoads[partitionID])
		replica := snapshot.PartitionOwners[partitionID]
		replicaDeficit := math.Max(0, index.replicaMean-index.replicaLoads[replica])
		priority := math.Min(r.load, partitionDeficit) +
			policy.Weights.ReplicaBalance*math.Min(r.load, replicaDeficit)
		if recentlyHosted(snapshot, r.entry.TenantID, replica, policy.LocalityWindow) {
			priority += policy.Weights.LocalityMiss * r.load
		}
		destinations = append(destinations, rankedPartition{id: partitionID, priority: priority})
	}
	if len(destinations) <= limit {
		sort.Slice(destinations, func(i, j int) bool { return destinations[i].id < destinations[j].id })
		return destinations
	}
	sort.Slice(destinations, func(i, j int) bool {
		if math.Abs(destinations[i].priority-destinations[j].priority) > costEpsilon {
			return destinations[i].priority > destinations[j].priority
		}
		return destinations[i].id < destinations[j].id
	})
	return destinations[:limit]
}

// splitPriority estimates net resolution benefit after fragmentation and event costs.
func splitPriority(r rangeState, index candidateIndex, snapshot Snapshot, policy Policy) float64 {
	resolutionBenefit := policy.Weights.Resolution * r.load * rangeFraction(r.entry.Range) / 2
	fragmentationCost := policy.Weights.Fragmentation / float64(max(1, len(snapshot.ActivePartitions)*index.tenantCount))
	eventCost := policy.Weights.TransitionEvents * policy.ActionMultipliers.Split / float64(policy.MaxActions)
	return resolutionBenefit - fragmentationCost - eventCost
}

// mergePriority estimates net fragmentation benefit after resolution and relocation costs.
func mergePriority(
	merge candidate,
	left, right rangeState,
	state planningState,
	index candidateIndex,
	snapshot Snapshot,
	policy Policy,
) float64 {
	fragmentationBenefit := policy.Weights.Fragmentation / float64(max(1, len(snapshot.ActivePartitions)*index.tenantCount))
	beforeResolution := left.load*rangeFraction(left.entry.Range) + right.load*rangeFraction(right.entry.Range)
	afterResolution := (left.load + right.load) * rangeFraction(assignment.HashRange{Lo: left.entry.Range.Lo, Hi: right.entry.Range.Hi})
	resolutionCost := policy.Weights.Resolution * (afterResolution - beforeResolution)
	return fragmentationBenefit - resolutionCost - transitionCost(merge, state, snapshot, policy).WeightedTotal
}

// limitCandidates applies a stable highest-priority cutoff to one candidate set.
func limitCandidates(candidates []candidate, limit int) []candidate {
	if len(candidates) <= limit {
		return candidates
	}
	sort.Slice(candidates, func(i, j int) bool {
		if math.Abs(candidates[i].priority-candidates[j].priority) > costEpsilon {
			return candidates[i].priority > candidates[j].priority
		}
		return candidates[i].key() < candidates[j].key()
	})
	return candidates[:limit]
}

func candidateCounts(moves, splits, merges []candidate) CandidateCounts {
	return CandidateCounts{
		Move:  len(moves),
		Split: len(splits),
		Merge: len(merges),
		Total: len(moves) + len(splits) + len(merges),
	}
}

func countCandidates(candidates []candidate) CandidateCounts {
	var counts CandidateCounts
	for _, candidate := range candidates {
		switch candidate.kind {
		case ActionMove:
			counts.Move++
		case ActionSplit:
			counts.Split++
		case ActionMerge:
			counts.Merge++
		}
	}
	counts.Total = len(candidates)
	return counts
}

func subtractCounts(left, right CandidateCounts) CandidateCounts {
	return CandidateCounts{
		Move:  max(0, left.Move-right.Move),
		Split: max(0, left.Split-right.Split),
		Merge: max(0, left.Merge-right.Merge),
		Total: max(0, left.Total-right.Total),
	}
}

func rangeStateKey(r rangeState) string {
	return fmt.Sprintf("%s/%010d/%010d/%010d", r.entry.TenantID, r.entry.Range.Lo, r.entry.Range.Hi, r.entry.PartitionID)
}

// partitionMovedFrom identifies the non-target side relocated by a cross-partition merge.
func partitionMovedFrom(left, right rangeState, target int32) int32 {
	if left.entry.PartitionID == right.entry.PartitionID {
		return target
	}
	if target == left.entry.PartitionID {
		return right.entry.PartitionID
	}
	return left.entry.PartitionID
}

// key returns the stable identity used to order candidates and break equal-cost ties.
func (c candidate) key() string {
	return fmt.Sprintf("%s/%s/%010d/%010d/%s/%010d",
		c.kind, c.tenantID, c.r.Lo, c.r.Hi, formatOptionalRange(c.other), c.toPartition)
}

func formatOptionalRange(r assignment.HashRange) string {
	if r == (assignment.HashRange{}) {
		return "-"
	}
	return fmt.Sprintf("%010d-%010d", r.Lo, r.Hi)
}

// project returns an isolated planning state with one candidate applied.
func project(state planningState, c candidate) planningState {
	next := state.clone()
	switch c.kind {
	case ActionMove:
		next.ranges[c.index].entry.PartitionID = c.toPartition

	case ActionSplit:
		parent := next.ranges[c.index]
		midpoint := parent.entry.Range.Lo + uint32((uint64(parent.entry.Range.Hi)-uint64(parent.entry.Range.Lo))/2)
		left := rangeState{
			entry: assignment.Entry{
				TenantID:    parent.entry.TenantID,
				Range:       assignment.HashRange{Lo: parent.entry.Range.Lo, Hi: midpoint},
				PartitionID: parent.entry.PartitionID,
			},
			load:     parent.load / 2,
			observed: false,
		}
		right := rangeState{
			entry: assignment.Entry{
				TenantID:    parent.entry.TenantID,
				Range:       assignment.HashRange{Lo: midpoint + 1, Hi: parent.entry.Range.Hi},
				PartitionID: parent.entry.PartitionID,
			},
			load:     parent.load - left.load,
			observed: false,
		}
		replacement := make([]rangeState, 0, len(next.ranges)+1)
		replacement = append(replacement, next.ranges[:c.index]...)
		replacement = append(replacement, left, right)
		replacement = append(replacement, next.ranges[c.index+1:]...)
		next.ranges = replacement

	case ActionMerge:
		left, right := next.ranges[c.index], next.ranges[c.otherIndex]
		merged := rangeState{
			entry: assignment.Entry{
				TenantID:    left.entry.TenantID,
				Range:       assignment.HashRange{Lo: left.entry.Range.Lo, Hi: right.entry.Range.Hi},
				PartitionID: c.toPartition,
			},
			load:     left.load + right.load,
			observed: true,
		}
		replacement := make([]rangeState, 0, len(next.ranges)-1)
		replacement = append(replacement, next.ranges[:c.index]...)
		replacement = append(replacement, merged)
		replacement = append(replacement, next.ranges[c.otherIndex+1:]...)
		next.ranges = replacement
	}
	return next
}
