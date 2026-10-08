// SPDX-License-Identifier: AGPL-3.0-only

package scallop

// This file builds bounded range and partition candidates and projects their effects.

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
	partitionID   int32
	fromReplica   string
	toReplica     string

	load              float64
	movedLoad         float64
	movedHashFraction float64
	tenantLoads       map[string]float64
	priority          float64
}

// candidateIndex is the shared cost and topology view used by discovery,
// exact delta scoring, and committed-action updates.
type candidateIndex struct {
	partitionLoads    map[int32]float64
	replicaLoads      map[string]float64
	partitionOwners   map[int32]string
	partitionMean     float64
	replicaMean       float64
	partitionPeak     float64
	replicaPeak       float64
	partitionPressure loadPeak[int32]
	replicaPressure   loadPeak[string]
	totals            planTotals
	tenantRangeCounts map[string]int
	resolution        float64
	rangeCount        int

	partitions              []int32
	replicas                []string
	partitionsByDeficit     []int32
	replicasByDeficit       []string
	rangeIndexesByPartition map[int32][]int
	rangeIndexesByReplica   map[string][]int
	partitionIDsByReplica   map[string][]int32
	rangeIndexesByTenant    map[string][]int
	tenantIDs               []string
	splittableRangeIndexes  []int
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

type rankedReplica struct {
	id       string
	priority float64
}

// candidateGeneration describes which pairwise-interacting actions remain
// available. Splits are not generated here; see selectSplitWave.
type candidateGeneration struct {
	move                   bool
	merge                  bool
	movePartition          bool
	selectedMovesByTenant  map[string]int
	movePerTenant          int
	selectedMergesByTenant map[string]int
	mergePerTenant         int
}

// generateCandidates returns a deterministic bounded shortlist of legal move, merge, and partition actions.
func generateCandidates(state planningState, snapshot Snapshot, policy Policy) ([]candidate, CandidateSearchDiagnostics) {
	return generateCandidatesFor(state, totalsOf(state), snapshot, policy, candidateGeneration{
		move:           true,
		merge:          true,
		movePartition:  true,
		movePerTenant:  len(state.ranges) + 1,
		mergePerTenant: len(state.ranges) + 1,
	})
}

// generateCandidatesFor omits action kinds whose execution budgets are exhausted in the current plan.
func generateCandidatesFor(
	state planningState,
	totals planTotals,
	snapshot Snapshot,
	policy Policy,
	generation candidateGeneration,
) ([]candidate, CandidateSearchDiagnostics) {
	index := buildCandidateIndex(state, totals, snapshot)
	return generateCandidatesFromIndex(state, index, snapshot, policy, generation)
}

// mergeCandidateForTarget describes one legal destination for an adjacent pair.
func mergeCandidateForTarget(index int, left, right rangeState, target int32) candidate {
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
	return candidate{
		kind:              ActionMerge,
		index:             index,
		otherIndex:        index + 1,
		tenantID:          left.entry.TenantID,
		r:                 left.entry.Range,
		other:             right.entry.Range,
		fromPartition:     partitionMovedFrom(left, right, target),
		toPartition:       target,
		load:              left.load + right.load,
		movedLoad:         movedLoad,
		movedHashFraction: movedHashFraction,
	}
}

// moveSourcePriority estimates the weighted balance relief available from relocating one range.
func moveSourcePriority(r rangeState, index candidateIndex, policy Policy) float64 {
	partitionSurplus := math.Max(0, index.partitionLoads[r.entry.PartitionID]-index.partitionMean)
	replica := index.partitionOwners[r.entry.PartitionID]
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
	state planningState,
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
		replica := state.partitionOwners[partitionID]
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

// splitPriority is the exact weighted cost reduction of one split. Both
// children stay on the parent partition, so balance is unchanged; only
// resolution, fragmentation, and the split's own transition cost move.
func splitPriority(r rangeState, totals planTotals, policy Policy, transition float64) float64 {
	resolutionBenefit := policy.Weights.Resolution * r.load * rangeFraction(r.entry.Range) / 2
	fragmentationCost := policy.Weights.Fragmentation / float64(max(1, totals.tenants))
	return resolutionBenefit - fragmentationCost - transition
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
	fragmentationBenefit := policy.Weights.Fragmentation / float64(max(1, index.totals.tenants))
	beforeResolution := left.load*rangeFraction(left.entry.Range) + right.load*rangeFraction(right.entry.Range)
	afterResolution := (left.load + right.load) * rangeFraction(assignment.HashRange{Lo: left.entry.Range.Lo, Hi: right.entry.Range.Hi})
	resolutionCost := policy.Weights.Resolution * (afterResolution - beforeResolution)
	balanceBenefit := mergeBalanceBenefit(merge, index, state, policy)
	return balanceBenefit + fragmentationBenefit -
		resolutionCost - transitionCost(merge, state, index.totals, snapshot, policy).WeightedTotal
}

// mergeBalanceBenefit computes exact affected-load relief without projecting a full assignment.
func mergeBalanceBenefit(
	merge candidate,
	index candidateIndex,
	state planningState,
	policy Policy,
) float64 {
	if merge.fromPartition == merge.toPartition || merge.movedLoad == 0 {
		return 0
	}
	beforePartition := index.partitionPeak
	afterPartition := peakAfterTransfer(
		index.partitionLoads, index.partitionPressure,
		merge.fromPartition, merge.toPartition, merge.movedLoad, index.partitionMean,
	)
	partitionBenefit := beforePartition - afterPartition

	fromReplica := state.partitionOwners[merge.fromPartition]
	toReplica := state.partitionOwners[merge.toPartition]
	if fromReplica == toReplica {
		return partitionBenefit
	}
	beforeReplica := index.replicaPeak
	afterReplica := peakAfterTransfer(
		index.replicaLoads, index.replicaPressure,
		fromReplica, toReplica, merge.movedLoad, index.replicaMean,
	)
	replicaBenefit := beforeReplica - afterReplica
	return partitionBenefit + policy.Weights.ReplicaBalance*replicaBenefit
}

// peakExcessWithMean computes max/mean minus one when the stable mean is already known.
func peakExcessWithMean[K comparable](loads map[K]float64, mean float64) float64 {
	if mean <= 0 {
		return 0
	}
	maximum := 0.0
	for _, load := range loads {
		maximum = math.Max(maximum, load)
	}
	return maximum/mean - 1
}

// limitMergesFair reserves shortlist capacity round-robin across the most fragmented tenants.
func limitMergesFair(candidates []candidate, rangeCounts map[string]int, limit int) []candidate {
	if len(candidates) <= limit {
		return candidates
	}
	byTenant := map[string][]candidate{}
	for _, candidate := range candidates {
		byTenant[candidate.tenantID] = append(byTenant[candidate.tenantID], candidate)
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
	selected := make([]candidate, 0, limit)
	reservedByTenant := make(map[string]int, len(tenants))
	reservation := max(1, limit/len(tenants))
	for _, tenantID := range tenants {
		count := min(reservation, len(byTenant[tenantID]), limit-len(selected))
		selected = append(selected, byTenant[tenantID][:count]...)
		reservedByTenant[tenantID] = count
		if len(selected) == limit {
			return selected
		}
	}

	remaining := make([]candidate, 0, len(candidates)-len(selected))
	for _, tenantID := range tenants {
		remaining = append(remaining, byTenant[tenantID][reservedByTenant[tenantID]:]...)
	}
	sortCandidatesByPriority(remaining)
	selected = append(selected, remaining[:min(limit-len(selected), len(remaining))]...)
	return selected
}

// selectPartitionSources keeps the bounded partitions with the greatest owning-replica surplus.
func selectPartitionSources(sources []rankedPartition, limit int) []rankedPartition {
	if len(sources) <= limit {
		sort.Slice(sources, func(i, j int) bool { return sources[i].id < sources[j].id })
		return sources
	}
	sort.Slice(sources, func(i, j int) bool {
		if math.Abs(sources[i].priority-sources[j].priority) > costEpsilon {
			return sources[i].priority > sources[j].priority
		}
		return sources[i].id < sources[j].id
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

// sortCandidatesByPriority orders a shortlist by estimated benefit and stable identity.
func sortCandidatesByPriority(candidates []candidate) {
	sort.Slice(candidates, func(i, j int) bool {
		if math.Abs(candidates[i].priority-candidates[j].priority) > costEpsilon {
			return candidates[i].priority > candidates[j].priority
		}
		return candidates[i].key() < candidates[j].key()
	})
}

// limitCandidates applies a stable highest-priority cutoff to one candidate set.
func limitCandidates(candidates []candidate, limit int) []candidate {
	if len(candidates) <= limit {
		return candidates
	}
	sortCandidatesByPriority(candidates)
	return candidates[:limit]
}

func candidateCounts(moves, merges, partitionMoves []candidate) CandidateCounts {
	return CandidateCounts{
		Move:          len(moves),
		Merge:         len(merges),
		MovePartition: len(partitionMoves),
		Total:         len(moves) + len(merges) + len(partitionMoves),
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
		case ActionMovePartition:
			counts.MovePartition++
		}
	}
	counts.Total = len(candidates)
	return counts
}

func subtractCounts(left, right CandidateCounts) CandidateCounts {
	return CandidateCounts{
		Move:          max(0, left.Move-right.Move),
		Split:         max(0, left.Split-right.Split),
		Merge:         max(0, left.Merge-right.Merge),
		MovePartition: max(0, left.MovePartition-right.MovePartition),
		Total:         max(0, left.Total-right.Total),
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
	if c.kind == ActionMovePartition {
		return fmt.Sprintf("%s/%010d/%s/%s", c.kind, c.partitionID, c.fromReplica, c.toReplica)
	}
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
		left, right := splitChildren(next.ranges[c.index])
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

	case ActionMovePartition:
		next.partitionOwners[c.partitionID] = c.toReplica
	}
	return next
}

// splitChildren halves a range at its hash midpoint, assumes evenly spread
// load, and keeps both unobserved children on the parent's partition.
func splitChildren(parent rangeState) (rangeState, rangeState) {
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
	return left, right
}
