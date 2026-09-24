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

// candidateIndex is the cheap first-stage view used to avoid fully projecting
// every legal action. Candidate generation first aggregates current partition
// and replica load here, uses those aggregates to rank likely range and
// partition actions, and then sends only the bounded shortlist to Plan for exact
// projection, validation, and cost comparison. The index is rebuilt after
// every selected action, so priorities always describe the current projected
// state rather than the original snapshot.
type candidateIndex struct {
	partitionLoads    map[int32]float64
	replicaLoads      map[string]float64
	partitionOwners   map[int32]string
	partitionMean     float64
	replicaMean       float64
	partitionPeak     float64
	replicaPeak       float64
	totalLoad         float64
	tenantCount       int
	tenantRangeCounts map[string]int
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

type candidateGeneration struct {
	move                   bool
	split                  bool
	merge                  bool
	movePartition          bool
	selectedMergesByTenant map[string]int
	mergePerTenant         int
}

// generateCandidates returns a deterministic bounded shortlist of legal range and partition actions.
func generateCandidates(state planningState, snapshot Snapshot, policy Policy) ([]candidate, CandidateSearchDiagnostics) {
	return generateCandidatesFor(state, snapshot, policy, candidateGeneration{
		move:           true,
		split:          true,
		merge:          true,
		movePartition:  true,
		mergePerTenant: len(state.ranges) + 1,
	})
}

// generateCandidatesFor omits action kinds whose execution budgets are exhausted in the current plan.
func generateCandidatesFor(
	state planningState,
	snapshot Snapshot,
	policy Policy,
	generation candidateGeneration,
) ([]candidate, CandidateSearchDiagnostics) {
	partitions := append([]int32(nil), snapshot.ActivePartitions...)
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	replicas := append([]string(nil), snapshot.ActiveReplicas...)
	sort.Strings(replicas)
	limits := policy.CandidateSearch
	diagnostics := CandidateSearchDiagnostics{Limits: limits}
	index := buildCandidateIndex(state, snapshot)

	moveSources := make([]rankedRange, 0, len(state.ranges))
	splits := make([]candidate, 0, min(len(state.ranges), limits.MaxSplitCandidates))
	for i, r := range state.ranges {
		if !r.observed {
			continue
		}
		if generation.move && len(partitions) > 1 {
			diagnostics.LegalMoveSources++
			diagnostics.LegalMoveDestinations += len(partitions) - 1
			diagnostics.Legal.Move += len(partitions) - 1
			moveSources = append(moveSources, rankedRange{
				index:    i,
				priority: moveSourcePriority(r, index, policy),
				key:      rangeStateKey(r),
			})
		}
		if generation.split && r.entry.Range.Size() > 1 {
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
		destinations := selectDestinations(r, partitions, index, state, snapshot, policy, limits.MaxDestinationsPerRange)
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
	for i := 0; generation.merge && i+1 < len(state.ranges); i++ {
		left, right := state.ranges[i], state.ranges[i+1]
		if !left.observed || !right.observed {
			continue
		}
		if left.entry.TenantID != right.entry.TenantID ||
			generation.selectedMergesByTenant[left.entry.TenantID] >= generation.mergePerTenant ||
			left.entry.Range.Hi == ^uint32(0) ||
			left.entry.Range.Hi+1 != right.entry.Range.Lo {
			continue
		}

		targets := []int32{left.entry.PartitionID}
		if right.entry.PartitionID != left.entry.PartitionID {
			targets = append(targets, right.entry.PartitionID)
		}
		for _, target := range targets {
			merge := mergeCandidateForTarget(i, left, right, target)
			merge.priority = mergePriority(merge, left, right, state, index, snapshot, policy)
			merges = append(merges, merge)
			diagnostics.Legal.Merge++
		}
	}

	moves = limitCandidates(moves, len(moves))
	diagnostics.DiscardedByBudget.Splits = max(0, len(splits)-limits.MaxSplitCandidates)
	diagnostics.DiscardedByBudget.Merges = max(0, len(merges)-limits.MaxMergeCandidates)
	splits = limitCandidates(splits, limits.MaxSplitCandidates)
	merges = limitMergesFair(merges, index.tenantRangeCounts, limits.MaxMergeCandidates)

	partitionMoves := make([]candidate, 0, limits.MaxPartitionMoveCandidates)
	partitionSources := make([]rankedPartition, 0, len(partitions))
	if generation.movePartition && len(replicas) > 1 {
		for _, partitionID := range partitions {
			diagnostics.LegalPartitionMoveSources++
			diagnostics.LegalPartitionMoveDestinations += len(replicas) - 1
			diagnostics.Legal.MovePartition += len(replicas) - 1
			sourceReplica := state.partitionOwners[partitionID]
			surplus := math.Max(0, index.replicaLoads[sourceReplica]-index.replicaMean)
			partitionSources = append(partitionSources, rankedPartition{
				id:       partitionID,
				priority: math.Min(index.partitionLoads[partitionID], surplus),
			})
		}
	}
	selectedPartitionSources := selectPartitionSources(partitionSources, limits.MaxPartitionMoveSources)
	diagnostics.DiscardedByBudget.PartitionMoveSources =
		(len(partitionSources) - len(selectedPartitionSources)) * max(0, len(replicas)-1)
	for _, source := range selectedPartitionSources {
		sourceReplica := state.partitionOwners[source.id]
		destinations := selectReplicaDestinations(source.id, replicas, index, state, limits.MaxDestinationsPerPartition)
		diagnostics.DiscardedByBudget.PartitionMoveDestinations += len(replicas) - 1 - len(destinations)
		tenantLoads := partitionTenantLoads(state, source.id)
		for _, destination := range destinations {
			move := candidate{
				kind:          ActionMovePartition,
				partitionID:   source.id,
				fromPartition: source.id,
				toPartition:   source.id,
				fromReplica:   sourceReplica,
				toReplica:     destination.id,
				load:          index.partitionLoads[source.id],
				movedLoad:     index.partitionLoads[source.id],
				tenantLoads:   tenantLoads,
				priority:      source.priority + destination.priority,
			}
			move.priority -= transitionCost(move, state, snapshot, policy).WeightedTotal
			partitionMoves = append(partitionMoves, move)
		}
	}
	diagnostics.DiscardedByBudget.PartitionMoves =
		max(0, len(partitionMoves)-limits.MaxPartitionMoveCandidates)
	partitionMoves = limitCandidates(partitionMoves, limits.MaxPartitionMoveCandidates)
	diagnostics.Admitted = candidateCounts(moves, splits, merges, partitionMoves)

	candidates := make([]candidate, 0, diagnostics.Admitted.Total)
	candidates = append(candidates, moves...)
	candidates = append(candidates, splits...)
	candidates = append(candidates, merges...)
	candidates = append(candidates, partitionMoves...)
	diagnostics.DiscardedByBudget.FullyScored = max(0, len(candidates)-limits.MaxFullyScored)
	candidates = limitCandidates(candidates, limits.MaxFullyScored)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].key() < candidates[j].key()
	})
	diagnostics.FullyScored = countCandidates(candidates)
	diagnostics.Legal.Total = diagnostics.Legal.Move + diagnostics.Legal.Split +
		diagnostics.Legal.Merge + diagnostics.Legal.MovePartition
	diagnostics.Discarded = subtractCounts(diagnostics.Legal, diagnostics.FullyScored)
	diagnostics.Truncated = diagnostics.Discarded.Total > 0
	return candidates, diagnostics
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

// buildCandidateIndex caches aggregate loads and tenant count used to rank candidates cheaply.
func buildCandidateIndex(state planningState, snapshot Snapshot) candidateIndex {
	index := candidateIndex{
		partitionLoads:    make(map[int32]float64, len(snapshot.ActivePartitions)),
		replicaLoads:      make(map[string]float64, len(snapshot.ActiveReplicas)),
		partitionOwners:   state.partitionOwners,
		tenantRangeCounts: map[string]int{},
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
		index.tenantRangeCounts[r.entry.TenantID]++
	}
	partitions := append([]int32(nil), snapshot.ActivePartitions...)
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	for _, partitionID := range partitions {
		load := index.partitionLoads[partitionID]
		index.replicaLoads[state.partitionOwners[partitionID]] += load
	}
	index.partitionMean = index.totalLoad / float64(len(snapshot.ActivePartitions))
	index.replicaMean = index.totalLoad / float64(len(snapshot.ActiveReplicas))
	index.partitionPeak = peakExcessWithMean(index.partitionLoads, index.partitionMean)
	index.replicaPeak = peakExcessWithMean(index.replicaLoads, index.replicaMean)
	index.tenantCount = len(tenants)
	return index
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

// splitPriority estimates net resolution benefit after fragmentation and event costs.
func splitPriority(r rangeState, index candidateIndex, snapshot Snapshot, policy Policy) float64 {
	resolutionBenefit := policy.Weights.Resolution * r.load * rangeFraction(r.entry.Range) / 2
	fragmentationCost := policy.Weights.Fragmentation / float64(max(1, index.tenantCount))
	eventCost := policy.Weights.TransitionEvents * policy.ActionMultipliers.Split /
		float64(max(1, len(snapshot.Assignment.Entries)))
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
	fragmentationBenefit := policy.Weights.Fragmentation / float64(max(1, index.tenantCount))
	beforeResolution := left.load*rangeFraction(left.entry.Range) + right.load*rangeFraction(right.entry.Range)
	afterResolution := (left.load + right.load) * rangeFraction(assignment.HashRange{Lo: left.entry.Range.Lo, Hi: right.entry.Range.Hi})
	resolutionCost := policy.Weights.Resolution * (afterResolution - beforeResolution)
	balanceBenefit := mergeBalanceBenefit(merge, index, state, policy)
	return balanceBenefit + fragmentationBenefit -
		resolutionCost - transitionCost(merge, state, snapshot, policy).WeightedTotal
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
	afterPartition := peakExcessAfterTransfer(
		index.partitionLoads, merge.fromPartition, merge.toPartition, merge.movedLoad, index.partitionMean,
	)
	partitionBenefit := beforePartition - afterPartition

	fromReplica := state.partitionOwners[merge.fromPartition]
	toReplica := state.partitionOwners[merge.toPartition]
	if fromReplica == toReplica {
		return partitionBenefit
	}
	beforeReplica := index.replicaPeak
	afterReplica := peakExcessAfterTransfer(
		index.replicaLoads, fromReplica, toReplica, merge.movedLoad, index.replicaMean,
	)
	replicaBenefit := beforeReplica - afterReplica
	return partitionBenefit + policy.Weights.ReplicaBalance*replicaBenefit
}

// peakExcessAfterTransfer evaluates an affected-load move without cloning an aggregate map.
func peakExcessAfterTransfer[K comparable](
	loads map[K]float64,
	from, to K,
	amount, mean float64,
) float64 {
	if from == to {
		return peakExcessWithMean(loads, mean)
	}
	if mean <= 0 {
		return 0
	}
	maximum := 0.0
	for key, load := range loads {
		switch key {
		case from:
			load -= amount
		case to:
			load += amount
		}
		maximum = math.Max(maximum, load)
	}
	return maximum/mean - 1
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

// selectReplicaDestinations ranks replica owners by their current load deficit.
func selectReplicaDestinations(
	partitionID int32,
	replicas []string,
	index candidateIndex,
	state planningState,
	limit int,
) []rankedReplica {
	source := state.partitionOwners[partitionID]
	load := index.partitionLoads[partitionID]
	destinations := make([]rankedReplica, 0, len(replicas)-1)
	for _, replica := range replicas {
		if replica == source {
			continue
		}
		deficit := math.Max(0, index.replicaMean-index.replicaLoads[replica])
		destinations = append(destinations, rankedReplica{id: replica, priority: math.Min(load, deficit)})
	}
	sort.Slice(destinations, func(i, j int) bool {
		if math.Abs(destinations[i].priority-destinations[j].priority) > costEpsilon {
			return destinations[i].priority > destinations[j].priority
		}
		return destinations[i].id < destinations[j].id
	})
	return destinations[:min(limit, len(destinations))]
}

// partitionTenantLoads captures the tenant composition needed for partition-move locality cost.
func partitionTenantLoads(state planningState, partitionID int32) map[string]float64 {
	loads := map[string]float64{}
	for _, r := range state.ranges {
		if r.entry.PartitionID == partitionID {
			loads[r.entry.TenantID] += r.load
		}
	}
	return loads
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

func candidateCounts(moves, splits, merges, partitionMoves []candidate) CandidateCounts {
	return CandidateCounts{
		Move:          len(moves),
		Split:         len(splits),
		Merge:         len(merges),
		MovePartition: len(partitionMoves),
		Total:         len(moves) + len(splits) + len(merges) + len(partitionMoves),
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

	case ActionMovePartition:
		next.partitionOwners[c.partitionID] = c.toReplica
	}
	return next
}
