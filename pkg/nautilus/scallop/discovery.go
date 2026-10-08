// SPDX-License-Identifier: AGPL-3.0-only

package scallop

import (
	"math"
	"sort"
)

type partitionOffender struct {
	id       int32
	priority float64
}

type replicaOffender struct {
	id       string
	priority float64
}

type tenantOffender struct {
	id         string
	rangeCount int
}

// generateCandidatesFromIndex expands separate bounded offender frontiers for
// partition pressure, replica pressure, and fragmentation. Only possibilities
// reached through those frontiers are streamed into the bounded collectors.
func generateCandidatesFromIndex(
	state planningState,
	index candidateIndex,
	snapshot Snapshot,
	policy Policy,
	generation candidateGeneration,
) ([]candidate, CandidateSearchDiagnostics) {
	limits := policy.CandidateSearch
	diagnostics := CandidateSearchDiagnostics{Limits: limits}

	moves := discoverRangeMoves(state, index, snapshot, policy, generation, &diagnostics)
	merges := discoverMerges(state, index, snapshot, policy, generation, &diagnostics)
	partitionMoves := discoverPartitionMoves(state, index, snapshot, policy, generation, &diagnostics)

	diagnostics.Work.Yielded = candidateCounts(moves, merges, partitionMoves)
	diagnostics.Admitted = diagnostics.Work.Yielded
	candidates := make([]candidate, 0, diagnostics.Admitted.Total)
	candidates = append(candidates, moves...)
	candidates = append(candidates, merges...)
	candidates = append(candidates, partitionMoves...)
	diagnostics.DiscardedByBudget.FullyScored = max(0, len(candidates)-limits.MaxFullyScored)
	candidates = limitCandidates(candidates, limits.MaxFullyScored)
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].key() < candidates[j].key() })
	diagnostics.FullyScored = countCandidates(candidates)
	diagnostics.Legal.Total = diagnostics.Legal.Move + diagnostics.Legal.Merge + diagnostics.Legal.MovePartition
	diagnostics.Discarded = subtractCounts(diagnostics.Legal, diagnostics.FullyScored)
	diagnostics.Truncated = diagnostics.Discarded.Total > 0 || diagnostics.Pruned.total() > 0
	return candidates, diagnostics
}

func discoverRangeMoves(
	state planningState,
	index candidateIndex,
	snapshot Snapshot,
	policy Policy,
	generation candidateGeneration,
	diagnostics *CandidateSearchDiagnostics,
) []candidate {
	if !generation.move || len(index.partitions) < 2 {
		return nil
	}
	limits := policy.CandidateSearch
	sourceIndexes := map[int]struct{}{}
	considerRange := func(rangeIndex int) {
		if _, exists := sourceIndexes[rangeIndex]; exists {
			return
		}
		r := state.ranges[rangeIndex]
		if !r.observed || generation.selectedMovesByTenant[r.entry.TenantID] >= generation.movePerTenant {
			return
		}
		sourceIndexes[rangeIndex] = struct{}{}
		diagnostics.Work.RangesInspected++
	}

	// Small snapshots retain complete source coverage.
	if len(state.ranges) <= limits.MaxMoveSources {
		for rangeIndex := range state.ranges {
			considerRange(rangeIndex)
		}
	} else {
		partitions := partitionOffenders(index)
		partitionLimit := min(len(partitions), limits.MaxPartitionOffenders)
		diagnostics.Pruned.PartitionOffenders += len(partitions) - partitionLimit
		diagnostics.Work.OffendersExpanded.Partitions += partitionLimit
		for _, offender := range partitions[:partitionLimit] {
			ranges := index.rangeIndexesByPartition[offender.id]
			rangeLimit := min(len(ranges), limits.MaxRangesPerOffender)
			diagnostics.Pruned.Ranges += len(ranges) - rangeLimit
			for _, rangeIndex := range ranges[:rangeLimit] {
				considerRange(rangeIndex)
			}
		}

		replicas := replicaOffenders(index)
		replicaLimit := min(len(replicas), limits.MaxReplicaOffenders)
		diagnostics.Pruned.ReplicaOffenders += len(replicas) - replicaLimit
		diagnostics.Work.OffendersExpanded.Replicas += replicaLimit
		for _, offender := range replicas[:replicaLimit] {
			ranges := index.rangeIndexesByReplica[offender.id]
			rangeLimit := min(len(ranges), limits.MaxRangesPerOffender)
			diagnostics.Pruned.Ranges += len(ranges) - rangeLimit
			for _, rangeIndex := range ranges[:rangeLimit] {
				considerRange(rangeIndex)
			}
		}
	}

	sources := make([]rankedRange, 0, len(sourceIndexes))
	for rangeIndex := range sourceIndexes {
		r := state.ranges[rangeIndex]
		sources = append(sources, rankedRange{
			index:    rangeIndex,
			priority: moveSourcePriority(r, index, policy),
			key:      rangeStateKey(r),
		})
	}
	diagnostics.LegalMoveSources += len(sources)
	diagnostics.LegalMoveDestinations += len(sources) * (len(index.partitions) - 1)
	diagnostics.Legal.Move += len(sources) * (len(index.partitions) - 1)
	selectedSources := selectMoveSources(sources, limits.MaxMoveSources)
	diagnostics.DiscardedByBudget.MoveSources +=
		(len(sources) - len(selectedSources)) * max(0, len(index.partitions)-1)

	moves := make([]candidate, 0, len(selectedSources)*limits.MaxDestinationsPerRange)
	for _, source := range selectedSources {
		r := state.ranges[source.index]
		destinations, inspected, pruned := discoverRangeDestinations(r, index, snapshot, policy)
		diagnostics.Work.DestinationsInspected += inspected
		diagnostics.Pruned.DestinationPartitions += pruned
		destinations = limitRankedPartitions(destinations, limits.MaxDestinationsPerRange)
		diagnostics.DiscardedByBudget.Destinations += len(index.partitions) - 1 - len(destinations)
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
	return moves
}

func discoverRangeDestinations(
	r rangeState,
	index candidateIndex,
	snapshot Snapshot,
	policy Policy,
) ([]rankedPartition, int, int) {
	limit := policy.CandidateSearch.MaxDestinationPartitions
	if len(index.partitions)-1 <= limit {
		destinations := selectDestinations(r, index.partitions, index, planningState{partitionOwners: index.partitionOwners}, snapshot, policy, len(index.partitions))
		return destinations, len(destinations), 0
	}

	ids := make([]int32, 0, limit)
	seen := map[int32]struct{}{r.entry.PartitionID: {}}
	add := func(partitionID int32) {
		if len(ids) >= limit {
			return
		}
		if _, exists := seen[partitionID]; exists {
			return
		}
		seen[partitionID] = struct{}{}
		ids = append(ids, partitionID)
	}
	if warm := snapshot.LastHostedAt[r.entry.TenantID]; len(warm) > 0 {
		replicas := make([]string, 0, len(warm))
		for replica := range warm {
			replicas = append(replicas, replica)
		}
		sort.Strings(replicas)
		warmLimit := max(1, limit/4)
		for _, replica := range replicas {
			partitions := index.partitionIDsByReplica[replica]
			if len(partitions) > 0 {
				add(leastLoadedPartition(partitions, index.partitionLoads))
			}
			if len(ids) == warmLimit {
				break
			}
		}
	}
	// Interleave partition and replica deficits so neither balance term hides
	// the other. A reserved warm-replica slice keeps locality from being hidden
	// by either balance frontier.
	for i := 0; len(ids) < limit && (i < len(index.partitionsByDeficit) || i < len(index.replicasByDeficit)); i++ {
		if i < len(index.partitionsByDeficit) {
			add(index.partitionsByDeficit[i])
		}
		if i < len(index.replicasByDeficit) {
			partitions := index.partitionIDsByReplica[index.replicasByDeficit[i]]
			if len(partitions) > 0 {
				add(leastLoadedPartition(partitions, index.partitionLoads))
			}
		}
	}
	destinations := selectDestinations(r, ids, index, planningState{partitionOwners: index.partitionOwners}, snapshot, policy, len(ids))
	return destinations, len(ids), max(0, len(index.partitions)-1-len(ids))
}

func discoverMerges(
	state planningState,
	index candidateIndex,
	snapshot Snapshot,
	policy Policy,
	generation candidateGeneration,
	diagnostics *CandidateSearchDiagnostics,
) []candidate {
	if !generation.merge {
		return nil
	}
	limits := policy.CandidateSearch
	tenants := make([]tenantOffender, 0, len(index.tenantIDs))
	for _, tenantID := range index.tenantIDs {
		if index.tenantRangeCounts[tenantID] > 1 &&
			generation.selectedMergesByTenant[tenantID] < generation.mergePerTenant {
			tenants = append(tenants, tenantOffender{id: tenantID, rangeCount: index.tenantRangeCounts[tenantID]})
		}
	}
	sort.Slice(tenants, func(i, j int) bool {
		if tenants[i].rangeCount != tenants[j].rangeCount {
			return tenants[i].rangeCount > tenants[j].rangeCount
		}
		return tenants[i].id < tenants[j].id
	})
	tenantLimit := min(len(tenants), limits.MaxTenantOffenders)
	diagnostics.Pruned.TenantOffenders += len(tenants) - tenantLimit
	diagnostics.Work.OffendersExpanded.Tenants += tenantLimit

	// Keep enough alternatives to survive pair overlap and the two possible
	// destinations of a cross-partition edge while still bounding memory.
	// When few tenants are expanded, distribute the whole collector capacity
	// among them so small snapshots retain complete candidate coverage.
	perTenantCap := max(
		8,
		generation.mergePerTenant*8,
		limits.MaxMergeCandidates/max(1, tenantLimit),
	)
	merges := make([]candidate, 0, min(limits.MaxMergeCandidates, tenantLimit*perTenantCap))
	for _, offender := range tenants[:tenantLimit] {
		indexes := index.rangeIndexesByTenant[offender.id]
		edgeLimit := min(max(0, len(indexes)-1), limits.MaxAdjacencyPerTenant)
		diagnostics.Pruned.AdjacencyEdges += max(0, len(indexes)-1-edgeLimit)
		tenantCandidates := make([]candidate, 0, edgeLimit*2)
		for edge := 0; edge < edgeLimit; edge++ {
			leftIndex, rightIndex := indexes[edge], indexes[edge+1]
			diagnostics.Work.AdjacencyEdgesInspected++
			if rightIndex != leftIndex+1 {
				continue
			}
			left, right := state.ranges[leftIndex], state.ranges[rightIndex]
			if !left.observed || !right.observed ||
				left.entry.Range.Hi == ^uint32(0) ||
				left.entry.Range.Hi+1 != right.entry.Range.Lo {
				continue
			}
			targets := []int32{left.entry.PartitionID}
			if right.entry.PartitionID != left.entry.PartitionID {
				targets = append(targets, right.entry.PartitionID)
			}
			for _, target := range targets {
				merge := mergeCandidateForTarget(leftIndex, left, right, target)
				merge.priority = mergePriority(merge, left, right, state, index, snapshot, policy)
				tenantCandidates = append(tenantCandidates, merge)
				diagnostics.Legal.Merge++
			}
		}
		merges = append(merges, limitCandidates(tenantCandidates, perTenantCap)...)
	}
	selected := limitMergesFair(merges, index.tenantRangeCounts, limits.MaxMergeCandidates)
	diagnostics.DiscardedByBudget.Merges = max(0, diagnostics.Legal.Merge-len(selected))
	return selected
}

func discoverPartitionMoves(
	state planningState,
	index candidateIndex,
	snapshot Snapshot,
	policy Policy,
	generation candidateGeneration,
	diagnostics *CandidateSearchDiagnostics,
) []candidate {
	if !generation.movePartition || len(index.replicas) < 2 {
		return nil
	}
	limits := policy.CandidateSearch
	replicas := replicaOffenders(index)
	replicaLimit := min(len(replicas), limits.MaxReplicaOffenders)
	diagnostics.Pruned.ReplicaOffenders += len(replicas) - replicaLimit
	diagnostics.Work.OffendersExpanded.Replicas += replicaLimit

	sources := make([]rankedPartition, 0, limits.MaxPartitionMoveSources)
	seen := map[int32]struct{}{}
	for _, offender := range replicas[:replicaLimit] {
		partitionIDs := index.partitionIDsByReplica[offender.id]
		partitionLimit := min(len(partitionIDs), limits.MaxRangesPerOffender)
		diagnostics.Pruned.Ranges += len(partitionIDs) - partitionLimit
		for _, partitionID := range partitionIDs[:partitionLimit] {
			if _, exists := seen[partitionID]; exists {
				continue
			}
			seen[partitionID] = struct{}{}
			sources = append(sources, rankedPartition{
				id:       partitionID,
				priority: math.Min(index.partitionLoads[partitionID], offender.priority),
			})
		}
	}
	diagnostics.LegalPartitionMoveSources += len(sources)
	diagnostics.LegalPartitionMoveDestinations += len(sources) * (len(index.replicas) - 1)
	diagnostics.Legal.MovePartition += len(sources) * (len(index.replicas) - 1)
	selectedSources := selectPartitionSources(sources, limits.MaxPartitionMoveSources)
	diagnostics.DiscardedByBudget.PartitionMoveSources +=
		(len(sources) - len(selectedSources)) * max(0, len(index.replicas)-1)

	partitionMoves := make([]candidate, 0, limits.MaxPartitionMoveCandidates)
	for _, source := range selectedSources {
		sourceReplica := index.partitionOwners[source.id]
		destinations, inspected, pruned := discoverReplicaDestinations(source.id, index, limits)
		diagnostics.Work.DestinationsInspected += inspected
		diagnostics.Pruned.DestinationReplicas += pruned
		destinations = limitRankedReplicas(destinations, limits.MaxDestinationsPerPartition)
		diagnostics.DiscardedByBudget.PartitionMoveDestinations += len(index.replicas) - 1 - len(destinations)
		tenantLoads := partitionTenantLoadsFromIndex(state, index.rangeIndexesByPartition[source.id])
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
			move.priority -= transitionCost(move, state, index.totals, snapshot, policy).WeightedTotal
			partitionMoves = append(partitionMoves, move)
		}
	}
	diagnostics.DiscardedByBudget.PartitionMoves =
		max(0, len(partitionMoves)-limits.MaxPartitionMoveCandidates)
	return limitCandidates(partitionMoves, limits.MaxPartitionMoveCandidates)
}

func partitionOffenders(index candidateIndex) []partitionOffender {
	out := make([]partitionOffender, 0, len(index.partitions))
	for _, partitionID := range index.partitions {
		surplus := index.partitionLoads[partitionID] - index.partitionMean
		if surplus > costEpsilon {
			out = append(out, partitionOffender{id: partitionID, priority: surplus})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].priority != out[j].priority {
			return out[i].priority > out[j].priority
		}
		return out[i].id < out[j].id
	})
	return out
}

func replicaOffenders(index candidateIndex) []replicaOffender {
	out := make([]replicaOffender, 0, len(index.replicas))
	for _, replica := range index.replicas {
		surplus := index.replicaLoads[replica] - index.replicaMean
		if surplus > costEpsilon {
			out = append(out, replicaOffender{id: replica, priority: surplus})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].priority != out[j].priority {
			return out[i].priority > out[j].priority
		}
		return out[i].id < out[j].id
	})
	return out
}

func leastLoadedPartition(partitions []int32, loads map[int32]float64) int32 {
	best := partitions[0]
	for _, partitionID := range partitions[1:] {
		if loads[partitionID] < loads[best] || (loads[partitionID] == loads[best] && partitionID < best) {
			best = partitionID
		}
	}
	return best
}

func discoverReplicaDestinations(
	partitionID int32,
	index candidateIndex,
	limits CandidateSearchLimits,
) ([]rankedReplica, int, int) {
	source := index.partitionOwners[partitionID]
	candidates := index.replicasByDeficit
	inspectLimit := min(len(candidates), limits.MaxDestinationReplicas)
	out := make([]rankedReplica, 0, inspectLimit)
	load := index.partitionLoads[partitionID]
	for _, replica := range candidates {
		if replica == source {
			continue
		}
		deficit := math.Max(0, index.replicaMean-index.replicaLoads[replica])
		out = append(out, rankedReplica{id: replica, priority: math.Min(load, deficit)})
		if len(out) == inspectLimit {
			break
		}
	}
	return out, len(out), max(0, len(index.replicas)-1-len(out))
}

func limitRankedPartitions(values []rankedPartition, limit int) []rankedPartition {
	sort.Slice(values, func(i, j int) bool {
		if math.Abs(values[i].priority-values[j].priority) > costEpsilon {
			return values[i].priority > values[j].priority
		}
		return values[i].id < values[j].id
	})
	return values[:min(limit, len(values))]
}

func limitRankedReplicas(values []rankedReplica, limit int) []rankedReplica {
	sort.Slice(values, func(i, j int) bool {
		if math.Abs(values[i].priority-values[j].priority) > costEpsilon {
			return values[i].priority > values[j].priority
		}
		return values[i].id < values[j].id
	})
	return values[:min(limit, len(values))]
}

func partitionTenantLoadsFromIndex(state planningState, indexes []int) map[string]float64 {
	loads := map[string]float64{}
	for _, rangeIndex := range indexes {
		r := state.ranges[rangeIndex]
		loads[r.entry.TenantID] += r.load
	}
	return loads
}
