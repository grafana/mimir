// SPDX-License-Identifier: AGPL-3.0-only

package scallop

import (
	"sort"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// scoreCandidate evaluates one candidate exactly from affected aggregates.
// It does not clone, materialize, sort, or validate the complete assignment.
func scoreCandidate(
	candidate candidate,
	before CostBreakdown,
	index candidateIndex,
	state planningState,
	snapshot Snapshot,
	policy Policy,
) (CostBreakdown, CostBreakdown, float64) {
	after := before
	switch candidate.kind {
	case ActionMove:
		after.PartitionBalance = peakAfterTransfer(
			index.partitionLoads,
			index.partitionPressure,
			candidate.fromPartition,
			candidate.toPartition,
			candidate.movedLoad,
			index.partitionMean,
		)
		fromReplica := index.partitionOwners[candidate.fromPartition]
		toReplica := index.partitionOwners[candidate.toPartition]
		if fromReplica != toReplica {
			after.ReplicaBalance = peakAfterTransfer(
				index.replicaLoads,
				index.replicaPressure,
				fromReplica,
				toReplica,
				candidate.movedLoad,
				index.replicaMean,
			)
		}
	case ActionMovePartition:
		after.ReplicaBalance = peakAfterTransfer(
			index.replicaLoads,
			index.replicaPressure,
			candidate.fromReplica,
			candidate.toReplica,
			index.partitionLoads[candidate.partitionID],
			index.replicaMean,
		)
	case ActionMerge:
		if candidate.fromPartition != candidate.toPartition && candidate.movedLoad > 0 {
			after.PartitionBalance = peakAfterTransfer(
				index.partitionLoads,
				index.partitionPressure,
				candidate.fromPartition,
				candidate.toPartition,
				candidate.movedLoad,
				index.partitionMean,
			)
			fromReplica := index.partitionOwners[candidate.fromPartition]
			toReplica := index.partitionOwners[candidate.toPartition]
			if fromReplica != toReplica {
				after.ReplicaBalance = peakAfterTransfer(
					index.replicaLoads,
					index.replicaPressure,
					fromReplica,
					toReplica,
					candidate.movedLoad,
					index.replicaMean,
				)
			}
		}
		left, right := state.ranges[candidate.index], state.ranges[candidate.otherIndex]
		merged := assignment.HashRange{Lo: left.entry.Range.Lo, Hi: right.entry.Range.Hi}
		after.Fragmentation -= 1 / float64(max(1, index.totals.tenants))
		after.Resolution += (left.load+right.load)*rangeFraction(merged) -
			left.load*rangeFraction(left.entry.Range) -
			right.load*rangeFraction(right.entry.Range)
	}
	weightStateCost(&after, policy)
	transition := transitionCost(candidate, state, index.totals, snapshot, policy)
	return after, transition, after.WeightedTotal + transition.WeightedTotal
}

func updateMoveStructures(index *candidateIndex, candidate candidate, state planningState) {
	index.rangeIndexesByPartition[candidate.fromPartition] =
		removeInt(index.rangeIndexesByPartition[candidate.fromPartition], candidate.index)
	index.rangeIndexesByPartition[candidate.toPartition] =
		append(index.rangeIndexesByPartition[candidate.toPartition], candidate.index)
	fromReplica := index.partitionOwners[candidate.fromPartition]
	toReplica := index.partitionOwners[candidate.toPartition]
	if fromReplica != toReplica {
		index.rangeIndexesByReplica[fromReplica] =
			removeInt(index.rangeIndexesByReplica[fromReplica], candidate.index)
		index.rangeIndexesByReplica[toReplica] =
			append(index.rangeIndexesByReplica[toReplica], candidate.index)
	}
	sortRangeIndexes(index.rangeIndexesByPartition[candidate.fromPartition], state)
	sortRangeIndexes(index.rangeIndexesByPartition[candidate.toPartition], state)
	sortRangeIndexes(index.rangeIndexesByReplica[fromReplica], state)
	sortRangeIndexes(index.rangeIndexesByReplica[toReplica], state)
}

func updatePartitionMoveStructures(index *candidateIndex, candidate candidate, state planningState) {
	index.partitionIDsByReplica[candidate.fromReplica] =
		removeInt32(index.partitionIDsByReplica[candidate.fromReplica], candidate.partitionID)
	index.partitionIDsByReplica[candidate.toReplica] =
		append(index.partitionIDsByReplica[candidate.toReplica], candidate.partitionID)
	for _, rangeIndex := range index.rangeIndexesByPartition[candidate.partitionID] {
		index.rangeIndexesByReplica[candidate.fromReplica] =
			removeInt(index.rangeIndexesByReplica[candidate.fromReplica], rangeIndex)
		index.rangeIndexesByReplica[candidate.toReplica] =
			append(index.rangeIndexesByReplica[candidate.toReplica], rangeIndex)
	}
	sort.Slice(index.partitionIDsByReplica[candidate.fromReplica], func(i, j int) bool {
		return index.partitionIDsByReplica[candidate.fromReplica][i] < index.partitionIDsByReplica[candidate.fromReplica][j]
	})
	sort.Slice(index.partitionIDsByReplica[candidate.toReplica], func(i, j int) bool {
		return index.partitionIDsByReplica[candidate.toReplica][i] < index.partitionIDsByReplica[candidate.toReplica][j]
	})
	sortRangeIndexes(index.rangeIndexesByReplica[candidate.fromReplica], state)
	sortRangeIndexes(index.rangeIndexesByReplica[candidate.toReplica], state)
}

func removeInt(values []int, target int) []int {
	for i, value := range values {
		if value == target {
			return append(values[:i], values[i+1:]...)
		}
	}
	return values
}

func removeInt32(values []int32, target int32) []int32 {
	for i, value := range values {
		if value == target {
			return append(values[:i], values[i+1:]...)
		}
	}
	return values
}

func sortRangeIndexes(indexes []int, state planningState) {
	sort.Slice(indexes, func(i, j int) bool {
		left, right := state.ranges[indexes[i]], state.ranges[indexes[j]]
		if left.load != right.load {
			return left.load > right.load
		}
		return rangeStateLess(left, right)
	})
}
