// SPDX-License-Identifier: AGPL-3.0-only

package scallop

import (
	"math"
	"sort"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// loadPeak keeps enough information to evaluate one transfer without scanning
// every partition or replica. second equals max when the maximum is tied.
type loadPeak[K comparable] struct {
	key    K
	max    float64
	second float64
	count  int
}

func buildLoadPeak[K comparable](loads map[K]float64) loadPeak[K] {
	var out loadPeak[K]
	out.max = math.Inf(-1)
	out.second = math.Inf(-1)
	for key, load := range loads {
		switch {
		case load > out.max:
			out.second = out.max
			out.max = load
			out.key = key
			out.count = 1
		case load == out.max:
			out.second = out.max
			out.count++
		case load > out.second:
			out.second = load
		}
	}
	if math.IsInf(out.max, -1) {
		out.max = 0
		out.second = 0
	}
	if math.IsInf(out.second, -1) {
		out.second = 0
	}
	return out
}

func peakAfterTransfer[K comparable](
	loads map[K]float64,
	peak loadPeak[K],
	from, to K,
	amount, mean float64,
) float64 {
	if mean <= 0 {
		return 0
	}
	if from == to {
		return peak.max/mean - 1
	}
	unaffected := peak.max
	if from == peak.key && peak.count == 1 {
		unaffected = peak.second
	}
	maximum := max(unaffected, loads[from]-amount, loads[to]+amount)
	return maximum/mean - 1
}

// buildCandidateIndex constructs the one shared indexed view of a projected
// state. It is linear in ranges, partitions, and replicas.
func buildCandidateIndex(state planningState, totals planTotals, snapshot Snapshot) candidateIndex {
	index := candidateIndex{
		partitionLoads:          make(map[int32]float64, len(snapshot.ActivePartitions)),
		replicaLoads:            make(map[string]float64, len(snapshot.ActiveReplicas)),
		partitionOwners:         state.partitionOwners,
		totals:                  totals,
		tenantRangeCounts:       make(map[string]int, totals.tenants),
		rangeCount:              len(state.ranges),
		partitions:              append([]int32(nil), snapshot.ActivePartitions...),
		replicas:                append([]string(nil), snapshot.ActiveReplicas...),
		rangeIndexesByPartition: make(map[int32][]int, len(snapshot.ActivePartitions)),
		rangeIndexesByReplica:   make(map[string][]int, len(snapshot.ActiveReplicas)),
		partitionIDsByReplica:   make(map[string][]int32, len(snapshot.ActiveReplicas)),
		rangeIndexesByTenant:    make(map[string][]int, totals.tenants),
		splittableRangeIndexes:  make([]int, 0, len(state.ranges)),
	}
	sort.Slice(index.partitions, func(i, j int) bool { return index.partitions[i] < index.partitions[j] })
	sort.Strings(index.replicas)
	for _, partitionID := range index.partitions {
		index.partitionLoads[partitionID] = 0
		owner := state.partitionOwners[partitionID]
		index.partitionIDsByReplica[owner] = append(index.partitionIDsByReplica[owner], partitionID)
	}
	for _, replica := range index.replicas {
		index.replicaLoads[replica] = 0
	}
	for i, r := range state.ranges {
		partitionID := r.entry.PartitionID
		replica := state.partitionOwners[partitionID]
		index.partitionLoads[partitionID] += r.load
		index.tenantRangeCounts[r.entry.TenantID]++
		index.resolution += r.load * rangeFraction(r.entry.Range)
		index.rangeIndexesByPartition[partitionID] = append(index.rangeIndexesByPartition[partitionID], i)
		index.rangeIndexesByReplica[replica] = append(index.rangeIndexesByReplica[replica], i)
		index.rangeIndexesByTenant[r.entry.TenantID] = append(index.rangeIndexesByTenant[r.entry.TenantID], i)
		if r.observed && r.entry.Range.Size() > 1 {
			index.splittableRangeIndexes = append(index.splittableRangeIndexes, i)
		}
	}
	index.tenantIDs = make([]string, 0, len(index.rangeIndexesByTenant))
	for tenantID := range index.rangeIndexesByTenant {
		index.tenantIDs = append(index.tenantIDs, tenantID)
	}
	sort.Strings(index.tenantIDs)
	for _, partitionID := range index.partitions {
		index.replicaLoads[state.partitionOwners[partitionID]] += index.partitionLoads[partitionID]
	}
	rangeLess := func(left, right int) bool {
		if state.ranges[left].load != state.ranges[right].load {
			return state.ranges[left].load > state.ranges[right].load
		}
		return rangeStateLess(state.ranges[left], state.ranges[right])
	}
	for partitionID := range index.rangeIndexesByPartition {
		sort.Slice(index.rangeIndexesByPartition[partitionID], func(i, j int) bool {
			return rangeLess(index.rangeIndexesByPartition[partitionID][i], index.rangeIndexesByPartition[partitionID][j])
		})
	}
	for replica := range index.rangeIndexesByReplica {
		sort.Slice(index.rangeIndexesByReplica[replica], func(i, j int) bool {
			return rangeLess(index.rangeIndexesByReplica[replica][i], index.rangeIndexesByReplica[replica][j])
		})
	}
	for replica := range index.partitionIDsByReplica {
		sort.Slice(index.partitionIDsByReplica[replica], func(i, j int) bool {
			left := index.partitionIDsByReplica[replica][i]
			right := index.partitionIDsByReplica[replica][j]
			if index.partitionLoads[left] != index.partitionLoads[right] {
				return index.partitionLoads[left] > index.partitionLoads[right]
			}
			return left < right
		})
	}
	index.partitionsByDeficit = append([]int32(nil), index.partitions...)
	sort.Slice(index.partitionsByDeficit, func(i, j int) bool {
		left, right := index.partitionsByDeficit[i], index.partitionsByDeficit[j]
		if index.partitionLoads[left] != index.partitionLoads[right] {
			return index.partitionLoads[left] < index.partitionLoads[right]
		}
		return left < right
	})
	index.replicasByDeficit = append([]string(nil), index.replicas...)
	sort.Slice(index.replicasByDeficit, func(i, j int) bool {
		left, right := index.replicasByDeficit[i], index.replicasByDeficit[j]
		if index.replicaLoads[left] != index.replicaLoads[right] {
			return index.replicaLoads[left] < index.replicaLoads[right]
		}
		return left < right
	})
	index.partitionMean = totals.load / float64(len(index.partitions))
	index.replicaMean = totals.load / float64(len(index.replicas))
	index.refreshPeaks()
	return index
}

func rangeStateLess(left, right rangeState) bool {
	if left.entry.TenantID != right.entry.TenantID {
		return left.entry.TenantID < right.entry.TenantID
	}
	if left.entry.Range.Lo != right.entry.Range.Lo {
		return left.entry.Range.Lo < right.entry.Range.Lo
	}
	if left.entry.Range.Hi != right.entry.Range.Hi {
		return left.entry.Range.Hi < right.entry.Range.Hi
	}
	return left.entry.PartitionID < right.entry.PartitionID
}

func (index *candidateIndex) refreshPeaks() {
	index.partitionPressure = buildLoadPeak(index.partitionLoads)
	index.replicaPressure = buildLoadPeak(index.replicaLoads)
	index.partitionPeak = peakExcessWithMean(index.partitionLoads, index.partitionMean)
	index.replicaPeak = peakExcessWithMean(index.replicaLoads, index.replicaMean)
	sort.Slice(index.partitionsByDeficit, func(i, j int) bool {
		left, right := index.partitionsByDeficit[i], index.partitionsByDeficit[j]
		if index.partitionLoads[left] != index.partitionLoads[right] {
			return index.partitionLoads[left] < index.partitionLoads[right]
		}
		return left < right
	})
	sort.Slice(index.replicasByDeficit, func(i, j int) bool {
		left, right := index.replicasByDeficit[i], index.replicasByDeficit[j]
		if index.replicaLoads[left] != index.replicaLoads[right] {
			return index.replicaLoads[left] < index.replicaLoads[right]
		}
		return left < right
	})
	for replica := range index.partitionIDsByReplica {
		sort.Slice(index.partitionIDsByReplica[replica], func(i, j int) bool {
			left, right := index.partitionIDsByReplica[replica][i], index.partitionIDsByReplica[replica][j]
			if index.partitionLoads[left] != index.partitionLoads[right] {
				return index.partitionLoads[left] > index.partitionLoads[right]
			}
			return left < right
		})
	}
}

func (index candidateIndex) cloneLoads() candidateIndex {
	cloned := index
	cloned.partitionLoads = cloneLoads(index.partitionLoads)
	cloned.replicaLoads = cloneLoads(index.replicaLoads)
	cloned.partitionOwners = clonePartitionOwners(index.partitionOwners)
	cloned.partitionsByDeficit = append([]int32(nil), index.partitionsByDeficit...)
	cloned.replicasByDeficit = append([]string(nil), index.replicasByDeficit...)
	cloned.partitionIDsByReplica = make(map[string][]int32, len(index.partitionIDsByReplica))
	for replica, partitions := range index.partitionIDsByReplica {
		cloned.partitionIDsByReplica[replica] = append([]int32(nil), partitions...)
	}
	cloned.tenantRangeCounts = make(map[string]int, len(index.tenantRangeCounts))
	for tenantID, count := range index.tenantRangeCounts {
		cloned.tenantRangeCounts[tenantID] = count
	}
	return cloned
}

func cloneLoads[K comparable](in map[K]float64) map[K]float64 {
	out := make(map[K]float64, len(in))
	for key, load := range in {
		out[key] = load
	}
	return out
}

// stateCostFromIndex returns the exact persistent cost represented by index.
func stateCostFromIndex(index candidateIndex, policy Policy) CostBreakdown {
	out := CostBreakdown{
		PartitionBalance: index.partitionPeak,
		ReplicaBalance:   index.replicaPeak,
		Fragmentation:    float64(index.rangeCount) / float64(max(1, index.totals.tenants)),
		Resolution:       index.resolution,
	}
	weightStateCost(&out, policy)
	return out
}

func weightStateCost(out *CostBreakdown, policy Policy) {
	out.WeightedPartitionBalance = out.PartitionBalance
	out.WeightedReplicaBalance = policy.Weights.ReplicaBalance * out.ReplicaBalance
	out.WeightedFragmentation = policy.Weights.Fragmentation * out.Fragmentation
	out.WeightedResolution = policy.Weights.Resolution * out.Resolution
	out.WeightedTotal = out.WeightedPartitionBalance +
		out.WeightedReplicaBalance +
		out.WeightedFragmentation +
		out.WeightedResolution
}

// applyCandidateToIndex mutates only aggregates affected by one committed
// action. Merge and split batches rebuild structural indexes after materializing
// their selected wave.
func applyCandidateToIndex(index *candidateIndex, candidate candidate, state planningState) {
	switch candidate.kind {
	case ActionMove, ActionMerge:
		if candidate.fromPartition != candidate.toPartition && candidate.movedLoad > 0 {
			index.partitionLoads[candidate.fromPartition] -= candidate.movedLoad
			index.partitionLoads[candidate.toPartition] += candidate.movedLoad
			fromReplica := index.partitionOwners[candidate.fromPartition]
			toReplica := index.partitionOwners[candidate.toPartition]
			if fromReplica != toReplica {
				index.replicaLoads[fromReplica] -= candidate.movedLoad
				index.replicaLoads[toReplica] += candidate.movedLoad
			}
		}
		if candidate.kind == ActionMerge {
			left, right := state.ranges[candidate.index], state.ranges[candidate.otherIndex]
			index.resolution += (left.load+right.load)*rangeFraction(
				assignment.HashRange{Lo: candidate.r.Lo, Hi: candidate.other.Hi},
			) - left.load*rangeFraction(left.entry.Range) - right.load*rangeFraction(right.entry.Range)
			index.rangeCount--
			index.tenantRangeCounts[candidate.tenantID]--
		}
	case ActionMovePartition:
		load := index.partitionLoads[candidate.partitionID]
		index.replicaLoads[candidate.fromReplica] -= load
		index.replicaLoads[candidate.toReplica] += load
		index.partitionOwners[candidate.partitionID] = candidate.toReplica
	}
	index.refreshPeaks()
}
