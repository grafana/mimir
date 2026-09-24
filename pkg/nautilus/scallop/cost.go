// SPDX-License-Identifier: AGPL-3.0-only

package scallop

// This file computes persistent placement costs and one-action transition costs.

import (
	"math"
	"sort"
	"time"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// stateCost evaluates balance, fragmentation, and resolution for one projected placement.
func stateCost(state planningState, snapshot Snapshot, policy Policy) CostBreakdown {
	partitionLoads := make(map[int32]float64, len(snapshot.ActivePartitions))
	for _, partitionID := range snapshot.ActivePartitions {
		partitionLoads[partitionID] = 0
	}
	totalLoad := 0.0
	tenants := make(map[string]struct{})
	resolution := 0.0
	for _, r := range state.ranges {
		partitionLoads[r.entry.PartitionID] += r.load
		totalLoad += r.load
		tenants[r.entry.TenantID] = struct{}{}
		resolution += r.load * float64(r.entry.Range.Size()) / hashSpaceSize
	}

	replicaLoads := make(map[string]float64, len(snapshot.ActiveReplicas))
	for _, replica := range snapshot.ActiveReplicas {
		replicaLoads[replica] = 0
	}
	partitions := append([]int32(nil), snapshot.ActivePartitions...)
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	for _, partitionID := range partitions {
		load := partitionLoads[partitionID]
		replicaLoads[state.partitionOwners[partitionID]] += load
	}

	fragmentationDenominator := len(tenants)
	if fragmentationDenominator == 0 {
		fragmentationDenominator = 1
	}
	out := CostBreakdown{
		PartitionBalance: peakExcess(partitionLoads),
		ReplicaBalance:   peakExcess(replicaLoads),
		Fragmentation:    float64(len(state.ranges)) / float64(fragmentationDenominator),
		Resolution:       resolution,
	}
	out.WeightedPartitionBalance = out.PartitionBalance
	out.WeightedReplicaBalance = policy.Weights.ReplicaBalance * out.ReplicaBalance
	out.WeightedFragmentation = policy.Weights.Fragmentation * out.Fragmentation
	out.WeightedResolution = policy.Weights.Resolution * out.Resolution
	out.WeightedTotal = out.WeightedPartitionBalance +
		out.WeightedReplicaBalance +
		out.WeightedFragmentation +
		out.WeightedResolution
	return out
}

// peakExcess computes max/mean minus one, with zero representing perfect or zero-load balance.
func peakExcess[K comparable](loads map[K]float64) float64 {
	if len(loads) == 0 {
		return 0
	}
	values := make([]float64, 0, len(loads))
	for _, load := range loads {
		values = append(values, load)
	}
	sort.Float64s(values)
	var total, maximum float64
	for _, load := range values {
		total += load
		maximum = math.Max(maximum, load)
	}
	if total <= 0 {
		return 0
	}
	mean := total / float64(len(loads))
	return maximum/mean - 1
}

// transitionCost evaluates control-plane work, relocation, hash movement, and locality for one action.
func transitionCost(action candidate, state planningState, snapshot Snapshot, policy Policy) CostBreakdown {
	totalLoad := 0.0
	for _, r := range state.ranges {
		totalLoad += r.load
	}

	eventMultiplier := 0.0
	switch action.kind {
	case ActionMove:
		eventMultiplier = policy.ActionMultipliers.Move
	case ActionSplit:
		eventMultiplier = policy.ActionMultipliers.Split
	case ActionMerge:
		eventMultiplier = policy.ActionMultipliers.Merge
	case ActionMovePartition:
		eventMultiplier = policy.ActionMultipliers.MovePartition
	}

	out := CostBreakdown{
		TransitionEvents: eventMultiplier / float64(max(1, len(snapshot.ActivePartitions))),
	}
	if totalLoad > 0 {
		out.TransitionLoad = action.movedLoad / totalLoad
	}
	out.TransitionHashSpace = action.movedHashFraction / float64(max(1, tenantCount(state)))

	if action.movedLoad > 0 {
		if action.kind == ActionMovePartition {
			coldLoad := 0.0
			tenantIDs := make([]string, 0, len(action.tenantLoads))
			for tenantID := range action.tenantLoads {
				tenantIDs = append(tenantIDs, tenantID)
			}
			sort.Strings(tenantIDs)
			for _, tenantID := range tenantIDs {
				load := action.tenantLoads[tenantID]
				if !recentlyHosted(snapshot, tenantID, action.toReplica, policy.LocalityWindow) {
					coldLoad += load
				}
			}
			if totalLoad > 0 {
				out.LocalityMiss = coldLoad / totalLoad
			}
		} else {
			destination := state.partitionOwners[action.toPartition]
			if !recentlyHosted(snapshot, action.tenantID, destination, policy.LocalityWindow) && totalLoad > 0 {
				out.LocalityMiss = action.movedLoad / totalLoad
			}
		}
	}

	out.WeightedTransitionEvents = policy.Weights.TransitionEvents * out.TransitionEvents
	out.WeightedTransitionLoad = policy.Weights.TransitionLoad * out.TransitionLoad
	out.WeightedTransitionHashSpace = policy.Weights.TransitionHashSpace * out.TransitionHashSpace
	out.WeightedLocalityMiss = policy.Weights.LocalityMiss * out.LocalityMiss
	out.WeightedTotal =
		out.WeightedTransitionEvents +
			out.WeightedTransitionLoad +
			out.WeightedTransitionHashSpace +
			out.WeightedLocalityMiss
	return out
}

// tenantCount returns the number of tenants represented in one projected state.
func tenantCount(state planningState) int {
	tenants := map[string]struct{}{}
	for _, r := range state.ranges {
		tenants[r.entry.TenantID] = struct{}{}
	}
	return len(tenants)
}

// recentlyHosted reports whether locality history keeps a tenant-replica move warm within the policy window.
func recentlyHosted(snapshot Snapshot, tenantID, replica string, windowDuration time.Duration) bool {
	if windowDuration <= 0 {
		return true
	}
	byReplica := snapshot.LastHostedAt[tenantID]
	last, ok := byReplica[replica]
	if !ok || last.IsZero() {
		return false
	}
	return !last.Before(snapshot.At.Add(-windowDuration))
}

// accumulateTransition adds one selected action's transition terms to the final plan totals.
func accumulateTransition(dst *CostBreakdown, src CostBreakdown) {
	dst.TransitionEvents += src.TransitionEvents
	dst.TransitionLoad += src.TransitionLoad
	dst.TransitionHashSpace += src.TransitionHashSpace
	dst.LocalityMiss += src.LocalityMiss
	dst.WeightedTransitionEvents += src.WeightedTransitionEvents
	dst.WeightedTransitionLoad += src.WeightedTransitionLoad
	dst.WeightedTransitionHashSpace += src.WeightedTransitionHashSpace
	dst.WeightedLocalityMiss += src.WeightedLocalityMiss
	dst.WeightedTotal += src.WeightedTotal
}

// rangeFraction converts an inclusive uint32 range width into a fraction of one tenant's hash space.
func rangeFraction(r assignment.HashRange) float64 {
	return float64(r.Size()) / hashSpaceSize
}
