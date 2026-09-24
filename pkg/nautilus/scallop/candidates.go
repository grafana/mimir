// SPDX-License-Identifier: AGPL-3.0-only

package scallop

// This file enumerates deterministic range actions and projects their assignment effects.

import (
	"fmt"
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
}

// generateCandidates returns every currently legal move, midpoint split, and adjacent merge in stable order.
func generateCandidates(state planningState, activePartitions []int32) []candidate {
	partitions := append([]int32(nil), activePartitions...)
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

	candidates := make([]candidate, 0, len(state.ranges)*(len(partitions)+2))
	for i, r := range state.ranges {
		if !r.observed {
			continue
		}
		for _, destination := range partitions {
			if destination == r.entry.PartitionID {
				continue
			}
			candidates = append(candidates, candidate{
				kind:              ActionMove,
				index:             i,
				otherIndex:        -1,
				tenantID:          r.entry.TenantID,
				r:                 r.entry.Range,
				fromPartition:     r.entry.PartitionID,
				toPartition:       destination,
				load:              r.load,
				movedLoad:         r.load,
				movedHashFraction: rangeFraction(r.entry.Range),
			})
		}
		if r.entry.Range.Size() > 1 {
			candidates = append(candidates, candidate{
				kind:          ActionSplit,
				index:         i,
				otherIndex:    -1,
				tenantID:      r.entry.TenantID,
				r:             r.entry.Range,
				fromPartition: r.entry.PartitionID,
				toPartition:   r.entry.PartitionID,
				load:          r.load,
			})
		}
	}

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
			candidates = append(candidates, candidate{
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
			})
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].key() < candidates[j].key()
	})
	return candidates
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
