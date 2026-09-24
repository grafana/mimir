// SPDX-License-Identifier: AGPL-3.0-only

package scallop

// This file defines Scallop's public planning contract and private per-call state.

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

const hashSpaceSize = float64(uint64(math.MaxUint32) + 1)

// RangeKey identifies one tenant-scoped hash range.
type RangeKey struct {
	TenantID string               `json:"tenant_id"`
	Range    assignment.HashRange `json:"range"`
}

// Snapshot is the complete observed input to one planning round.
//
// RangeLoads must contain an observation for every assignment entry. Gaussian
// components, future observations, and other workload-generator state do not
// belong here.
type Snapshot struct {
	At time.Time `json:"at"`

	Assignment *assignment.Assignment `json:"assignment"`
	RangeLoads map[RangeKey]float64   `json:"range_loads"`

	ActivePartitions []int32          `json:"active_partitions"`
	PartitionOwners  map[int32]string `json:"partition_owners"`
	ActiveReplicas   []string         `json:"active_replicas"`

	// LastHostedAt is caller-owned locality history, keyed by tenant and
	// logical readcache replica. Plan reads but never mutates it.
	LastHostedAt map[string]map[string]time.Time `json:"last_hosted_at,omitempty"`
}

// Weights defines the relative importance of each cost. PartitionBalance is
// deliberately absent: it is fixed at 1 and establishes the cost scale.
type Weights struct {
	ReplicaBalance      float64 `json:"replica_balance"`
	TransitionEvents    float64 `json:"transition_events"`
	TransitionLoad      float64 `json:"transition_load"`
	TransitionHashSpace float64 `json:"transition_hash_space"`
	LocalityMiss        float64 `json:"locality_miss"`
	Fragmentation       float64 `json:"fragmentation"`
	Resolution          float64 `json:"resolution"`
}

// ActionMultipliers models fixed per-action control-plane overhead.
type ActionMultipliers struct {
	Move  float64 `json:"move"`
	Split float64 `json:"split"`
	Merge float64 `json:"merge"`
}

// Policy defines what Scallop considers preferable. It contains no observed
// cluster state and is safe to reuse across planning rounds.
type Policy struct {
	Weights           Weights           `json:"weights"`
	ActionMultipliers ActionMultipliers `json:"action_multipliers"`
	LocalityWindow    time.Duration     `json:"locality_window"`
	MaxActions        int               `json:"max_actions"`
}

// DefaultPolicy returns conservative, non-zero weights suitable for examples
// and as the center of the simulator's weight search.
func DefaultPolicy() Policy {
	return Policy{
		Weights: Weights{
			ReplicaBalance:      1,
			TransitionEvents:    0.05,
			TransitionLoad:      0.1,
			TransitionHashSpace: 0.1,
			LocalityMiss:        0.1,
			Fragmentation:       0.05,
			Resolution:          0.001,
		},
		ActionMultipliers: ActionMultipliers{Move: 1, Split: 1, Merge: 1},
		LocalityWindow:    30 * time.Minute,
		MaxActions:        8,
	}
}

// ActionKind identifies a projected hash-range operation.
type ActionKind string

const (
	ActionMove  ActionKind = "move_range"
	ActionSplit ActionKind = "split_range"
	ActionMerge ActionKind = "merge_ranges"
)

// CostBreakdown contains raw cost terms and their weighted total.
type CostBreakdown struct {
	PartitionBalance    float64 `json:"partition_balance"`
	ReplicaBalance      float64 `json:"replica_balance"`
	TransitionEvents    float64 `json:"transition_events"`
	TransitionLoad      float64 `json:"transition_load"`
	TransitionHashSpace float64 `json:"transition_hash_space"`
	LocalityMiss        float64 `json:"locality_miss"`
	Fragmentation       float64 `json:"fragmentation"`
	Resolution          float64 `json:"resolution"`

	WeightedPartitionBalance    float64 `json:"weighted_partition_balance"`
	WeightedReplicaBalance      float64 `json:"weighted_replica_balance"`
	WeightedTransitionEvents    float64 `json:"weighted_transition_events"`
	WeightedTransitionLoad      float64 `json:"weighted_transition_load"`
	WeightedTransitionHashSpace float64 `json:"weighted_transition_hash_space"`
	WeightedLocalityMiss        float64 `json:"weighted_locality_miss"`
	WeightedFragmentation       float64 `json:"weighted_fragmentation"`
	WeightedResolution          float64 `json:"weighted_resolution"`
	WeightedTotal               float64 `json:"weighted_total"`
}

// Action describes one selected operation and why it beat no-op.
type Action struct {
	Kind ActionKind `json:"kind"`

	TenantID string                `json:"tenant_id"`
	Range    assignment.HashRange  `json:"range"`
	Other    *assignment.HashRange `json:"other_range,omitempty"`

	FromPartition int32 `json:"from_partition"`
	ToPartition   int32 `json:"to_partition"`

	Load              float64 `json:"load"`
	MovedLoad         float64 `json:"moved_load"`
	MovedHashFraction float64 `json:"moved_hash_fraction"`

	Before         CostBreakdown `json:"before"`
	After          CostBreakdown `json:"after"`
	Transition     CostBreakdown `json:"transition"`
	CandidateTotal float64       `json:"candidate_total"`
	Explanation    string        `json:"explanation"`
}

// PlanResult is a complete, replayable planning result.
type PlanResult struct {
	Assignment *assignment.Assignment `json:"assignment"`

	// PartitionOwners is the projected partition-to-logical-replica
	// placement. It is unchanged in Phase 1, which has no partition moves.
	PartitionOwners map[int32]string `json:"partition_owners"`
	Actions         []Action         `json:"actions"`

	InitialCost CostBreakdown `json:"initial_cost"`
	FinalCost   CostBreakdown `json:"final_cost"`
}

// validate rejects policy values that would make planning unsafe or nondeterministic.
func (p Policy) validate() error {
	if p.MaxActions <= 0 {
		return fmt.Errorf("max actions must be positive")
	}
	if p.LocalityWindow < 0 {
		return fmt.Errorf("locality window must be non-negative")
	}
	values := map[string]float64{
		"replica balance":       p.Weights.ReplicaBalance,
		"transition events":     p.Weights.TransitionEvents,
		"transition load":       p.Weights.TransitionLoad,
		"transition hash space": p.Weights.TransitionHashSpace,
		"locality miss":         p.Weights.LocalityMiss,
		"fragmentation":         p.Weights.Fragmentation,
		"resolution":            p.Weights.Resolution,
		"move multiplier":       p.ActionMultipliers.Move,
		"split multiplier":      p.ActionMultipliers.Split,
		"merge multiplier":      p.ActionMultipliers.Merge,
	}
	for name, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return fmt.Errorf("%s must be finite and non-negative, got %v", name, value)
		}
	}
	return nil
}

// validate ensures a snapshot is complete, internally consistent, and fully observed.
func (s Snapshot) validate() error {
	if s.Assignment == nil {
		return fmt.Errorf("assignment is required")
	}
	if err := s.Assignment.Validate(); err != nil {
		return fmt.Errorf("invalid assignment: %w", err)
	}
	if len(s.ActivePartitions) == 0 {
		return fmt.Errorf("active partitions are required")
	}
	active := make(map[int32]struct{}, len(s.ActivePartitions))
	for _, partitionID := range s.ActivePartitions {
		if _, duplicate := active[partitionID]; duplicate {
			return fmt.Errorf("active partition %d is duplicated", partitionID)
		}
		active[partitionID] = struct{}{}
		if s.PartitionOwners[partitionID] == "" {
			return fmt.Errorf("active partition %d has no logical replica owner", partitionID)
		}
	}
	replicas := make(map[string]struct{}, len(s.ActiveReplicas))
	for _, replica := range s.ActiveReplicas {
		if replica == "" {
			return fmt.Errorf("active replica ID is empty")
		}
		if _, duplicate := replicas[replica]; duplicate {
			return fmt.Errorf("active replica %q is duplicated", replica)
		}
		replicas[replica] = struct{}{}
	}
	if len(replicas) == 0 {
		return fmt.Errorf("active replicas are required")
	}
	for partitionID, owner := range s.PartitionOwners {
		if _, ok := active[partitionID]; !ok {
			continue
		}
		if _, ok := replicas[owner]; !ok {
			return fmt.Errorf("partition %d owner %q is not an active replica", partitionID, owner)
		}
	}
	for _, entry := range s.Assignment.Entries {
		if _, ok := active[entry.PartitionID]; !ok {
			return fmt.Errorf("tenant %q range [%d,%d] uses inactive partition %d", entry.TenantID, entry.Range.Lo, entry.Range.Hi, entry.PartitionID)
		}
		load, ok := s.RangeLoads[RangeKey{TenantID: entry.TenantID, Range: entry.Range}]
		if !ok {
			return fmt.Errorf("tenant %q range [%d,%d] has no observed load", entry.TenantID, entry.Range.Lo, entry.Range.Hi)
		}
		if math.IsNaN(load) || math.IsInf(load, 0) || load < 0 {
			return fmt.Errorf("tenant %q range [%d,%d] has invalid load %v", entry.TenantID, entry.Range.Lo, entry.Range.Hi, load)
		}
	}
	if len(s.RangeLoads) != len(s.Assignment.Entries) {
		return fmt.Errorf("range loads contain %d observations for %d assignment entries", len(s.RangeLoads), len(s.Assignment.Entries))
	}
	return nil
}

type rangeState struct {
	entry    assignment.Entry
	load     float64
	observed bool
}

type planningState struct {
	ranges []rangeState
}

// stateFromSnapshot copies caller-owned assignment entries and observations into mutable projected state.
func stateFromSnapshot(s Snapshot) planningState {
	ranges := make([]rangeState, len(s.Assignment.Entries))
	for i, entry := range s.Assignment.Entries {
		ranges[i] = rangeState{
			entry:    entry,
			load:     s.RangeLoads[RangeKey{TenantID: entry.TenantID, Range: entry.Range}],
			observed: true,
		}
	}
	return planningState{ranges: ranges}
}

func (s planningState) clone() planningState {
	return planningState{ranges: append([]rangeState(nil), s.ranges...)}
}

// assignment materializes projected ranges as a sorted, caller-owned assignment.
func (s planningState) assignment() *assignment.Assignment {
	entries := make([]assignment.Entry, len(s.ranges))
	for i, r := range s.ranges {
		entries[i] = r.entry
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].TenantID != entries[j].TenantID {
			return entries[i].TenantID < entries[j].TenantID
		}
		return entries[i].Range.Lo < entries[j].Range.Lo
	})
	return &assignment.Assignment{Entries: entries}
}
