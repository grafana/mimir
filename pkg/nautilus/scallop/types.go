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
	// ReplicaBalance weights logical-replica max/mean load excess.
	ReplicaBalance float64 `json:"replica_balance"`
	// TransitionEvents weights fixed overhead normalized by active partition count.
	TransitionEvents float64 `json:"transition_events"`
	// TransitionLoad weights the fraction of total observed load relocated.
	TransitionLoad float64 `json:"transition_load"`
	// TransitionHashSpace weights relocated hash space averaged across active tenants.
	TransitionHashSpace float64 `json:"transition_hash_space"`
	// LocalityMiss weights relocated load sent to a replica without recent tenant history.
	LocalityMiss float64 `json:"locality_miss"`
	// Fragmentation weights mean ranges per tenant, discouraging persistent fanout.
	Fragmentation float64 `json:"fragmentation"`
	// Resolution weights load times hash width, making coarse hot ranges expensive.
	Resolution float64 `json:"resolution"`
}

// ActionMultipliers models fixed per-action control-plane overhead.
type ActionMultipliers struct {
	Move          float64 `json:"move"`
	Split         float64 `json:"split"`
	Merge         float64 `json:"merge"`
	MovePartition float64 `json:"move_partition"`
}

// CandidateSearchLimits bounds expensive candidate projection while allowing
// small legal candidate sets to pass through in full.
type CandidateSearchLimits struct {
	MaxMoveSources              int `json:"max_move_sources"`
	MaxDestinationsPerRange     int `json:"max_destinations_per_range"`
	MaxSplitCandidates          int `json:"max_split_candidates"`
	MaxMergeCandidates          int `json:"max_merge_candidates"`
	MaxPartitionMoveSources     int `json:"max_partition_move_sources"`
	MaxDestinationsPerPartition int `json:"max_destinations_per_partition"`
	MaxPartitionMoveCandidates  int `json:"max_partition_move_candidates"`
	MaxFullyScored              int `json:"max_fully_scored"`
}

// DefaultCandidateSearchLimits bounds large-cell work while admitting complete fair merge waves.
func DefaultCandidateSearchLimits() CandidateSearchLimits {
	return CandidateSearchLimits{
		MaxMoveSources:              80,
		MaxDestinationsPerRange:     4,
		MaxSplitCandidates:          80,
		MaxMergeCandidates:          3200,
		MaxPartitionMoveSources:     32,
		MaxDestinationsPerPartition: 4,
		MaxPartitionMoveCandidates:  128,
		MaxFullyScored:              4096,
	}
}

// ActionLimits bounds selected work per planning round independently from candidate-search effort.
type ActionLimits struct {
	Total int `json:"total"`
	Move  int `json:"move"`
	Split int `json:"split"`
	Merge int `json:"merge"`
	// MergePerTenant limits how quickly one tenant can consolidate from a single observation.
	MergePerTenant int `json:"merge_per_tenant"`
	MovePartition  int `json:"move_partition"`
}

// Policy defines what Scallop considers preferable. It contains no observed
// cluster state and is safe to reuse across planning rounds.
type Policy struct {
	Weights           Weights               `json:"weights"`
	ActionMultipliers ActionMultipliers     `json:"action_multipliers"`
	CandidateSearch   CandidateSearchLimits `json:"candidate_search"`
	ActionLimits      ActionLimits          `json:"action_limits"`
	LocalityWindow    time.Duration         `json:"locality_window"`
}

// DefaultPolicy returns non-zero evidence-centered weights and bounded execution budgets.
func DefaultPolicy() Policy {
	return Policy{
		Weights: Weights{
			ReplicaBalance:      1,
			TransitionEvents:    0.05,
			TransitionLoad:      0.1,
			TransitionHashSpace: 0.1,
			LocalityMiss:        0.1,
			Fragmentation:       10,
			Resolution:          10,
		},
		ActionMultipliers: ActionMultipliers{Move: 1, Split: 1, Merge: 1, MovePartition: 1},
		CandidateSearch:   DefaultCandidateSearchLimits(),
		ActionLimits: ActionLimits{
			Total:          1612,
			Move:           4,
			Split:          4,
			Merge:          1600,
			MergePerTenant: 4,
			MovePartition:  4,
		},
		LocalityWindow: 30 * time.Minute,
	}
}

// ActionKind identifies a projected hash-range or partition-placement operation.
type ActionKind string

const (
	ActionMove          ActionKind = "move_range"
	ActionSplit         ActionKind = "split_range"
	ActionMerge         ActionKind = "merge_ranges"
	ActionMovePartition ActionKind = "move_partition"
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

	FromPartition int32  `json:"from_partition"`
	ToPartition   int32  `json:"to_partition"`
	PartitionID   int32  `json:"partition_id"`
	FromReplica   string `json:"from_replica,omitempty"`
	ToReplica     string `json:"to_replica,omitempty"`

	Load              float64 `json:"load"`
	MovedLoad         float64 `json:"moved_load"`
	MovedHashFraction float64 `json:"moved_hash_fraction"`

	Before         CostBreakdown `json:"before"`
	After          CostBreakdown `json:"after"`
	Transition     CostBreakdown `json:"transition"`
	CandidateTotal float64       `json:"candidate_total"`
	Explanation    string        `json:"explanation"`
}

// CandidateCounts reports candidate volume by action kind.
type CandidateCounts struct {
	Move          int `json:"move"`
	Split         int `json:"split"`
	Merge         int `json:"merge"`
	MovePartition int `json:"move_partition"`
	Total         int `json:"total"`
}

// CandidateBudgetDiscards attributes pruning to each configured search limit.
type CandidateBudgetDiscards struct {
	MoveSources               int `json:"move_sources"`
	Destinations              int `json:"destinations"`
	Splits                    int `json:"splits"`
	Merges                    int `json:"merges"`
	PartitionMoveSources      int `json:"partition_move_sources"`
	PartitionMoveDestinations int `json:"partition_move_destinations"`
	PartitionMoves            int `json:"partition_moves"`
	FullyScored               int `json:"fully_scored"`
}

// CandidateSearchDiagnostics makes bounded-search decisions inspectable.
type CandidateSearchDiagnostics struct {
	Limits                         CandidateSearchLimits   `json:"limits"`
	Iterations                     int                     `json:"iterations"`
	LegalMoveSources               int                     `json:"legal_move_sources"`
	LegalMoveDestinations          int                     `json:"legal_move_destinations"`
	LegalPartitionMoveSources      int                     `json:"legal_partition_move_sources"`
	LegalPartitionMoveDestinations int                     `json:"legal_partition_move_destinations"`
	Legal                          CandidateCounts         `json:"legal"`
	Admitted                       CandidateCounts         `json:"admitted"`
	FullyScored                    CandidateCounts         `json:"fully_scored"`
	Discarded                      CandidateCounts         `json:"discarded"`
	DiscardedByBudget              CandidateBudgetDiscards `json:"discarded_by_budget"`
	Truncated                      bool                    `json:"truncated"`
}

// PlanResult is a complete, replayable planning result.
type PlanResult struct {
	Assignment *assignment.Assignment `json:"assignment"`

	// PartitionOwners is the projected partition-to-logical-replica placement.
	PartitionOwners map[int32]string `json:"partition_owners"`
	Actions         []Action         `json:"actions"`

	InitialCost     CostBreakdown              `json:"initial_cost"`
	FinalCost       CostBreakdown              `json:"final_cost"`
	CandidateSearch CandidateSearchDiagnostics `json:"candidate_search"`
}

// validate rejects policy values that would make planning unsafe or nondeterministic.
func (p Policy) validate() error {
	if p.ActionLimits.Total <= 0 ||
		p.ActionLimits.Move < 0 ||
		p.ActionLimits.Split < 0 ||
		p.ActionLimits.Merge < 0 ||
		p.ActionLimits.MergePerTenant < 0 ||
		p.ActionLimits.MovePartition < 0 {
		return fmt.Errorf("action limits must have a positive total and non-negative per-kind values")
	}
	if p.ActionLimits.Merge > 0 && p.ActionLimits.MergePerTenant == 0 {
		return fmt.Errorf("merge-per-tenant action limit must be positive when merges are enabled")
	}
	if p.LocalityWindow < 0 {
		return fmt.Errorf("locality window must be non-negative")
	}
	limits := p.CandidateSearch
	if limits.MaxMoveSources <= 0 ||
		limits.MaxDestinationsPerRange <= 0 ||
		limits.MaxSplitCandidates <= 0 ||
		limits.MaxMergeCandidates <= 0 ||
		limits.MaxPartitionMoveSources <= 0 ||
		limits.MaxDestinationsPerPartition <= 0 ||
		limits.MaxPartitionMoveCandidates <= 0 ||
		limits.MaxFullyScored <= 0 {
		return fmt.Errorf("all candidate search limits must be positive")
	}
	values := map[string]float64{
		"replica balance":           p.Weights.ReplicaBalance,
		"transition events":         p.Weights.TransitionEvents,
		"transition load":           p.Weights.TransitionLoad,
		"transition hash space":     p.Weights.TransitionHashSpace,
		"locality miss":             p.Weights.LocalityMiss,
		"fragmentation":             p.Weights.Fragmentation,
		"resolution":                p.Weights.Resolution,
		"move multiplier":           p.ActionMultipliers.Move,
		"split multiplier":          p.ActionMultipliers.Split,
		"merge multiplier":          p.ActionMultipliers.Merge,
		"partition move multiplier": p.ActionMultipliers.MovePartition,
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
	ranges          []rangeState
	partitionOwners map[int32]string
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
	return planningState{ranges: ranges, partitionOwners: clonePartitionOwners(s.PartitionOwners)}
}

func (s planningState) clone() planningState {
	return planningState{
		ranges:          append([]rangeState(nil), s.ranges...),
		partitionOwners: clonePartitionOwners(s.partitionOwners),
	}
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
