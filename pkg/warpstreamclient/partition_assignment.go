// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"github.com/cespare/xxhash/v2"
)

// topicPartition is the map key used by leader and secondary assignment maps.
// The topic field is required because AgentPool tracks multiple topics and the
// strategy maps must distinguish (topic_a, partition 0) from (topic_b, partition 0).
type topicPartition struct {
	topic     string
	partition int32
}

// PartitionAssignmentStrategy selects the primary and secondary agent for a given partition.
// Both methods share the same signature so a method value can be passed as a
// generic agent resolver (e.g. to RecordBuffer).
type PartitionAssignmentStrategy interface {
	// Primary returns the Kafka NodeID designated as partition leader by Metadata.
	// Returns (0, false) if the partition has no known leader.
	Primary(topic string, partition int32) (nodeID int32, ok bool)

	// Secondary returns a deterministic alternate NodeID for hedging.
	// Returns (0, false) if no secondary is available.
	Secondary(topic string, partition int32) (nodeID int32, ok bool)
}

// DefaultPartitionAssignmentStrategy implements PartitionAssignmentStrategy from an
// immutable snapshot of the agent pool. Both the leader map and the secondary map
// are precomputed in the constructor so reads are alloc-free and lock-free.
// A new instance is created by AgentPool.Refresh on every call.
type DefaultPartitionAssignmentStrategy struct {
	leaders     map[topicPartition]int32
	secondaries map[topicPartition]int32 // keys without an entry have no secondary; values are valid NodeIDs (including 0)
}

func newDefaultPartitionAssignmentStrategy(agents []int32, leaders map[topicPartition]int32) *DefaultPartitionAssignmentStrategy {
	// Precompute the secondary for every known (topic, partition). selectSecondary
	// returns ok=false when no secondary exists; we record the absence implicitly
	// by leaving the key out of the secondaries map.
	secondaries := make(map[topicPartition]int32, len(leaders))
	for tp, primary := range leaders {
		if id, ok := selectSecondary(tp.topic, tp.partition, primary, agents); ok {
			secondaries[tp] = id
		}
	}
	return &DefaultPartitionAssignmentStrategy{
		leaders:     leaders,
		secondaries: secondaries,
	}
}

// Primary returns the partition leader NodeID.
func (s *DefaultPartitionAssignmentStrategy) Primary(topic string, partition int32) (int32, bool) {
	id, ok := s.leaders[topicPartition{topic: topic, partition: partition}]
	return id, ok
}

// Secondary returns the precomputed secondary NodeID for hedging.
func (s *DefaultPartitionAssignmentStrategy) Secondary(topic string, partition int32) (int32, bool) {
	id, ok := s.secondaries[topicPartition{topic: topic, partition: partition}]
	return id, ok
}

// selectSecondary picks a secondary NodeID deterministically by hashing
// (topic, partition) over the sorted candidate list (all agents minus primary).
// Returns (0, false) if no secondary is available.
//
// The walk is alloc-free: rather than building a candidate slice, it counts
// non-primary agents and maps the hash to one via modular arithmetic.
func selectSecondary(topic string, partition int32, primary int32, all []int32) (int32, bool) {
	if len(all) < 2 {
		return 0, false
	}

	h := hashTopicPartition(topic, partition)

	nonPrimary := len(all) - 1
	if nonPrimary == 0 {
		return 0, false
	}
	idx := int(h % uint64(nonPrimary))

	seen := 0
	for _, id := range all {
		if id == primary {
			continue
		}
		if seen == idx {
			return id, true
		}
		seen++
	}

	return 0, false
}

// hashTopicPartition hashes a (topic, partition) pair without heap allocation.
func hashTopicPartition(topic string, partition int32) uint64 {
	// Mix partition into topic hash using a large prime multiplier to spread
	// bits from the low-value partition number across the full 64-bit range.
	return xxhash.Sum64String(topic) ^ (uint64(partition) * 0x9e3779b97f4a7c15)
}
