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

// PartitionAssignmentStrategy maps a partition to a primary agent (used for
// normal routing) and a secondary agent (used for hedging).
//
// The "secondary" concept is the half of the design that needs justifying.
// In vanilla Kafka, a partition has exactly one leader and clients route
// strictly to it; "secondary" makes no sense. With Warpstream, every agent
// can serve every partition, but secondary selection is far from free: each
// agent that accepts records for a partition writes its own segment file
// to object storage, and Warpstream's control-plane RSM has to track every
// (partition → segment) mapping it produces. If every client process
// picked a random secondary on every hedge, records for a single partition
// would be scattered across as many segment files as there are agents —
// inflating object-storage write amplification, fanning out fetch-side
// reads across many small segments, and bloating the control-plane state
// the RSM has to keep consistent.
//
// Determinism contains the blast radius. Hedge traffic for a given
// partition lands on the *same* alternate agent across every Kafka client
// instance, so the per-partition footprint stays at "one primary segment
// stream + one secondary segment stream" rather than fanning out across
// the whole pool. The implementation hashes (topic, partition) over the
// sorted agent list (minus the primary): all clients with the same
// Metadata view compute the same secondary, and as agents come and go
// secondaries only shift for the partitions actually affected.
//
// Both methods share the (topic, partition) signature so a method value
// can be handed off as a generic AgentResolver to RecordBuffer.
type PartitionAssignmentStrategy interface {
	// Primary returns the Kafka NodeID designated as partition leader by Metadata.
	// Returns (0, false) if the partition has no known leader.
	Primary(topic string, partition int32) (nodeID int32, ok bool)

	// Secondary returns a deterministic alternate NodeID for hedging.
	// Returns (0, false) if no secondary is available.
	Secondary(topic string, partition int32) (nodeID int32, ok bool)
}

// LazyPartitionAssignmentStrategy resolves the underlying strategy on every
// call. The strategy is rebuilt by AgentPool.Refresh, but consumers like
// Hedger and ClusterRecordBuffer are wired once at startup; this indirection
// lets them pick up the latest snapshot without rewiring.
type LazyPartitionAssignmentStrategy struct {
	resolve func() PartitionAssignmentStrategy
}

// NewLazyPartitionAssignmentStrategy calls resolve on every lookup.
func NewLazyPartitionAssignmentStrategy(resolve func() PartitionAssignmentStrategy) *LazyPartitionAssignmentStrategy {
	return &LazyPartitionAssignmentStrategy{resolve: resolve}
}

// Primary implements PartitionAssignmentStrategy.
func (l *LazyPartitionAssignmentStrategy) Primary(topic string, partition int32) (int32, bool) {
	return l.resolve().Primary(topic, partition)
}

// Secondary implements PartitionAssignmentStrategy.
func (l *LazyPartitionAssignmentStrategy) Secondary(topic string, partition int32) (int32, bool) {
	return l.resolve().Secondary(topic, partition)
}

// DefaultPartitionAssignmentStrategy is an immutable snapshot of the agent
// pool. Both the leader and secondary maps are precomputed in the
// constructor so the produce hot path reads them lock-free and alloc-free.
// AgentPool.Refresh creates a new instance on every refresh.
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

// selectSecondary picks a NodeID deterministically by hashing (topic,
// partition) over the candidate list (sorted agents minus primary). Walks
// the slice without allocating an intermediate one.
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
