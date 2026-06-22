package wgo

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
)

// topicPartition is the map key used by leader and secondary assignment maps.
// The topic field is required because AgentPool tracks multiple topics and the
// strategy maps must distinguish (topic_a, partition 0) from (topic_b, partition 0).
type topicPartition struct {
	topic     string
	partition int32
}

// AgentState describes the health of a Agent as understood by the
// strategy.
type AgentState int

const (
	// AgentStateHealthy indicates the agent has no recent failure signal.
	AgentStateHealthy AgentState = iota

	// AgentStateDemoted indicates the agent has been demoted because of
	// recent failures. The strategy only emits a AgentStateDemoted in the
	// primary slot when it has *intentionally* chosen to send traffic to
	// the demoted agent — a probe to observe whether the agent has
	// recovered. The Demoter rate-limits these probes per agent (see
	// DemoterConfig.ProbeInterval); the Hedger reads the state on the
	// primary slot to know it should fire the fallback immediately
	// rather than waiting for the hedge delay.
	AgentStateDemoted
)

// Agent is one routing option for a (topic, partition) produced by the
// PartitionAssignmentStrategy. NodeID identifies the agent; State reports
// how the strategy classifies its current health.
type Agent struct {
	NodeID int32
	State  AgentState
}

// cloneWithState returns a copy of a with State overridden.
func (a Agent) cloneWithState(state AgentState) Agent {
	a.State = state
	return a
}

// PartitionAssignmentStrategy maps a partition to an ordered list of
// candidate agents. The first candidate is the primary (used for normal
// routing); the rest are deterministic alternates used for hedging.
//
// The "secondary" concept (and the general "candidate" list) is the
// half of the design that needs justifying. In vanilla Kafka, a partition
// has exactly one leader and clients route strictly to it; alternates make
// no sense. With Warpstream, every agent can serve every partition, but
// secondary selection is far from free: each agent that accepts records
// for a partition writes its own segment file to object storage, and
// Warpstream's control-plane RSM has to track every (partition → segment)
// mapping it produces. If every client process picked a random secondary
// on every hedge, records for a single partition would be scattered
// across as many segment files as there are agents — inflating
// object-storage write amplification, fanning out fetch-side reads across
// many small segments, and bloating the control-plane state the RSM has
// to keep consistent.
//
// Determinism contains the blast radius. Implementations are expected to place
// hedge traffic for a given partition on the *same* alternate agent across
// every Kafka client instance, so the per-partition footprint stays at "one
// primary segment stream + one secondary segment stream + ..." rather than
// fanning out across the whole pool.
type PartitionAssignmentStrategy interface {
	// Candidates returns up to maxCandidates candidate agents for
	// (topic, partition), or nil if none are available. Callers must read
	// each Agent's State rather than assuming a fixed slot meaning: a wrapper
	// may elide the primary, stamp [0] as demoted, or return fewer entries.
	Candidates(topic string, partition int32, maxCandidates int) []Agent
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

// Candidates implements PartitionAssignmentStrategy.
func (l *LazyPartitionAssignmentStrategy) Candidates(topic string, partition int32, maxCandidates int) []Agent {
	return l.resolve().Candidates(topic, partition, maxCandidates)
}

// DefaultPartitionAssignmentStrategy is an immutable snapshot of the agent
// pool. The leader map is precomputed in the constructor so the produce hot
// path reads it lock-free; Candidates is computed lazily over the same agent
// slice. AgentPool.Refresh creates a new instance on every refresh.
//
// Placement is fully deterministic: all clients with the same Metadata view
// compute the same candidate order, and as agents come and go orderings only
// shift for the partitions actually affected.
type DefaultPartitionAssignmentStrategy struct {
	agents  []int32 // sorted ascending, snapshot at construction
	leaders map[topicPartition]int32
}

func newDefaultPartitionAssignmentStrategy(agents []int32, leaders map[topicPartition]int32) *DefaultPartitionAssignmentStrategy {
	return &DefaultPartitionAssignmentStrategy{
		agents:  agents,
		leaders: leaders,
	}
}

// Candidates returns the ordered candidate agents for (topic, partition):
// the partition leader first, then deterministic hash-walked alternates. Every
// entry is reported as AgentStateHealthy; this strategy has no health signal of
// its own.
func (s *DefaultPartitionAssignmentStrategy) Candidates(topic string, partition int32, maxCandidates int) []Agent {
	if maxCandidates <= 0 {
		return nil
	}
	leader, ok := s.leaders[topicPartition{topic: topic, partition: partition}]
	if !ok {
		return nil
	}

	out := make([]Agent, 0, maxCandidates)
	out = append(out, Agent{NodeID: leader, State: AgentStateHealthy})
	if maxCandidates == 1 {
		return out
	}

	// Walk the non-leader agents in deterministic hash order: start at
	// hash(topic, partition) mod nonLeaderCount and step forward. nthNonLeader
	// re-scans from index 0 each step, so emitting k candidates is O(k*n);
	// acceptable here because n (agent count) is small.
	nonLeaderCount := len(s.agents) - 1
	if nonLeaderCount <= 0 {
		return out
	}
	h := hashTopicPartition(topic, partition)
	start := int(h % uint64(nonLeaderCount))
	for offset := 0; offset < nonLeaderCount && len(out) < maxCandidates; offset++ {
		idx := (start + offset) % nonLeaderCount
		out = append(out, Agent{NodeID: nthNonLeader(s.agents, leader, idx), State: AgentStateHealthy})
	}
	return out
}

// nthNonLeader returns the idx-th element of agents skipping leader. idx is
// assumed to be in [0, len(agents)-1).
func nthNonLeader(agents []int32, leader int32, idx int) int32 {
	seen := 0
	for _, id := range agents {
		if id == leader {
			continue
		}
		if seen == idx {
			return id
		}
		seen++
	}
	return 0 // unreachable when idx is in range
}

// hashTopicPartition hashes the (topic, partition) pair without heap
// allocation. The streaming Digest and the stack-allocated partition bytes both
// stay on the stack, so this avoids building a combined key string while still
// giving a full-avalanche hash of the whole key.
func hashTopicPartition(topic string, partition int32) uint64 {
	var d xxhash.Digest
	d.Reset()
	// xxhash.Digest's Write/WriteString never return an error.
	_, _ = d.WriteString(topic)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(partition))
	_, _ = d.Write(b[:])
	return d.Sum64()
}
