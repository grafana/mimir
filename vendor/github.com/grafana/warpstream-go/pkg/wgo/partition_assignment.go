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

// routeOutcome is how a strategy resolved one initial lookup. The zero value
// means the lookup was not classified, as for a custom strategy: a successful
// route is not counted, and a miss is counted as "other".
type routeOutcome int8

const (
	routeUnclassified routeOutcome = iota
	routeLeader
	routeStandIn
	routeMissEmptyPool
	routeMissUnknownTopic
	routeMissNoLeader
	routeMissOutOfRange
)

// routeClassifier is implemented by strategies that report the outcome from the
// same snapshot that produced the candidates.
type routeClassifier interface {
	candidatesWithRoute(topic string, partition int32, maxCandidates int) ([]Agent, routeOutcome)
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
// Hedger and ClusterBuffer are wired once at startup; this indirection
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

func (l *LazyPartitionAssignmentStrategy) candidatesWithRoute(topic string, partition int32, maxCandidates int) ([]Agent, routeOutcome) {
	return candidatesOf(l.resolve(), topic, partition, maxCandidates)
}

// candidatesOf looks up candidates with the strategy's classification when it
// reports one.
func candidatesOf(s PartitionAssignmentStrategy, topic string, partition int32, maxCandidates int) ([]Agent, routeOutcome) {
	if rc, ok := s.(routeClassifier); ok {
		return rc.candidatesWithRoute(topic, partition, maxCandidates)
	}
	return s.Candidates(topic, partition, maxCandidates), routeUnclassified
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

	// knownTopics is every topic with at least one entry in leaders, plus every
	// topic whose partitions this refresh listed and then excluded entirely.
	// Candidates uses it to tell "this topic is known, one partition's
	// leader is just missing" (safe to fall back to another agent) apart
	// from "this topic is unknown to Metadata" (falling back would hide a
	// topic that still needs an on-demand refresh).
	//
	// A topic Metadata has never returned, and a topic-level error, stay out.
	// A partition whose Leader was below 0 stays out of the fallback too,
	// even when this topic is known through some other partition.
	knownTopics map[string]struct{}
	noLeader    map[topicPartition]struct{}

	// partitionCounts is one past the highest partition index Metadata
	// reported for each known topic. Candidates uses it to reject a
	// partition that doesn't exist instead of guessing a fallback agent
	// for it — no agent owns a partition that was never created.
	partitionCounts map[string]int32
}

func newDefaultPartitionAssignmentStrategy(agents []int32, leaders map[topicPartition]int32, topicsWithNoLiveLeader map[string]struct{}, noLeader map[topicPartition]struct{}, partitionCounts map[string]int32) *DefaultPartitionAssignmentStrategy {
	// Built from empty, not sized off leaders: there are far fewer
	// distinct topics than partitions.
	knownTopics := make(map[string]struct{})
	for topic := range topicsWithNoLiveLeader {
		knownTopics[topic] = struct{}{}
	}
	for tp := range leaders {
		knownTopics[tp.topic] = struct{}{}
	}
	return &DefaultPartitionAssignmentStrategy{
		agents:          agents,
		leaders:         leaders,
		knownTopics:     knownTopics,
		noLeader:        noLeader,
		partitionCounts: partitionCounts,
	}
}

// Candidates returns the ordered candidate agents for (topic, partition):
// the partition leader first, then deterministic hash-walked alternates. Every
// entry is reported as AgentStateHealthy; this strategy has no health signal of
// its own.
//
// If a partition has no known leader but its topic is otherwise known,
// this falls back to a deterministic pick from the live agent set instead
// of returning no candidates. Any live agent can serve any partition, so
// this is a safe guess while the real leader is still unclear. A topic
// whose partitions this refresh listed and then excluded entirely counts
// as known. A topic Metadata has never returned is left alone, so it still
// gets an on-demand refresh. A partition whose Leader was below 0 returns
// nil: WarpStream named no agent, so this does not pick one. A partition
// index at or beyond the topic's last-reported count (or negative) also
// returns nil: no agent owns a partition that doesn't exist. A later
// refresh that grows the count (e.g. WarpStream's partition auto-scaler)
// makes it routable again.
//
// Caveat: two clients that refreshed at different times can pick
// different fallback agents for the same partition — a real leader
// doesn't have that problem. The extra cost from that (more segment
// streams per partition) is unmeasured; not assumed to be small.
func (s *DefaultPartitionAssignmentStrategy) Candidates(topic string, partition int32, maxCandidates int) []Agent {
	agents, _ := s.candidatesWithRoute(topic, partition, maxCandidates)
	return agents
}

func (s *DefaultPartitionAssignmentStrategy) candidatesWithRoute(topic string, partition int32, maxCandidates int) ([]Agent, routeOutcome) {
	if maxCandidates <= 0 {
		return nil, routeUnclassified
	}

	var h uint64
	tp := topicPartition{topic: topic, partition: partition}
	leader, ok := s.leaders[tp]
	route := routeLeader
	if !ok {
		if _, unnamed := s.noLeader[tp]; unnamed {
			return nil, routeMissNoLeader
		}
		if len(s.agents) == 0 {
			return nil, routeMissEmptyPool
		}
		if _, topicKnown := s.knownTopics[topic]; !topicKnown {
			// A topic with no named leaders is in partitionCounts but not in
			// knownTopics. Use the count only to pick the label. Adding the
			// topic to knownTopics would route its holes to a stand-in.
			if n, listed := s.partitionCounts[topic]; listed && (partition < 0 || partition >= n) {
				return nil, routeMissOutOfRange
			}
			return nil, routeMissUnknownTopic
		}
		if partition < 0 || partition >= s.partitionCounts[topic] {
			return nil, routeMissOutOfRange
		}
		h = hashTopicPartition(topic, partition)
		leader = s.agents[h%uint64(len(s.agents))]
		route = routeStandIn
	}

	out := make([]Agent, 0, maxCandidates)
	out = append(out, Agent{NodeID: leader, State: AgentStateHealthy})
	if maxCandidates == 1 {
		return out, route
	}

	// Walk the non-leader agents in deterministic hash order: start at
	// hash(topic, partition) mod nonLeaderCount and step forward. nthNonLeader
	// re-scans from index 0 each step, so emitting k candidates is O(k*n);
	// acceptable here because n (agent count) is small.
	nonLeaderCount := len(s.agents) - 1
	if nonLeaderCount <= 0 {
		return out, route
	}
	// The one-candidate return above never reaches this hash. A fallback
	// pick already stored it.
	if ok {
		h = hashTopicPartition(topic, partition)
	}
	start := int(h % uint64(nonLeaderCount))
	for offset := 0; offset < nonLeaderCount && len(out) < maxCandidates; offset++ {
		idx := (start + offset) % nonLeaderCount
		out = append(out, Agent{NodeID: nthNonLeader(s.agents, leader, idx), State: AgentStateHealthy})
	}
	return out, route
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
