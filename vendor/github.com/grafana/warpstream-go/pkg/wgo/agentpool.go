package wgo

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// poolState holds the agent pool snapshot updated atomically by Refresh.
// Bundling the fields into one pointer prevents readers from observing a torn
// snapshot across the strategy, topic UUIDs, and agent list.
type poolState struct {
	agents   []int32 // sorted ascending so the strategy's hash-walk over non-leaders is consistent
	topicIDs map[string][16]byte
	strategy *DefaultPartitionAssignmentStrategy
}

// AgentPool is the source of truth for "what agents exist right now and
// which one does Warpstream consider leader for each partition". Every
// produce decision in this package — primary routing, secondary selection
// for hedging, partition splitting on retries — is keyed off a snapshot
// produced by Refresh.
//
// We could in principle ask the embedded *kgo.Client for this on every
// produce, but doing so has two problems. First, kgo's metadata view is
// updated on its own schedule and can be stale in different ways than ours.
// Second, the produce hot path needs read access without taking any lock
// the metadata refresher might hold; we want a consistent snapshot, not
// a live query. AgentPool therefore owns its own copy of the data, refreshed
// on a fixed cadence by the WarpstreamClient, and exposes it as an immutable
// poolState pointer swapped in atomically.
//
// The snapshot bundles three things together — agent list, per-topic UUIDs,
// and the partition-assignment strategy — because the produce path needs
// to read all three coherently. Splitting them across separate atomics
// would let a reader observe a strategy built from one set of agents while
// looking up a topic UUID from a different one, which would manifest as
// rare, hard-to-diagnose hedging-to-departed-agent bugs.
//
// Refresh has one data-preservation rule: when Metadata reports a topic
// with a non-zero ErrorCode (LEADER_NOT_AVAILABLE during reassignment,
// etc.) the response zeroes the topic UUID. Carrying the previous UUID
// forward keeps the topic known during transient errors; the topic's leaders
// are still dropped, so routing blocks until they reappear.
type AgentPool struct {
	client *kgo.Client

	// state is replaced atomically on every Refresh. Refresh must not be called
	// concurrently from multiple goroutines; readers go through Strategy() and TopicID().
	state atomic.Pointer[poolState]
}

// NewAgentPool returns an AgentPool with an empty (non-nil) strategy until
// the first Refresh.
func NewAgentPool(client *kgo.Client) *AgentPool {
	p := &AgentPool{client: client}
	p.state.Store(&poolState{
		topicIDs: map[string][16]byte{},
		strategy: newDefaultPartitionAssignmentStrategy(nil, nil, nil, nil),
	})
	return p
}

// Refresh atomically replaces the snapshot. Returns the NodeIDs that have left
// the cluster since the last Refresh — callers must purge per-agent state
// (stats, etc.) for those IDs. Not safe for concurrent calls.
func (p *AgentPool) Refresh(ctx context.Context) ([]int32, error) {
	removed, _, err := p.refresh(ctx)
	return removed, err
}

// refresh atomically replaces the snapshot. removed is the NodeIDs that have
// left since the last refresh. dropped counts leaders excluded from the broker
// set and names one when the count is above zero. Not safe for concurrent calls.
func (p *AgentPool) refresh(ctx context.Context) (removed []int32, dropped leaderDrops, err error) {
	// Topics=nil requests metadata for every topic in the cluster.
	// A tiny positive cache age keeps production refreshes fresh while using
	// kgo's bounded internal Metadata retry policy.
	req := kmsg.NewPtrMetadataRequest()
	meta, err := p.client.RequestCachedMetadata(ctx, req, time.Nanosecond)
	if err != nil {
		return nil, leaderDrops{}, fmt.Errorf("fetching metadata: %w", err)
	}

	newAgents := make([]int32, 0, len(meta.Brokers))
	for _, b := range meta.Brokers {
		newAgents = append(newAgents, b.NodeID)
	}
	sort.Slice(newAgents, func(i, j int) bool { return newAgents[i] < newAgents[j] })

	agentSet := make(map[int32]struct{}, len(newAgents))
	for _, id := range newAgents {
		agentSet[id] = struct{}{}
	}

	prev := p.state.Load()
	newLeaders, newTopicIDs, noLiveLeader, noLeader, dropped := buildLeadersAndTopicIDs(meta.Topics, agentSet, prev.topicIDs)
	removed = diffRemovedAgents(prev.agents, agentSet)

	p.state.Store(&poolState{
		agents:   newAgents,
		topicIDs: newTopicIDs,
		strategy: newDefaultPartitionAssignmentStrategy(newAgents, newLeaders, noLiveLeader, noLeader),
	})
	return removed, dropped, nil
}

// Strategy returns the strategy from the last Refresh.
func (p *AgentPool) Strategy() PartitionAssignmentStrategy {
	return p.state.Load().strategy
}

// Agents returns the NodeIDs known as of the last Refresh, sorted ascending.
// The slice is owned by the immutable snapshot and must not be mutated.
func (p *AgentPool) Agents() []int32 {
	return p.state.Load().agents
}

// TopicID returns the UUID of topic from the last successful Metadata refresh, or
// ok=false if the topic was unknown. UUIDs are required for Produce v13+.
func (p *AgentPool) TopicID(topic string) ([16]byte, bool) {
	id, ok := p.state.Load().topicIDs[topic]
	return id, ok
}

// leaderDrops counts leaders whose NodeID was absent from the broker set.
// Topic, Partition, and NodeID are one sample when Count is above zero.
type leaderDrops struct {
	Count     int
	Topic     string
	Partition int32
	NodeID    int32
}

// buildLeadersAndTopicIDs extracts the leader map and topic UUIDs from a
// Metadata response, plus topics whose named leaders were all excluded (nil
// when there are none) and the excluded-leader count with one sample.
// Drops leaders pointing to NodeIDs absent from agentSet (transient
// mid-update window). Carries previous UUIDs forward for topics returned
// with a non-zero ErrorCode, so transient errors don't blank the topic
// from the producer's view. A topic-level ErrorCode is not a drop. A
// partition Leader below 0 names no node, so it is not a drop and not a
// fallback. noLeader is nil when there are none.
func buildLeadersAndTopicIDs(
	respTopics []kmsg.MetadataResponseTopic,
	agentSet map[int32]struct{},
	prevTopicIDs map[string][16]byte,
) (map[topicPartition]int32, map[string][16]byte, map[string]struct{}, map[topicPartition]struct{}, leaderDrops) {
	topicIDs := make(map[string][16]byte, len(respTopics))
	leaders := make(map[topicPartition]int32, len(respTopics)*8)
	var topicsWithNoLiveLeader map[string]struct{}
	var noLeader map[topicPartition]struct{}
	var dropped leaderDrops
	for _, t := range respTopics {
		if t.Topic == nil {
			continue
		}
		name := *t.Topic
		if t.ErrorCode != 0 {
			// Carry the prior UUID over for this topic only; skip its (empty) partition list.
			if id, ok := prevTopicIDs[name]; ok {
				topicIDs[name] = id
			}
			continue
		}
		topicIDs[name] = t.TopicID
		kept, excluded := 0, 0
		for _, part := range t.Partitions {
			// Leader below 0 names nobody. It is not a node id missing from
			// the broker list, so it must not count as a drop or fall back.
			if part.Leader < 0 {
				if noLeader == nil {
					noLeader = make(map[topicPartition]struct{})
				}
				noLeader[topicPartition{topic: name, partition: part.Partition}] = struct{}{}
				continue
			}
			if _, known := agentSet[part.Leader]; !known {
				if dropped.Count == 0 {
					dropped.Topic = name
					dropped.Partition = part.Partition
					dropped.NodeID = part.Leader
				}
				dropped.Count++
				excluded++
				continue
			}
			kept++
			leaders[topicPartition{topic: name, partition: part.Partition}] = part.Leader
		}
		if excluded > 0 && kept == 0 {
			if topicsWithNoLiveLeader == nil {
				topicsWithNoLiveLeader = make(map[string]struct{})
			}
			topicsWithNoLiveLeader[name] = struct{}{}
		}
	}
	return leaders, topicIDs, topicsWithNoLiveLeader, noLeader, dropped
}

// diffRemovedAgents returns NodeIDs in old that are absent from newSet.
func diffRemovedAgents(old []int32, newSet map[int32]struct{}) []int32 {
	var removed []int32
	for _, id := range old {
		if _, still := newSet[id]; !still {
			removed = append(removed, id)
		}
	}
	return removed
}
