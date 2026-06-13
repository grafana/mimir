// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// poolState holds the agent pool snapshot updated atomically by Refresh.
// Bundling the fields into one pointer prevents readers from observing a torn
// snapshot across the strategy, topic UUIDs, and agent list.
type poolState struct {
	agents   []int32 // sorted ascending, used by Refresh to detect removed agents
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
// etc.) the response zeroes the topic UUID, so we carry the previous UUID
// forward to keep producing during transient hiccups.
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
		strategy: newDefaultPartitionAssignmentStrategy(nil, nil),
	})
	return p
}

// Refresh atomically replaces the snapshot. Uses the kgo.Client's cached
// Metadata when fresh, falls back to a real MetadataRequest when stale.
// Returns the NodeIDs that have left the cluster since the last Refresh —
// callers must purge per-agent state (stats, etc.) for those IDs. Not safe
// for concurrent calls.
func (p *AgentPool) Refresh(ctx context.Context) (removed []int32, err error) {
	// Topics=nil requests metadata for every topic in the cluster.
	req := kmsg.NewPtrMetadataRequest()
	meta, err := p.client.RequestCachedMetadata(ctx, req, 0)
	if err != nil {
		return nil, fmt.Errorf("fetching metadata: %w", err)
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
	newLeaders, newTopicIDs := buildLeadersAndTopicIDs(meta.Topics, agentSet, prev.topicIDs)
	removed = diffRemovedAgents(prev.agents, agentSet)

	p.state.Store(&poolState{
		agents:   newAgents,
		topicIDs: newTopicIDs,
		strategy: newDefaultPartitionAssignmentStrategy(newAgents, newLeaders),
	})
	return removed, nil
}

// Strategy returns the strategy from the last Refresh.
func (p *AgentPool) Strategy() PartitionAssignmentStrategy {
	return p.state.Load().strategy
}

// TopicID returns the UUID of topic from the last Metadata refresh, or
// ok=false if the topic was unknown. UUIDs are required for Produce v13+.
func (p *AgentPool) TopicID(topic string) ([16]byte, bool) {
	id, ok := p.state.Load().topicIDs[topic]
	return id, ok
}

// buildLeadersAndTopicIDs extracts the leader map and topic UUIDs from a
// Metadata response. Drops leaders pointing to NodeIDs absent from agentSet
// (transient mid-update window). Carries previous UUIDs forward for topics
// returned with a non-zero ErrorCode, so transient errors don't blank the
// topic from the producer's view.
func buildLeadersAndTopicIDs(
	respTopics []kmsg.MetadataResponseTopic,
	agentSet map[int32]struct{},
	prevTopicIDs map[string][16]byte,
) (map[topicPartition]int32, map[string][16]byte) {
	topicIDs := make(map[string][16]byte, len(respTopics))
	leaders := make(map[topicPartition]int32, len(respTopics)*8)
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
		for _, part := range t.Partitions {
			if _, known := agentSet[part.Leader]; !known {
				continue
			}
			leaders[topicPartition{topic: name, partition: part.Partition}] = part.Leader
		}
	}
	return leaders, topicIDs
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
