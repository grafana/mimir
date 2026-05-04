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

// AgentPool discovers and maintains the current set of Warpstream agents and
// the partition-leader mapping for every topic in the cluster. Each Refresh
// produces a new DefaultPartitionAssignmentStrategy from the updated snapshot.
type AgentPool struct {
	client *kgo.Client

	// state is replaced atomically on every Refresh. Refresh must not be called
	// concurrently from multiple goroutines; readers go through Strategy() and TopicID().
	state atomic.Pointer[poolState]
}

// NewAgentPool returns an AgentPool. Strategy() returns an empty (but non-nil)
// strategy until Refresh is called for the first time.
func NewAgentPool(client *kgo.Client) *AgentPool {
	p := &AgentPool{client: client}
	p.state.Store(&poolState{
		topicIDs: map[string][16]byte{},
		strategy: newDefaultPartitionAssignmentStrategy(nil, nil),
	})
	return p
}

// Refresh updates the agent pool and the leader mapping for every topic in the
// cluster from the kgo.Client's metadata cache (via RequestCachedMetadata),
// which avoids a duplicate network round-trip when the client's own background
// refresh has already fetched fresh data within MetadataMinAge. Falls back
// to a real MetadataRequest when the cache is stale.
//
// Refresh must not be called concurrently from multiple goroutines.
//
// Returns the NodeIDs of agents that were present before and are now absent;
// callers must purge these from any per-agent state (e.g. AgentStatsTracker).
// A new DefaultPartitionAssignmentStrategy is always created from the updated snapshot.
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

// Strategy returns the PartitionAssignmentStrategy built from the last Refresh.
func (p *AgentPool) Strategy() PartitionAssignmentStrategy {
	return p.state.Load().strategy
}

// TopicID returns the UUID of the given topic as reported by the last Metadata refresh.
// Returns the zero UUID for topics that did not exist in the cluster at the last refresh.
// Required for Produce API v13+, which addresses topics by UUID rather than name.
func (p *AgentPool) TopicID(topic string) [16]byte {
	return p.state.Load().topicIDs[topic]
}

// buildLeadersAndTopicIDs extracts the leader map and topic UUIDs from a Metadata
// response covering every topic in the cluster (Topics=nil request).
//
// The response is authoritative: topics absent from it have been deleted and are
// dropped from the snapshot. Topics returned with a non-zero ErrorCode are a
// special case — the response carries no useful UUID or partition data (zero
// UUID, empty Partitions), so we carry over the previous UUID to ride out
// transient errors like LEADER_NOT_AVAILABLE without breaking the producer.
//
// Leaders whose NodeID is not in agentSet are dropped, guarding against
// transient mid-update responses where a partition references a broker that
// has already left the cluster.
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

// diffRemovedAgents returns the NodeIDs in old that are absent from newSet.
func diffRemovedAgents(old []int32, newSet map[int32]struct{}) []int32 {
	var removed []int32
	for _, id := range old {
		if _, still := newSet[id]; !still {
			removed = append(removed, id)
		}
	}
	return removed
}
