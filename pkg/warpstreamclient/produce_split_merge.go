// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// splitProduceRequestToSecondaryAgents groups the partitions of req by their per-partition
// secondary, returning one sub-*ProduceRequest per unique secondary nodeID.
// The split is all-or-nothing: if any partition lacks a designated
// secondary, the function returns an error and no map. The caller should
// treat this as "fanout is not possible for this request" and surface the
// upstream (primary) error instead.
//
// The sub-requests share top-level fields (Version, TransactionID, Acks,
// TimeoutMillis) with the original; only the per-topic partition lists are
// pruned. TopicID is preserved per topic.
func splitProduceRequestToSecondaryAgents(req *kmsg.ProduceRequest, strategy PartitionAssignmentStrategy) (map[int32]*kmsg.ProduceRequest, error) {
	bySecondary := make(map[int32]*kmsg.ProduceRequest)

	for _, t := range req.Topics {
		for _, p := range t.Partitions {
			secID, ok := strategy.Secondary(t.Topic, p.Partition)
			if !ok {
				return nil, fmt.Errorf("no secondary available for topic %q partition %d", t.Topic, p.Partition)
			}
			sub, exists := bySecondary[secID]
			if !exists {
				sub = &kmsg.ProduceRequest{
					Version:       req.Version,
					TransactionID: req.TransactionID,
					Acks:          req.Acks,
					TimeoutMillis: req.TimeoutMillis,
				}
				bySecondary[secID] = sub
			}

			// Locate the topic entry in this sub-request, appending a new one if
			// absent, so we can append the partition in place.
			topicIdx := -1
			for i := range sub.Topics {
				if sub.Topics[i].Topic == t.Topic {
					topicIdx = i
					break
				}
			}
			if topicIdx == -1 {
				sub.Topics = append(sub.Topics, kmsg.ProduceRequestTopic{Topic: t.Topic, TopicID: t.TopicID})
				topicIdx = len(sub.Topics) - 1
			}
			sub.Topics[topicIdx].Partitions = append(sub.Topics[topicIdx].Partitions, p)
		}
	}
	if len(bySecondary) == 0 {
		return nil, fmt.Errorf("request has no partitions to split")
	}
	return bySecondary, nil
}

// mergeProduceResponses concatenates per-partition entries from a set of
// disjoint sub-responses (one per secondary in a fanout) into a single
// merged response. Each sub-response is expected to cover only its own
// subset of partitions; together they cover every partition in the
// original request.
//
// The merge is unconditional: it does not inspect ErrorCode. Per-partition
// errors stay in the merged response so the caller can see them, but the
// caller is expected to treat the fanout as all-or-nothing — if any
// sub-leg returned a transport error, the caller should not call this
// function and should return the error instead.
//
// Version is taken from the first non-nil sub-response (sub-responses
// share the same wire version as the original request, since each was
// issued at that version). ThrottleMillis is the maximum across
// sub-responses — the agent that asked the client to back off the most
// is the one whose throttle the caller should observe.
func mergeProduceResponses(resps []*kmsg.ProduceResponse) *kmsg.ProduceResponse {
	var (
		merged     = &kmsg.ProduceResponse{}
		versionSet bool
		topicIdx   = make(map[string]int)
	)

	for _, res := range resps {
		if res == nil {
			continue
		}
		if !versionSet {
			merged.Version = res.Version
			versionSet = true
		}
		merged.ThrottleMillis = max(merged.ThrottleMillis, res.ThrottleMillis)

		for _, t := range res.Topics {
			idx, ok := topicIdx[t.Topic]
			if !ok {
				idx = len(merged.Topics)
				topicIdx[t.Topic] = idx
				merged.Topics = append(merged.Topics, kmsg.ProduceResponseTopic{Topic: t.Topic, TopicID: t.TopicID})
			}
			merged.Topics[idx].Partitions = append(merged.Topics[idx].Partitions, t.Partitions...)
		}
	}
	return merged
}
