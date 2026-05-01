// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"github.com/twmb/franz-go/pkg/kmsg"
)

// mergeProduceResponses concatenates per-partition entries from a set of
// disjoint sub-responses (one per secondary in a hedge fanout) into a
// single merged response. The merge is unconditional — per-partition
// error codes pass through.
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
		// Version: take the first non-nil sub-response — sub-responses
		// share the same wire version as the original request. Throttle
		// is the max across sub-responses (most-throttling agent wins).
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
