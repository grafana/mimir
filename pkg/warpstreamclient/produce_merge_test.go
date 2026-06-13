// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestMergeProduceResponses(t *testing.T) {
	t.Run("concatenates per-topic partitions across sub-responses", func(t *testing.T) {
		respA := &kmsg.ProduceResponse{Version: 12, Topics: []kmsg.ProduceResponseTopic{{
			Topic:   "t",
			TopicID: [16]byte{0xab},
			Partitions: []kmsg.ProduceResponseTopicPartition{
				{Partition: 0, BaseOffset: 10},
				{Partition: 2, BaseOffset: 20},
			},
		}}}
		respB := &kmsg.ProduceResponse{Version: 12, Topics: []kmsg.ProduceResponseTopic{{
			Topic:   "t",
			TopicID: [16]byte{0xab},
			Partitions: []kmsg.ProduceResponseTopicPartition{
				{Partition: 1, BaseOffset: 30},
			},
		}}}

		merged := mergeProduceResponses([]*kmsg.ProduceResponse{respA, respB})

		assert.Equal(t, int16(12), merged.Version)
		require.Len(t, merged.Topics, 1)
		assert.Equal(t, [16]byte{0xab}, merged.Topics[0].TopicID)
		require.Len(t, merged.Topics[0].Partitions, 3)
		// All entries from both sub-responses are present.
		var partitions []int32
		for _, p := range merged.Topics[0].Partitions {
			partitions = append(partitions, p.Partition)
		}
		assert.ElementsMatch(t, []int32{0, 1, 2}, partitions)
	})

	t.Run("merges responses spanning multiple topics", func(t *testing.T) {
		respA := &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{
			{Topic: "a", Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0}}},
			{Topic: "b", Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0}}},
		}}
		respB := &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{
			{Topic: "a", Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 1}}},
		}}

		merged := mergeProduceResponses([]*kmsg.ProduceResponse{respA, respB})

		require.Len(t, merged.Topics, 2)
		// Topic "a" sees both sub-responses' partitions.
		var aParts []int32
		for _, p := range merged.Topics[0].Partitions {
			aParts = append(aParts, p.Partition)
		}
		assert.ElementsMatch(t, []int32{0, 1}, aParts)
		// Topic "b" sees its single partition.
		require.Len(t, merged.Topics[1].Partitions, 1)
	})

	t.Run("ThrottleMillis is the max across sub-responses", func(t *testing.T) {
		respA := &kmsg.ProduceResponse{ThrottleMillis: 3, Topics: []kmsg.ProduceResponseTopic{{
			Topic:      "t",
			Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0}},
		}}}
		respB := &kmsg.ProduceResponse{ThrottleMillis: 7, Topics: []kmsg.ProduceResponseTopic{{
			Topic:      "t",
			Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 1}},
		}}}
		assert.Equal(t, int32(7), mergeProduceResponses([]*kmsg.ProduceResponse{respA, respB}).ThrottleMillis)
	})

	t.Run("nil sub-responses are skipped", func(t *testing.T) {
		respA := &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{
			Topic:      "t",
			Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0}},
		}}}
		merged := mergeProduceResponses([]*kmsg.ProduceResponse{nil, respA, nil})
		require.Len(t, merged.Topics, 1)
		require.Len(t, merged.Topics[0].Partitions, 1)
	})

	t.Run("empty input returns an empty response", func(t *testing.T) {
		merged := mergeProduceResponses(nil)
		assert.Equal(t, int16(0), merged.Version)
		assert.Empty(t, merged.Topics)
	})
}
