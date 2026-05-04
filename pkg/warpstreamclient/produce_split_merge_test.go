// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestSplitProduceRequest(t *testing.T) {
	const topic = "t"
	makeReq := func(parts ...int32) *kmsg.ProduceRequest {
		var partitions []kmsg.ProduceRequestTopicPartition
		for _, p := range parts {
			partitions = append(partitions, kmsg.ProduceRequestTopicPartition{Partition: p, Records: []byte{byte(p)}})
		}
		return &kmsg.ProduceRequest{
			Version:       12,
			Acks:          -1,
			TimeoutMillis: 1000,
			Topics: []kmsg.ProduceRequestTopic{{
				Topic:      topic,
				TopicID:    [16]byte{0xab},
				Partitions: partitions,
			}},
		}
	}

	t.Run("groups partitions by per-partition secondary", func(t *testing.T) {
		strat := &mockPartitionAssignmentStrategy{
			secondary: map[partitionKey]int32{
				{topic, 0}: 7,
				{topic, 1}: 9,
				{topic, 2}: 7,
			},
		}
		out, err := splitProduceRequestToSecondaryAgents(makeReq(0, 1, 2), strat)
		require.NoError(t, err)
		require.Len(t, out, 2)
		assert.ElementsMatch(t, []int32{0, 2}, partitionsOf(out[7], topic))
		assert.ElementsMatch(t, []int32{1}, partitionsOf(out[9], topic))
	})

	t.Run("empty input fails the split", func(t *testing.T) {
		strat := &mockPartitionAssignmentStrategy{}
		out, err := splitProduceRequestToSecondaryAgents(&kmsg.ProduceRequest{}, strat)
		require.Error(t, err)
		assert.Nil(t, out)
		assert.ErrorContains(t, err, "no partitions")
	})

	t.Run("partition without a secondary fails the whole split", func(t *testing.T) {
		strat := &mockPartitionAssignmentStrategy{
			secondary: map[partitionKey]int32{
				{topic, 0}: 7,
				// (topic, 1) → no secondary
			},
		}
		out, err := splitProduceRequestToSecondaryAgents(makeReq(0, 1), strat)
		require.Error(t, err)
		assert.Nil(t, out)
		assert.ErrorContains(t, err, "partition 1")
	})

	t.Run("preserves top-level fields and topic IDs", func(t *testing.T) {
		strat := &mockPartitionAssignmentStrategy{secondary: map[partitionKey]int32{{topic, 0}: 7}}
		out, err := splitProduceRequestToSecondaryAgents(makeReq(0), strat)
		require.NoError(t, err)
		sub := out[7]
		assert.Equal(t, int16(12), sub.Version)
		assert.Equal(t, int16(-1), sub.Acks)
		assert.Equal(t, int32(1000), sub.TimeoutMillis)
		assert.Equal(t, [16]byte{0xab}, sub.Topics[0].TopicID)
	})

	t.Run("multi-topic split", func(t *testing.T) {
		req := &kmsg.ProduceRequest{
			Topics: []kmsg.ProduceRequestTopic{
				{Topic: "a", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0}, {Partition: 1}}},
				{Topic: "b", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0}}},
			},
		}
		strat := &mockPartitionAssignmentStrategy{
			secondary: map[partitionKey]int32{
				{"a", 0}: 7,
				{"a", 1}: 9,
				{"b", 0}: 7,
			},
		}
		out, err := splitProduceRequestToSecondaryAgents(req, strat)
		require.NoError(t, err)
		require.Len(t, out, 2)
		// Secondary 7 should carry (a, 0) and (b, 0).
		assert.ElementsMatch(t, []int32{0}, partitionsOf(out[7], "a"))
		assert.ElementsMatch(t, []int32{0}, partitionsOf(out[7], "b"))
		// Secondary 9 should carry only (a, 1).
		assert.ElementsMatch(t, []int32{1}, partitionsOf(out[9], "a"))
	})
}

func partitionsOf(req *kmsg.ProduceRequest, topic string) []int32 {
	for _, t := range req.Topics {
		if t.Topic == topic {
			parts := make([]int32, len(t.Partitions))
			for i, p := range t.Partitions {
				parts[i] = p.Partition
			}
			return parts
		}
	}
	return nil
}

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
