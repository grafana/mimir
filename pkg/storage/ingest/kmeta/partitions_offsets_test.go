// SPDX-License-Identifier: AGPL-3.0-only

package kmeta

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPartitionOffsets_ForKafkaCluster(t *testing.T) {
	offsets := NewMultiClusterPartitionOffsets([]int64{10, 20, -1})

	assert.Equal(t, 3, offsets.NumKafkaClusters())
	assert.Equal(t, int64(10), offsets.ForKafkaCluster(0))
	assert.Equal(t, int64(20), offsets.ForKafkaCluster(1))
	assert.Equal(t, int64(-1), offsets.ForKafkaCluster(2))

	// Out-of-range (including negative) cluster IDs return -1, handled identically to an empty partition.
	assert.Equal(t, int64(-1), offsets.ForKafkaCluster(3))
	assert.Equal(t, int64(-1), offsets.ForKafkaCluster(-1))

	// The single-cluster constructor yields exactly one cluster.
	single := NewSingleClusterPartitionOffsets(7)
	assert.Equal(t, 1, single.NumKafkaClusters())
	assert.Equal(t, int64(7), single.ForKafkaCluster(0))
	assert.Equal(t, int64(-1), single.ForKafkaCluster(1))
}

func TestMergeTopicsPartitionsOffsets(t *testing.T) {
	tests := map[string]struct {
		perCluster []map[string]map[int32]int64
		expected   TopicsPartitionsOffsets
	}{
		"no clusters": {
			perCluster: nil,
			expected:   TopicsPartitionsOffsets{},
		},
		"single cluster, single topic, multiple partitions": {
			perCluster: []map[string]map[int32]int64{
				{"topic-0": {0: 5, 1: -1}},
			},
			expected: TopicsPartitionsOffsets{
				"topic-0": {
					0: NewMultiClusterPartitionOffsets([]int64{5}),
					1: NewMultiClusterPartitionOffsets([]int64{-1}),
				},
			},
		},
		"two clusters, full overlap": {
			perCluster: []map[string]map[int32]int64{
				{"topic-0": {0: 2, 1: 1}},
				{"topic-0": {0: 0, 1: 3}},
			},
			expected: TopicsPartitionsOffsets{
				"topic-0": {
					0: NewMultiClusterPartitionOffsets([]int64{2, 0}),
					1: NewMultiClusterPartitionOffsets([]int64{1, 3}),
				},
			},
		},
		"two clusters, partial overlap defaults absent topic-partitions to -1": {
			perCluster: []map[string]map[int32]int64{
				{"topic-0": {0: 2, 1: 1, 2: 0}, "topic-1": {0: 0}},
				{"topic-0": {0: 0}},
			},
			expected: TopicsPartitionsOffsets{
				"topic-0": {
					0: NewMultiClusterPartitionOffsets([]int64{2, 0}),
					1: NewMultiClusterPartitionOffsets([]int64{1, -1}),
					2: NewMultiClusterPartitionOffsets([]int64{0, -1}),
				},
				"topic-1": {
					0: NewMultiClusterPartitionOffsets([]int64{0, -1}),
				},
			},
		},
		"topic present only on the second cluster": {
			perCluster: []map[string]map[int32]int64{
				{},
				{"topic-1": {0: 7}},
			},
			expected: TopicsPartitionsOffsets{
				"topic-1": {
					0: NewMultiClusterPartitionOffsets([]int64{-1, 7}),
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, MergeTopicsPartitionsOffsets(testData.perCluster))
		})
	}
}
