// SPDX-License-Identifier: AGPL-3.0-only

package kmeta

import "fmt"

// PartitionOffsets carries the offset of a single partition across one or more Kafka clusters, indexed by
// Kafka cluster ID. Offsets live in independent per-cluster offset spaces, so the same partition has a
// distinct offset in each Kafka cluster. The offset of Kafka cluster N is held at index N; in the
// single-cluster case there is only cluster 0.
type PartitionOffsets []int64

// NewMultiClusterPartitionOffsets returns a PartitionOffsets holding one offset per Kafka cluster,
// indexed by Kafka cluster ID. It does not copy offsets: the returned value shares the backing array, so
// the caller must not mutate offsets afterwards.
func NewMultiClusterPartitionOffsets(offsets []int64) PartitionOffsets {
	return PartitionOffsets(offsets)
}

// NewSingleClusterPartitionOffsets returns a PartitionOffsets holding a single offset for Kafka cluster
// 0. It is used in the single Kafka cluster case.
func NewSingleClusterPartitionOffsets(offset int64) PartitionOffsets {
	return PartitionOffsets{offset}
}

// NumKafkaClusters returns the number of Kafka clusters the offsets cover.
func (o PartitionOffsets) NumKafkaClusters() int {
	return len(o)
}

// ForKafkaCluster returns the offset for the given Kafka cluster, or -1 if no offset is set for it (for
// example when the cluster ID is out of range).
func (o PartitionOffsets) ForKafkaCluster(clusterID int) int64 {
	if clusterID < 0 || clusterID >= len(o) {
		return -1
	}
	return o[clusterID]
}

// String returns the per-Kafka-cluster offsets, indexed by Kafka cluster ID.
func (o PartitionOffsets) String() string {
	return fmt.Sprintf("%v", []int64(o))
}

// PartitionsOffsets holds the offsets of several partitions, indexed by partition ID. Each partition's
// offsets span one or more Kafka clusters (see PartitionOffsets).
type PartitionsOffsets = map[int32]PartitionOffsets

// TopicsPartitionsOffsets holds the offsets of several topics, indexed by topic name then partition ID.
type TopicsPartitionsOffsets = map[string]PartitionsOffsets

// MergeTopicsPartitionsOffsets merges the last produced offsets fetched from each Kafka cluster — indexed by
// topic, then partition — into one PartitionOffsets per (topic, partition), carrying one offset per Kafka
// cluster. perCluster is indexed by Kafka cluster ID (its position in the slice). A (topic, partition)
// absent from a cluster gets a -1 offset for that cluster, handled identically to an empty partition.
func MergeTopicsPartitionsOffsets(perCluster []map[string]map[int32]int64) TopicsPartitionsOffsets {
	numClusters := len(perCluster)

	// Accumulate one offset slice per (topic, partition), indexed by Kafka cluster ID and defaulting to -1.
	merged := make(map[string]map[int32][]int64)
	for kafkaClusterID, byTopic := range perCluster {
		for topic, byPartition := range byTopic {
			topicOffsets := merged[topic]
			if topicOffsets == nil {
				topicOffsets = make(map[int32][]int64)
				merged[topic] = topicOffsets
			}

			for partitionID, offset := range byPartition {
				clusterOffsets := topicOffsets[partitionID]
				if clusterOffsets == nil {
					clusterOffsets = make([]int64, numClusters)
					for i := range clusterOffsets {
						clusterOffsets[i] = -1
					}
					topicOffsets[partitionID] = clusterOffsets
				}
				clusterOffsets[kafkaClusterID] = offset
			}
		}
	}

	result := make(TopicsPartitionsOffsets, len(merged))
	for topic, byPartition := range merged {
		topicOffsets := make(PartitionsOffsets, len(byPartition))
		for partitionID, clusterOffsets := range byPartition {
			topicOffsets[partitionID] = NewMultiClusterPartitionOffsets(clusterOffsets)
		}
		result[topic] = topicOffsets
	}

	return result
}
