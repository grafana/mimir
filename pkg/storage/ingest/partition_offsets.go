// SPDX-License-Identifier: AGPL-3.0-only

package ingest

// PartitionOffsets carries the offset of a single partition across one or more Kafka clusters. Offsets
// live in independent per-cluster offset spaces, so the same partition has a distinct offset in each
// Kafka cluster. The offset of Kafka cluster N is held at index N; in the single-cluster case there is
// only cluster 0.
type PartitionOffsets struct {
	// offsets is indexed by Kafka cluster ID. Cluster IDs are consecutive and start from 0.
	offsets []int64
}

// NewMultiClusterPartitionOffsets returns a PartitionOffsets holding one offset per Kafka cluster,
// indexed by Kafka cluster ID.
func NewMultiClusterPartitionOffsets(offsets []int64) PartitionOffsets {
	return PartitionOffsets{offsets: offsets}
}

// NewSingleClusterPartitionOffsets returns a PartitionOffsets holding a single offset for Kafka cluster
// 0. It is used in the single Kafka cluster case.
func NewSingleClusterPartitionOffsets(offset int64) PartitionOffsets {
	return PartitionOffsets{offsets: []int64{offset}}
}

// NumKafkaClusters returns the number of Kafka clusters the offsets cover.
func (o PartitionOffsets) NumKafkaClusters() int {
	return len(o.offsets)
}

// ForKafkaCluster returns the offset for the given Kafka cluster, or -1 if no offset is set for it (for
// example when the cluster ID is out of range).
func (o PartitionOffsets) ForKafkaCluster(clusterID int) int64 {
	if clusterID < 0 || clusterID >= len(o.offsets) {
		return -1
	}
	return o.offsets[clusterID]
}
