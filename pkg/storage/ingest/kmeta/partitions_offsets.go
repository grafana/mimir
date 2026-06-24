// SPDX-License-Identifier: AGPL-3.0-only

package kmeta

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
