// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-kit/log"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
)

// CommittedOffsetsClient fetches the last committed offsets of a consumer group and partition
// from every Kafka cluster the ingester consumes from. It owns one Kafka client per cluster,
// released by Close.
type CommittedOffsetsClient struct {
	partition int32

	// clusters is indexed by Kafka cluster ID: the position of the KafkaConfig in the slice
	// passed to NewCommittedOffsetsClient. This matches the indexing of PartitionReader.LastSeenOffsets
	// and kmeta.PartitionOffsets, so per-cluster values line up across the three.
	clusters []committedOffsetsClusterClient
}

// committedOffsetsClusterClient fetches the committed offsets from a single Kafka cluster.
type committedOffsetsClusterClient struct {
	admin *kadm.Client

	group     string
	topic     string
	partition int32
}

// NewCommittedOffsetsClient returns a CommittedOffsetsClient fetching the committed offsets of
// consumerGroup from one Kafka cluster per given config, for the given partition. configs must
// hold one config per Kafka cluster the ingester consumes from, indexed by cluster ID, as
// returned by the ingester's consumptionKafkaConfigs.
func NewCommittedOffsetsClient(configs []KafkaConfig, consumerGroup string, partitionID int32, logger log.Logger) (*CommittedOffsetsClient, error) {
	c := &CommittedOffsetsClient{
		partition: partitionID,
		clusters:  make([]committedOffsetsClusterClient, len(configs)),
	}
	for clusterID, cfg := range configs {
		// The client only issues offset fetch requests, so the reader's fetch configuration
		// doesn't apply here; the common options carry the addressing and authentication.
		client, err := kgo.NewClient(commonKafkaClientOptions(cfg, nil, logger)...)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("creating kafka client for committed offsets of kafka cluster %d: %w", clusterID, err)
		}
		c.clusters[clusterID] = committedOffsetsClusterClient{
			admin:     kadm.NewClient(client),
			group:     consumerGroup,
			topic:     cfg.Topic,
			partition: partitionID,
		}
	}
	return c, nil
}

// FetchCommitted returns the last committed offsets of the consumer group and partition, one per
// Kafka cluster, indexed by cluster ID. A cluster the consumer group has not committed to yields
// offset -1. anyCommitted reports whether the group committed to at least one cluster.
//
// If any cluster's fetch fails, no offsets are returned: the caller is expected to keep using
// the previously fetched ones.
func (c *CommittedOffsetsClient) FetchCommitted(ctx context.Context) (offsets kmeta.PartitionOffsets, anyCommitted bool, _ error) {
	fetched := make([]int64, len(c.clusters))
	for clusterID, cl := range c.clusters {
		offset, exists, err := cl.fetchLastCommittedOffset(ctx)
		if err != nil {
			return nil, false, fmt.Errorf("fetching committed offsets of kafka cluster %d: %w", clusterID, err)
		}
		if !exists {
			offset = -1
		}
		fetched[clusterID] = offset
		anyCommitted = anyCommitted || exists
	}
	return kmeta.NewMultiClusterPartitionOffsets(fetched), anyCommitted, nil
}

// PartitionID returns the partition the client fetches committed offsets for.
func (c *CommittedOffsetsClient) PartitionID() int32 {
	return c.partition
}

// Close closes the Kafka clients, rendering the client unusable.
func (c *CommittedOffsetsClient) Close() {
	for _, cl := range c.clusters {
		cl.admin.Close()
	}
}

// fetchLastCommittedOffset returns the cluster's last committed offset for the consumer group and
// partition. exists=false when the consumer group or the partition has no committed offset.
func (c *committedOffsetsClusterClient) fetchLastCommittedOffset(ctx context.Context) (offset int64, exists bool, _ error) {
	offsets, err := c.admin.FetchOffsets(ctx, c.group)
	if errors.Is(err, kerr.GroupIDNotFound) || errors.Is(err, kerr.UnknownTopicOrPartition) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("fetch group %s offsets: %w", c.group, err)
	}

	offsetRes, exists := offsets.Lookup(c.topic, c.partition)
	if !exists {
		return 0, false, nil
	}
	if offsetRes.Err != nil {
		return 0, false, fmt.Errorf("fetch group %s offset: topic %s, partition %d: %w", c.group, c.topic, c.partition, offsetRes.Err)
	}

	return offsetRes.At, true, nil
}
