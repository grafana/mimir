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

// FetchCommittedOffsets fetches the last committed offsets of consumerGroup and the given partition
// from one Kafka cluster per config, indexed by Kafka cluster ID (the position in configs; the same
// indexing as PartitionReader.LastSeenOffsets and kmeta.PartitionOffsets). A cluster the consumer
// group has not committed to yields offset -1. anyCommitted reports whether the group committed to
// at least one cluster.
//
// The committed offsets are polled infrequently, so no client is worth keeping alive: each call
// opens and closes one short-lived Kafka client per cluster. The clients only issue offset fetch
// requests, so the reader's fetch configuration doesn't apply; the common options carry the
// addressing and authentication.
//
// If any cluster's fetch fails, no offsets are returned: the caller is expected to keep using the
// previously fetched ones.
func FetchCommittedOffsets(ctx context.Context, configs []KafkaConfig, consumerGroup string, partitionID int32, logger log.Logger) (offsets kmeta.PartitionOffsets, anyCommitted bool, _ error) {
	fetched := make([]int64, len(configs))
	for clusterID, kafkaCfg := range configs {
		offset, exists, err := fetchLastCommittedOffset(ctx, kafkaCfg, consumerGroup, partitionID, logger)
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

// fetchLastCommittedOffset returns the cluster's last committed offset for the consumer group and
// partition. exists=false when the consumer group or the partition has no committed offset.
func fetchLastCommittedOffset(ctx context.Context, kafkaCfg KafkaConfig, consumerGroup string, partitionID int32, logger log.Logger) (offset int64, exists bool, _ error) {
	client, err := kgo.NewClient(commonKafkaClientOptions(kafkaCfg, nil, logger)...)
	if err != nil {
		return 0, false, fmt.Errorf("creating kafka client: %w", err)
	}
	defer client.Close()

	offsets, err := kadm.NewClient(client).FetchOffsets(ctx, consumerGroup)
	if errors.Is(err, kerr.GroupIDNotFound) || errors.Is(err, kerr.UnknownTopicOrPartition) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("fetch group %s offsets: %w", consumerGroup, err)
	}

	offsetRes, exists := offsets.Lookup(kafkaCfg.Topic, partitionID)
	if !exists {
		return 0, false, nil
	}
	if offsetRes.Err != nil {
		return 0, false, fmt.Errorf("fetch group %s offset: topic %s, partition %d: %w", consumerGroup, kafkaCfg.Topic, partitionID, offsetRes.Err)
	}

	return offsetRes.At, true, nil
}
