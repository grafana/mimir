// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestCommittedOffsetClient_FetchLastCommittedOffset(t *testing.T) {
	const (
		numPartitions = 2
		topicName     = "test"
		partitionID   = int32(1)
		consumerGroup = "test-group"
	)

	t.Run("returns exists=false when no offset is committed", func(t *testing.T) {
		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		kafkaCfg := createTestKafkaConfig(clusterAddr, topicName)
		client := createTestKafkaClient(t, kafkaCfg)

		c := NewCommittedOffsetClient(client, topicName)
		_, exists, err := c.FetchLastCommittedOffset(t.Context(), consumerGroup, partitionID)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("returns committed offset", func(t *testing.T) {
		ctx := t.Context()

		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		kafkaCfg := createTestKafkaConfig(clusterAddr, topicName)
		client := createTestKafkaClient(t, kafkaCfg)

		produceRecord(ctx, t, client, topicName, partitionID, []byte("msg"))

		adm := kadm.NewClient(client)
		offsets := kadm.Offsets{}
		offsets.Add(kadm.Offset{Topic: topicName, Partition: partitionID, At: 1})
		_, err := adm.CommitOffsets(ctx, consumerGroup, offsets)
		require.NoError(t, err)

		c := NewCommittedOffsetClient(client, topicName)
		offset, exists, err := c.FetchLastCommittedOffset(ctx, consumerGroup, partitionID)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, int64(1), offset)
	})

	t.Run("returns exists=false for unknown partition", func(t *testing.T) {
		ctx := t.Context()

		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		kafkaCfg := createTestKafkaConfig(clusterAddr, topicName)
		client := createTestKafkaClient(t, kafkaCfg)

		produceRecord(ctx, t, client, topicName, partitionID, []byte("msg"))

		adm := kadm.NewClient(client)
		offsets := kadm.Offsets{}
		offsets.Add(kadm.Offset{Topic: topicName, Partition: partitionID, At: 1})
		_, err := adm.CommitOffsets(ctx, consumerGroup, offsets)
		require.NoError(t, err)

		c := NewCommittedOffsetClient(client, topicName)
		// Partition 99 was never committed.
		_, exists, err := c.FetchLastCommittedOffset(ctx, consumerGroup, int32(99))
		require.NoError(t, err)
		require.False(t, exists)
	})
}
