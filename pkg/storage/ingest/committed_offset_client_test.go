// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestCommittedOffsetsClient_FetchCommitted(t *testing.T) {
	const (
		numPartitions = 2
		topicName     = "test"
		partitionID   = int32(1)
		consumerGroup = "test-group"
	)

	// commitOffset commits the given offset for the partition in the Kafka cluster at addr,
	// under the given consumer group.
	commitOffset := func(t *testing.T, clusterAddr, group string, offset int64) {
		t.Helper()

		kafkaCfg := createTestKafkaConfig(clusterAddr, topicName)
		client := createTestKafkaClient(t, kafkaCfg)

		// Produce a record so the committed offset points to a consumed record.
		produceRecord(t.Context(), t, client, topicName, partitionID, []byte("msg"))

		offsets := kadm.Offsets{}
		offsets.Add(kadm.Offset{Topic: topicName, Partition: partitionID, At: offset})
		_, err := kadm.NewClient(client).CommitOffsets(t.Context(), group, offsets)
		require.NoError(t, err)
	}

	newClient := func(t *testing.T, addrs ...string) *CommittedOffsetsClient {
		t.Helper()
		configs := make([]KafkaConfig, len(addrs))
		for i, addr := range addrs {
			configs[i] = createTestKafkaConfig(addr, topicName)
		}
		client, err := NewCommittedOffsetsClient(configs, consumerGroup, partitionID, log.NewNopLogger())
		require.NoError(t, err)
		t.Cleanup(client.Close)
		return client
	}

	t.Run("returns no committed offsets until the group commits to any cluster", func(t *testing.T) {
		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		client := newClient(t, clusterAddr)

		offsets, anyCommitted, err := client.FetchCommitted(t.Context())
		require.NoError(t, err)
		require.False(t, anyCommitted)
		require.Equal(t, kmeta.NewMultiClusterPartitionOffsets([]int64{-1}), offsets)
	})

	t.Run("returns the committed offset of every cluster", func(t *testing.T) {
		// Each Kafka cluster has an independent offset space, so the same partition holds
		// a different committed offset in each cluster.
		_, clusterAddr0 := testkafka.CreateCluster(t, numPartitions, topicName)
		_, clusterAddr1 := testkafka.CreateCluster(t, numPartitions, topicName)
		commitOffset(t, clusterAddr0, consumerGroup, 1)
		commitOffset(t, clusterAddr1, consumerGroup, 5)

		client := newClient(t, clusterAddr0, clusterAddr1)

		offsets, anyCommitted, err := client.FetchCommitted(t.Context())
		require.NoError(t, err)
		require.True(t, anyCommitted)
		require.Equal(t, kmeta.NewMultiClusterPartitionOffsets([]int64{1, 5}), offsets)
	})

	t.Run("returns -1 for a cluster the group did not commit to", func(t *testing.T) {
		_, clusterAddr0 := testkafka.CreateCluster(t, numPartitions, topicName)
		_, clusterAddr1 := testkafka.CreateCluster(t, numPartitions, topicName)
		commitOffset(t, clusterAddr0, consumerGroup, 1)

		client := newClient(t, clusterAddr0, clusterAddr1)

		offsets, anyCommitted, err := client.FetchCommitted(t.Context())
		require.NoError(t, err)
		require.True(t, anyCommitted)
		require.Equal(t, kmeta.NewMultiClusterPartitionOffsets([]int64{1, -1}), offsets)
	})

	t.Run("returns -1 for a partition the group did not commit to", func(t *testing.T) {
		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)

		configs := []KafkaConfig{createTestKafkaConfig(clusterAddr, topicName)}
		client, err := NewCommittedOffsetsClient(configs, consumerGroup, 99, log.NewNopLogger())
		require.NoError(t, err)
		t.Cleanup(client.Close)

		offsets, anyCommitted, err := client.FetchCommitted(t.Context())
		require.NoError(t, err)
		require.False(t, anyCommitted)
		require.Equal(t, kmeta.NewMultiClusterPartitionOffsets([]int64{-1}), offsets)
	})
}
