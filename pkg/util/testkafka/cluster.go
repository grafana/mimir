// SPDX-License-Identifier: AGPL-3.0-only

package testkafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// CreateCluster returns a fake Kafka cluster for unit testing.
func CreateCluster(t testing.TB, numPartitions int32, topicName string) (*kfake.Cluster, string) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(numPartitions, topicName))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	addrs := cluster.ListenAddrs()
	require.Len(t, addrs, 1)

	addSupportForConsumerGroups(t, cluster, topicName, numPartitions)

	return cluster, addrs[0]
}

// addSupportForConsumerGroups adds very bare-bones support for one consumer group.
// It expects that only one partition is consumed at a time.
func addSupportForConsumerGroups(t testing.TB, cluster *kfake.Cluster, topicName string, numPartitions int32) {
	committedOffsets := map[string][]int64{}

	ensureConsumerGroupExists := func(consumerGroup string) {
		if _, ok := committedOffsets[consumerGroup]; ok {
			return
		}
		committedOffsets[consumerGroup] = make([]int64, numPartitions+1)
	}

	cluster.ControlKey(kmsg.OffsetCommit.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		commitR := request.(*kmsg.OffsetCommitRequest)
		consumerGroup := commitR.Group
		ensureConsumerGroupExists(consumerGroup)
		assert.Len(t, commitR.Topics, 1, "test only has support for one topic per request")
		topic := commitR.Topics[0]
		assert.Equal(t, topicName, topic.Topic)
		assert.Len(t, topic.Partitions, 1, "test only has support for one partition per request")

		partitionID := topic.Partitions[0].Partition
		committedOffsets[consumerGroup][partitionID] = topic.Partitions[0].Offset

		resp := request.ResponseKind().(*kmsg.OffsetCommitResponse)
		resp.Default()
		resp.Topics = []kmsg.OffsetCommitResponseTopic{
			{
				Topic:      topicName,
				Partitions: []kmsg.OffsetCommitResponseTopicPartition{{Partition: partitionID}},
			},
		}

		return resp, nil, true
	})

	cluster.ControlKey(kmsg.OffsetFetch.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		commitR := request.(*kmsg.OffsetFetchRequest)
		assert.Len(t, commitR.Groups, 1, "test only has support for one consumer group per request")
		consumerGroup := commitR.Groups[0].Group
		ensureConsumerGroupExists(consumerGroup)

		const allPartitions = -1
		var partitionID int32

		if len(commitR.Groups[0].Topics) == 0 {
			// An empty request means fetch all topic-partitions for this group.
			partitionID = allPartitions
		} else {
			partitionID = commitR.Groups[0].Topics[0].Partitions[0]
			assert.Len(t, commitR.Groups[0], 1, "test only has support for one partition per request")
			assert.Len(t, commitR.Groups[0].Topics[0].Partitions, 1, "test only has support for one partition per request")
		}

		var partitionsResp []kmsg.OffsetFetchResponseGroupTopicPartition
		if partitionID == allPartitions {
			for i := int32(1); i < numPartitions+1; i++ {
				partitionsResp = append(partitionsResp, kmsg.OffsetFetchResponseGroupTopicPartition{
					Partition: i,
					Offset:    committedOffsets[consumerGroup][i],
				})
			}
		} else {
			partitionsResp = append(partitionsResp, kmsg.OffsetFetchResponseGroupTopicPartition{
				Partition: partitionID,
				Offset:    committedOffsets[consumerGroup][partitionID],
			})
		}

		resp := request.ResponseKind().(*kmsg.OffsetFetchResponse)
		resp.Default()
		resp.Groups = []kmsg.OffsetFetchResponseGroup{
			{
				Group: consumerGroup,
				Topics: []kmsg.OffsetFetchResponseGroupTopic{
					{
						Topic:      topicName,
						Partitions: partitionsResp,
					},
				},
			},
		}
		return resp, nil, true
	})
}
