// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestReader(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 1
	)

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, clusterAddr := createTestCluster(t, partitionID+1, topicName)

	content := []byte("special content")
	consumer := newTestConsumer(2)

	startReader(ctx, t, clusterAddr, topicName, partitionID, consumer)

	writeClient := newKafkaProduceClient(t, clusterAddr)

	produceRecord(ctx, t, writeClient, topicName, partitionID, content)
	produceRecord(ctx, t, writeClient, topicName, partitionID, content)

	messages, err := consumer.waitRecords(2, 5*time.Second, 0)
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{content, content}, messages)
}

func TestReader_IgnoredConsumerErrors(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 1
	)

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, clusterAddr := createTestCluster(t, partitionID+1, topicName)

	content := []byte("special content")
	consumer := newTestConsumer(1)
	startReader(ctx, t, clusterAddr, topicName, partitionID, consumer)

	// Write to Kafka.
	writeClient := newKafkaProduceClient(t, clusterAddr)

	produceRecord(ctx, t, writeClient, topicName, partitionID, content)

	messages, err := consumer.waitRecords(1, time.Second, 0)
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{content}, messages)

	produceRecord(ctx, t, writeClient, topicName, partitionID, content)

	messages, err = consumer.waitRecords(1, time.Second, 0)
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{content}, messages)
}

func newKafkaProduceClient(t *testing.T, addrs string) *kgo.Client {
	writeClient, err := kgo.NewClient(
		kgo.SeedBrokers(addrs),
		// We will choose the partition of each record.
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	t.Cleanup(writeClient.Close)
	return writeClient
}

func produceRecord(ctx context.Context, t *testing.T, writeClient *kgo.Client, topicName string, partitionID int32, content []byte) {
	rec := &kgo.Record{
		Value:     content,
		Topic:     topicName,
		Partition: partitionID,
	}
	produceResult := writeClient.ProduceSync(ctx, rec)
	require.NoError(t, produceResult.FirstErr())
}

type readerTestCfg struct {
	addr           string
	topicName      string
	partitionID    int32
	consumer       recordConsumer
	registry       *prometheus.Registry
	logger         log.Logger
	commitInterval time.Duration
}

type readerTestCfgOtp func(cfg *readerTestCfg)

func withCommitInterval(i time.Duration) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.commitInterval = i
	}
}

func defaultReaderTestConfig(addr string, topicName string, partitionID int32, consumer recordConsumer) *readerTestCfg {
	return &readerTestCfg{
		registry:       prometheus.NewPedanticRegistry(),
		logger:         log.NewNopLogger(),
		addr:           addr,
		topicName:      topicName,
		partitionID:    partitionID,
		consumer:       consumer,
		commitInterval: 10 * time.Second,
	}
}

func startReader(ctx context.Context, t *testing.T, addr string, topicName string, partitionID int32, consumer recordConsumer, opts ...readerTestCfgOtp) *PartitionReader {
	cfg := defaultReaderTestConfig(addr, topicName, partitionID, consumer)
	for _, o := range opts {
		o(cfg)
	}
	reader, err := newReader(cfg.addr, cfg.topicName, "", cfg.partitionID, cfg.consumer, cfg.logger, newReaderMetrics(partitionID, cfg.registry))
	require.NoError(t, err)
	reader.commitInterval = cfg.commitInterval

	require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(ctx, reader) })

	return reader
}

func TestReader_Commit(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 1
	)

	t.Run("resume at committed", func(t *testing.T) {
		const commitInterval = 100 * time.Millisecond
		ctx, cancel := context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(errors.New("test done")) })

		cluster, clusterAddr := createTestCluster(t, partitionID+1, topicName)
		addSupportForConsumerGroups(t, cluster, topicName, partitionID)

		consumer := newTestConsumer(3)
		reader := startReader(ctx, t, clusterAddr, topicName, partitionID, consumer, withCommitInterval(commitInterval))

		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("1"))
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("2"))
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("3"))

		_, err := consumer.waitRecords(3, time.Second, commitInterval*2) // wait for a few commits to make sure empty commits don't cause issues
		require.NoError(t, err)

		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

		messageSentAfterShutdown := []byte("4")
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, messageSentAfterShutdown)

		startReader(ctx, t, clusterAddr, topicName, partitionID, consumer, withCommitInterval(commitInterval))

		messages, err := consumer.waitRecords(1, time.Second, 0)
		assert.NoError(t, err)
		assert.Equal(t, [][]byte{messageSentAfterShutdown}, messages)
	})

	t.Run("respect commit interval", func(t *testing.T) {
		// a very long commit interval effectively means no commits
		const commitInterval = time.Second * 15
		ctx, cancel := context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(errors.New("test done")) })

		cluster, clusterAddr := createTestCluster(t, partitionID+1, topicName)
		addSupportForConsumerGroups(t, cluster, topicName, partitionID)

		consumer := newTestConsumer(4)
		reader := startReader(ctx, t, clusterAddr, topicName, partitionID, consumer, withCommitInterval(commitInterval))

		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("1"))
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("2"))
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("3"))

		_, err := consumer.waitRecords(3, time.Second, 0)
		require.NoError(t, err)

		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("4"))
		startReader(ctx, t, clusterAddr, topicName, partitionID, consumer, withCommitInterval(commitInterval))

		_, err = consumer.waitRecords(4, time.Second, 0)
		assert.NoError(t, err)
	})
}

// addSupportForConsumerGroups adds very bare-bones support for one consumer group.
func addSupportForConsumerGroups(t *testing.T, cluster *kfake.Cluster, topicName string, partitionID int32) {
	var committedOffset int64

	cluster.ControlKey(kmsg.OffsetCommit.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		commitR := request.(*kmsg.OffsetCommitRequest)
		assert.Equal(t, consumerGroup, commitR.Group)
		assert.Len(t, commitR.Topics, 1)
		topic := commitR.Topics[0]
		assert.Equal(t, topicName, topic.Topic)
		assert.Len(t, topic.Partitions, 1)
		assert.EqualValues(t, partitionID, topic.Partitions[0].Partition)

		committedOffset = topic.Partitions[0].Offset

		resp := request.ResponseKind().(*kmsg.OffsetCommitResponse)
		resp.Topics = []kmsg.OffsetCommitResponseTopic{{}}
		resp.Topics[0].Topic = topicName
		resp.Topics[0].Partitions = []kmsg.OffsetCommitResponseTopicPartition{{}}
		resp.Topics[0].Partitions[0].Partition = partitionID
		t.Log("recorded committed offset", committedOffset)
		return resp, nil, true
	})

	cluster.ControlKey(kmsg.OffsetFetch.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		commitR := request.(*kmsg.OffsetFetchRequest)
		assert.Len(t, commitR.Groups, 1)
		assert.Equal(t, commitR.Groups[0].Group, consumerGroup)

		resp := request.ResponseKind().(*kmsg.OffsetFetchResponse)
		resp.Groups = []kmsg.OffsetFetchResponseGroup{{}}
		resp.Groups[0].Group = consumerGroup
		resp.Groups[0].Topics = []kmsg.OffsetFetchResponseGroupTopic{{}}
		resp.Groups[0].Topics[0].Topic = topicName
		resp.Groups[0].Topics[0].Partitions = []kmsg.OffsetFetchResponseGroupTopicPartition{{}}
		resp.Groups[0].Topics[0].Partitions[0].Partition = partitionID
		resp.Groups[0].Topics[0].Partitions[0].Offset = committedOffset
		t.Log("responding with committed offset", committedOffset)
		return resp, nil, true
	})
}

type testConsumer struct {
	messages chan []byte
}

func newTestConsumer(capacity int) testConsumer {
	return testConsumer{
		messages: make(chan []byte, capacity),
	}
}

func (t testConsumer) consume(_ context.Context, records []record) error {
	for _, r := range records {
		t.messages <- r.content
	}
	return nil
}

// waitRecords expects to receive numRecords records within waitTimeout.
// waitRecords waits for drainPeriod after receiving numRecords records to ensure that no more records are received.
// waitRecords returns an error if a different number of records is received.
func (t testConsumer) waitRecords(numRecords int, waitTimeout, drainPeriod time.Duration) ([][]byte, error) {
	var messages [][]byte
	timeout := time.After(waitTimeout)
	for {
		select {
		case msg := <-t.messages:
			messages = append(messages, msg)
			if len(messages) != numRecords {
				continue
			}
			if drainPeriod == 0 {
				return messages, nil
			}
			timeout = time.After(drainPeriod)
		case <-timeout:
			if len(messages) != numRecords {
				return nil, fmt.Errorf("waiting for records: received %d, expected %d", len(messages), numRecords)
			}
			return messages, nil
		}
	}
}
