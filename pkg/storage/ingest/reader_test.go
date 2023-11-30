package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/atomic"
)

func TestReader(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 1
	)

	var (
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		reg         = prometheus.NewPedanticRegistry()
	)
	t.Cleanup(cancel)

	// Create fake Kafka cluster.
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(partitionID+1, topicName))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	addrs := cluster.ListenAddrs()
	require.Len(t, addrs, 1)

	const content = "special content"
	recordsProcessed := atomic.NewInt64(0)
	consumer := consumerFunc(func(record Record) error {
		assert.EqualValues(t, content, record.Content)
		recordsProcessed.Inc()
		return nil
	})

	startReader(t, ctx, addrs[0], topicName, partitionID, consumer, reg)

	writeClient := newKafkaProduceClient(t, addrs[0])

	produceRecord(t, ctx, writeClient, topicName, partitionID, []byte(content))
	produceRecord(t, ctx, writeClient, topicName, partitionID, []byte(content))

	assert.Eventually(t, func() bool {
		return recordsProcessed.Load() == 2
	}, 5*time.Second, 10*time.Millisecond, "couldn't receive message from kafka")
}

func TestReader_IgnoredConsumerErrors(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 1
	)

	var (
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		reg         = prometheus.NewPedanticRegistry()
	)
	t.Cleanup(cancel)

	// Create fake Kafka cluster.
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(partitionID+1, topicName))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	addrs := cluster.ListenAddrs()
	require.Len(t, addrs, 1)

	const content = "special content"
	recordsProcessed := atomic.NewInt64(0)
	consumer := consumerFunc(func(record Record) error {
		assert.EqualValues(t, content, record.Content)
		recordsProcessed.Inc()
		return errors.New("failed")
	})

	startReader(t, ctx, addrs[0], topicName, partitionID, consumer, reg)

	// Write to Kafka.
	writeClient := newKafkaProduceClient(t, addrs[0])

	produceRecord(t, ctx, writeClient, topicName, partitionID, []byte(content))

	assert.Eventually(t, func() bool {
		return recordsProcessed.Load() == 1
	}, time.Second, 10*time.Millisecond, "couldn't receive message from kafka")

	produceRecord(t, ctx, writeClient, topicName, partitionID, []byte(content))

	assert.Eventually(t, func() bool {
		return recordsProcessed.Load() == 2
	}, 500*time.Millisecond, 10*time.Millisecond, "couldn't receive message from kafka")
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

func produceRecord(t *testing.T, ctx context.Context, writeClient *kgo.Client, topicName string, partitionID int32, content []byte) {
	rec := &kgo.Record{
		Value:     content,
		Topic:     topicName,
		Partition: partitionID,
	}
	produceResult := writeClient.ProduceSync(ctx, rec)
	require.NoError(t, produceResult.FirstErr())
}

func startReader(t *testing.T, ctx context.Context, addr string, topicName string, partitionID int32, consumer consumerFunc, reg *prometheus.Registry) {
	reader, err := NewReader(addr, topicName, "", partitionID, consumer, log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(ctx, reader) })
}

type consumerFunc func(Record) error

func (f consumerFunc) Consume(ctx context.Context, record Record) error { return f(record) }
