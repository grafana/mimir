// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"
)

func TestPartitionReader(t *testing.T) {
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

	records, err := consumer.waitRecords(2, 5*time.Second, 0)
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{content, content}, records)
}

func TestReader_ConsumerError(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 1
	)

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, clusterAddr := createTestCluster(t, partitionID+1, topicName)

	invocations := atomic.NewInt64(0)
	returnErrors := atomic.NewBool(true)
	trackingConsumer := newTestConsumer(2)
	consumer := consumerFunc(func(ctx context.Context, records []record) error {
		invocations.Inc()
		if !returnErrors.Load() {
			return trackingConsumer.consume(ctx, records)
		}
		// There may be more records, but we only care that the one we failed to consume in the first place is still there.
		assert.Equal(t, "1", string(records[0].content))
		return errors.New("consumer error")
	})
	startReader(ctx, t, clusterAddr, topicName, partitionID, consumer)

	// Write to Kafka.
	writeClient := newKafkaProduceClient(t, clusterAddr)

	produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("1"))
	produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("2"))

	// There are more than one invocation because the reader will retry.
	assert.Eventually(t, func() bool { return invocations.Load() > 1 }, 5*time.Second, 100*time.Millisecond)

	returnErrors.Store(false)

	records, err := trackingConsumer.waitRecords(2, time.Second, 0)
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("1"), []byte("2")}, records)
}

func TestPartitionReader_WaitReadConsistency(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 0
	)

	var (
		ctx = context.Background()
	)

	setup := func(t *testing.T) (testConsumer, *PartitionReader, *kgo.Client, *prometheus.Registry) {
		reg := prometheus.NewPedanticRegistry()

		_, clusterAddr := createTestCluster(t, 1, topicName)

		// Create a consumer with no buffer capacity.
		consumer := newTestConsumer(0)

		// Configure the reader to poll the "last produced offset" frequently.
		reader := startReader(ctx, t, clusterAddr, topicName, partitionID, consumer,
			withLastProducedOffsetPollInterval(100*time.Millisecond),
			withRegistry(reg))

		writeClient := newKafkaProduceClient(t, clusterAddr)

		return consumer, reader, writeClient, reg
	}

	t.Run("should return after all produced records up have been consumed", func(t *testing.T) {
		t.Parallel()

		consumer, reader, writeClient, reg := setup(t)

		// Produce some records.
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-1"))
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-2"))
		t.Log("produced 2 records")

		// WaitReadConsistency() should return after all records produced up until this
		// point have been consumed.
		runAsyncAndAssertCompletionOrder(t,
			func() {
				records, err := consumer.waitRecords(1, time.Second, 0)
				assert.NoError(t, err)
				assert.Equal(t, [][]byte{[]byte("record-1")}, records)
				t.Logf("consumed records: %s", records)

				// Wait some time before consuming next record.
				time.Sleep(time.Second)

				records, err = consumer.waitRecords(1, time.Second, 0)
				assert.NoError(t, err)
				assert.Equal(t, [][]byte{[]byte("record-2")}, records)
				t.Logf("consumed records: %s", records)
			},
			func() {
				t.Log("started waiting for read consistency")

				err := reader.WaitReadConsistency(ctx)
				require.NoError(t, err)

				t.Log("finished waiting for read consistency")
			},
		)

		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_strong_consistency_requests_total Total number of requests for which strong consistency has been requested.
			# TYPE cortex_ingest_storage_strong_consistency_requests_total counter
			cortex_ingest_storage_strong_consistency_requests_total 1

			# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
			# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
			cortex_ingest_storage_strong_consistency_failures_total 0
		`), "cortex_ingest_storage_strong_consistency_requests_total", "cortex_ingest_storage_strong_consistency_failures_total"))
	})

	t.Run("should block until the context deadline exceed if produced records are not consumed", func(t *testing.T) {
		t.Parallel()

		consumer, reader, writeClient, reg := setup(t)

		// Produce some records.
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-1"))
		t.Log("produced 1 record")

		err := reader.WaitReadConsistency(createTestContextWithTimeout(t, time.Second))
		require.ErrorIs(t, err, context.DeadlineExceeded)

		// Consume the records.
		records, err := consumer.waitRecords(1, time.Second, 0)
		assert.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("record-1")}, records)

		// Now the WaitReadConsistency() should return soon.
		err = reader.WaitReadConsistency(createTestContextWithTimeout(t, time.Second))
		require.NoError(t, err)

		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_strong_consistency_requests_total Total number of requests for which strong consistency has been requested.
			# TYPE cortex_ingest_storage_strong_consistency_requests_total counter
			cortex_ingest_storage_strong_consistency_requests_total 2

			# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
			# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
			cortex_ingest_storage_strong_consistency_failures_total 1
		`), "cortex_ingest_storage_strong_consistency_requests_total", "cortex_ingest_storage_strong_consistency_failures_total"))
	})

	t.Run("should return if no records have been produced yet", func(t *testing.T) {
		t.Parallel()

		_, reader, _, reg := setup(t)

		err := reader.WaitReadConsistency(createTestContextWithTimeout(t, time.Second))
		require.NoError(t, err)

		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_strong_consistency_requests_total Total number of requests for which strong consistency has been requested.
			# TYPE cortex_ingest_storage_strong_consistency_requests_total counter
			cortex_ingest_storage_strong_consistency_requests_total 1

			# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
			# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
			cortex_ingest_storage_strong_consistency_failures_total 0
		`), "cortex_ingest_storage_strong_consistency_requests_total", "cortex_ingest_storage_strong_consistency_failures_total"))
	})

	t.Run("should return an error if the PartitionReader is not running", func(t *testing.T) {
		t.Parallel()

		_, reader, _, reg := setup(t)

		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

		err := reader.WaitReadConsistency(createTestContextWithTimeout(t, time.Second))
		require.ErrorContains(t, err, "partition reader service is not running")

		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_strong_consistency_requests_total Total number of requests for which strong consistency has been requested.
			# TYPE cortex_ingest_storage_strong_consistency_requests_total counter
			cortex_ingest_storage_strong_consistency_requests_total 1

			# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
			# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
			cortex_ingest_storage_strong_consistency_failures_total 1
		`), "cortex_ingest_storage_strong_consistency_requests_total", "cortex_ingest_storage_strong_consistency_failures_total"))
	})
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
	kafka          KafkaConfig
	partitionID    int32
	consumer       recordConsumer
	registry       prometheus.Registerer
	logger         log.Logger
	commitInterval time.Duration
}

type readerTestCfgOtp func(cfg *readerTestCfg)

func withCommitInterval(i time.Duration) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.commitInterval = i
	}
}

func withLastProducedOffsetPollInterval(i time.Duration) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.kafka.LastProducedOffsetPollInterval = i
	}
}

func withRegistry(reg prometheus.Registerer) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.registry = reg
	}
}

func defaultReaderTestConfig(addr string, topicName string, partitionID int32, consumer recordConsumer) *readerTestCfg {
	return &readerTestCfg{
		registry:       prometheus.NewPedanticRegistry(),
		logger:         log.NewNopLogger(),
		kafka:          createTestKafkaConfig(addr, topicName),
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
	reader, err := newPartitionReader(cfg.kafka, cfg.partitionID, cfg.consumer, cfg.logger, cfg.registry)
	require.NoError(t, err)
	reader.commitInterval = cfg.commitInterval

	require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
	})

	return reader
}

func TestPartitionReader_Commit(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 1
	)

	t.Run("resume at committed", func(t *testing.T) {
		t.Parallel()

		const commitInterval = 100 * time.Millisecond
		ctx, cancel := context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(errors.New("test done")) })

		_, clusterAddr := createTestCluster(t, partitionID+1, topicName)

		consumer := newTestConsumer(3)
		reader := startReader(ctx, t, clusterAddr, topicName, partitionID, consumer, withCommitInterval(commitInterval))

		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("1"))
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("2"))
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("3"))

		_, err := consumer.waitRecords(3, time.Second, commitInterval*2) // wait for a few commits to make sure empty commits don't cause issues
		require.NoError(t, err)

		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

		recordsSentAfterShutdown := []byte("4")
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, recordsSentAfterShutdown)

		startReader(ctx, t, clusterAddr, topicName, partitionID, consumer, withCommitInterval(commitInterval))

		records, err := consumer.waitRecords(1, time.Second, 0)
		assert.NoError(t, err)
		assert.Equal(t, [][]byte{recordsSentAfterShutdown}, records)
	})

	t.Run("commit at shutdown", func(t *testing.T) {
		t.Parallel()

		// A very long commit interval effectively means no regular commits.
		const commitInterval = time.Second * 15
		ctx, cancel := context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(errors.New("test done")) })

		_, clusterAddr := createTestCluster(t, partitionID+1, topicName)

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

		// There should be only one record - the one produced after the shutdown.
		// The offset of record "3" should have been committed at shutdown and the reader should have resumed from there.
		_, err = consumer.waitRecords(1, time.Second, time.Second)
		assert.NoError(t, err)
	})

	t.Run("commit at shutdown doesn't persist if we haven't consumed any records since startup", func(t *testing.T) {
		t.Parallel()
		// A very long commit interval effectively means no regular commits.
		const commitInterval = time.Second * 15
		ctx, cancel := context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(errors.New("test done")) })

		_, clusterAddr := createTestCluster(t, partitionID+1, topicName)

		consumer := newTestConsumer(4)
		reader := startReader(ctx, t, clusterAddr, topicName, partitionID, consumer, withCommitInterval(commitInterval))

		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("1"))
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("2"))
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("3"))

		_, err := consumer.waitRecords(3, time.Second, 0)
		require.NoError(t, err)

		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		reader = startReader(ctx, t, clusterAddr, topicName, partitionID, consumer, withCommitInterval(commitInterval))

		// No new records since the last commit.
		_, err = consumer.waitRecords(0, time.Second, 0)
		assert.NoError(t, err)

		// Shut down without having consumed any records.
		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		_ = startReader(ctx, t, clusterAddr, topicName, partitionID, consumer, withCommitInterval(commitInterval))

		// No new records since the last commit (2 shutdowns ago).
		_, err = consumer.waitRecords(0, time.Second, 0)
		assert.NoError(t, err)
	})
}

// addSupportForConsumerGroups adds very bare-bones support for one consumer group.
// It expects that only one partition is consumed at a time.
func addSupportForConsumerGroups(t *testing.T, cluster *kfake.Cluster, topicName string, numPartitions int32) {
	committedOffsets := make([]int64, numPartitions+1)

	cluster.ControlKey(kmsg.OffsetCommit.Int16(), func(request kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		commitR := request.(*kmsg.OffsetCommitRequest)
		assert.Equal(t, consumerGroup, commitR.Group)
		assert.Len(t, commitR.Topics, 1, "test only has support for one topic per request")
		topic := commitR.Topics[0]
		assert.Equal(t, topicName, topic.Topic)
		assert.Len(t, topic.Partitions, 1, "test only has support for one partition per request")

		partitionID := topic.Partitions[0].Partition
		committedOffsets[partitionID] = topic.Partitions[0].Offset

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
		assert.Equal(t, commitR.Groups[0].Group, consumerGroup)

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
					Offset:    committedOffsets[i],
				})
			}
		} else {
			partitionsResp = append(partitionsResp, kmsg.OffsetFetchResponseGroupTopicPartition{
				Partition: partitionID,
				Offset:    committedOffsets[partitionID],
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

type testConsumer struct {
	records chan []byte
}

func newTestConsumer(capacity int) testConsumer {
	return testConsumer{
		records: make(chan []byte, capacity),
	}
}

func (t testConsumer) consume(ctx context.Context, records []record) error {
	for _, r := range records {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case t.records <- r.content:
			// Nothing to do.
		}
	}
	return nil
}

// waitRecords expects to receive numRecords records within waitTimeout.
// waitRecords waits for an additional drainPeriod after receiving numRecords records to ensure that no more records are received.
// waitRecords returns an error if a different number of records is received.
func (t testConsumer) waitRecords(numRecords int, waitTimeout, drainPeriod time.Duration) ([][]byte, error) {
	var records [][]byte
	timeout := time.After(waitTimeout)
	for {
		select {
		case msg := <-t.records:
			records = append(records, msg)
			if len(records) != numRecords {
				continue
			}
			if drainPeriod == 0 {
				return records, nil
			}
			timeout = time.After(drainPeriod)
		case <-timeout:
			if len(records) != numRecords {
				return nil, fmt.Errorf("waiting for records: received %d, expected %d", len(records), numRecords)
			}
			return records, nil
		}
	}
}

type consumerFunc func(ctx context.Context, records []record) error

func (c consumerFunc) consume(ctx context.Context, records []record) error {
	return c(ctx, records)
}
