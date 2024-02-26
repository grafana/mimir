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
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestPartitionReader(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 1
	)

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)

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

	_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)

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

	setup := func(t *testing.T, consumer recordConsumer) (*PartitionReader, *kgo.Client, *prometheus.Registry) {
		reg := prometheus.NewPedanticRegistry()

		_, clusterAddr := testkafka.CreateCluster(t, 1, topicName)

		// Configure the reader to poll the "last produced offset" frequently.
		reader := startReader(ctx, t, clusterAddr, topicName, partitionID, consumer,
			withLastProducedOffsetPollInterval(100*time.Millisecond),
			withRegistry(reg))

		writeClient := newKafkaProduceClient(t, clusterAddr)

		return reader, writeClient, reg
	}

	t.Run("should return after all produced records have been consumed", func(t *testing.T) {
		t.Parallel()

		consumedRecords := atomic.NewInt64(0)

		// We define a custom consume function which introduces a delay once the 2nd record
		// has been consumed but before the function returns. From the PartitionReader perspective,
		// the 2nd record consumption will be delayed.
		consumer := consumerFunc(func(ctx context.Context, records []record) error {
			for _, record := range records {
				// Introduce a delay before returning from the consume function once
				// the 2nd record has been consumed.
				if consumedRecords.Load()+1 == 2 {
					time.Sleep(time.Second)
				}

				consumedRecords.Inc()
				assert.Equal(t, fmt.Sprintf("record-%d", consumedRecords.Load()), string(record.content))
				t.Logf("consumed record: %s", string(record.content))
			}

			return nil
		})

		reader, writeClient, reg := setup(t, consumer)

		// Produce some records.
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-1"))
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-2"))
		t.Log("produced 2 records")

		// WaitReadConsistency() should return after all records produced up until this
		// point have been consumed.
		t.Log("started waiting for read consistency")

		err := reader.WaitReadConsistency(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(2), consumedRecords.Load())
		t.Log("finished waiting for read consistency")

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

		// Create a consumer with no buffer capacity.
		consumer := newTestConsumer(0)

		reader, writeClient, reg := setup(t, consumer)

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

		reader, _, reg := setup(t, newTestConsumer(0))

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

		reader, _, reg := setup(t, newTestConsumer(0))

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
	reader, err := newPartitionReader(cfg.kafka, cfg.partitionID, "test-group", cfg.consumer, cfg.logger, cfg.registry)
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

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)

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

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)

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

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)

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
