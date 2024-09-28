// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"

	mimirtest "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestKafkaStartOffset(t *testing.T) {
	t.Run("should match Kafka client start offset", func(t *testing.T) {
		expected := kgo.NewOffset().AtStart().EpochOffset().Offset
		assert.Equal(t, expected, kafkaOffsetStart)
	})
}

func TestKafkaEndOffset(t *testing.T) {
	t.Run("should match Kafka client end offset", func(t *testing.T) {
		expected := kgo.NewOffset().AtEnd().EpochOffset().Offset
		assert.Equal(t, expected, kafkaOffsetEnd)
	})
}

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

	createAndStartReader(ctx, t, clusterAddr, topicName, partitionID, consumer)

	writeClient := newKafkaProduceClient(t, clusterAddr)

	produceRecord(ctx, t, writeClient, topicName, partitionID, content)
	produceRecord(ctx, t, writeClient, topicName, partitionID, content)

	records, err := consumer.waitRecords(2, 5*time.Second, 0)
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{content, content}, records)
}

func TestPartitionReader_logFetchErrors(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 1
	)

	cfg := defaultReaderTestConfig(t, "", topicName, partitionID, nil)
	reader, err := newPartitionReader(cfg.kafka, cfg.partitionID, "test-group", cfg.consumer, cfg.logger, cfg.registry)
	require.NoError(t, err)

	reader.logFetchErrors(kgo.Fetches{
		kgo.Fetch{Topics: []kgo.FetchTopic{
			{
				Topic: topicName,
				Partitions: []kgo.FetchPartition{
					{Partition: partitionID, Err: nil},
					{Partition: partitionID, Err: context.Canceled},                            // not counted in metrics
					{Partition: partitionID, Err: fmt.Errorf("wrapped: %w", context.Canceled)}, // not counted in metrics
					{Partition: partitionID, Err: fmt.Errorf("real error")},                    // counted
				},
			},
		}},
	})

	assert.NoError(t, promtest.GatherAndCompare(cfg.registry, strings.NewReader(`
			# HELP cortex_ingest_storage_reader_fetch_errors_total The number of fetch errors encountered by the consumer.
        	# TYPE cortex_ingest_storage_reader_fetch_errors_total counter
        	cortex_ingest_storage_reader_fetch_errors_total 1
	`), "cortex_ingest_storage_reader_fetch_errors_total"))
}

func TestPartitionReader_ConsumerError(t *testing.T) {
	t.Parallel()

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
	createAndStartReader(ctx, t, clusterAddr, topicName, partitionID, consumer)

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

func TestPartitionReader_ConsumerStopping(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 1
	)

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)

	// consumerErrs will store the last error returned by the consumer; its initial value doesn't matter, but it must be non-nil.
	consumerErrs := atomic.NewError(errors.New("dummy error"))
	type consumerCall struct {
		f    func() []record
		resp chan error
	}
	consumeCalls := make(chan consumerCall)
	consumer := consumerFunc(func(ctx context.Context, records []record) (err error) {
		defer consumerErrs.Store(err)

		call := consumerCall{
			f:    func() []record { return records },
			resp: make(chan error),
		}
		consumeCalls <- call
		err = <-call.resp
		// The service is about to transition into its stopping phase. But the consumer must not observe it via the parent context.
		assert.NoError(t, ctx.Err())

		return err
	})
	reader := createReader(t, clusterAddr, topicName, partitionID, consumer)
	require.NoError(t, services.StartAndAwaitRunning(ctx, reader))

	// Write to Kafka.
	writeClient := newKafkaProduceClient(t, clusterAddr)
	produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("1"))

	// After this point, we know that the consumer is in the in-flight.
	call := <-consumeCalls
	// Explicitly begin to stop the service while it's still consuming the records. This shouldn't cancel the in-flight consumption.
	reader.StopAsync()

	go func() {
		// Simulate a slow consumer, that blocks reader from stopping.
		time.Sleep(time.Second)

		defer close(call.resp)

		records := call.f()
		require.Len(t, records, 1)
		require.Equal(t, []byte("1"), records[0].content)
	}()

	// Wait for the reader to stop completely.
	require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
	// Checks the consumer returned a non-errored result.
	require.NoError(t, consumerErrs.Load())
}

func TestPartitionReader_WaitReadConsistencyUntilLastProducedOffset_And_WaitReadConsistencyUntilOffset(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 0
	)

	var (
		ctx = context.Background()
	)

	setup := func(t *testing.T, consumer recordConsumer, opts ...readerTestCfgOtp) (*PartitionReader, *kgo.Client, *prometheus.Registry) {
		reg := prometheus.NewPedanticRegistry()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)

		// Configure the reader to poll the "last produced offset" frequently.
		reader := createAndStartReader(ctx, t, clusterAddr, topicName, partitionID, consumer,
			append([]readerTestCfgOtp{
				withLastProducedOffsetPollInterval(100 * time.Millisecond),
				withRegistry(reg),
			}, opts...)...)

		writeClient := newKafkaProduceClient(t, clusterAddr)

		return reader, writeClient, reg
	}

	t.Run("should return after all produced records have been consumed", func(t *testing.T) {
		t.Parallel()

		for _, withOffset := range []bool{false, true} {
			t.Run(fmt.Sprintf("with offset %v", withOffset), func(t *testing.T) {
				t.Parallel()

				consumedRecords := atomic.NewInt64(0)

				// We define a custom consume function which introduces a delay once the 2nd record
				// has been consumed but before the function returns. From the PartitionReader perspective,
				// the 2nd record consumption will be delayed.
				consumer := consumerFunc(func(_ context.Context, records []record) error {
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
				lastRecordOffset := produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-2"))
				t.Logf("produced 2 records (last record offset: %d)", lastRecordOffset)

				// WaitReadConsistencyUntilLastProducedOffset() should return after all records produced up until this
				// point have been consumed.
				t.Log("started waiting for read consistency")

				if withOffset {
					require.NoError(t, reader.WaitReadConsistencyUntilOffset(ctx, lastRecordOffset))
				} else {
					require.NoError(t, reader.WaitReadConsistencyUntilLastProducedOffset(ctx))
				}

				assert.Equal(t, int64(2), consumedRecords.Load())
				t.Log("finished waiting for read consistency")

				assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_ingest_storage_strong_consistency_requests_total Total number of requests for which strong consistency has been requested. The metric distinguishes between requests with an offset specified and requests requesting to enforce strong consistency up until the last produced offset.
					# TYPE cortex_ingest_storage_strong_consistency_requests_total counter
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", with_offset="%t"} 1
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", with_offset="%t"} 0
		
					# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
					# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
					cortex_ingest_storage_strong_consistency_failures_total{component="partition-reader"} 0
				`, withOffset, !withOffset)),
					"cortex_ingest_storage_strong_consistency_requests_total",
					"cortex_ingest_storage_strong_consistency_failures_total"))
			})
		}

	})

	t.Run("should block until the request context deadline is exceeded if produced records are not consumed", func(t *testing.T) {
		t.Parallel()

		for _, withOffset := range []bool{false, true} {
			t.Run(fmt.Sprintf("with offset %v", withOffset), func(t *testing.T) {
				t.Parallel()

				// Create a consumer with no buffer capacity.
				consumer := newTestConsumer(0)

				reader, writeClient, reg := setup(t, consumer, withWaitStrongReadConsistencyTimeout(0))

				// Produce some records.
				lastRecordOffset := produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-1"))
				t.Logf("produced 1 record (last record offset: %d)", lastRecordOffset)

				waitCtx := createTestContextWithTimeout(t, time.Second)
				var err error

				if withOffset {
					err = reader.WaitReadConsistencyUntilOffset(waitCtx, lastRecordOffset)
				} else {
					err = reader.WaitReadConsistencyUntilLastProducedOffset(waitCtx)
				}

				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.NotErrorIs(t, err, errWaitStrongReadConsistencyTimeoutExceeded)

				// Consume the records.
				records, err := consumer.waitRecords(1, 2*time.Second, 0)
				assert.NoError(t, err)
				assert.Equal(t, [][]byte{[]byte("record-1")}, records)

				assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_ingest_storage_strong_consistency_requests_total Total number of requests for which strong consistency has been requested. The metric distinguishes between requests with an offset specified and requests requesting to enforce strong consistency up until the last produced offset.
					# TYPE cortex_ingest_storage_strong_consistency_requests_total counter
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", with_offset="%t"} 1
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", with_offset="%t"} 0
		
					# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
					# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
					cortex_ingest_storage_strong_consistency_failures_total{component="partition-reader"} 1
				`, withOffset, !withOffset)),
					"cortex_ingest_storage_strong_consistency_requests_total",
					"cortex_ingest_storage_strong_consistency_failures_total"))

				// Now the WaitReadConsistencyUntilLastProducedOffset() should return soon.
				err = reader.WaitReadConsistencyUntilLastProducedOffset(createTestContextWithTimeout(t, time.Second))
				require.NoError(t, err)
			})
		}
	})

	t.Run("should block until the configured wait timeout is exceeded if produced records are not consumed", func(t *testing.T) {
		t.Parallel()

		for _, withOffset := range []bool{false, true} {
			t.Run(fmt.Sprintf("with offset %v", withOffset), func(t *testing.T) {
				t.Parallel()

				// Create a consumer with no buffer capacity.
				consumer := newTestConsumer(0)

				reader, writeClient, reg := setup(t, consumer, withWaitStrongReadConsistencyTimeout(time.Second))

				// Produce some records.
				lastRecordOffset := produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-1"))
				t.Logf("produced 1 record (last record offset: %d)", lastRecordOffset)

				ctx := context.Background()
				var err error

				if withOffset {
					err = reader.WaitReadConsistencyUntilOffset(ctx, lastRecordOffset)
				} else {
					err = reader.WaitReadConsistencyUntilLastProducedOffset(ctx)
				}

				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.ErrorIs(t, err, errWaitStrongReadConsistencyTimeoutExceeded)

				// Consume the records.
				records, err := consumer.waitRecords(1, 2*time.Second, 0)
				assert.NoError(t, err)
				assert.Equal(t, [][]byte{[]byte("record-1")}, records)

				assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_ingest_storage_strong_consistency_requests_total Total number of requests for which strong consistency has been requested. The metric distinguishes between requests with an offset specified and requests requesting to enforce strong consistency up until the last produced offset.
					# TYPE cortex_ingest_storage_strong_consistency_requests_total counter
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", with_offset="%t"} 1
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", with_offset="%t"} 0
		
					# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
					# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
					cortex_ingest_storage_strong_consistency_failures_total{component="partition-reader"} 1
				`, withOffset, !withOffset)),
					"cortex_ingest_storage_strong_consistency_requests_total",
					"cortex_ingest_storage_strong_consistency_failures_total"))

				// Now the WaitReadConsistencyUntilLastProducedOffset() should return soon.
				err = reader.WaitReadConsistencyUntilLastProducedOffset(ctx)
				require.NoError(t, err)
			})
		}
	})

	t.Run("should return if no records have been produced yet", func(t *testing.T) {
		t.Parallel()

		for _, withOffset := range []bool{false, true} {
			t.Run(fmt.Sprintf("with offset %v", withOffset), func(t *testing.T) {
				t.Parallel()

				reader, _, reg := setup(t, newTestConsumer(0))
				waitCtx := createTestContextWithTimeout(t, time.Second)
				createTestContextWithTimeout(t, time.Second)

				if withOffset {
					require.NoError(t, reader.WaitReadConsistencyUntilOffset(waitCtx, -1))
				} else {
					require.NoError(t, reader.WaitReadConsistencyUntilLastProducedOffset(waitCtx))
				}

				assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_ingest_storage_strong_consistency_requests_total Total number of requests for which strong consistency has been requested. The metric distinguishes between requests with an offset specified and requests requesting to enforce strong consistency up until the last produced offset.
					# TYPE cortex_ingest_storage_strong_consistency_requests_total counter
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", with_offset="%t"} 1
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", with_offset="%t"} 0
		
					# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
					# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
					cortex_ingest_storage_strong_consistency_failures_total{component="partition-reader"} 0
				`, withOffset, !withOffset)),
					"cortex_ingest_storage_strong_consistency_requests_total",
					"cortex_ingest_storage_strong_consistency_failures_total"))
			})
		}
	})

	t.Run("should return an error if the PartitionReader is not running", func(t *testing.T) {
		t.Parallel()

		for _, withOffset := range []bool{false, true} {
			t.Run(fmt.Sprintf("with offset %v", withOffset), func(t *testing.T) {
				t.Parallel()

				reader, _, reg := setup(t, newTestConsumer(0))
				require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

				waitCtx := createTestContextWithTimeout(t, time.Second)
				var err error

				if withOffset {
					err = reader.WaitReadConsistencyUntilOffset(waitCtx, -1)
				} else {
					err = reader.WaitReadConsistencyUntilLastProducedOffset(waitCtx)
				}

				require.ErrorContains(t, err, "partition reader service is not running")

				assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_ingest_storage_strong_consistency_requests_total Total number of requests for which strong consistency has been requested. The metric distinguishes between requests with an offset specified and requests requesting to enforce strong consistency up until the last produced offset.
					# TYPE cortex_ingest_storage_strong_consistency_requests_total counter
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", with_offset="%t"} 1
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", with_offset="%t"} 0

					# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
					# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
					cortex_ingest_storage_strong_consistency_failures_total{component="partition-reader"} 1
				`, withOffset, !withOffset)),
					"cortex_ingest_storage_strong_consistency_requests_total",
					"cortex_ingest_storage_strong_consistency_failures_total"))
			})
		}
	})
}

func TestPartitionReader_ConsumeAtStartup(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 1
	)

	ctx := context.Background()

	t.Run("should immediately switch to Running state if partition is empty", func(t *testing.T) {
		t.Parallel()

		var (
			_, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
			consumer       = consumerFunc(func(context.Context, []record) error { return nil })
			reg            = prometheus.NewPedanticRegistry()
		)

		// Create and start the reader. We expect the reader to start even if partition is empty.
		reader := createReader(t, clusterAddr, topicName, partitionID, consumer, withTargetAndMaxConsumerLagAtStartup(time.Second, time.Second), withRegistry(reg))
		require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

		// The last consumed offset should be -1, since nothing has been consumed yet.
		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
			# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
			cortex_ingest_storage_reader_last_consumed_offset{partition="1"} -1
		`), "cortex_ingest_storage_reader_last_consumed_offset"))
	})

	t.Run("should immediately switch to Running state if configured target / max lag is 0", func(t *testing.T) {
		t.Parallel()

		var (
			cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
			consumer             = consumerFunc(func(context.Context, []record) error { return nil })
			reg                  = prometheus.NewPedanticRegistry()
		)

		// Mock Kafka to fail the Fetch request.
		cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()

			return nil, errors.New("mocked error"), true
		})

		// Produce some records.
		writeClient := newKafkaProduceClient(t, clusterAddr)
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-1"))
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-2"))
		t.Log("produced 2 records")

		// Create and start the reader. We expect the reader to start even if Fetch is failing.
		reader := createReader(t, clusterAddr, topicName, partitionID, consumer, withTargetAndMaxConsumerLagAtStartup(0, 0), withRegistry(reg))
		require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

		// The last consumed offset should be -1, since nothing has been consumed yet (Fetch requests are failing).
		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
			# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
			cortex_ingest_storage_reader_last_consumed_offset{partition="1"} -1
		`), "cortex_ingest_storage_reader_last_consumed_offset"))
	})

	t.Run("should consume partition from start if last committed offset is missing and wait until target lag is honored", func(t *testing.T) {
		t.Parallel()

		var (
			cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
			fetchRequestsCount   = atomic.NewInt64(0)
			fetchShouldFail      = atomic.NewBool(true)
			consumedRecordsCount = atomic.NewInt64(0)
		)

		consumer := consumerFunc(func(_ context.Context, records []record) error {
			consumedRecordsCount.Add(int64(len(records)))
			return nil
		})

		cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			fetchRequestsCount.Inc()

			if fetchShouldFail.Load() {
				return nil, errors.New("mocked error"), true
			}

			return nil, nil, false
		})

		// Produce some records.
		writeClient := newKafkaProduceClient(t, clusterAddr)
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-1"))
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-2"))
		t.Log("produced 2 records")

		// Create and start the reader.
		reg := prometheus.NewPedanticRegistry()
		logs := &concurrency.SyncBuffer{}
		reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
			withTargetAndMaxConsumerLagAtStartup(time.Second, 2*time.Second),
			withRegistry(reg),
			withLogger(log.NewLogfmtLogger(logs)))
		require.NoError(t, reader.StartAsync(ctx))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		})

		// Wait until the Kafka cluster received few Fetch requests.
		test.Poll(t, 5*time.Second, true, func() interface{} {
			return fetchRequestsCount.Load() > 2
		})

		// Since the mocked Kafka cluster is configured to fail any Fetch we expect the reader hasn't
		// catched up yet, and it's still in Starting state.
		assert.Equal(t, services.Starting, reader.State())
		assert.Equal(t, int64(0), consumedRecordsCount.Load())

		// Unblock the Fetch requests. Now they will succeed.
		fetchShouldFail.Store(false)

		// We expect the reader to catch up, and then switch to Running state.
		test.Poll(t, 5*time.Second, services.Running, func() interface{} {
			return reader.State()
		})

		// We expect the reader to have switched to running because target consumer lag has been honored.
		assert.Contains(t, logs.String(), "partition reader consumed partition and current lag is lower than configured target consumer lag")

		assert.Equal(t, int64(2), consumedRecordsCount.Load())

		// We expect the last consumed offset to be tracked in a metric.
		test.Poll(t, time.Second, nil, func() interface{} {
			return promtest.GatherAndCompare(reg, strings.NewReader(`
				# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
				# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
				cortex_ingest_storage_reader_last_consumed_offset{partition="1"} 1
			`), "cortex_ingest_storage_reader_last_consumed_offset")
		})
	})

	t.Run("should consume partition from start if last committed offset is missing and wait until target lag is honored and retry if a failure occurs when fetching last produced offset", func(t *testing.T) {
		t.Parallel()

		var (
			cluster, clusterAddr     = testkafka.CreateCluster(t, partitionID+1, topicName)
			listOffsetsRequestsCount = atomic.NewInt64(0)
			listOffsetsShouldFail    = atomic.NewBool(true)
			consumedRecordsCount     = atomic.NewInt64(0)
		)

		consumer := consumerFunc(func(_ context.Context, records []record) error {
			consumedRecordsCount.Add(int64(len(records)))
			return nil
		})

		cluster.ControlKey(int16(kmsg.ListOffsets), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			listOffsetsRequestsCount.Inc()

			if listOffsetsShouldFail.Load() {
				return nil, errors.New("mocked error"), true
			}

			return nil, nil, false
		})

		// Produce some records.
		writeClient := newKafkaProduceClient(t, clusterAddr)
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-1"))
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-2"))
		t.Log("produced 2 records")

		// Create and start the reader.
		reg := prometheus.NewPedanticRegistry()
		logs := &concurrency.SyncBuffer{}
		reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
			withTargetAndMaxConsumerLagAtStartup(time.Second, 2*time.Second),
			withRegistry(reg),
			withLogger(log.NewLogfmtLogger(logs)))
		require.NoError(t, reader.StartAsync(ctx))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		})

		// Wait until the Kafka cluster received few ListOffsets requests.
		test.Poll(t, 5*time.Second, true, func() interface{} {
			return listOffsetsRequestsCount.Load() > 2
		})

		// Since the mocked Kafka cluster is configured to fail any ListOffsets request we expect the reader hasn't
		// catched up yet, and it's still in Starting state.
		assert.Equal(t, services.Starting, reader.State())
		assert.Equal(t, int64(0), consumedRecordsCount.Load())

		// Unblock the ListOffsets requests. Now they will succeed.
		listOffsetsShouldFail.Store(false)

		// We expect the reader to catch up, and then switch to Running state.
		test.Poll(t, 5*time.Second, services.Running, func() interface{} {
			return reader.State()
		})

		// We expect the reader to have switched to running because target consumer lag has been honored.
		assert.Contains(t, logs.String(), "partition reader consumed partition and current lag is lower than configured target consumer lag")

		assert.Equal(t, int64(2), consumedRecordsCount.Load())

		// We expect the last consumed offset to be tracked in a metric.
		test.Poll(t, time.Second, nil, func() interface{} {
			return promtest.GatherAndCompare(reg, strings.NewReader(`
				# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
				# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
				cortex_ingest_storage_reader_last_consumed_offset{partition="1"} 1
			`), "cortex_ingest_storage_reader_last_consumed_offset")
		})
	})

	t.Run("should consume partition from end if position=end, and skip honoring target / max lag", func(t *testing.T) {
		t.Parallel()

		var (
			cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
			reg                  = prometheus.NewPedanticRegistry()
			fetchRequestsCount   = atomic.NewInt64(0)
			fetchShouldFail      = atomic.NewBool(true)
			consumedRecordsMx    sync.Mutex
			consumedRecords      []string
		)

		consumer := consumerFunc(func(_ context.Context, records []record) error {
			consumedRecordsMx.Lock()
			defer consumedRecordsMx.Unlock()

			for _, r := range records {
				consumedRecords = append(consumedRecords, string(r.content))
			}
			return nil
		})

		cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()

			fetchRequestsCount.Inc()
			if fetchShouldFail.Load() {
				return nil, errors.New("mocked error"), true
			}

			return nil, nil, false
		})

		// Produce some records.
		writeClient := newKafkaProduceClient(t, clusterAddr)
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-1"))
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-2"))
		t.Log("produced 2 records before starting the reader")

		// Create and start the reader.
		reader := createReader(t, clusterAddr, topicName, partitionID, consumer, withConsumeFromPositionAtStartup(consumeFromEnd), withTargetAndMaxConsumerLagAtStartup(time.Second, time.Second), withRegistry(reg))
		require.NoError(t, reader.StartAsync(ctx))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		})

		// The reader service should start even if Fetch is failing because max log is skipped.
		test.Poll(t, time.Second, services.Running, func() interface{} {
			return reader.State()
		})

		// Make Fetch working.
		fetchShouldFail.Store(false)

		// Wait until Fetch request has been issued at least once, in order to avoid any race condition
		// (the problem is that we may produce the next record before the client fetched the partition end position).
		require.Eventually(t, func() bool {
			return fetchRequestsCount.Load() > 0
		}, 5*time.Second, 10*time.Millisecond)

		// Produce one more record.
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-3"))
		t.Log("produced 1 record after starting the reader")

		// Since the reader has been configured with position=end we expect to consume only
		// the record produced after reader has been started.
		test.Poll(t, 5*time.Second, []string{"record-3"}, func() interface{} {
			consumedRecordsMx.Lock()
			defer consumedRecordsMx.Unlock()
			return slices.Clone(consumedRecords)
		})

		// We expect the last consumed offset to be tracked in a metric.
		test.Poll(t, time.Second, nil, func() interface{} {
			return promtest.GatherAndCompare(reg, strings.NewReader(`
				# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
				# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
				cortex_ingest_storage_reader_last_consumed_offset{partition="1"} 2
			`), "cortex_ingest_storage_reader_last_consumed_offset")
		})
	})

	t.Run("should consume partition from start if position=start, and wait until target lag is honored", func(t *testing.T) {
		t.Parallel()

		var (
			cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
			fetchRequestsCount   = atomic.NewInt64(0)
			fetchShouldFail      = atomic.NewBool(false)
			consumedRecordsMx    sync.Mutex
			consumedRecords      []string
		)

		consumer := consumerFunc(func(_ context.Context, records []record) error {
			consumedRecordsMx.Lock()
			defer consumedRecordsMx.Unlock()

			for _, r := range records {
				consumedRecords = append(consumedRecords, string(r.content))
			}
			return nil
		})

		cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			fetchRequestsCount.Inc()

			if fetchShouldFail.Load() {
				return nil, errors.New("mocked error"), true
			}

			return nil, nil, false
		})

		// Produce some records.
		writeClient := newKafkaProduceClient(t, clusterAddr)
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-1"))
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-2"))
		t.Log("produced 2 records")

		// Run the test twice with the same Kafka cluster to show that second time it consumes all records again.
		for run := 1; run <= 2; run++ {
			t.Run(fmt.Sprintf("Run %d", run), func(t *testing.T) {
				// Reset the test.
				fetchShouldFail.Store(true)
				fetchRequestsCount.Store(0)
				consumedRecordsMx.Lock()
				consumedRecords = nil
				consumedRecordsMx.Unlock()

				// Create and start the reader.
				reg := prometheus.NewPedanticRegistry()
				logs := &concurrency.SyncBuffer{}
				reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
					withConsumeFromPositionAtStartup(consumeFromStart),
					withTargetAndMaxConsumerLagAtStartup(time.Second, 2*time.Second),
					withRegistry(reg),
					withLogger(log.NewLogfmtLogger(logs)))
				require.NoError(t, reader.StartAsync(ctx))
				t.Cleanup(func() {
					require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
				})

				// Wait until the Kafka cluster received few Fetch requests.
				test.Poll(t, 5*time.Second, true, func() interface{} {
					return fetchRequestsCount.Load() > 2
				})

				// Since the mocked Kafka cluster is configured to fail any Fetch we expect the reader hasn't
				// catched up yet, and it's still in Starting state.
				assert.Equal(t, services.Starting, reader.State())

				// Unblock the Fetch requests. Now they will succeed.
				fetchShouldFail.Store(false)

				// We expect the reader to catch up, and then switch to Running state.
				test.Poll(t, 5*time.Second, services.Running, func() interface{} {
					return reader.State()
				})

				// We expect the reader to have switched to running because target consumer lag has been honored.
				assert.Contains(t, logs.String(), "partition reader consumed partition and current lag is lower than configured target consumer lag")

				// We expect the reader to have consumed the partition from start.
				test.Poll(t, time.Second, []string{"record-1", "record-2"}, func() interface{} {
					consumedRecordsMx.Lock()
					defer consumedRecordsMx.Unlock()
					return slices.Clone(consumedRecords)
				})

				// We expect the last consumed offset to be tracked in a metric.
				test.Poll(t, time.Second, nil, func() interface{} {
					return promtest.GatherAndCompare(reg, strings.NewReader(`
						# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
						# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
						cortex_ingest_storage_reader_last_consumed_offset{partition="1"} 1
					`), "cortex_ingest_storage_reader_last_consumed_offset")
				})
			})
		}
	})

	t.Run("should consume partition from start if position=start, and wait until target lag is honored, and then consume some records after lag is honored", func(t *testing.T) {
		t.Parallel()

		var (
			cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
			fetchRequestsCount   = atomic.NewInt64(0)
			fetchShouldFail      = atomic.NewBool(false)
			consumedRecordsMx    sync.Mutex
			consumedRecords      []string
		)

		consumer := consumerFunc(func(_ context.Context, records []record) error {
			consumedRecordsMx.Lock()
			defer consumedRecordsMx.Unlock()

			for _, r := range records {
				consumedRecords = append(consumedRecords, string(r.content))
			}
			return nil
		})

		cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			fetchRequestsCount.Inc()

			if fetchShouldFail.Load() {
				return nil, errors.New("mocked error"), true
			}

			return nil, nil, false
		})

		// Produce some records.
		writeClient := newKafkaProduceClient(t, clusterAddr)
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-1"))
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-2"))
		t.Log("produced 2 records")

		// Run the test twice with the same Kafka cluster to show that second time it consumes all records again.
		// Reset the test.
		fetchShouldFail.Store(true)
		fetchRequestsCount.Store(0)
		consumedRecordsMx.Lock()
		consumedRecords = nil
		consumedRecordsMx.Unlock()

		// Create and start the reader.
		reg := prometheus.NewPedanticRegistry()
		logs := &concurrency.SyncBuffer{}
		reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
			withConsumeFromPositionAtStartup(consumeFromStart),
			withTargetAndMaxConsumerLagAtStartup(time.Second, 2*time.Second),
			withRegistry(reg),
			withLogger(log.NewLogfmtLogger(logs)))
		require.NoError(t, reader.StartAsync(ctx))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		})

		// Wait until the Kafka cluster received few Fetch requests.
		test.Poll(t, 5*time.Second, true, func() interface{} {
			return fetchRequestsCount.Load() > 2
		})

		// Since the mocked Kafka cluster is configured to fail any Fetch we expect the reader hasn't
		// catched up yet, and it's still in Starting state.
		assert.Equal(t, services.Starting, reader.State())

		// Unblock the Fetch requests. Now they will succeed.
		fetchShouldFail.Store(false)

		// We expect the reader to catch up, and then switch to Running state.
		test.Poll(t, 5*time.Second, services.Running, func() interface{} {
			return reader.State()
		})

		// We expect the reader to have switched to running because target consumer lag has been honored.
		assert.Contains(t, logs.String(), "partition reader consumed partition and current lag is lower than configured target consumer lag")

		// We expect the reader to have consumed the partition from start.
		test.Poll(t, time.Second, []string{"record-1", "record-2"}, func() interface{} {
			consumedRecordsMx.Lock()
			defer consumedRecordsMx.Unlock()
			return slices.Clone(consumedRecords)
		})

		// We expect the last consumed offset to be tracked in a metric.
		test.Poll(t, time.Second, nil, func() interface{} {
			return promtest.GatherAndCompare(reg, strings.NewReader(`
				# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
				# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
				cortex_ingest_storage_reader_last_consumed_offset{partition="1"} 1
			`), "cortex_ingest_storage_reader_last_consumed_offset")
		})

		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-3"))
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-4"))
		t.Log("produced 2 records")

		// We expect the reader to have consumed the partition from start.
		test.Poll(t, time.Second, []string{"record-1", "record-2", "record-3", "record-4"}, func() interface{} {
			consumedRecordsMx.Lock()
			defer consumedRecordsMx.Unlock()
			return slices.Clone(consumedRecords)
		})

		// We expect the last consumed offset to be tracked in a metric.
		test.Poll(t, time.Second, nil, func() interface{} {
			return promtest.GatherAndCompare(reg, strings.NewReader(`
				# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
				# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
				cortex_ingest_storage_reader_last_consumed_offset{partition="1"} 3
			`), "cortex_ingest_storage_reader_last_consumed_offset")
		})
	})

	t.Run("should consume partition from the timestamp if position=timestamp, and wait until target lag is honored", func(t *testing.T) {
		t.Parallel()

		var (
			cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
			fetchRequestsCount   = atomic.NewInt64(0)
			fetchShouldFail      = atomic.NewBool(false)
			consumedRecordsMx    sync.Mutex
			consumedRecords      []string
		)

		consumer := consumerFunc(func(_ context.Context, records []record) error {
			consumedRecordsMx.Lock()
			defer consumedRecordsMx.Unlock()

			for _, r := range records {
				consumedRecords = append(consumedRecords, string(r.content))
			}
			return nil
		})

		cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			fetchRequestsCount.Inc()

			if fetchShouldFail.Load() {
				return nil, errors.New("mocked error"), true
			}

			return nil, nil, false
		})

		writeClient := newKafkaProduceClient(t, clusterAddr)

		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-1"))
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-2"))

		// Consume from after the records in the head. The sleep guaranties a full second gap between the head and tail of the topic.
		time.Sleep(time.Second)
		consumeFromTs := time.Now()

		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-3"))
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-4"))

		// Create and start the reader.
		fetchShouldFail.Store(true)
		fetchRequestsCount.Store(0)
		consumedRecordsMx.Lock()
		consumedRecords = nil
		consumedRecordsMx.Unlock()

		reg := prometheus.NewPedanticRegistry()
		logs := &concurrency.SyncBuffer{}
		reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
			withConsumeFromTimestampAtStartup(consumeFromTs.UnixMilli()),
			withTargetAndMaxConsumerLagAtStartup(time.Second, 2*time.Second),
			withRegistry(reg),
			withLogger(log.NewLogfmtLogger(logs)))
		require.NoError(t, reader.StartAsync(ctx))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		})

		// Wait until the Kafka cluster received few Fetch requests.
		test.Poll(t, 5*time.Second, true, func() interface{} {
			return fetchRequestsCount.Load() > 0
		})

		// Since the mocked Kafka cluster is configured to fail any Fetch we expect the reader hasn't
		// catched up yet, and it's still in Starting state.
		assert.Equal(t, services.Starting, reader.State())

		// Unblock the Fetch requests. Now they will succeed.
		fetchShouldFail.Store(false)

		// We expect the reader to catch up, and then switch to Running state.
		test.Poll(t, 5*time.Second, services.Running, func() interface{} {
			return reader.State()
		})

		// We expect the reader to have switched to running because target consumer lag has been honored.
		assert.Contains(t, logs.String(), "partition reader consumed partition and current lag is lower than configured target consumer lag")

		// We expect the reader to have consumed the partition from the third record.
		test.Poll(t, time.Second, []string{"record-3", "record-4"}, func() interface{} {
			consumedRecordsMx.Lock()
			defer consumedRecordsMx.Unlock()
			return slices.Clone(consumedRecords)
		})

		// We expect the last consumed offset to be tracked in a metric.
		expectedConsumedOffset := 3
		test.Poll(t, time.Second, nil, func() interface{} {
			return promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
				# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
				# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
				cortex_ingest_storage_reader_last_consumed_offset{partition="1"} %d
			`, expectedConsumedOffset)), "cortex_ingest_storage_reader_last_consumed_offset")
		})
	})

	t.Run("should consume partition from last committed offset if position=last-offset, and wait until target lag is honored", func(t *testing.T) {
		t.Parallel()

		var (
			cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
			fetchRequestsCount   = atomic.NewInt64(0)
			fetchShouldFail      = atomic.NewBool(false)
			consumedRecordsMx    sync.Mutex
			consumedRecords      []string
		)

		consumer := consumerFunc(func(_ context.Context, records []record) error {
			consumedRecordsMx.Lock()
			defer consumedRecordsMx.Unlock()

			for _, r := range records {
				consumedRecords = append(consumedRecords, string(r.content))
			}
			return nil
		})

		cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			fetchRequestsCount.Inc()

			if fetchShouldFail.Load() {
				return nil, errors.New("mocked error"), true
			}

			return nil, nil, false
		})

		// Run the test twice with the same Kafka cluster to show that second time it consumes only new records.
		for run := 1; run <= 2; run++ {
			t.Run(fmt.Sprintf("Run %d", run), func(t *testing.T) {
				// Reset the test.
				fetchShouldFail.Store(true)
				fetchRequestsCount.Store(0)
				consumedRecordsMx.Lock()
				consumedRecords = nil
				consumedRecordsMx.Unlock()

				// Produce a record before each test run.
				writeClient := newKafkaProduceClient(t, clusterAddr)
				produceRecord(ctx, t, writeClient, topicName, partitionID, []byte(fmt.Sprintf("record-%d", run)))
				t.Log("produced 1 record")

				// Create and start the reader.
				reg := prometheus.NewPedanticRegistry()
				logs := &concurrency.SyncBuffer{}
				reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
					withConsumeFromPositionAtStartup(consumeFromLastOffset),
					withTargetAndMaxConsumerLagAtStartup(time.Second, 2*time.Second),
					withRegistry(reg),
					withLogger(log.NewLogfmtLogger(logs)))
				require.NoError(t, reader.StartAsync(ctx))
				t.Cleanup(func() {
					require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
				})

				// Wait until the Kafka cluster received few Fetch requests.
				test.Poll(t, 5*time.Second, true, func() interface{} {
					return fetchRequestsCount.Load() > 2
				})

				// Since the mocked Kafka cluster is configured to fail any Fetch we expect the reader hasn't
				// catched up yet, and it's still in Starting state.
				assert.Equal(t, services.Starting, reader.State())

				// Unblock the Fetch requests. Now they will succeed.
				fetchShouldFail.Store(false)

				// We expect the reader to catch up, and then switch to Running state.
				test.Poll(t, 5*time.Second, services.Running, func() interface{} {
					return reader.State()
				})

				// We expect the reader to have consumed the partition from last offset.
				test.Poll(t, time.Second, []string{fmt.Sprintf("record-%d", run)}, func() interface{} {
					consumedRecordsMx.Lock()
					defer consumedRecordsMx.Unlock()
					return slices.Clone(consumedRecords)
				})

				// We expect the last consumed offset to be tracked in a metric.
				expectedConsumedOffset := run - 1
				test.Poll(t, time.Second, nil, func() interface{} {
					return promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
						# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
						# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
						cortex_ingest_storage_reader_last_consumed_offset{partition="1"} %d
					`, expectedConsumedOffset)), "cortex_ingest_storage_reader_last_consumed_offset")
				})
			})
		}
	})

	t.Run("should consume partition from last committed offset if position=last-offset, and wait until max lag is honored if can't honor target lag", func(t *testing.T) {
		t.Parallel()

		var (
			cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
			writeClient          = newKafkaProduceClient(t, clusterAddr)
			nextRecordID         = atomic.NewInt32(0)
			targetLag            = 500 * time.Millisecond
			maxLag               = 2 * time.Second
		)

		// Wait until all goroutines used in this test have done.
		testRoutines := sync.WaitGroup{}
		t.Cleanup(testRoutines.Wait)

		// Create a channel to signal goroutines once the test has done.
		testDone := make(chan struct{})
		t.Cleanup(func() {
			close(testDone)
		})

		consumer := consumerFunc(func(_ context.Context, _ []record) error {
			return nil
		})

		cluster.ControlKey(int16(kmsg.ListOffsets), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()

			// Slow down each ListOffsets request to take longer than the target lag.
			req := kreq.(*kmsg.ListOffsetsRequest)
			if len(req.Topics) > 0 && len(req.Topics[0].Partitions) > 0 && req.Topics[0].Partitions[0].Timestamp == kafkaOffsetEnd {
				cluster.SleepControl(func() {
					testRoutines.Add(1)
					defer testRoutines.Done()

					delay := time.Duration(float64(targetLag) * 1.1)
					t.Logf("artificially slowing down OffsetFetch request by %s", delay.String())

					select {
					case <-testDone:
					case <-time.After(delay):
					}
				})
			}

			return nil, nil, false
		})

		// Produce a record.
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte(fmt.Sprintf("record-%d", nextRecordID.Inc())))
		t.Log("produced 1 record")

		// Continue to produce records at a high pace, so that we simulate the case there are always new
		// records to fetch.
		testRoutines.Add(1)
		go func() {
			defer testRoutines.Done()

			for {
				select {
				case <-testDone:
					return

				case <-time.After(targetLag / 2):
					produceRecord(ctx, t, writeClient, topicName, partitionID, []byte(fmt.Sprintf("record-%d", nextRecordID.Inc())))
					t.Log("produced 1 record")
				}
			}
		}()

		// Create and start the reader.
		reg := prometheus.NewPedanticRegistry()
		logs := &concurrency.SyncBuffer{}
		reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
			withConsumeFromPositionAtStartup(consumeFromLastOffset),
			withTargetAndMaxConsumerLagAtStartup(targetLag, maxLag),
			withRegistry(reg),
			withLogger(log.NewLogfmtLogger(logs)))
		require.NoError(t, reader.StartAsync(ctx))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		})

		// We expect the reader to catch up, and then switch to Running state.
		test.Poll(t, maxLag*5, services.Running, func() interface{} {
			return reader.State()
		})

		// We expect the reader to have switched to running because max consumer lag has been honored
		// but target lag has not.
		assert.Contains(t, logs.String(), "partition reader consumed partition and current lag is lower than configured max consumer lag but higher than target consumer lag")
	})

	t.Run("should not wait indefinitely if context is cancelled while fetching last produced offset", func(t *testing.T) {
		t.Parallel()

		var (
			cluster, clusterAddr     = testkafka.CreateCluster(t, partitionID+1, topicName)
			consumer                 = consumerFunc(func(context.Context, []record) error { return nil })
			listOffsetsRequestsCount = atomic.NewInt64(0)
		)

		// Mock Kafka to always fail the ListOffsets request.
		cluster.ControlKey(int16(kmsg.ListOffsets), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()

			listOffsetsRequestsCount.Inc()
			return nil, errors.New("mocked error"), true
		})

		// Create and start the reader.
		reader := createReader(t, clusterAddr, topicName, partitionID, consumer, withTargetAndMaxConsumerLagAtStartup(time.Second, time.Second))

		readerCtx, cancelReaderCtx := context.WithCancel(ctx)
		require.NoError(t, reader.StartAsync(readerCtx))

		// Wait until the Kafka cluster received at least 1 ListOffsets request.
		test.Poll(t, 5*time.Second, true, func() interface{} {
			return listOffsetsRequestsCount.Load() > 0
		})

		// Cancelling the context should cause the service to switch to a terminal state.
		assert.Equal(t, services.Starting, reader.State())
		cancelReaderCtx()

		test.Poll(t, 5*time.Second, services.Failed, func() interface{} {
			return reader.State()
		})
	})

	t.Run("should not wait indefinitely if context is cancelled while fetching records", func(t *testing.T) {
		t.Parallel()

		var (
			cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
			consumer             = consumerFunc(func(context.Context, []record) error { return nil })
			fetchRequestsCount   = atomic.NewInt64(0)
		)

		// Mock Kafka to always fail the Fetch request.
		cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()

			fetchRequestsCount.Inc()
			return nil, errors.New("mocked error"), true
		})

		// Produce some records.
		writeClient := newKafkaProduceClient(t, clusterAddr)
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-1"))
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-2"))
		t.Log("produced 2 records")

		// Create and start the reader.
		reader := createReader(t, clusterAddr, topicName, partitionID, consumer, withTargetAndMaxConsumerLagAtStartup(time.Second, time.Second))

		readerCtx, cancelReaderCtx := context.WithCancel(ctx)
		require.NoError(t, reader.StartAsync(readerCtx))

		// Wait until the Kafka cluster received at least 1 Fetch request.
		test.Poll(t, 5*time.Second, true, func() interface{} {
			return fetchRequestsCount.Load() > 0
		})

		// Cancelling the context should cause the service to switch to a terminal state.
		assert.Equal(t, services.Starting, reader.State())
		cancelReaderCtx()

		test.Poll(t, 5*time.Second, services.Failed, func() interface{} {
			return reader.State()
		})
	})

	t.Run("should not wait indefinitely if there are no records to consume from Kafka but partition start offset is > 0 (e.g. all previous records have been deleted by Kafka retention)", func(t *testing.T) {
		t.Parallel()

		for _, consumeFromPosition := range consumeFromPositionOptions {
			t.Run(fmt.Sprintf("consume from position: %s", consumeFromPosition), func(t *testing.T) {
				t.Parallel()

				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)

				consumer := consumerFunc(func(context.Context, []record) error {
					return nil
				})

				cluster, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
				cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
					cluster.KeepControl()

					// Throttle the Fetch request.
					select {
					case <-ctx.Done():
					case <-time.After(time.Second):
					}

					return nil, nil, false
				})

				// Produce some records.
				writeClient := newKafkaProduceClient(t, clusterAddr)
				produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-1"))
				produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-2"))
				t.Log("produced 2 records")

				// Fetch the partition end offset, which is the offset of the next record that will be produced.
				adminClient := kadm.NewClient(writeClient)
				endOffsets, err := adminClient.ListEndOffsets(ctx, topicName)
				require.NoError(t, err)
				endOffset, exists := endOffsets.Lookup(topicName, partitionID)
				require.True(t, exists)
				require.NoError(t, endOffset.Err)
				t.Logf("fetched partition end offset: %d", endOffset.Offset)

				// Issue a request to delete produced records so far. What Kafka does under the hood is to advance
				// the partition start offset to the specified offset.
				advancePartitionStartTo := kadm.Offsets{}
				advancePartitionStartTo.Add(kadm.Offset{Topic: topicName, Partition: partitionID, At: endOffset.Offset})
				_, err = adminClient.DeleteRecords(ctx, advancePartitionStartTo)
				require.NoError(t, err)
				t.Logf("advanced partition start offset to: %d", endOffset.Offset)

				// Create and start the reader. We expect the reader to immediately switch to Running state.
				reg := prometheus.NewPedanticRegistry()
				reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
					withConsumeFromPositionAtStartup(consumeFromPosition),
					withConsumeFromTimestampAtStartup(time.Now().UnixMilli()), // For the test where position=timestamp.
					withTargetAndMaxConsumerLagAtStartup(time.Second, time.Second),
					withRegistry(reg))

				require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
				t.Cleanup(func() {
					require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
				})

				// We expect no record has been consumed.
				require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
					# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
					# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
					cortex_ingest_storage_reader_last_consumed_offset{partition="1"} -1

					# HELP cortex_ingest_storage_reader_last_committed_offset The last consumed offset successfully committed by the partition reader. Set to -1 if not offset has been committed yet.
					# TYPE cortex_ingest_storage_reader_last_committed_offset gauge
					cortex_ingest_storage_reader_last_committed_offset{partition="1"} -1
				`), "cortex_ingest_storage_reader_last_consumed_offset", "cortex_ingest_storage_reader_last_committed_offset"))
			})
		}
	})
}

func TestPartitionReader_fetchLastCommittedOffset(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 1
	)

	var (
		ctx = context.Background()
	)

	t.Run("should return 'not exists' if Kafka returns GroupIDNotFound", func(t *testing.T) {
		t.Parallel()

		var (
			cluster, clusterAddr = testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, partitionID+1, topicName)
			consumer             = consumerFunc(func(context.Context, []record) error { return nil })
			reader               = createReader(t, clusterAddr, topicName, partitionID, consumer, withTargetAndMaxConsumerLagAtStartup(time.Second, time.Second))
		)

		cluster.ControlKey(int16(kmsg.OffsetFetch), func(request kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()

			req := request.(*kmsg.OffsetFetchRequest)
			res := req.ResponseKind().(*kmsg.OffsetFetchResponse)
			res.Default()
			res.Groups = []kmsg.OffsetFetchResponseGroup{
				{
					Group:     reader.consumerGroup,
					ErrorCode: kerr.GroupIDNotFound.Code,
				},
			}

			return res, nil, true
		})

		client := newKafkaProduceClient(t, clusterAddr)
		_, exists, err := reader.fetchLastCommittedOffset(ctx, client)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("should return 'not exists' if Kafka returns no offsets for the requested partition", func(t *testing.T) {
		t.Parallel()

		var (
			cluster, clusterAddr = testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, partitionID+1, topicName)
			consumer             = consumerFunc(func(context.Context, []record) error { return nil })
			reader               = createReader(t, clusterAddr, topicName, partitionID, consumer, withTargetAndMaxConsumerLagAtStartup(time.Second, time.Second))
		)

		cluster.ControlKey(int16(kmsg.OffsetFetch), func(request kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()

			req := request.(*kmsg.OffsetFetchRequest)
			res := req.ResponseKind().(*kmsg.OffsetFetchResponse)
			res.Default()
			res.Groups = []kmsg.OffsetFetchResponseGroup{
				{
					Group: reader.consumerGroup,
					Topics: []kmsg.OffsetFetchResponseGroupTopic{
						{
							Topic: topicName,
							Partitions: []kmsg.OffsetFetchResponseGroupTopicPartition{
								{
									Partition: partitionID + 1, // Another partition.
									Offset:    456,
								},
							},
						},
					},
				},
			}

			return res, nil, true
		})

		client := newKafkaProduceClient(t, clusterAddr)
		_, exists, err := reader.fetchLastCommittedOffset(ctx, client)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("should return the committed  to Kafka offset", func(t *testing.T) {
		t.Parallel()

		var (
			cluster, clusterAddr = testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, partitionID+1, topicName)
			consumer             = consumerFunc(func(context.Context, []record) error { return nil })
			reader               = createReader(t, clusterAddr, topicName, partitionID, consumer, withTargetAndMaxConsumerLagAtStartup(time.Second, time.Second))
		)

		cluster.ControlKey(int16(kmsg.OffsetFetch), func(request kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()

			req := request.(*kmsg.OffsetFetchRequest)
			res := req.ResponseKind().(*kmsg.OffsetFetchResponse)
			res.Default()
			res.Groups = []kmsg.OffsetFetchResponseGroup{
				{
					Group: reader.consumerGroup,
					Topics: []kmsg.OffsetFetchResponseGroupTopic{
						{
							Topic: topicName,
							Partitions: []kmsg.OffsetFetchResponseGroupTopicPartition{
								{
									Partition: partitionID, // Our partition.
									Offset:    123,
								}, {
									Partition: partitionID + 1, // Another partition.
									Offset:    456,
								},
							},
						},
					},
				},
			}

			return res, nil, true
		})

		client := newKafkaProduceClient(t, clusterAddr)
		offset, exists, err := reader.fetchLastCommittedOffset(ctx, client)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, int64(123), offset)
	})
}

func TestPartitionCommitter(t *testing.T) {
	t.Parallel()

	const (
		topicName     = "test-topic"
		consumerGroup = "test-group"
		partitionID   = 1
	)

	t.Run("should keep trying to commit offset if the last commit failed, even if the offset has not been incremented", func(t *testing.T) {
		t.Parallel()

		cluster, clusterAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, partitionID+1, topicName)

		// Mock the cluster to control OffsetCommit request.
		commitRequestsCount := atomic.NewInt64(0)
		commitRequestsShouldFail := atomic.NewBool(true)

		cluster.ControlKey(kmsg.OffsetCommit.Int16(), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()

			res := kreq.ResponseKind().(*kmsg.OffsetCommitResponse)
			res.Default()

			if commitRequestsShouldFail.Load() {
				res.Topics = []kmsg.OffsetCommitResponseTopic{
					{Topic: topicName, Partitions: []kmsg.OffsetCommitResponseTopicPartition{{Partition: partitionID, ErrorCode: kerr.InvalidCommitOffsetSize.Code}}},
				}
			} else {
				res.Topics = []kmsg.OffsetCommitResponseTopic{
					{Topic: topicName, Partitions: []kmsg.OffsetCommitResponseTopicPartition{{Partition: partitionID}}},
				}
			}

			return res, nil, true
		})

		logger := testutil.NewLogger(t)
		cfg := createTestKafkaConfig(clusterAddr, topicName)
		client, err := kgo.NewClient(commonKafkaClientOptions(cfg, nil, logger)...)
		require.NoError(t, err)
		t.Cleanup(client.Close)

		adm := kadm.NewClient(client)
		reg := prometheus.NewPedanticRegistry()

		committer := newPartitionCommitter(cfg, adm, partitionID, consumerGroup, logger, reg)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), committer))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), committer))
		})

		committer.enqueueOffset(123)

		// Since we mocked the Kafka cluster to fail the OffsetCommit requests, we wait until the
		// first failure is tracked by the partition committer.
		require.Eventually(t, func() bool {
			return promtest.ToFloat64(committer.commitFailuresTotal) > 0
		}, 5*time.Second, 10*time.Millisecond)

		// At least 1 commit failed. Now we unblock it.
		commitRequestsShouldFail.Store(false)

		// Now we expect the commit to succeed, once the committer will trigger the commit the next interval.
		test.Poll(t, 10*cfg.ConsumerGroupOffsetCommitInterval, nil, func() interface{} {
			return promtest.GatherAndCompare(reg, strings.NewReader(`
				# HELP cortex_ingest_storage_reader_last_committed_offset The last consumed offset successfully committed by the partition reader. Set to -1 if not offset has been committed yet.
				# TYPE cortex_ingest_storage_reader_last_committed_offset gauge
				cortex_ingest_storage_reader_last_committed_offset{partition="1"} 123

				# HELP cortex_ingest_storage_reader_offset_commit_failures_total Total number of failed requests to commit the last consumed offset.
				# TYPE cortex_ingest_storage_reader_offset_commit_failures_total counter
				cortex_ingest_storage_reader_offset_commit_failures_total{partition="1"} 1

				# HELP cortex_ingest_storage_reader_offset_commit_requests_total Total number of requests issued to commit the last consumed offset (includes both successful and failed requests).
				# TYPE cortex_ingest_storage_reader_offset_commit_requests_total counter
				cortex_ingest_storage_reader_offset_commit_requests_total{partition="1"} 2
			`),
				"cortex_ingest_storage_reader_offset_commit_requests_total",
				"cortex_ingest_storage_reader_offset_commit_failures_total",
				"cortex_ingest_storage_reader_last_committed_offset")
		})

		// Since we haven't enqueued any other offset and the last enqueued one has been successfully committed,
		// we expect the committer to not issue any other request in the future.
		expectedRequestsCount := commitRequestsCount.Load()
		time.Sleep(3 * cfg.ConsumerGroupOffsetCommitInterval)
		assert.Equal(t, expectedRequestsCount, commitRequestsCount.Load())
	})
}

func TestPartitionCommitter_commit(t *testing.T) {
	t.Parallel()

	const (
		topicName     = "test-topic"
		consumerGroup = "test-group"
		partitionID   = 1
	)

	t.Run("should track metrics on successful commit", func(t *testing.T) {
		t.Parallel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)

		cfg := createTestKafkaConfig(clusterAddr, topicName)
		client, err := kgo.NewClient(commonKafkaClientOptions(cfg, nil, log.NewNopLogger())...)
		require.NoError(t, err)
		t.Cleanup(client.Close)

		adm := kadm.NewClient(client)
		reg := prometheus.NewPedanticRegistry()
		committer := newPartitionCommitter(cfg, adm, partitionID, consumerGroup, log.NewNopLogger(), reg)

		require.NoError(t, committer.commit(context.Background(), 123))

		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_reader_last_committed_offset The last consumed offset successfully committed by the partition reader. Set to -1 if not offset has been committed yet.
			# TYPE cortex_ingest_storage_reader_last_committed_offset gauge
			cortex_ingest_storage_reader_last_committed_offset{partition="1"} 123

			# HELP cortex_ingest_storage_reader_offset_commit_failures_total Total number of failed requests to commit the last consumed offset.
			# TYPE cortex_ingest_storage_reader_offset_commit_failures_total counter
			cortex_ingest_storage_reader_offset_commit_failures_total{partition="1"} 0

			# HELP cortex_ingest_storage_reader_offset_commit_requests_total Total number of requests issued to commit the last consumed offset (includes both successful and failed requests).
			# TYPE cortex_ingest_storage_reader_offset_commit_requests_total counter
			cortex_ingest_storage_reader_offset_commit_requests_total{partition="1"} 1
		`),
			"cortex_ingest_storage_reader_offset_commit_requests_total",
			"cortex_ingest_storage_reader_offset_commit_failures_total",
			"cortex_ingest_storage_reader_last_committed_offset"))
	})

	t.Run("should track metrics on failed commit", func(t *testing.T) {
		t.Parallel()

		cluster, clusterAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, partitionID+1, topicName)

		// Mock the cluster to fail any offset commit request.
		cluster.ControlKey(kmsg.OffsetCommit.Int16(), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			return nil, errors.New("mocked error"), true
		})

		cfg := createTestKafkaConfig(clusterAddr, topicName)
		client, err := kgo.NewClient(commonKafkaClientOptions(cfg, nil, log.NewNopLogger())...)
		require.NoError(t, err)
		t.Cleanup(client.Close)

		adm := kadm.NewClient(client)
		reg := prometheus.NewPedanticRegistry()
		committer := newPartitionCommitter(cfg, adm, partitionID, consumerGroup, log.NewNopLogger(), reg)

		require.Error(t, committer.commit(context.Background(), 123))

		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_reader_last_committed_offset The last consumed offset successfully committed by the partition reader. Set to -1 if not offset has been committed yet.
			# TYPE cortex_ingest_storage_reader_last_committed_offset gauge
			cortex_ingest_storage_reader_last_committed_offset{partition="1"} -1

			# HELP cortex_ingest_storage_reader_offset_commit_failures_total Total number of failed requests to commit the last consumed offset.
			# TYPE cortex_ingest_storage_reader_offset_commit_failures_total counter
			cortex_ingest_storage_reader_offset_commit_failures_total{partition="1"} 1

			# HELP cortex_ingest_storage_reader_offset_commit_requests_total Total number of requests issued to commit the last consumed offset (includes both successful and failed requests).
			# TYPE cortex_ingest_storage_reader_offset_commit_requests_total counter
			cortex_ingest_storage_reader_offset_commit_requests_total{partition="1"} 1
		`),
			"cortex_ingest_storage_reader_offset_commit_requests_total",
			"cortex_ingest_storage_reader_offset_commit_failures_total",
			"cortex_ingest_storage_reader_last_committed_offset"))
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

func produceRecord(ctx context.Context, t *testing.T, writeClient *kgo.Client, topicName string, partitionID int32, content []byte) int64 {
	rec := &kgo.Record{
		Value:     content,
		Topic:     topicName,
		Partition: partitionID,
	}
	produceResult := writeClient.ProduceSync(ctx, rec)
	require.NoError(t, produceResult.FirstErr())

	return rec.Offset
}

type readerTestCfg struct {
	kafka       KafkaConfig
	partitionID int32
	consumer    recordConsumer
	registry    *prometheus.Registry
	logger      log.Logger
}

type readerTestCfgOtp func(cfg *readerTestCfg)

func withCommitInterval(i time.Duration) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.kafka.ConsumerGroupOffsetCommitInterval = i
	}
}

func withLastProducedOffsetPollInterval(i time.Duration) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.kafka.LastProducedOffsetPollInterval = i
	}
}

func withTargetAndMaxConsumerLagAtStartup(targetLag, maxLag time.Duration) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.kafka.TargetConsumerLagAtStartup = targetLag
		cfg.kafka.MaxConsumerLagAtStartup = maxLag
	}
}

func withConsumeFromPositionAtStartup(position string) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.kafka.ConsumeFromPositionAtStartup = position
	}
}

func withConsumeFromTimestampAtStartup(ts int64) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.kafka.ConsumeFromPositionAtStartup = consumeFromTimestamp
		cfg.kafka.ConsumeFromTimestampAtStartup = ts
	}
}

func withWaitStrongReadConsistencyTimeout(timeout time.Duration) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.kafka.WaitStrongReadConsistencyTimeout = timeout
	}
}

func withRegistry(reg *prometheus.Registry) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.registry = reg
	}
}

func withLogger(logger log.Logger) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.logger = logger
	}
}

var testingLogger = mimirtest.NewTestingLogger(nil)

func defaultReaderTestConfig(t *testing.T, addr string, topicName string, partitionID int32, consumer recordConsumer) *readerTestCfg {
	return &readerTestCfg{
		registry:    prometheus.NewPedanticRegistry(),
		logger:      testingLogger.WithT(t),
		kafka:       createTestKafkaConfig(addr, topicName),
		partitionID: partitionID,
		consumer:    consumer,
	}
}

func createReader(t *testing.T, addr string, topicName string, partitionID int32, consumer recordConsumer, opts ...readerTestCfgOtp) *PartitionReader {
	cfg := defaultReaderTestConfig(t, addr, topicName, partitionID, consumer)
	for _, o := range opts {
		o(cfg)
	}

	// Ensure the config is valid.
	require.NoError(t, cfg.kafka.Validate())

	reader, err := newPartitionReader(cfg.kafka, cfg.partitionID, "test-group", cfg.consumer, cfg.logger, cfg.registry)
	require.NoError(t, err)

	// Reduce the time the fake kafka would wait for new records. Sometimes this blocks startup.
	reader.concurrentFetchersMinBytesMaxWaitTime = 500 * time.Millisecond

	return reader
}

func createAndStartReader(ctx context.Context, t *testing.T, addr string, topicName string, partitionID int32, consumer recordConsumer, opts ...readerTestCfgOtp) *PartitionReader {
	reader := createReader(t, addr, topicName, partitionID, consumer, opts...)

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
		reader := createAndStartReader(ctx, t, clusterAddr, topicName, partitionID, consumer, withCommitInterval(commitInterval))

		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("1"))
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("2"))
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("3"))

		_, err := consumer.waitRecords(3, time.Second, commitInterval*2) // wait for a few commits to make sure empty commits don't cause issues
		require.NoError(t, err)

		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

		recordsSentAfterShutdown := []byte("4")
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, recordsSentAfterShutdown)

		createAndStartReader(ctx, t, clusterAddr, topicName, partitionID, consumer, withCommitInterval(commitInterval))

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
		reader := createAndStartReader(ctx, t, clusterAddr, topicName, partitionID, consumer, withCommitInterval(commitInterval))

		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("1"))
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("2"))
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("3"))

		_, err := consumer.waitRecords(3, time.Second, 0)
		require.NoError(t, err)

		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("4"))
		createAndStartReader(ctx, t, clusterAddr, topicName, partitionID, consumer, withCommitInterval(commitInterval))

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
		reader := createAndStartReader(ctx, t, clusterAddr, topicName, partitionID, consumer, withCommitInterval(commitInterval))

		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("1"))
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("2"))
		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("3"))

		_, err := consumer.waitRecords(3, time.Second, 0)
		require.NoError(t, err)

		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		reader = createAndStartReader(ctx, t, clusterAddr, topicName, partitionID, consumer, withCommitInterval(commitInterval))

		// No new records since the last commit.
		_, err = consumer.waitRecords(0, time.Second, 0)
		assert.NoError(t, err)

		// Shut down without having consumed any records.
		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		_ = createAndStartReader(ctx, t, clusterAddr, topicName, partitionID, consumer, withCommitInterval(commitInterval))

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

func createTestContextWithTimeout(t *testing.T, timeout time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}
