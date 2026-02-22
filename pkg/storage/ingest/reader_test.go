// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	crypto_rand "crypto/rand"
	"errors"
	"fmt"
	"iter"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
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

	consumer := newTestConsumer(2)

	createAndStartReader(ctx, t, clusterAddr, topicName, partitionID, consumer)

	writeClient := newKafkaProduceClient(t, clusterAddr)

	produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record 1"))
	produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record 2"))
	produceRecordWithVersion(ctx, t, writeClient, topicName, partitionID, []byte("record 3"), 0)
	produceRecordWithVersion(ctx, t, writeClient, topicName, partitionID, []byte("record 4"), 2)

	records, err := consumer.waitRecordsAndMetadata(4, 5*time.Second, 0)

	assert.NoError(t, err)
	assert.Len(t, records, 4)
	assert.Equal(t, []byte("record 1"), records[0].Value)
	assert.Equal(t, 1, ParseRecordVersion(records[0]))
	assert.Equal(t, []byte("record 2"), records[1].Value)
	assert.Equal(t, 1, ParseRecordVersion(records[1]))
	assert.Equal(t, []byte("record 3"), records[2].Value)
	assert.Equal(t, 0, ParseRecordVersion(records[2]))
	assert.Equal(t, []byte("record 4"), records[3].Value)
	assert.Equal(t, 2, ParseRecordVersion(records[3]))
}

func TestPartitionReader_RequiresOffsetFilePath(t *testing.T) {
	cfg := defaultReaderTestConfig(t, "", "test", 1, nil)
	_, err := newPartitionReader(cfg.kafka, cfg.partitionID, "test-group", "", cfg.consumer, &NoOpPreCommitNotifier{}, cfg.logger, cfg.registry)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "offset file path must be specified")
}

func TestPartitionReader_ShouldHonorConfiguredFetchMaxWait(t *testing.T) {
	const (
		topicName    = "test"
		partitionID  = 1
		fetchMaxWait = 12 * time.Second
	)

	cfg := defaultReaderTestConfig(t, "", topicName, partitionID, nil)
	cfg.kafka.FetchMaxWait = fetchMaxWait

	reader, err := newPartitionReader(cfg.kafka, cfg.partitionID, "test-group", cfg.offsetFilePath, cfg.consumer, &NoOpPreCommitNotifier{}, cfg.logger, cfg.registry)
	require.NoError(t, err)
	require.Equal(t, fetchMaxWait, reader.concurrentFetchersMinBytesMaxWaitTime)
}

func TestPartitionReader_logFetchErrors(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 1
	)

	cfg := defaultReaderTestConfig(t, "", topicName, partitionID, nil)
	reader, err := newPartitionReader(cfg.kafka, cfg.partitionID, "test-group", cfg.offsetFilePath, cfg.consumer, &NoOpPreCommitNotifier{}, cfg.logger, cfg.registry)
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

	// We want to run this test with different concurrency config.
	concurrencyVariants := map[string][]readerTestCfgOpt{
		"without concurrency":    {withFetchConcurrency(0)},
		"with fetch concurrency": {withFetchConcurrency(2)},
	}

	for concurrencyName, concurrencyVariant := range concurrencyVariants {
		concurrencyVariant := concurrencyVariant

		t.Run(concurrencyName, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancelCause(context.Background())
			t.Cleanup(func() { cancel(errors.New("test done")) })

			_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)

			invocations := atomic.NewInt64(0)
			returnErrors := atomic.NewBool(true)
			trackingConsumer := newTestConsumer(2)
			consumer := consumerFunc(func(ctx context.Context, records iter.Seq[*kgo.Record]) error {
				invocations.Inc()
				if !returnErrors.Load() {
					return trackingConsumer.Consume(ctx, records)
				}
				// There may be more records, but we only care that the one we failed to consume in the first place is still there.
				recs := slices.Collect(records)
				assert.Equal(t, "1", string(recs[0].Value))
				return errors.New("consumer error")
			})
			createAndStartReader(ctx, t, clusterAddr, topicName, partitionID, consumer, concurrencyVariant...)

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
		})
	}
}

func TestPartitionReader_ConsumerStopping(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 1
	)

	// We want to run this test with different concurrency config.
	concurrencyVariants := map[string][]readerTestCfgOpt{
		"without concurrency":    {withFetchConcurrency(0)},
		"with fetch concurrency": {withFetchConcurrency(2)},
	}

	for concurrencyName, concurrencyVariant := range concurrencyVariants {
		concurrencyVariant := concurrencyVariant

		t.Run(concurrencyName, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancelCause(context.Background())
			t.Cleanup(func() { cancel(errors.New("test done")) })

			_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)

			// consumerErrs will store the last error returned by the consumer; its initial value doesn't matter, but it must be non-nil.
			consumerErrs := atomic.NewError(errors.New("dummy error"))
			type consumerCall struct {
				f    func() []*kgo.Record
				resp chan error
			}
			consumeCalls := make(chan consumerCall)
			consumer := consumerFunc(func(ctx context.Context, records iter.Seq[*kgo.Record]) (err error) {
				defer consumerErrs.Store(err)

				call := consumerCall{
					f:    func() []*kgo.Record { return slices.Collect(records) },
					resp: make(chan error),
				}
				consumeCalls <- call
				err = <-call.resp
				// The service is about to transition into its stopping phase. But the consumer must not observe it via the parent context.
				assert.NoError(t, ctx.Err())

				return err
			})
			reader := createReader(t, clusterAddr, topicName, partitionID, consumer, concurrencyVariant...)
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
				require.Equal(t, []byte("1"), records[0].Value)
			}()

			// Wait for the reader to stop completely.
			require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
			// Checks the consumer returned a non-errored result.
			require.NoError(t, consumerErrs.Load())
		})
	}
}

func TestPartitionReader_WaitReadConsistencyUntilLastProducedOffset_And_WaitReadConsistencyUntilOffset(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 0
	)

	ctx := t.Context()

	setup := func(t *testing.T, consumer RecordConsumer, opts ...readerTestCfgOpt) (*PartitionReader, *kgo.Client, *prometheus.Registry) {
		reg := prometheus.NewPedanticRegistry()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)

		// Configure the reader to poll the "last produced offset" frequently.
		reader := createAndStartReader(ctx, t, clusterAddr, topicName, partitionID, consumer,
			append([]readerTestCfgOpt{
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
				consumer := consumerFunc(func(_ context.Context, records iter.Seq[*kgo.Record]) error {
					for record := range records {
						// Introduce a delay before returning from the consume function once
						// the 2nd record has been consumed.
						if consumedRecords.Load()+1 == 2 {
							time.Sleep(time.Second)
						}

						consumedRecords.Inc()
						assert.Equal(t, fmt.Sprintf("record-%d", consumedRecords.Load()), string(record.Value))
						t.Logf("consumed record: %s", string(record.Value))
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
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", topic="%s", with_offset="%t"} 1
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", topic="%s", with_offset="%t"} 0

					# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
					# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
					cortex_ingest_storage_strong_consistency_failures_total{component="partition-reader", topic="%s"} 0
				`, topicName, withOffset, topicName, !withOffset, topicName)),
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
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", topic="%s", with_offset="%t"} 1
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", topic="%s", with_offset="%t"} 0

					# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
					# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
					cortex_ingest_storage_strong_consistency_failures_total{component="partition-reader", topic="%s"} 1
				`, topicName, withOffset, topicName, !withOffset, topicName)),
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
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", topic="%s", with_offset="%t"} 1
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", topic="%s", with_offset="%t"} 0

					# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
					# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
					cortex_ingest_storage_strong_consistency_failures_total{component="partition-reader", topic="%s"} 1
				`, topicName, withOffset, topicName, !withOffset, topicName)),
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
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", topic="%s", with_offset="%t"} 1
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", topic="%s", with_offset="%t"} 0

					# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
					# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
					cortex_ingest_storage_strong_consistency_failures_total{component="partition-reader", topic="%s"} 0
				`, topicName, withOffset, topicName, !withOffset, topicName)),
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
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", topic="%s", with_offset="%t"} 1
					cortex_ingest_storage_strong_consistency_requests_total{component="partition-reader", topic="%s", with_offset="%t"} 0

					# HELP cortex_ingest_storage_strong_consistency_failures_total Total number of failures while waiting for strong consistency to be enforced.
					# TYPE cortex_ingest_storage_strong_consistency_failures_total counter
					cortex_ingest_storage_strong_consistency_failures_total{component="partition-reader", topic="%s"} 1
				`, topicName, withOffset, topicName, !withOffset, topicName)),
					"cortex_ingest_storage_strong_consistency_requests_total",
					"cortex_ingest_storage_strong_consistency_failures_total"))
			})
		}
	})
}

func TestPartitionReader_EnforceReadMaxDelay(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 0
	)

	ctx := t.Context()

	setup := func(t *testing.T, consumer RecordConsumer) (*PartitionReader, *kgo.Client) {
		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		reader := createAndStartReader(ctx, t, clusterAddr, topicName, partitionID, consumer)

		// In this test we produce records with an old timestamp. We need to set a very high write timeout
		// otherwise records expire before the Kafka client even try to send them on the wire.
		writeClient := newKafkaProduceClient(t, clusterAddr, withWriteTimeout(10*time.Minute))

		return reader, writeClient
	}

	t.Run("should succeed if no record has been consumed yet", func(t *testing.T) {
		t.Parallel()

		consumer := consumerFunc(func(_ context.Context, records iter.Seq[*kgo.Record]) error {
			return nil
		})

		reader, _ := setup(t, consumer)

		assert.Zero(t, reader.highestConsumedTimestampBeforePartitionEnd.Load())
		assert.NoError(t, reader.EnforceReadMaxDelay(time.Minute))
	})

	t.Run("should succeed if the partition has been consumed until the end", func(t *testing.T) {
		t.Parallel()

		for _, recordTimestampAge := range []time.Duration{0, 5 * time.Minute} {
			t.Run(fmt.Sprintf("record timestamp age: %s", recordTimestampAge.String()), func(t *testing.T) {
				var (
					consumedRecordsMx sync.Mutex
					consumedRecords   []string
				)

				consumer := consumerFunc(func(_ context.Context, records iter.Seq[*kgo.Record]) error {
					consumedRecordsMx.Lock()
					defer consumedRecordsMx.Unlock()

					for r := range records {
						consumedRecords = append(consumedRecords, string(r.Value))
					}
					return nil
				})

				reader, writeClient := setup(t, consumer)

				// Produce records.
				produceRecordWithTimestamp(ctx, t, writeClient, topicName, partitionID, []byte("record-1"), time.Now().Add(-recordTimestampAge))
				produceRecordWithTimestamp(ctx, t, writeClient, topicName, partitionID, []byte("record-2"), time.Now().Add(-recordTimestampAge))
				t.Log("produced 2 records")

				// Wait until they've been consumed.
				test.Poll(t, time.Second, []string{"record-1", "record-2"}, func() interface{} {
					consumedRecordsMx.Lock()
					defer consumedRecordsMx.Unlock()
					return slices.Clone(consumedRecords)
				})

				// Wait until highest consumed timestamp is reset to the zero value because we reached the
				// end of the partition.
				test.Poll(t, 5*time.Second, true, func() interface{} {
					return reader.highestConsumedTimestampBeforePartitionEnd.Load().IsZero()
				})

				assert.NoError(t, reader.EnforceReadMaxDelay(time.Minute))
			})
		}
	})

	t.Run("should fail if the partition has not been consumed until the end and the current consumption delay is above the max delay", func(t *testing.T) {
		t.Parallel()

		var (
			firstRecordConsumed     = atomic.NewBool(false)
			firstRecordConsumedWait = make(chan struct{})
		)

		// Mock the customer to stop processing records after the first one, to reproduce the case
		// there are more records in Kafka and consumption hasn't reached the end of the partition.
		consumer := consumerFunc(func(_ context.Context, records iter.Seq[*kgo.Record]) error {
			if firstRecordConsumed.CompareAndSwap(false, true) {
				close(firstRecordConsumedWait)
				return nil
			}

			return errors.New("mocked error to stop consuming records")
		})

		reader, writeClient := setup(t, consumer)

		// Produce a large number of records with an old timestamp (total 1GB of data to make sure records end up in
		// different fetches).
		payload, err := generateRandomBytes(1024 * 1024)
		require.NoError(t, err)

		const numRecords = 1024
		for range numRecords {
			produceRecordWithTimestamp(ctx, t, writeClient, topicName, partitionID, payload, time.Now().Add(-5*time.Minute))
		}
		t.Logf("produced %d records", numRecords)

		// Wait until the first record is consumed.
		select {
		case <-firstRecordConsumedWait:
		case <-t.Context().Done():
			t.Fatal("test timed out")
		}

		// At this point we expect the max delay to not be honored. Due to async consumption it may not be immediate,
		// so we wait until a timestamp is stored.
		test.Poll(t, 5*time.Second, true, func() interface{} {
			return !reader.highestConsumedTimestampBeforePartitionEnd.Load().IsZero()
		})

		require.NotZero(t, reader.highestConsumedTimestampBeforePartitionEnd.Load())
		assert.Greater(t, time.Since(reader.highestConsumedTimestampBeforePartitionEnd.Load()), time.Minute)
		assert.Error(t, reader.EnforceReadMaxDelay(time.Minute))
	})
}

func TestPartitionReader_ConsumeAtStartup(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 1
	)

	ctx := context.Background()

	// We want to run all these tests with different concurrency config.
	concurrencyVariants := map[string][]readerTestCfgOpt{
		"without concurrency":      {withFetchConcurrency(0)},
		"with startup concurrency": {withFetchConcurrency(2)},
	}

	t.Run("should immediately switch to Running state if partition is empty", func(t *testing.T) {
		t.Parallel()

		for concurrencyName, concurrencyVariant := range concurrencyVariants {
			concurrencyVariant := concurrencyVariant

			t.Run(concurrencyName, func(t *testing.T) {
				t.Parallel()

				var (
					_, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
					consumer       = consumerFunc(func(context.Context, iter.Seq[*kgo.Record]) error { return nil })
					reg            = prometheus.NewPedanticRegistry()
				)

				// Create and start the reader. We expect the reader to start even if partition is empty.
				readerOpts := append([]readerTestCfgOpt{
					withTargetAndMaxConsumerLagAtStartup(time.Second, time.Second),
					withRegistry(reg),
				}, concurrencyVariant...)

				reader := createReader(t, clusterAddr, topicName, partitionID, consumer, readerOpts...)
				require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
				require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

				// The last consumed offset should be -1, since nothing has been consumed yet.
				assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
					# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
					# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
					cortex_ingest_storage_reader_last_consumed_offset{partition="1"} -1
				`), "cortex_ingest_storage_reader_last_consumed_offset"))
			})
		}
	})

	t.Run("should immediately switch to Running state if configured target / max lag is 0", func(t *testing.T) {
		t.Parallel()

		for concurrencyName, concurrencyVariant := range concurrencyVariants {
			concurrencyVariant := concurrencyVariant

			t.Run(concurrencyName, func(t *testing.T) {
				t.Parallel()

				var (
					cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
					consumer             = consumerFunc(func(context.Context, iter.Seq[*kgo.Record]) error { return nil })
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
				readerOpts := append([]readerTestCfgOpt{
					withTargetAndMaxConsumerLagAtStartup(0, 0),
					withRegistry(reg),
				}, concurrencyVariant...)

				reader := createReader(t, clusterAddr, topicName, partitionID, consumer, readerOpts...)
				require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
				require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

				// The last consumed offset should be -1, since nothing has been consumed yet (Fetch requests are failing).
				assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
					# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
					# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
					cortex_ingest_storage_reader_last_consumed_offset{partition="1"} -1
				`), "cortex_ingest_storage_reader_last_consumed_offset"))
			})
		}
	})

	t.Run("should consume partition from start if last committed offset is missing and wait until target lag is honored", func(t *testing.T) {
		t.Parallel()

		for concurrencyName, concurrencyVariant := range concurrencyVariants {
			concurrencyVariant := concurrencyVariant

			t.Run(concurrencyName, func(t *testing.T) {
				t.Parallel()

				var (
					cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
					fetchRequestsCount   = atomic.NewInt64(0)
					fetchShouldFail      = atomic.NewBool(true)
					consumedRecordsCount = atomic.NewInt64(0)
				)

				consumer := consumerFunc(func(_ context.Context, records iter.Seq[*kgo.Record]) error {
					consumedRecordsCount.Add(int64(len(slices.Collect(records))))
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
				readerOpts := append([]readerTestCfgOpt{
					withTargetAndMaxConsumerLagAtStartup(time.Second, 2*time.Second),
					withRegistry(reg),
					withLogger(log.NewLogfmtLogger(logs)),
				}, concurrencyVariant...)

				reader := createReader(t, clusterAddr, topicName, partitionID, consumer, readerOpts...)
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

						# HELP cortex_ingest_storage_reader_buffered_fetch_records_total Total number of records buffered within the client ready to be consumed
						# TYPE cortex_ingest_storage_reader_buffered_fetch_records_total gauge
						cortex_ingest_storage_reader_buffered_fetch_records_total{component="partition-reader"} 0
					`), "cortex_ingest_storage_reader_last_consumed_offset", "cortex_ingest_storage_reader_buffered_fetch_records_total")
				})
			})
		}
	})

	t.Run("should consume partition from start if last committed offset is missing and wait until target lag is honored and retry if a failure occurs when fetching last produced offset", func(t *testing.T) {
		t.Parallel()

		for concurrencyName, concurrencyVariant := range concurrencyVariants {
			concurrencyVariant := concurrencyVariant

			t.Run(concurrencyName, func(t *testing.T) {
				t.Parallel()

				var (
					cluster, clusterAddr     = testkafka.CreateCluster(t, partitionID+1, topicName)
					listOffsetsRequestsCount = atomic.NewInt64(0)
					listOffsetsShouldFail    = atomic.NewBool(true)
					consumedRecordsCount     = atomic.NewInt64(0)
				)

				consumer := consumerFunc(func(_ context.Context, records iter.Seq[*kgo.Record]) error {
					consumedRecordsCount.Add(int64(len(slices.Collect(records))))
					return nil
				})

				cluster.ControlKey(int16(kmsg.ListOffsets), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
					cluster.KeepControl()
					listOffsetsRequestsCount.Inc()

					if listOffsetsShouldFail.Load() {
						req := kreq.(*kmsg.ListOffsetsRequest)
						res := req.ResponseKind().(*kmsg.ListOffsetsResponse)
						res.Default()
						res.Topics = []kmsg.ListOffsetsResponseTopic{
							{Topic: topicName, Partitions: []kmsg.ListOffsetsResponseTopicPartition{{ErrorCode: kerr.NotLeaderForPartition.Code}}},
						}
						return res, nil, true
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
				readerOpts := append([]readerTestCfgOpt{
					withTargetAndMaxConsumerLagAtStartup(time.Second, 2*time.Second),
					withRegistry(reg),
					withLogger(log.NewLogfmtLogger(logs)),
				}, concurrencyVariant...)

				reader := createReader(t, clusterAddr, topicName, partitionID, consumer, readerOpts...)
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
				assert.Equal(t, services.Starting.String(), reader.State().String())
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

						# HELP cortex_ingest_storage_reader_buffered_fetch_records_total Total number of records buffered within the client ready to be consumed
						# TYPE cortex_ingest_storage_reader_buffered_fetch_records_total gauge
						cortex_ingest_storage_reader_buffered_fetch_records_total{component="partition-reader"} 0
					`), "cortex_ingest_storage_reader_last_consumed_offset", "cortex_ingest_storage_reader_buffered_fetch_records_total")
				})
			})
		}
	})

	t.Run("should consume partition from end if position=end, and skip honoring target max lag", func(t *testing.T) {
		t.Parallel()

		for _, fileEnforced := range []bool{false, true} {
			fileEnforced := fileEnforced
			t.Run(fmt.Sprintf("file_enforced_%v", fileEnforced), func(t *testing.T) {
				t.Parallel()

				for concurrencyName, concurrencyVariant := range concurrencyVariants {
					concurrencyVariant := concurrencyVariant

					t.Run(concurrencyName, func(t *testing.T) {
						t.Parallel()

						var (
							cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
							reg                  = prometheus.NewPedanticRegistry()
							fetchRequestsCount   = atomic.NewInt64(0)
							fetchShouldFail      = atomic.NewBool(true)
							consumedRecordsMx    sync.Mutex
							consumedRecords      []string
						)

						consumer := consumerFunc(func(_ context.Context, records iter.Seq[*kgo.Record]) error {
							consumedRecordsMx.Lock()
							defer consumedRecordsMx.Unlock()

							for r := range records {
								consumedRecords = append(consumedRecords, string(r.Value))
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
						readerOpts := append([]readerTestCfgOpt{
							withFileBasedOffsetEnforcement(fileEnforced),
							withConsumeFromPositionAtStartup(consumeFromEnd),
							withTargetAndMaxConsumerLagAtStartup(time.Second, time.Second),
							withRegistry(reg),
						}, concurrencyVariant...)

						reader := createReader(t, clusterAddr, topicName, partitionID, consumer, readerOpts...)
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

						# HELP cortex_ingest_storage_reader_buffered_fetch_records_total Total number of records buffered within the client ready to be consumed
						# TYPE cortex_ingest_storage_reader_buffered_fetch_records_total gauge
						cortex_ingest_storage_reader_buffered_fetch_records_total{component="partition-reader"} 0
					`), "cortex_ingest_storage_reader_last_consumed_offset", "cortex_ingest_storage_reader_buffered_fetch_records_total")
						})
					})
				}
			})
		}
	})

	t.Run("should consume partition from start if position=start, and wait until target lag is honored", func(t *testing.T) {
		t.Parallel()

		for _, fileEnforced := range []bool{false, true} {
			fileEnforced := fileEnforced
			t.Run(fmt.Sprintf("file_enforced_%v", fileEnforced), func(t *testing.T) {
				t.Parallel()

				for concurrencyName, concurrencyVariant := range concurrencyVariants {
					concurrencyVariant := concurrencyVariant

					t.Run(concurrencyName, func(t *testing.T) {
						t.Parallel()

						var (
							cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
							fetchRequestsCount   = atomic.NewInt64(0)
							fetchShouldFail      = atomic.NewBool(false)
							consumedRecordsMx    sync.Mutex
							consumedRecords      []string
						)

						consumer := consumerFunc(func(_ context.Context, records iter.Seq[*kgo.Record]) error {
							consumedRecordsMx.Lock()
							defer consumedRecordsMx.Unlock()

							for r := range records {
								consumedRecords = append(consumedRecords, string(r.Value))
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
								readerOpts := append([]readerTestCfgOpt{
									withFileBasedOffsetEnforcement(fileEnforced),
									withConsumeFromPositionAtStartup(consumeFromStart),
									withTargetAndMaxConsumerLagAtStartup(time.Second, 2*time.Second),
									withRegistry(reg),
									withLogger(log.NewLogfmtLogger(logs)),
								}, concurrencyVariant...)

								reader := createReader(t, clusterAddr, topicName, partitionID, consumer, readerOpts...)
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

								# HELP cortex_ingest_storage_reader_buffered_fetch_records_total Total number of records buffered within the client ready to be consumed
								# TYPE cortex_ingest_storage_reader_buffered_fetch_records_total gauge
								cortex_ingest_storage_reader_buffered_fetch_records_total{component="partition-reader"} 0
							`), "cortex_ingest_storage_reader_last_consumed_offset", "cortex_ingest_storage_reader_buffered_fetch_records_total")
								})
							})
						}
					})
				}
			})
		}
	})

	t.Run("should consume partition from start if position=start, and wait until target lag is honored, and then consume some records after lag is honored", func(t *testing.T) {
		t.Parallel()

		for _, fileEnforced := range []bool{false, true} {
			fileEnforced := fileEnforced
			t.Run(fmt.Sprintf("file_enforced_%v", fileEnforced), func(t *testing.T) {
				t.Parallel()

				for concurrencyName, concurrencyVariant := range concurrencyVariants {
					concurrencyVariant := concurrencyVariant

					t.Run(concurrencyName, func(t *testing.T) {
						t.Parallel()

						var (
							cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
							fetchRequestsCount   = atomic.NewInt64(0)
							fetchShouldFail      = atomic.NewBool(false)
							consumedRecordsMx    sync.Mutex
							consumedRecords      []string
						)

						consumer := consumerFunc(func(_ context.Context, records iter.Seq[*kgo.Record]) error {
							consumedRecordsMx.Lock()
							defer consumedRecordsMx.Unlock()

							for r := range records {
								consumedRecords = append(consumedRecords, string(r.Value))
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
						readerOpts := append([]readerTestCfgOpt{
							withFileBasedOffsetEnforcement(fileEnforced),
							withConsumeFromPositionAtStartup(consumeFromStart),
							withTargetAndMaxConsumerLagAtStartup(time.Second, 2*time.Second),
							withRegistry(reg),
							withLogger(log.NewLogfmtLogger(logs)),
						}, concurrencyVariant...)

						reader := createReader(t, clusterAddr, topicName, partitionID, consumer, readerOpts...)
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

						# HELP cortex_ingest_storage_reader_buffered_fetch_records_total Total number of records buffered within the client ready to be consumed
						# TYPE cortex_ingest_storage_reader_buffered_fetch_records_total gauge
						cortex_ingest_storage_reader_buffered_fetch_records_total{component="partition-reader"} 0
					`), "cortex_ingest_storage_reader_last_consumed_offset", "cortex_ingest_storage_reader_buffered_fetch_records_total")
						})
					})
				}
			})
		}
	})

	t.Run("should consume partition from the timestamp if position=timestamp, and wait until target lag is honored", func(t *testing.T) {
		t.Parallel()

		for concurrencyName, concurrencyVariant := range concurrencyVariants {
			concurrencyVariant := concurrencyVariant

			t.Run(concurrencyName, func(t *testing.T) {
				t.Parallel()

				var (
					cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
					fetchRequestsCount   = atomic.NewInt64(0)
					fetchShouldFail      = atomic.NewBool(false)
					consumedRecordsMx    sync.Mutex
					consumedRecords      []string
				)

				consumer := consumerFunc(func(_ context.Context, records iter.Seq[*kgo.Record]) error {
					consumedRecordsMx.Lock()
					defer consumedRecordsMx.Unlock()

					for r := range records {
						consumedRecords = append(consumedRecords, string(r.Value))
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
				readerOpts := append([]readerTestCfgOpt{
					withConsumeFromTimestampAtStartup(consumeFromTs.UnixMilli()),
					withTargetAndMaxConsumerLagAtStartup(time.Second, 2*time.Second),
					withRegistry(reg),
					withLogger(log.NewLogfmtLogger(logs)),
				}, concurrencyVariant...)

				reader := createReader(t, clusterAddr, topicName, partitionID, consumer, readerOpts...)
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

						# HELP cortex_ingest_storage_reader_buffered_fetch_records_total Total number of records buffered within the client ready to be consumed
						# TYPE cortex_ingest_storage_reader_buffered_fetch_records_total gauge
						cortex_ingest_storage_reader_buffered_fetch_records_total{component="partition-reader"} 0
					`, expectedConsumedOffset)), "cortex_ingest_storage_reader_last_consumed_offset", "cortex_ingest_storage_reader_buffered_fetch_records_total")
				})
			})
		}
	})

	t.Run("should consume partition from last committed offset if position=last-offset, and wait until target lag is honored", func(t *testing.T) {
		t.Parallel()

		for concurrencyName, concurrencyVariant := range concurrencyVariants {
			concurrencyVariant := concurrencyVariant

			t.Run(concurrencyName, func(t *testing.T) {
				t.Parallel()

				var (
					cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
					fetchRequestsCount   = atomic.NewInt64(0)
					fetchShouldFail      = atomic.NewBool(false)
					consumedRecordsMx    sync.Mutex
					consumedRecords      []string
				)

				consumer := consumerFunc(func(_ context.Context, records iter.Seq[*kgo.Record]) error {
					consumedRecordsMx.Lock()
					defer consumedRecordsMx.Unlock()

					for r := range records {
						consumedRecords = append(consumedRecords, string(r.Value))
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
						readerOpts := append([]readerTestCfgOpt{
							withConsumeFromPositionAtStartup(consumeFromLastOffset),
							withTargetAndMaxConsumerLagAtStartup(time.Second, 2*time.Second),
							withRegistry(reg),
							withLogger(log.NewLogfmtLogger(logs)),
						}, concurrencyVariant...)

						reader := createReader(t, clusterAddr, topicName, partitionID, consumer, readerOpts...)
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

								# HELP cortex_ingest_storage_reader_buffered_fetch_records_total Total number of records buffered within the client ready to be consumed
								# TYPE cortex_ingest_storage_reader_buffered_fetch_records_total gauge
								cortex_ingest_storage_reader_buffered_fetch_records_total{component="partition-reader"} 0
							`, expectedConsumedOffset)), "cortex_ingest_storage_reader_last_consumed_offset", "cortex_ingest_storage_reader_buffered_fetch_records_total")
						})
					})
				}
			})
		}
	})

	t.Run("should consume partition from last committed offset if position=last-offset, and wait until max lag is honored if can't honor target lag", func(t *testing.T) {
		t.Parallel()

		for concurrencyName, concurrencyVariant := range concurrencyVariants {
			concurrencyVariant := concurrencyVariant

			t.Run(concurrencyName, func(t *testing.T) {
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

				consumer := consumerFunc(func(context.Context, iter.Seq[*kgo.Record]) error { return nil })

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
				readerOpts := append([]readerTestCfgOpt{
					withConsumeFromPositionAtStartup(consumeFromLastOffset),
					withTargetAndMaxConsumerLagAtStartup(targetLag, maxLag),
					withRegistry(reg),
					withLogger(log.NewLogfmtLogger(logs)),
				}, concurrencyVariant...)

				reader := createReader(t, clusterAddr, topicName, partitionID, consumer, readerOpts...)
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
		}
	})

	t.Run("should not wait indefinitely if context is cancelled while fetching last produced offset", func(t *testing.T) {
		t.Parallel()

		for concurrencyName, concurrencyVariant := range concurrencyVariants {
			concurrencyVariant := concurrencyVariant

			t.Run(concurrencyName, func(t *testing.T) {
				t.Parallel()

				var (
					cluster, clusterAddr     = testkafka.CreateCluster(t, partitionID+1, topicName)
					consumer                 = consumerFunc(func(context.Context, iter.Seq[*kgo.Record]) error { return nil })
					listOffsetsRequestsCount = atomic.NewInt64(0)
					contextCancelled         = atomic.NewBool(false)
				)

				// Mock Kafka to always fail the ListOffsets request.
				cluster.ControlKey(int16(kmsg.ListOffsets), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
					cluster.KeepControl()

					listOffsetsRequestsCount.Inc()

					// If context has been cancelled, we want to make sure we return an error
					// that will allow the client to detect the cancellation faster.
					if contextCancelled.Load() {
						return nil, context.Canceled, true
					}

					// Return a proper Kafka error response instead of a raw error
					// to ensure franz-go will retry this error
					req := kreq.(*kmsg.ListOffsetsRequest)
					res := req.ResponseKind().(*kmsg.ListOffsetsResponse)
					res.Default()
					res.Topics = []kmsg.ListOffsetsResponseTopic{
						{Topic: topicName, Partitions: []kmsg.ListOffsetsResponseTopicPartition{{ErrorCode: kerr.NotLeaderForPartition.Code}}},
					}
					return res, nil, true
				})

				// Create and start the reader.
				readerOpts := append([]readerTestCfgOpt{
					withTargetAndMaxConsumerLagAtStartup(time.Second, time.Second),
				}, concurrencyVariant...)
				reader := createReader(t, clusterAddr, topicName, partitionID, consumer, readerOpts...)

				readerCtx, cancelReaderCtx := context.WithCancel(ctx)
				require.NoError(t, reader.StartAsync(readerCtx))
				t.Cleanup(func() {
					// Interrupting startup should fail the service.
					// A context cancellation error shouldn't be swallowed and interpreted as "startup went ok"
					assert.ErrorIs(t, services.StopAndAwaitTerminated(ctx, reader), context.Canceled)
				})

				// Wait until the Kafka cluster received at least 2 ListOffsets requests.
				// This ensures the reader is actively trying to fetch offsets and retrying.
				test.Poll(t, 5*time.Second, true, func() interface{} {
					return listOffsetsRequestsCount.Load() > 1
				})

				// Cancelling the context should cause the service to switch to a terminal state.
				assert.Equal(t, services.Starting, reader.State())

				// Mark that context is being cancelled so subsequent requests fail faster
				contextCancelled.Store(true)
				cancelReaderCtx()

				// franz-go has internal retries that can last up to 10s
				test.Poll(t, 15*time.Second, services.Failed, func() interface{} {
					return reader.State()
				})
			})
		}
	})

	t.Run("should not wait indefinitely if context is cancelled while fetching records", func(t *testing.T) {
		t.Parallel()

		for concurrencyName, concurrencyVariant := range concurrencyVariants {
			concurrencyVariant := concurrencyVariant

			t.Run(concurrencyName, func(t *testing.T) {
				t.Parallel()

				var (
					cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
					consumer             = consumerFunc(func(context.Context, iter.Seq[*kgo.Record]) error { return nil })
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
				readerOpts := append([]readerTestCfgOpt{
					withTargetAndMaxConsumerLagAtStartup(time.Second, time.Second),
				}, concurrencyVariant...)
				reader := createReader(t, clusterAddr, topicName, partitionID, consumer, readerOpts...)

				readerCtx, cancelReaderCtx := context.WithCancel(ctx)
				require.NoError(t, reader.StartAsync(readerCtx))
				t.Cleanup(func() {
					// Interrupting startup should fail the service.
					// A context cancellation error shouldn't be swallowed and interpreted as "startup went ok"
					assert.ErrorIs(t, services.StopAndAwaitTerminated(ctx, reader), context.Canceled)
				})

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
		}
	})

	t.Run("should not wait indefinitely if there are no records to consume from Kafka but partition start offset is > 0 (e.g. all previous records have been deleted by Kafka retention)", func(t *testing.T) {
		t.Parallel()

		for _, consumeFromPosition := range consumeFromPositionOptions {
			consumeFromPosition := consumeFromPosition

			t.Run(fmt.Sprintf("consume from position: %s", consumeFromPosition), func(t *testing.T) {
				t.Parallel()

				for _, fileEnforced := range []bool{false, true} {
					fileEnforced := fileEnforced
					t.Run(fmt.Sprintf("file_enforced_%v", fileEnforced), func(t *testing.T) {
						t.Parallel()

						for concurrencyName, concurrencyVariant := range concurrencyVariants {
							concurrencyVariant := concurrencyVariant

							t.Run(concurrencyName, func(t *testing.T) {
								t.Parallel()

								ctx, cancel := context.WithCancel(context.Background())
								t.Cleanup(cancel)

								consumer := consumerFunc(func(context.Context, iter.Seq[*kgo.Record]) error { return nil })

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
								readerOpts := append([]readerTestCfgOpt{
									withFileBasedOffsetEnforcement(fileEnforced),
									withConsumeFromPositionAtStartup(consumeFromPosition),
									withConsumeFromTimestampAtStartup(time.Now().UnixMilli()), // For the test where position=timestamp.
									withTargetAndMaxConsumerLagAtStartup(time.Second, time.Second),
									withRegistry(reg),
								}, concurrencyVariant...)

								reader := createReader(t, clusterAddr, topicName, partitionID, consumer, readerOpts...)

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

									# HELP cortex_ingest_storage_reader_buffered_fetch_records_total Total number of records buffered within the client ready to be consumed
									# TYPE cortex_ingest_storage_reader_buffered_fetch_records_total gauge
									cortex_ingest_storage_reader_buffered_fetch_records_total{component="partition-reader"} 0
								`), "cortex_ingest_storage_reader_last_consumed_offset", "cortex_ingest_storage_reader_last_committed_offset", "cortex_ingest_storage_reader_buffered_fetch_records_total"))
							})
						}
					})
				}
			})
		}
	})

	t.Run("should read target lag and then consume more records after switching to 0 ongoing concurrency if position=start, startup_fetch_concurrency=2, ongoing_fetch_concurrency=0", func(t *testing.T) {
		t.Parallel()

		for _, fileEnforced := range []bool{false, true} {
			fileEnforced := fileEnforced
			t.Run(fmt.Sprintf("file_enforced_%v", fileEnforced), func(t *testing.T) {
				t.Parallel()

				var (
					cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
					fetchRequestsCount   = atomic.NewInt64(0)
					fetchShouldFail      = atomic.NewBool(true)
					consumedRecordsMx    sync.Mutex
					consumedRecords      []string
				)

				consumer := consumerFunc(func(_ context.Context, records iter.Seq[*kgo.Record]) error {
					consumedRecordsMx.Lock()
					defer consumedRecordsMx.Unlock()

					for r := range records {
						consumedRecords = append(consumedRecords, string(r.Value))
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

				// Create and start the reader.
				reg := prometheus.NewPedanticRegistry()
				logs := &concurrency.SyncBuffer{}
				reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
					withFileBasedOffsetEnforcement(fileEnforced),
					withConsumeFromPositionAtStartup(consumeFromStart),
					withTargetAndMaxConsumerLagAtStartup(time.Second, 2*time.Second),
					withRegistry(reg),
					withLogger(log.NewLogfmtLogger(logs)),
					withFetchConcurrency(2))

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

				// We expect the last consumed offset to be tracked in a metric, and there are no buffered records reported.
				test.Poll(t, time.Second, nil, func() interface{} {
					return promtest.GatherAndCompare(reg, strings.NewReader(`
				# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
				# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
				cortex_ingest_storage_reader_last_consumed_offset{partition="1"} 1

				# HELP cortex_ingest_storage_reader_buffered_fetch_records_total Total number of records buffered within the client ready to be consumed
				# TYPE cortex_ingest_storage_reader_buffered_fetch_records_total gauge
				cortex_ingest_storage_reader_buffered_fetch_records_total{component="partition-reader"} 0
			`), "cortex_ingest_storage_reader_last_consumed_offset", "cortex_ingest_storage_reader_buffered_fetch_records_total")
				})

				produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-3"))
				produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-4"))
				t.Log("produced 2 records")

				// We expect the reader to consume subsequent records too.
				test.Poll(t, time.Second, []string{"record-1", "record-2", "record-3", "record-4"}, func() interface{} {
					consumedRecordsMx.Lock()
					defer consumedRecordsMx.Unlock()
					return slices.Clone(consumedRecords)
				})

				// We expect the last consumed offset to be tracked in a metric, and there are no buffered records reported.
				test.Poll(t, time.Second, nil, func() interface{} {
					return promtest.GatherAndCompare(reg, strings.NewReader(`
				# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
				# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
				cortex_ingest_storage_reader_last_consumed_offset{partition="1"} 3

				# HELP cortex_ingest_storage_reader_buffered_fetch_records_total Total number of records buffered within the client ready to be consumed
				# TYPE cortex_ingest_storage_reader_buffered_fetch_records_total gauge
				cortex_ingest_storage_reader_buffered_fetch_records_total{component="partition-reader"} 0
			`), "cortex_ingest_storage_reader_last_consumed_offset", "cortex_ingest_storage_reader_buffered_fetch_records_total")
				})
			})
		}
	})
}

func TestPartitionReader_ShouldNotBufferRecordsInTheKafkaClientWhenDone(t *testing.T) {
	const (
		topicName        = "test"
		partitionID      = 1
		maxBufferedBytes = 2_000_000
	)

	tc := map[string]struct {
		concurrencyVariant                []readerTestCfgOpt
		expectedBufferedRecords           int
		expectedBufferedBytes             int
		expectedBufferedRecordsFromClient int
	}{
		"without concurrency": {
			concurrencyVariant:                []readerTestCfgOpt{withFetchConcurrency(0)},
			expectedBufferedRecords:           1,
			expectedBufferedBytes:             19,
			expectedBufferedRecordsFromClient: 1,
		},
		"with fetch concurrency": {
			concurrencyVariant:      []readerTestCfgOpt{withFetchConcurrency(2)},
			expectedBufferedRecords: 1,
			// only one fetcher is active because we don't fetch over the HWM. That fetcher should fetch 1MB.
			// The 5% padding is overridden because 1MB+5% is beyond the limit for the individual fetcher.
			expectedBufferedBytes:             1_000_000,
			expectedBufferedRecordsFromClient: 0,
		},
		"with higher fetch concurrency": {
			concurrencyVariant:      []readerTestCfgOpt{withFetchConcurrency(4)},
			expectedBufferedRecords: 1,
			// There is one fetcher fetching. That fetcher should fetch 500KB.
			expectedBufferedBytes:             500_000,
			expectedBufferedRecordsFromClient: 0,
		},
	}

	for _, fileEnforced := range []bool{false, true} {
		fileEnforced := fileEnforced
		t.Run(fmt.Sprintf("file_enforced_%v", fileEnforced), func(t *testing.T) {
			t.Parallel()

			for concurrencyName, tt := range tc {
				concurrencyVariant := tt.concurrencyVariant

				t.Run(concurrencyName, func(t *testing.T) {
					t.Parallel()

					var (
						ctx               = context.Background()
						_, clusterAddr    = testkafka.CreateCluster(t, partitionID+1, topicName)
						consumedRecordsMx sync.Mutex
						consumedRecords   []string
						blocked           = atomic.NewBool(false)
					)

					consumer := consumerFunc(func(_ context.Context, records iter.Seq[*kgo.Record]) error {
						if blocked.Load() {
							blockedTicker := time.NewTicker(100 * time.Millisecond)
							defer blockedTicker.Stop()

							timeoutTimer := time.NewTimer(3 * time.Second)
							defer timeoutTimer.Stop()

						outer:
							for {
								select {
								case <-blockedTicker.C:
									if !blocked.Load() {
										break outer
									}
								case <-timeoutTimer.C:
									// This is basically a test failure as we never finish the test in time.
									t.Log("failed to finish unblocking the consumer in time")
									return nil
								}
							}
						}

						consumedRecordsMx.Lock()
						defer consumedRecordsMx.Unlock()
						for r := range records {
							consumedRecords = append(consumedRecords, string(r.Value))
						}
						return nil
					})

					// Produce some records.
					writeClient := newKafkaProduceClient(t, clusterAddr)
					produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-1"))
					produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-2"))
					t.Log("produced 2 records")

					// Create and start the reader.
					reg := prometheus.NewPedanticRegistry()
					logs := &concurrency.SyncBuffer{}

					readerOpts := append([]readerTestCfgOpt{
						withFileBasedOffsetEnforcement(fileEnforced),
						withConsumeFromPositionAtStartup(consumeFromStart),
						withTargetAndMaxConsumerLagAtStartup(time.Second, 2*time.Second),
						withRegistry(reg),
						withLogger(log.NewLogfmtLogger(logs)),
						withMaxBufferedBytes(maxBufferedBytes),
					}, concurrencyVariant...)

					reader := createReader(t, clusterAddr, topicName, partitionID, consumer, readerOpts...)
					require.NoError(t, reader.StartAsync(ctx))
					t.Cleanup(func() {
						require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
					})

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

					// Wait some time to give some time for the Kafka client to eventually read and buffer records.
					// We don't expect it, but to make sure it's not happening we have to give it some time.
					time.Sleep(time.Second)

					// We expect the last consumed offset to be tracked in a metric, and there are no buffered records reported.
					test.Poll(t, time.Second, nil, func() interface{} {
						return promtest.GatherAndCompare(reg, strings.NewReader(`
				# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
				# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
				cortex_ingest_storage_reader_last_consumed_offset{partition="1"} 1

				# HELP cortex_ingest_storage_reader_buffered_fetch_records_total Total number of records buffered within the client ready to be consumed
				# TYPE cortex_ingest_storage_reader_buffered_fetch_records_total gauge
				cortex_ingest_storage_reader_buffered_fetch_records_total{component="partition-reader"} 0

        		# HELP cortex_ingest_storage_reader_buffered_fetched_records The number of records fetched from Kafka by both concurrent fetchers and the Kafka client but not yet processed.
        		# TYPE cortex_ingest_storage_reader_buffered_fetched_records gauge
        		cortex_ingest_storage_reader_buffered_fetched_records 0
			`), "cortex_ingest_storage_reader_last_consumed_offset", "cortex_ingest_storage_reader_buffered_fetch_records_total", "cortex_ingest_storage_reader_buffered_fetched_records")
					})

					// Now, we want to assert that when the reader does have records buffered the metrics correctly reflect the current state.
					// First, make the consumer block on the next consumption.
					blocked.Store(true)

					// Now, produce more records after the reader has started.
					produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-3"))
					produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-4"))
					t.Log("produced 2 records")

					// Now, we expect to have some records buffered.
					test.Poll(t, time.Second, nil, func() interface{} {
						return promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
				# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
				# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
				cortex_ingest_storage_reader_last_consumed_offset{partition="1"} 1

				# HELP cortex_ingest_storage_reader_buffered_fetch_records_total Total number of records buffered within the client ready to be consumed
				# TYPE cortex_ingest_storage_reader_buffered_fetch_records_total gauge
				cortex_ingest_storage_reader_buffered_fetch_records_total{component="partition-reader"} %d

        		# HELP cortex_ingest_storage_reader_buffered_fetched_records The number of records fetched from Kafka by both concurrent fetchers and the Kafka client but not yet processed.
        		# TYPE cortex_ingest_storage_reader_buffered_fetched_records gauge
        		cortex_ingest_storage_reader_buffered_fetched_records %d

        		# HELP cortex_ingest_storage_reader_buffered_fetched_bytes The number of bytes fetched or requested from Kafka by both concurrent fetchers and the Kafka client but not yet processed. The value depends on -ingest-storage.kafka.use-compressed-bytes-as-fetch-max-bytes.
        		# TYPE cortex_ingest_storage_reader_buffered_fetched_bytes gauge
        		cortex_ingest_storage_reader_buffered_fetched_bytes %d
			`, tt.expectedBufferedRecordsFromClient, tt.expectedBufferedRecords, tt.expectedBufferedBytes)), "cortex_ingest_storage_reader_last_consumed_offset", "cortex_ingest_storage_reader_buffered_fetch_records_total", "cortex_ingest_storage_reader_buffered_fetched_records", "cortex_ingest_storage_reader_buffered_fetched_bytes")
					})

					// With that assertion done, we can unblock records consumption.
					blocked.Store(false)

					// We expect the reader to consume subsequent records too.
					test.Poll(t, time.Second, []string{"record-1", "record-2", "record-3", "record-4"}, func() interface{} {
						consumedRecordsMx.Lock()
						defer consumedRecordsMx.Unlock()
						return slices.Clone(consumedRecords)
					})

					// Wait some time to give some time for the Kafka client to eventually read and buffer records.
					// We don't expect it, but to make sure it's not happening we have to give it some time.
					time.Sleep(time.Second)

					// We expect the last consumed offset to be tracked in a metric, and there are no buffered records reported.
					test.Poll(t, time.Second, nil, func() interface{} {
						return promtest.GatherAndCompare(reg, strings.NewReader(`
				# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
				# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
				cortex_ingest_storage_reader_last_consumed_offset{partition="1"} 3

				# HELP cortex_ingest_storage_reader_buffered_fetch_records_total Total number of records buffered within the client ready to be consumed
				# TYPE cortex_ingest_storage_reader_buffered_fetch_records_total gauge
				cortex_ingest_storage_reader_buffered_fetch_records_total{component="partition-reader"} 0

        		# HELP cortex_ingest_storage_reader_buffered_fetched_records The number of records fetched from Kafka by both concurrent fetchers and the Kafka client but not yet processed.
        		# TYPE cortex_ingest_storage_reader_buffered_fetched_records gauge
        		cortex_ingest_storage_reader_buffered_fetched_records 0
			`), "cortex_ingest_storage_reader_last_consumed_offset", "cortex_ingest_storage_reader_buffered_fetch_records_total", "cortex_ingest_storage_reader_buffered_fetched_records")
					})
				})
			}
		})
	}
}

func TestPartitionReader_ShouldNotPanicIfBufferedRecordsIsCalledBeforeStarting(t *testing.T) {
	const (
		topicName   = "test"
		partitionID = 1
	)

	_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
	reader := createReader(t, clusterAddr, topicName, partitionID, nil)

	require.Zero(t, reader.BufferedRecords())
}

// This test is critical because it reproduces a bug that caused data loss. This test has been designed to
// *not* mock PartitionReader or concurrentFetchers, and just mock responses from Kafka to reproduce a scenario
// where *both* conditions are met:
// 1. Fetch request failures
// 2. Fetch responses containing less records than requested
func TestPartitionReader_ShouldNotMissRecordsIfFetchRequestContainPartialFailuresWithConcurrentFetcherIsUsed(t *testing.T) {
	t.Parallel()

	const (
		topicName   = "test-topic"
		partitionID = 1
		concurrency = 5
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	cluster, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
	client := newKafkaProduceClient(t, clusterAddr)

	// We generate records that match the initialBytesPerRecord expectation, so that
	// we get concurrent Fetch requests since the beginning (a part from the very first
	// Fetch request which is sequential because we don't have the HWM yet).
	const recordSizeBytes = initialBytesPerRecord
	const minFetchBytes = forcedMinValueForMaxBytes
	const recordsPerFetch = minFetchBytes / recordSizeBytes
	const maxBufferedBytes = concurrency * recordsPerFetch * recordSizeBytes

	// We expect that fetchers will fetch more than 50 records per Fetch request.
	require.Greater(t, recordsPerFetch, 50)

	// Produce enough records so that we'll fetch concurrently.
	const totalProducedRecords = concurrency * recordsPerFetch
	for i := 0; i < totalProducedRecords; i++ {
		produceRandomRecord(ctx, t, client, topicName, partitionID, recordSizeBytes, fmt.Sprintf("record-%05d", i))
	}

	t.Logf("Produced %d records", totalProducedRecords)

	// Fetch the raw record batches for each offset, so that it's easier to later mock the Kafka
	// server and control the returned batches.
	fetchResponseByRequestedOffset := fetchSmallestRecordsBatchForEachOffset(t, client, topicName, partitionID, 0, totalProducedRecords)
	t.Logf("Collected raw Fetch responses for all expected offsets")

	// Mock the Kafka server to intercept Fetch requests, return less records than requested and
	// inject random failures.
	cluster.ControlKey(kmsg.Fetch.Int16(), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()

		req := kreq.(*kmsg.FetchRequest)

		// We expect only 1 partition.
		if len(req.Topics) != 1 {
			return nil, fmt.Errorf("expected 1 topic, got %d", len(req.Topics)), true
		}
		if len(req.Topics[0].Partitions) != 1 {
			return nil, fmt.Errorf("expected 1 partition, got %d", len(req.Topics[0].Partitions)), true
		}

		// Simulate a 10% Kafka error rate.
		if rand.Int()%10 == 0 {
			return &kmsg.FetchResponse{
				Version:   req.Version,
				ErrorCode: kerr.UnknownServerError.Code,
				Topics: []kmsg.FetchResponseTopic{{
					Topic:   req.Topics[0].Topic,
					TopicID: req.Topics[0].TopicID,
					Partitions: []kmsg.FetchResponseTopicPartition{{
						Partition: req.Topics[0].Partitions[0].Partition,
						ErrorCode: kerr.UnknownServerError.Code,
					}},
				}},
			}, nil, true
		}

		// Lookup the response among the ones we previously fetched with a small "MaxBytes".
		res := fetchResponseByRequestedOffset[req.Topics[0].Partitions[0].FetchOffset]
		if res != nil {
			return res, nil, true
		}

		if req.Topics[0].Partitions[0].FetchOffset < totalProducedRecords {
			return nil, errors.New("the offset requested has not been found among the ones we previously fetched"), true
		}

		return nil, nil, false
	})

	// Consume all the records using the PartitionReader.
	var (
		totalConsumedRecords = atomic.NewInt64(0)
		consumedRecordIDs    = sync.Map{}
	)

	consumer := consumerFunc(func(_ context.Context, records iter.Seq[*kgo.Record]) error {
		for rec := range records {
			totalConsumedRecords.Inc()

			// Parse the record ID from the actual record data.
			recordID, err := strconv.ParseInt(string(rec.Value[7:12]), 10, 64)
			require.NoError(t, err)
			consumedRecordIDs.Store(recordID, struct{}{})
		}

		return nil
	})

	// Create and start the reader.
	readerOpts := []readerTestCfgOpt{
		withConsumeFromPositionAtStartup(consumeFromStart),
		withTargetAndMaxConsumerLagAtStartup(time.Second, 2*time.Second),
		withLogger(log.NewNopLogger()),
		withMaxBufferedBytes(maxBufferedBytes),
		withFetchConcurrency(concurrency),
	}

	reader := createReader(t, clusterAddr, topicName, partitionID, consumer, readerOpts...)
	require.NoError(t, reader.StartAsync(ctx))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
	})

	// Wait until all produced have been consumed.
	test.Poll(t, 60*time.Second, int64(totalProducedRecords), func() interface{} {
		return totalConsumedRecords.Load()
	})

	// Ensure that the actual records content match the expected one.
	for i := int64(0); i < totalProducedRecords; i++ {
		_, found := consumedRecordIDs.Load(i)
		require.Truef(t, found, "Expected to find a consumed record with ID %d", i)
	}
}

// This test reproduces a scenario that we don't think should happen but, if it happens, we want to make sure
// that we don't lose records. The scenario is when Kafka returns a Fetch response for a *single* topic-partition
// containing *both* the error code set and some records.
func TestPartitionReader_ShouldNotMissRecordsIfKafkaReturnsAFetchBothWithAnErrorAndSomeRecords(t *testing.T) {
	t.Parallel()

	const (
		topicName            = "test"
		partitionID          = 1
		totalProducedRecords = 10000
		recordSizeBytes      = initialBytesPerRecord
		maxBufferedBytes     = (totalProducedRecords * initialBytesPerRecord) / 100
	)

	// We want to run all these tests with different concurrency config.
	concurrencyVariants := map[string][]readerTestCfgOpt{
		"without concurrency":    {withFetchConcurrency(0)},
		"with fetch concurrency": {withFetchConcurrency(2)},
	}

	for _, fileEnforced := range []bool{false, true} {
		fileEnforced := fileEnforced
		t.Run(fmt.Sprintf("file_enforced_%v", fileEnforced), func(t *testing.T) {
			t.Parallel()

			for concurrencyName, concurrencyVariant := range concurrencyVariants {
				concurrencyVariant := concurrencyVariant

				t.Run(concurrencyName, func(t *testing.T) {
					t.Parallel()

					var (
						cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topicName)
						reg                  = prometheus.NewPedanticRegistry()
					)

					ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
					t.Cleanup(cancel)

					// Produce records.
					writeClient := newKafkaProduceClient(t, clusterAddr)
					for i := 0; i < totalProducedRecords; i++ {
						produceRandomRecord(ctx, t, writeClient, topicName, partitionID, recordSizeBytes, fmt.Sprintf("record-%05d", i))
					}

					// Fetch the raw record batches for each offset, so that it's easier to later mock the Kafka
					// server and control the returned batches.
					fetchResponseByRequestedOffset := fetchSmallestRecordsBatchForEachOffset(t, writeClient, topicName, partitionID, 0, totalProducedRecords)
					t.Logf("Collected raw Fetch responses for all expected offsets")

					// Mock the Kafka server to intercept Fetch requests, return less records than requested and
					// randomly include error codes in some fetches.
					cluster.ControlKey(kmsg.Fetch.Int16(), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
						cluster.KeepControl()

						req := kreq.(*kmsg.FetchRequest)

						// We expect only 1 partition in the request.
						if len(req.Topics) != 1 {
							return nil, fmt.Errorf("expected 1 topic in the request, got %d", len(req.Topics)), true
						}
						if len(req.Topics[0].Partitions) != 1 {
							return nil, fmt.Errorf("expected 1 partition in the request, got %d", len(req.Topics[0].Partitions)), true
						}

						// Lookup the response among the ones we previously fetched with a small "MaxBytes".
						res := fetchResponseByRequestedOffset[req.Topics[0].Partitions[0].FetchOffset]
						if res == nil {
							if req.Topics[0].Partitions[0].FetchOffset < totalProducedRecords {
								return nil, errors.New("the offset requested has not been found among the ones we previously fetched"), true
							}

							// It was requested that we haven't been previously produced (could be a future offset), we just let kfake handle it.
							return nil, nil, false
						}

						// We expect only 1 partition in the response.
						if len(res.Topics) != 1 {
							return nil, fmt.Errorf("expected 1 topic in the response, got %d", len(res.Topics)), true
						}
						if len(res.Topics[0].Partitions) != 1 {
							return nil, fmt.Errorf("expected 1 partition in the response, got %d", len(res.Topics[0].Partitions)), true
						}

						// Simulate a 10% error rate in the Kafka responses, mixed with records.
						if rand.Int()%10 == 0 {
							// Make a copy so we don't overwrite the cached version, which will be later requested again.
							resCopy := &kmsg.FetchResponse{Version: req.Version}
							if err := resCopy.ReadFrom(res.AppendTo(nil)); err != nil {
								return nil, fmt.Errorf("failed to make a copy of FetchResponse: %v", err), true
							}

							resCopy.Topics[0].Partitions[0].ErrorCode = kerr.UnknownServerError.Code
							res = resCopy
						}

						return res, nil, true
					})

					// Consume all records.
					var (
						totalConsumedRecords = atomic.NewInt64(0)
						consumedRecordIDs    = sync.Map{}
					)

					consumer := consumerFunc(func(_ context.Context, records iter.Seq[*kgo.Record]) error {
						for rec := range records {
							totalConsumedRecords.Inc()

							// Parse the record ID from the actual record data.
							recordID, err := strconv.ParseInt(string(rec.Value[7:12]), 10, 64)
							require.NoError(t, err)
							consumedRecordIDs.Store(recordID, struct{}{})
						}

						return nil
					})

					readerOpts := append([]readerTestCfgOpt{
						withFileBasedOffsetEnforcement(fileEnforced),
						withConsumeFromPositionAtStartup(consumeFromStart),
						withTargetAndMaxConsumerLagAtStartup(time.Second, 2*time.Second),
						withMaxBufferedBytes(maxBufferedBytes),
						withRegistry(reg),
						withLogger(log.NewNopLogger()),
					}, concurrencyVariant...)

					reader := createReader(t, clusterAddr, topicName, partitionID, consumer, readerOpts...)
					require.NoError(t, reader.StartAsync(ctx))
					t.Cleanup(func() {
						require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
					})

					// Wait until all produced have been consumed.
					test.Poll(t, 60*time.Second, int64(totalProducedRecords), func() interface{} {
						return totalConsumedRecords.Load()
					})

					// Ensure that the actual records content match the expected one.
					for i := int64(0); i < totalProducedRecords; i++ {
						_, found := consumedRecordIDs.Load(i)
						require.Truef(t, found, "Expected to find a consumed record with ID %d", i)
					}

					// We expect the last consumed offset to be tracked in a metric.
					test.Poll(t, time.Second, nil, func() interface{} {
						return promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_ingest_storage_reader_last_consumed_offset The last offset successfully consumed by the partition reader. Set to -1 if not offset has been consumed yet.
					# TYPE cortex_ingest_storage_reader_last_consumed_offset gauge
					cortex_ingest_storage_reader_last_consumed_offset{partition="1"} %d

					# HELP cortex_ingest_storage_reader_buffered_fetch_records_total Total number of records buffered within the client ready to be consumed
					# TYPE cortex_ingest_storage_reader_buffered_fetch_records_total gauge
					cortex_ingest_storage_reader_buffered_fetch_records_total{component="partition-reader"} 0
				`, totalProducedRecords-1)), "cortex_ingest_storage_reader_last_consumed_offset", "cortex_ingest_storage_reader_buffered_fetch_records_total")
					})
				})
			}
		})
	}
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
			consumer             = consumerFunc(func(context.Context, iter.Seq[*kgo.Record]) error { return nil })
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
			consumer             = consumerFunc(func(context.Context, iter.Seq[*kgo.Record]) error { return nil })
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
			consumer             = consumerFunc(func(context.Context, iter.Seq[*kgo.Record]) error { return nil })
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

func TestPartitionReader_getStartOffset_RetentionPeriodFallback(t *testing.T) {
	const (
		topicName   = "test-topic"
		partitionID = int32(1)
	)
	ctx := context.Background()

	setupTest := func(t *testing.T) (string, *consumerFunc, func()) {
		cluster, clusterAddr := testkafka.CreateClusterWithoutCustomConsumerGroupsSupport(t, partitionID+1, topicName)
		c := consumerFunc(func(context.Context, iter.Seq[*kgo.Record]) error { return nil })
		return clusterAddr, &c, cluster.Close
	}

	t.Run("uses retention period to calculate timestamp-based offset when file is missing", func(t *testing.T) {
		clusterAddr, consumer, cleanup := setupTest(t)
		defer cleanup()

		reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
			withConsumeFromPositionAtStartup(consumeFromLastOffset),
			withFileBasedOffsetEnforcement(true),
			withReplayFromDurationWhenFileOffsetMissing(1*time.Hour),
			withTargetAndMaxConsumerLagAtStartup(0, 0),
		)

		startOffset, lastConsumedOffset, err := reader.getStartOffset(ctx)

		require.NoError(t, err)
		assert.NotEqual(t, kafkaOffsetStart, startOffset)
		assert.Equal(t, startOffset-1, lastConsumedOffset)
	})

	t.Run("errors when retention period is zero", func(t *testing.T) {
		clusterAddr, consumer, cleanup := setupTest(t)
		defer cleanup()

		reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
			withConsumeFromPositionAtStartup(consumeFromLastOffset),
			withFileBasedOffsetEnforcement(true),
			withReplayFromDurationWhenFileOffsetMissing(0),
			withTargetAndMaxConsumerLagAtStartup(0, 0),
		)

		startOffset, lastConsumedOffset, err := reader.getStartOffset(ctx)

		require.Error(t, err)
		assert.Equal(t, int64(0), startOffset)
		assert.Equal(t, int64(-1), lastConsumedOffset)
	})

	t.Run("file offset takes precedence over retention period", func(t *testing.T) {
		clusterAddr, consumer, cleanup := setupTest(t)
		defer cleanup()

		tmpDir := t.TempDir()
		offsetFilePath := filepath.Join(tmpDir, "offset")
		offsetFile := newOffsetFile(offsetFilePath, partitionID, log.NewNopLogger())
		require.NoError(t, offsetFile.Write(42))

		reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
			withConsumeFromPositionAtStartup(consumeFromLastOffset),
			withFileBasedOffsetEnforcement(true),
			withReplayFromDurationWhenFileOffsetMissing(1*time.Hour),
			withTargetAndMaxConsumerLagAtStartup(0, 0),
			withOffsetFilePath(offsetFilePath),
		)

		startOffset, lastConsumedOffset, err := reader.getStartOffset(ctx)

		require.NoError(t, err)
		assert.Equal(t, int64(43), startOffset)
		assert.Equal(t, int64(42), lastConsumedOffset)
	})

	t.Run("ignores stale file offset when file offset is behind partition start (retention or lag)", func(t *testing.T) {
		clusterAddr, consumer, cleanup := setupTest(t)
		defer cleanup()

		writeClient := newKafkaProduceClient(t, clusterAddr)
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-1"))
		produceRecord(ctx, t, writeClient, topicName, partitionID, []byte("record-2"))

		adminClient := kadm.NewClient(writeClient)
		endOffsets, err := adminClient.ListEndOffsets(ctx, topicName)
		require.NoError(t, err)
		endOffset, exists := endOffsets.Lookup(topicName, partitionID)
		require.True(t, exists)
		require.NoError(t, endOffset.Err)

		advancePartitionStartTo := kadm.Offsets{}
		advancePartitionStartTo.Add(kadm.Offset{Topic: topicName, Partition: partitionID, At: endOffset.Offset})
		_, err = adminClient.DeleteRecords(ctx, advancePartitionStartTo)
		require.NoError(t, err)
		// Partition start is now endOffset.Offset (2).

		tmpDir := t.TempDir()
		offsetFilePath := filepath.Join(tmpDir, "offset")
		offsetFile := newOffsetFile(offsetFilePath, partitionID, log.NewNopLogger())
		require.NoError(t, offsetFile.Write(0)) // Stale: offset 0 is behind partition start 2.

		reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
			withConsumeFromPositionAtStartup(consumeFromLastOffset),
			withFileBasedOffsetEnforcement(true),
			withReplayFromDurationWhenFileOffsetMissing(1*time.Hour),
			withTargetAndMaxConsumerLagAtStartup(0, 0),
			withOffsetFilePath(offsetFilePath),
		)

		startOffset, lastConsumedOffset, err := reader.getStartOffset(ctx)
		require.NoError(t, err)
		// Stale file offset must be ignored; start should be partition start or retention-based (>= 2), not file+1 (1).
		assert.GreaterOrEqual(t, startOffset, int64(2), "stale file offset should be ignored; start should be partition start or retention-based")
		assert.True(t, lastConsumedOffset == -1 || lastConsumedOffset == startOffset-1)
	})

	// When ConsumeFromPositionAtStartup != last-offset, behavior must be identical with and without ConsumerGroupOffsetCommitFileEnforced.
	t.Run("behaviour unchanged for position != last-offset with and without file enforcement", func(t *testing.T) {
		clusterAddr, consumer, cleanup := setupTest(t)
		defer cleanup()

		positions := []struct {
			name     string
			position string
			opts     []readerTestCfgOpt
		}{
			{"position=start", consumeFromStart, []readerTestCfgOpt{withConsumeFromPositionAtStartup(consumeFromStart)}},
			{"position=end", consumeFromEnd, []readerTestCfgOpt{withConsumeFromPositionAtStartup(consumeFromEnd)}},
			{"position=timestamp", consumeFromTimestamp, []readerTestCfgOpt{withConsumeFromTimestampAtStartup(time.Now().Add(-1 * time.Hour).UnixMilli())}},
		}

		for _, pos := range positions {
			pos := pos
			t.Run(pos.name, func(t *testing.T) {
				var resultWithout, resultWith struct {
					startOffset        int64
					lastConsumedOffset int64
					err                error
				}
				for _, fileEnforced := range []bool{false, true} {
					opts := append([]readerTestCfgOpt{withFileBasedOffsetEnforcement(fileEnforced), withTargetAndMaxConsumerLagAtStartup(0, 0)}, pos.opts...)
					reader := createReader(t, clusterAddr, topicName, partitionID, consumer, opts...)
					startOffset, lastConsumedOffset, err := reader.getStartOffset(ctx)
					if fileEnforced {
						resultWith.startOffset = startOffset
						resultWith.lastConsumedOffset = lastConsumedOffset
						resultWith.err = err
					} else {
						resultWithout.startOffset = startOffset
						resultWithout.lastConsumedOffset = lastConsumedOffset
						resultWithout.err = err
					}
				}
				require.NoError(t, resultWithout.err)
				require.NoError(t, resultWith.err)
				assert.Equal(t, resultWithout.startOffset, resultWith.startOffset, "startOffset should be identical with and without file enforcement when position != last-offset")
				assert.Equal(t, resultWithout.lastConsumedOffset, resultWith.lastConsumedOffset, "lastConsumedOffset should be identical with and without file enforcement when position != last-offset")
			})
		}
	})

	// Explicit getStartOffset tests for each ConsumeFromPositionAtStartup mode.
	t.Run("returns expected values for each ConsumeFromPositionAtStartup mode", func(t *testing.T) {
		t.Run("position=start returns partition start", func(t *testing.T) {
			clusterAddr, consumer, cleanup := setupTest(t)
			defer cleanup()

			reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
				withConsumeFromPositionAtStartup(consumeFromStart),
				withTargetAndMaxConsumerLagAtStartup(0, 0),
			)
			startOffset, lastConsumedOffset, err := reader.getStartOffset(ctx)
			require.NoError(t, err)
			assert.Equal(t, kafkaOffsetStart, startOffset)
			assert.Equal(t, int64(-1), lastConsumedOffset)
		})

		t.Run("position=end returns partition end", func(t *testing.T) {
			clusterAddr, consumer, cleanup := setupTest(t)
			defer cleanup()

			reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
				withConsumeFromPositionAtStartup(consumeFromEnd),
				withTargetAndMaxConsumerLagAtStartup(0, 0),
			)
			startOffset, lastConsumedOffset, err := reader.getStartOffset(ctx)
			require.NoError(t, err)
			assert.Equal(t, kafkaOffsetEnd, startOffset)
			assert.Equal(t, int64(-1), lastConsumedOffset)
		})

		t.Run("position=timestamp returns first offset after timestamp or partition start", func(t *testing.T) {
			clusterAddr, consumer, cleanup := setupTest(t)
			defer cleanup()

			// Timestamp in the past: cluster may have no data, so we may get partition start.
			ts := time.Now().Add(-1 * time.Hour).UnixMilli()
			reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
				withConsumeFromTimestampAtStartup(ts),
				withTargetAndMaxConsumerLagAtStartup(0, 0),
			)
			startOffset, lastConsumedOffset, err := reader.getStartOffset(ctx)
			require.NoError(t, err)
			// Either (offset from timestamp, offset-1) or (partition start, -1) when no offset after timestamp.
			assert.True(t, startOffset >= 0 || startOffset == kafkaOffsetStart, "startOffset should be non-negative or partition start")
			assert.True(t, lastConsumedOffset == -1 || lastConsumedOffset == startOffset-1, "lastConsumedOffset should be -1 or startOffset-1")
		})

		t.Run("position=last-offset with no file and no Kafka group returns partition start", func(t *testing.T) {
			clusterAddr, consumer, cleanup := setupTest(t)
			defer cleanup()

			// No offset file, no consumer group support => no committed offset => fall back to partition start.
			reader := createReader(t, clusterAddr, topicName, partitionID, consumer,
				withConsumeFromPositionAtStartup(consumeFromLastOffset),
				withFileBasedOffsetEnforcement(false),
				withTargetAndMaxConsumerLagAtStartup(0, 0),
			)
			startOffset, lastConsumedOffset, err := reader.getStartOffset(ctx)
			require.NoError(t, err)
			assert.Equal(t, kafkaOffsetStart, startOffset)
			assert.Equal(t, int64(-1), lastConsumedOffset)
		})
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

		logger := mimirtest.NewTestingLogger(t)
		cfg := createTestKafkaConfig(clusterAddr, topicName)
		client, err := kgo.NewClient(commonKafkaClientOptions(cfg, nil, logger)...)
		require.NoError(t, err)
		t.Cleanup(client.Close)

		adm := kadm.NewClient(client)
		reg := prometheus.NewPedanticRegistry()
		offsetFile := newOffsetFile(filepath.Join(t.TempDir(), "offset.json"), partitionID, log.NewNopLogger())

		committer := newPartitionCommitter(cfg, adm, partitionID, consumerGroup, &NoOpPreCommitNotifier{}, offsetFile, logger, reg)
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
		offsetFile := newOffsetFile(filepath.Join(t.TempDir(), "offset.json"), partitionID, log.NewNopLogger())
		committer := newPartitionCommitter(cfg, adm, partitionID, consumerGroup, &NoOpPreCommitNotifier{}, offsetFile, log.NewNopLogger(), reg)

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
		offsetFile := newOffsetFile(filepath.Join(t.TempDir(), "offset.json"), partitionID, log.NewNopLogger())
		committer := newPartitionCommitter(cfg, adm, partitionID, consumerGroup, &NoOpPreCommitNotifier{}, offsetFile, log.NewNopLogger(), reg)

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

	t.Run("should increment commitFailuresTotal when Kafka commit succeeds but file write fails", func(t *testing.T) {
		t.Parallel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)

		// Use a path that is a directory so offset file write fails (cannot create file at directory path).
		tmpDir := t.TempDir()
		offsetFilePath := filepath.Join(tmpDir, "offset")
		require.NoError(t, os.MkdirAll(offsetFilePath, 0755))
		offsetFile := newOffsetFile(offsetFilePath, partitionID, log.NewNopLogger())

		cfg := createTestKafkaConfig(clusterAddr, topicName)
		client, err := kgo.NewClient(commonKafkaClientOptions(cfg, nil, log.NewNopLogger())...)
		require.NoError(t, err)
		t.Cleanup(client.Close)

		adm := kadm.NewClient(client)
		reg := prometheus.NewPedanticRegistry()
		committer := newPartitionCommitter(cfg, adm, partitionID, consumerGroup, &NoOpPreCommitNotifier{}, offsetFile, log.NewNopLogger(), reg)

		err = committer.commit(context.Background(), 123)
		require.Error(t, err)
		assert.True(t, errors.Is(err, errOffsetFileWrite), "expected errOffsetFileWrite, got %v", err)

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

	t.Run("should call pre-commit notifier before committing", func(t *testing.T) {
		t.Parallel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)

		cfg := createTestKafkaConfig(clusterAddr, topicName)

		committed := atomic.NewBool(false)

		notifier := &testPreCommitNotifier{
			onNotify: func() {
				if committed.Load() {
					t.Error("Commit happened before notification")
				}
			},
		}

		mockAdmin := &mockAdminClient{
			onCommit: func() {
				committed.Store(true)
			},
		}

		offsetFile := newOffsetFile(filepath.Join(t.TempDir(), "offset.json"), partitionID, log.NewNopLogger())
		committer := newPartitionCommitter(cfg, mockAdmin, partitionID, consumerGroup, notifier, offsetFile, log.NewNopLogger(), prometheus.NewPedanticRegistry())

		require.NoError(t, committer.commit(context.Background(), 123))
	})

	t.Run("should proceed with commit even if notifier fails", func(t *testing.T) {
		t.Parallel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)

		cfg := createTestKafkaConfig(clusterAddr, topicName)

		committed := atomic.NewBool(false)

		notifier := &testPreCommitNotifier{
			err: errors.New("notification failed"),
			onNotify: func() {
				if committed.Load() {
					t.Error("Commit happened before notification")
				}
			},
		}

		mockAdmin := &mockAdminClient{
			onCommit: func() {
				committed.Store(true)
			},
		}

		offsetFile := newOffsetFile(filepath.Join(t.TempDir(), "offset.json"), partitionID, log.NewNopLogger())
		committer := newPartitionCommitter(cfg, mockAdmin, partitionID, consumerGroup, notifier, offsetFile, log.NewNopLogger(), prometheus.NewPedanticRegistry())

		require.NoError(t, committer.commit(context.Background(), 123))
	})
}

type writerTestCfgOpt func(cfg *KafkaConfig)

func withWriteTimeout(timeout time.Duration) writerTestCfgOpt {
	return func(cfg *KafkaConfig) {
		cfg.WriteTimeout = timeout
	}
}

func newKafkaProduceClient(t *testing.T, addrs string, opts ...writerTestCfgOpt) *kgo.Client {
	// Configure it close to the writer client we use in the real producers, but
	// do not configure the linger to keep tests running fast.
	cfg := KafkaConfig{}
	flagext.DefaultValues(&cfg)
	cfg.Address = flagext.StringSliceCSV{addrs}
	cfg.DisableLinger = true

	for _, opt := range opts {
		opt(&cfg)
	}

	writeClient, err := NewKafkaWriterClient(cfg, defaultMaxInflightProduceRequests, testingLogger.WithT(t), prometheus.NewPedanticRegistry())

	require.NoError(t, err)
	t.Cleanup(writeClient.Close)
	return writeClient
}

func createRecord(topicName string, partitionID int32, content []byte, version int) *kgo.Record {
	rec := &kgo.Record{
		Value:     content,
		Topic:     topicName,
		Partition: partitionID,
	}
	if version == 0 {
		rec.Headers = nil
	} else {
		rec.Headers = []kgo.RecordHeader{RecordVersionHeader(version)}
	}

	return rec
}

func produceRecord(ctx context.Context, t *testing.T, writeClient *kgo.Client, topicName string, partitionID int32, content []byte) int64 {
	return produceRecordWithVersion(ctx, t, writeClient, topicName, partitionID, content, 1)
}

func produceRecordWithVersion(ctx context.Context, t *testing.T, writeClient *kgo.Client, topicName string, partitionID int32, content []byte, version int) int64 {
	rec := createRecord(topicName, partitionID, content, version)
	produceResult := writeClient.ProduceSync(ctx, rec)
	require.NoError(t, produceResult.FirstErr())

	return rec.Offset
}

func produceRecordWithTimestamp(ctx context.Context, t *testing.T, writeClient *kgo.Client, topicName string, partitionID int32, content []byte, timestamp time.Time) int64 {
	rec := createRecord(topicName, partitionID, content, 1)
	rec.Timestamp = timestamp

	produceResult := writeClient.ProduceSync(ctx, rec)
	require.NoError(t, produceResult.FirstErr())

	return rec.Offset
}

// produceRandomRecord produces a record with random data, in order to reduce the compression ratio and
// get the compressed record byte size as close as possible to the uncompressed one.
func produceRandomRecord(ctx context.Context, t *testing.T, writeClient *kgo.Client, topicName string, partitionID int32, dataSize int, dataPrefix string) {
	randomData := make([]byte, dataSize-len(dataPrefix))
	_, err := crypto_rand.Read(randomData)
	require.NoError(t, err)

	recordValue := append([]byte(dataPrefix), randomData...)
	produceRecord(ctx, t, writeClient, topicName, partitionID, recordValue)
}

type readerTestCfg struct {
	kafka                                   KafkaConfig
	partitionID                             int32
	consumer                                consumerFactory
	registry                                *prometheus.Registry
	logger                                  log.Logger
	preCommitNotifier                       PreCommitNotifier
	replayFromDurationWhenFileOffsetMissing time.Duration
	offsetFilePath                          string
}

type readerTestCfgOpt func(cfg *readerTestCfg)

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

func withPreCommitNotifier(notifier PreCommitNotifier) func(cfg *readerTestCfg) {
	return func(cfg *readerTestCfg) {
		cfg.preCommitNotifier = notifier
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

func withFetchConcurrency(i int) readerTestCfgOpt {
	return func(cfg *readerTestCfg) {
		cfg.kafka.FetchConcurrencyMax = i
	}
}

func withMaxBufferedBytes(i int) readerTestCfgOpt {
	return func(cfg *readerTestCfg) {
		cfg.kafka.MaxBufferedBytes = i
	}
}

func withFileBasedOffsetEnforcement(enabled bool) readerTestCfgOpt {
	return func(cfg *readerTestCfg) {
		cfg.kafka.ConsumerGroupOffsetCommitFileEnforced = enabled
	}
}

func withReplayFromDurationWhenFileOffsetMissing(period time.Duration) readerTestCfgOpt {
	return func(cfg *readerTestCfg) {
		cfg.replayFromDurationWhenFileOffsetMissing = period
	}
}

func withOffsetFilePath(path string) readerTestCfgOpt {
	return func(cfg *readerTestCfg) {
		cfg.offsetFilePath = path
	}
}

var testingLogger = mimirtest.NewTestingLogger(nil)

func defaultReaderTestConfig(t *testing.T, addr string, topicName string, partitionID int32, consumer RecordConsumer) *readerTestCfg {
	return &readerTestCfg{
		registry:       prometheus.NewPedanticRegistry(),
		logger:         testingLogger.WithT(t),
		kafka:          createTestKafkaConfig(addr, topicName),
		partitionID:    partitionID,
		offsetFilePath: filepath.Join(t.TempDir(), "offset.json"),
		consumer: consumerFactoryFunc(func() RecordConsumer {
			return consumer
		}),
	}
}

func createReader(t *testing.T, addr string, topicName string, partitionID int32, consumer RecordConsumer, opts ...readerTestCfgOpt) *PartitionReader {
	cfg := defaultReaderTestConfig(t, addr, topicName, partitionID, consumer)
	for _, o := range opts {
		o(cfg)
	}

	t.Cleanup(func() {
		// Assuming none of the tests intentionally create gaps in offsets, there should be no missed records.
		assert.NoError(t, promtest.GatherAndCompare(cfg.registry, strings.NewReader(`
			# HELP cortex_ingest_storage_reader_missed_records_total The number of offsets that were never consumed by the reader because they weren't fetched.
			# TYPE cortex_ingest_storage_reader_missed_records_total counter
			cortex_ingest_storage_reader_missed_records_total 0
		`), "cortex_ingest_storage_reader_missed_records_total"))
	})

	// Ensure the config is valid.
	require.NoError(t, cfg.kafka.Validate())

	notifier := cfg.preCommitNotifier
	if notifier == nil {
		notifier = &NoOpPreCommitNotifier{}
	}

	cfg.kafka.MaxReplayPeriod = cfg.replayFromDurationWhenFileOffsetMissing
	reader, err := newPartitionReader(cfg.kafka, cfg.partitionID, "test-group", cfg.offsetFilePath, cfg.consumer, notifier, cfg.logger, cfg.registry)
	require.NoError(t, err)

	// Reduce the time the fake kafka would wait for new records. Sometimes this blocks startup.
	reader.concurrentFetchersMinBytesMaxWaitTime = 500 * time.Millisecond

	return reader
}

func createAndStartReader(ctx context.Context, t *testing.T, addr string, topicName string, partitionID int32, consumer RecordConsumer, opts ...readerTestCfgOpt) *PartitionReader {
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

	t.Run("pre-commit notifier is called", func(t *testing.T) {
		t.Parallel()
		const commitInterval = 100 * time.Millisecond
		ctx, cancel := context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(errors.New("test done")) })

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)

		baseConsumer := newTestConsumer(1)

		notifier := &testPreCommitNotifier{}

		createAndStartReader(ctx, t, clusterAddr, topicName, partitionID, baseConsumer,
			withCommitInterval(commitInterval),
			withPreCommitNotifier(notifier),
		)

		produceRecord(ctx, t, newKafkaProduceClient(t, clusterAddr), topicName, partitionID, []byte("1"))

		_, err := baseConsumer.waitRecords(1, time.Second, 2*commitInterval)
		require.NoError(t, err)

		assert.Greater(t, notifier.notifyCount.Load(), int32(0))
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
	records chan *kgo.Record
}

func newTestConsumer(capacity int) testConsumer {
	return testConsumer{
		records: make(chan *kgo.Record, capacity),
	}
}

func (t testConsumer) Consume(ctx context.Context, records iter.Seq[*kgo.Record]) error {
	for r := range records {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case t.records <- r:
			// Nothing to do.
		}
	}
	return nil
}

// waitRecords expects to receive numRecords records within waitTimeout.
// waitRecords waits for an additional drainPeriod after receiving numRecords records to ensure that no more records are received.
// waitRecords returns an error if a different number of records is received.
func (t testConsumer) waitRecords(numRecords int, waitTimeout, drainPeriod time.Duration) ([][]byte, error) {
	recs, err := t.waitRecordsAndMetadata(numRecords, waitTimeout, drainPeriod)
	var content [][]byte
	for _, rec := range recs {
		content = append(content, rec.Value)
	}
	return content, err
}

func (t testConsumer) waitRecordsAndMetadata(numRecords int, waitTimeout, drainPeriod time.Duration) ([]*kgo.Record, error) {
	var records []*kgo.Record
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

type consumerFunc func(ctx context.Context, records iter.Seq[*kgo.Record]) error

func (c consumerFunc) Consume(ctx context.Context, records iter.Seq[*kgo.Record]) error {
	return c(ctx, records)
}

func createTestContextWithTimeout(t *testing.T, timeout time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}

func fetchSmallestRecordsBatchForEachOffset(t *testing.T, client *kgo.Client, topicName string, partitionID int32, startOffset, endOffset int64) map[int64]*kmsg.FetchResponse {
	// Get topic ID.
	topics, err := kadm.NewClient(client).ListTopics(context.Background(), topicName)
	require.NoError(t, err)
	require.NoError(t, topics.Error())
	require.True(t, topics.Has(topicName))
	topicID := topics[topicName].ID

	t.Logf("Fetched topic ID")

	// Fetch the raw record batches for each offset
	fetchResponseByRequestedOffset := map[int64]*kmsg.FetchResponse{}

	for offset := startOffset; offset <= endOffset; offset++ {
		// Build a Fetch request.
		req := kmsg.NewFetchRequest()
		req.MinBytes = 1
		req.Version = 13
		req.MaxWaitMillis = 1000
		req.MaxBytes = 1 // Request the minimum amount of bytes.

		reqTopic := kmsg.NewFetchRequestTopic()
		reqTopic.Topic = topicName
		reqTopic.TopicID = topicID

		reqPartition := kmsg.NewFetchRequestTopicPartition()
		reqPartition.Partition = partitionID
		reqPartition.FetchOffset = offset
		reqPartition.PartitionMaxBytes = 1  // Request the minimum amount of bytes.
		reqPartition.CurrentLeaderEpoch = 0 // Not needed here.

		reqTopic.Partitions = append(reqTopic.Partitions, reqPartition)
		req.Topics = append(req.Topics, reqTopic)

		// Issue the Fetch request.
		kres, err := client.Request(context.Background(), &req)
		require.NoError(t, err)

		res := kres.(*kmsg.FetchResponse)
		require.Equal(t, int16(0), res.ErrorCode)
		require.Equal(t, 1, len(res.Topics))
		require.Equal(t, 1, len(res.Topics[0].Partitions))

		// Parse the response, just to check how many records we got.
		parseOptions := kgo.ProcessFetchPartitionOpts{
			KeepControlRecords: false,
			Offset:             offset,
			IsolationLevel:     kgo.ReadUncommitted(),
			Topic:              topicName,
			Partition:          partitionID,
		}

		rawPartitionResp := res.Topics[0].Partitions[0]
		partition, _ := kgo.ProcessFetchPartition(parseOptions, &rawPartitionResp, kgo.DefaultDecompressor(), func(_ kgo.FetchBatchMetrics) {})

		// Ensure we got a low number of records, otherwise the premise of this test is wrong
		// because we want a single fetchWatch to be fulfilled in many Fetch requests.
		require.LessOrEqual(t, len(partition.Records), 5)

		// Keep track of the raw response.
		fetchResponseByRequestedOffset[offset] = res
	}

	return fetchResponseByRequestedOffset
}

type testPreCommitNotifier struct {
	notifyCount atomic.Int32
	err         error
	onNotify    func()
}

func (t *testPreCommitNotifier) NotifyPreCommit(_ context.Context) error {
	t.notifyCount.Inc()
	if t.onNotify != nil {
		t.onNotify()
	}
	return t.err
}

type mockAdminClient struct {
	onCommit func()
}

func (m *mockAdminClient) CommitOffsets(ctx context.Context, group string, offsets kadm.Offsets) (kadm.OffsetResponses, error) {
	if m.onCommit != nil {
		m.onCommit()
	}
	return kadm.OffsetResponses{}, nil
}

func (m *mockAdminClient) Close() {}
