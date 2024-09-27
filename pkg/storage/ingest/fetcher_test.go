package ingest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestHandleKafkaFetchErr(t *testing.T) {
	logger := log.NewNopLogger()

	tests := map[string]struct {
		err error
		lso int64
		fw  fetchWant

		expectedFw              fetchWant
		expectedBackoff         bool
		expectedMetadataRefresh bool
	}{
		"no error": {
			err: nil,
			lso: 1,
			fw: fetchWant{
				startOffset: 1,
				endOffset:   5,
			},
			expectedFw: fetchWant{
				startOffset: 1,
				endOffset:   5,
			},
		},
		"offset out of range - fetching slightly before start": {
			err: kerr.OffsetOutOfRange,
			lso: 5,
			fw: fetchWant{
				startOffset: 4,
				endOffset:   10,
			},
			expectedFw: fetchWant{
				startOffset: 5,
				endOffset:   10,
			},
		},
		"offset out of range - fetching completely outside of available offsets": {
			err: kerr.OffsetOutOfRange,
			lso: 5,
			fw: fetchWant{
				startOffset: 1,
				endOffset:   3,
			},
			expectedFw: fetchWant{
				startOffset: 3,
				endOffset:   3,
			},
		},
		"recoverable error": {
			err: kerr.KafkaStorageError,
			lso: -1, // unknown
			fw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedFw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedBackoff: true,
		},
		"NotLeaderForPartition": {
			err: kerr.NotLeaderForPartition,
			lso: 5,
			fw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedFw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedBackoff:         true,
			expectedMetadataRefresh: true,
		},
		"ReplicaNotAvailable": {
			err: kerr.ReplicaNotAvailable,
			lso: 5,
			fw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedFw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedBackoff:         true,
			expectedMetadataRefresh: true,
		},
		"UnknownLeaderEpoch": {
			err: kerr.UnknownLeaderEpoch,
			lso: 5,
			fw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedFw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedBackoff:         true,
			expectedMetadataRefresh: true,
		},
		"FencedLeaderEpoch": {
			err: kerr.FencedLeaderEpoch,
			lso: 5,
			fw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedFw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedBackoff:         true,
			expectedMetadataRefresh: true,
		},
		"LeaderNotAvailable": {
			err: kerr.LeaderNotAvailable,
			lso: 5,
			fw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedFw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedBackoff:         true,
			expectedMetadataRefresh: true,
		},
		"errUnknownPartitionLeader": {
			err: errUnknownPartitionLeader,
			lso: 5,
			fw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedFw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedBackoff:         true,
			expectedMetadataRefresh: true,
		},
	}

	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			waitedBackoff := false
			backoff := waiterFunc(func() { waitedBackoff = true })
			refreshed := false
			refresher := refresherFunc(func() { refreshed = true })

			offsetR := newGenericOffsetReader(func(_ context.Context) (int64, error) {
				return testCase.lso, nil
			}, time.Millisecond, logger)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), offsetR))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), offsetR))
			})

			actualFw := handleKafkaFetchErr(testCase.err, testCase.fw, backoff, offsetR, refresher, logger)
			assert.Equal(t, testCase.expectedFw, actualFw)
			assert.Equal(t, testCase.expectedBackoff, waitedBackoff)
			assert.Equal(t, testCase.expectedMetadataRefresh, refreshed)
		})
	}
}

func TestConcurrentFetchers(t *testing.T) {
	const (
		topicName       = "test-topic"
		partitionID     = 1
		recordsPerFetch = 3
		concurrency     = 2
	)

	t.Run("respect context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, recordsPerFetch)

		// This should not block forever now
		fetches, fetchCtx := fetchers.PollFetches(ctx)

		assert.Zero(t, fetches.NumRecords())
		assert.Error(t, fetchCtx.Err(), "Expected context to be cancelled")
	})

	t.Run("cold replay", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		// Produce some records before starting the fetchers
		for i := 0; i < 5; i++ {
			produceRecord(ctx, t, client, topicName, partitionID, []byte(fmt.Sprintf("record-%d", i)))
		}

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, recordsPerFetch)

		fetches, _ := fetchers.PollFetches(ctx)
		assert.Equal(t, fetches.NumRecords(), 5)
	})

	t.Run("fetch records produced after startup", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, recordsPerFetch)

		// Produce some records after starting the fetchers
		for i := 0; i < 3; i++ {
			produceRecord(ctx, t, client, topicName, partitionID, []byte(fmt.Sprintf("record-%d", i)))
		}

		fetches, _ := fetchers.PollFetches(ctx)
		assert.Equal(t, fetches.NumRecords(), 3)
	})

	t.Run("slow processing of fetches", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, recordsPerFetch)

		// Produce some records
		for i := 0; i < 5; i++ {
			produceRecord(ctx, t, client, topicName, partitionID, []byte(fmt.Sprintf("record-%d", i)))
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumedRecords := 0
			for consumedRecords < 10 {
				fetches, _ := fetchers.PollFetches(ctx)
				time.Sleep(1000 * time.Millisecond) // Simulate slow processing
				consumedRecords += fetches.NumRecords()
			}
			assert.Equal(t, 10, consumedRecords)
		}()

		// Produce more records while processing is slow
		for i := 5; i < 10; i++ {
			produceRecord(ctx, t, client, topicName, partitionID, []byte(fmt.Sprintf("record-%d", i)))
		}

		wg.Wait()
	})

	t.Run("fast processing of fetches", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, recordsPerFetch)

		// Produce some records
		for i := 0; i < 10; i++ {
			produceRecord(ctx, t, client, topicName, partitionID, []byte(fmt.Sprintf("record-%d", i)))
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumedRecords := 0
			for consumedRecords < 10 {
				fetches, _ := fetchers.PollFetches(ctx)
				consumedRecords += fetches.NumRecords()
				// no processing delay
			}
			assert.Equal(t, 10, consumedRecords)
		}()

		wg.Wait()
	})

	t.Run("fetch with different concurrency levels", func(t *testing.T) {
		for _, concurrency := range []int{1, 2, 4} {
			t.Run(fmt.Sprintf("concurrency-%d", concurrency), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
				client := newKafkaProduceClient(t, clusterAddr)

				fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, 2)

				// Produce some records
				for i := 0; i < 20; i++ {
					produceRecord(ctx, t, client, topicName, partitionID, []byte(fmt.Sprintf("record-%d", i)))
				}

				var totalRecords int
				for totalRecords < 20 {
					fetches, _ := fetchers.PollFetches(ctx)
					totalRecords += fetches.NumRecords()
				}

				assert.Equal(t, 20, totalRecords)
			})
		}
	})

	t.Run("start from mid-stream offset", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		// Produce some initial records
		for i := 0; i < 5; i++ {
			produceRecord(ctx, t, client, topicName, partitionID, []byte(fmt.Sprintf("record-%d", i)))
		}

		// Get the offset of the last produced record
		lastOffset := produceRecord(ctx, t, client, topicName, partitionID, []byte("last-initial-record"))

		// Start fetchers from the offset after the initial records
		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, lastOffset-1, concurrency, recordsPerFetch)

		// Produce some more records
		for i := 0; i < 3; i++ {
			produceRecord(ctx, t, client, topicName, partitionID, []byte(fmt.Sprintf("new-record-%d", i)))
		}

		const expectedRecords = 5
		fetchedRecordsContents := make([]string, 0, expectedRecords)
		for len(fetchedRecordsContents) < expectedRecords {
			fetches, _ := fetchers.PollFetches(ctx)
			fetches.EachRecord(func(r *kgo.Record) {
				fetchedRecordsContents = append(fetchedRecordsContents, string(r.Value))
			})
		}

		assert.Equal(t, []string{
			"record-4",
			"last-initial-record",
			"new-record-0",
			"new-record-1",
			"new-record-2",
		}, fetchedRecordsContents)
	})

	t.Run("synchronous produce and fetch", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, recordsPerFetch)

		for round := 0; round < 3; round++ {
			t.Log("starting round", round)
			const recordsPerRound = 4
			// Produce a few records
			expectedRecords := make([]string, 0, recordsPerRound)
			for i := 0; i < recordsPerRound; i++ {
				rec := []byte(fmt.Sprintf("round-%d-record-%d", round, i))
				expectedRecords = append(expectedRecords, string(rec))
				producedOffset := produceRecord(ctx, t, client, topicName, partitionID, rec)
				t.Log("produced", producedOffset, string(rec))
			}

			// Poll for fetches and verify
			fetchedRecords := make([]string, 0, recordsPerRound)
			for len(fetchedRecords) < recordsPerRound {
				fetches, _ := fetchers.PollFetches(ctx)
				fetches.EachRecord(func(r *kgo.Record) {
					fetchedRecords = append(fetchedRecords, string(r.Value))
					t.Log("fetched", r.Offset, string(r.Value))
				})
			}

			// Verify fetched records
			assert.Equal(t, expectedRecords, fetchedRecords, "Fetched records in round %d do not match expected", round)
		}
	})

	t.Run("concurrency can be updated", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		rec1 := []byte("record-1")
		rec2 := []byte("record-2")
		rec3 := []byte("record-3")

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)
		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, recordsPerFetch)

		produceRecordAndAssert := func(record []byte) {
			producedOffset := produceRecord(ctx, t, client, topicName, partitionID, record)
			// verify that the record is fetched.

			var fetches kgo.Fetches
			require.Eventually(t, func() bool {
				fetches, _ = fetchers.PollFetches(ctx)
				return len(fetches.Records()) == 1
			}, 5*time.Second, 100*time.Millisecond)

			require.Equal(t, fetches.Records()[0].Value, record)
			require.Equal(t, fetches.Records()[0].Offset, producedOffset)
		}

		// Ensure that the fetchers work with the initial concurrency.
		produceRecordAndAssert(rec1)

		// Now, update the concurrency.
		fetchers.Update(ctx, 1, 1)

		// Ensure that the fetchers work with the updated concurrency.
		produceRecordAndAssert(rec2)

		// Update and verify again.
		fetchers.Update(ctx, 10, 10)
		produceRecordAndAssert(rec3)

	})

	t.Run("update concurrency with continuous production", func(t *testing.T) {
		t.Parallel()
		const (
			testDuration       = 10 * time.Second
			produceInterval    = 10 * time.Millisecond
			initialConcurrency = 2
		)

		ctx, cancel := context.WithTimeout(context.Background(), testDuration)
		defer cancel()

		produceCtx, cancelProduce := context.WithTimeout(context.Background(), testDuration)
		defer cancelProduce()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		producedCount := atomic.NewInt64(0)

		// Start producing records continuously
		go func() {
			ticker := time.NewTicker(produceInterval)
			defer ticker.Stop()

			for {
				select {
				case <-produceCtx.Done():
					return
				case <-ticker.C:
					count := producedCount.Inc()
					record := fmt.Sprintf("record-%d", count)
					produceRecord(produceCtx, t, client, topicName, partitionID, []byte(record))
				}
			}
		}()

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, initialConcurrency, recordsPerFetch)

		fetchedRecords := make([]*kgo.Record, 0)
		fetchedCount := atomic.NewInt64(0)

		fetchRecords := func(duration time.Duration) {
			deadline := time.Now().Add(duration)
			for time.Now().Before(deadline) {
				fetches, _ := fetchers.PollFetches(ctx)
				fetches.EachRecord(func(r *kgo.Record) {
					fetchedRecords = append(fetchedRecords, r)
					fetchedCount.Inc()
				})
			}
		}

		// Initial fetch with starting concurrency
		fetchRecords(2 * time.Second)
		initialFetched := fetchedCount.Load()

		// Update to higher concurrency
		fetchers.Update(ctx, 4, recordsPerFetch)
		fetchRecords(3 * time.Second)
		highConcurrencyFetched := fetchedCount.Load() - initialFetched

		// Update to lower concurrency
		fetchers.Update(ctx, 1, recordsPerFetch)
		fetchRecords(3 * time.Second)

		cancelProduce()
		// Produce everything that's left now.
		fetchRecords(time.Second)
		totalProduced := producedCount.Load()
		totalFetched := fetchedCount.Load()

		// Verify fetched records
		assert.True(t, totalFetched > 0, "Expected to fetch some records")
		assert.Equal(t, totalFetched, totalProduced, "Should not fetch more records than produced")
		assert.True(t, highConcurrencyFetched > initialFetched, "Expected to fetch more records with higher concurrency")

		// Verify record contents
		for i, record := range fetchedRecords {
			expectedContent := fmt.Sprintf("record-%d", i+1)
			assert.Equal(t, expectedContent, string(record.Value),
				"Record %d has unexpected content: %s", i, string(record.Value))
		}

		// Log some statistics
		t.Logf("Total produced: %d, Total fetched: %d", totalProduced, totalFetched)
		t.Logf("Fetched with initial concurrency: %d", initialFetched)
		t.Logf("Fetched with high concurrency: %d", highConcurrencyFetched)
		t.Logf("Fetched with low concurrency: %d", totalFetched-initialFetched-highConcurrencyFetched)
	})

	t.Run("consume from end and update immediately", func(t *testing.T) {
		t.Parallel()
		const (
			initialRecords     = 100
			additionalRecords  = 50
			initialConcurrency = 2
			updatedConcurrency = 4
		)

		ctx := context.Background()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		// Produce initial records
		for i := 0; i < initialRecords; i++ {
			record := fmt.Sprintf("initial-record-%d", i+1)
			produceRecord(ctx, t, client, topicName, partitionID, []byte(record))
		}

		// Start concurrent fetchers from the end
		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, kafkaOffsetEnd, initialConcurrency, recordsPerFetch)

		// Immediately update concurrency
		fetchers.Update(ctx, updatedConcurrency, recordsPerFetch)

		// Produce additional records
		for i := 0; i < additionalRecords; i++ {
			record := fmt.Sprintf("additional-record-%d", i+1)
			produceRecord(ctx, t, client, topicName, partitionID, []byte(record))
		}

		fetchedRecords := make([]*kgo.Record, 0, additionalRecords)
		fetchDeadline := time.Now().Add(5 * time.Second)

		// Fetch records
		for len(fetchedRecords) < additionalRecords && time.Now().Before(fetchDeadline) {
			fetches, _ := fetchers.PollFetches(ctx)
			fetches.EachRecord(func(r *kgo.Record) {
				fetchedRecords = append(fetchedRecords, r)
			})
		}

		// Verify fetched records
		assert.LessOrEqual(t, len(fetchedRecords), additionalRecords,
			"Should not fetch more records than produced after start")

		// Verify record contents
		for i, record := range fetchedRecords {
			expectedContent := fmt.Sprintf("additional-record-%d", i+1)
			assert.Equal(t, expectedContent, string(record.Value),
				"Record %d has unexpected content: %s", i, string(record.Value))
		}

		// Log some statistics
		t.Logf("Total records produced: %d", initialRecords+additionalRecords)
		t.Logf("Records produced after start: %d", additionalRecords)
		t.Logf("Records fetched: %d", len(fetchedRecords))
	})
}

func createConcurrentFetchers(ctx context.Context, t *testing.T, client *kgo.Client, topic string, partition int32, startOffset int64, concurrency, recordsPerFetch int) *concurrentFetchers {
	logger := log.NewNopLogger()
	reg := prometheus.NewPedanticRegistry()
	metrics := newReaderMetrics(partition, reg)

	// This instantiates the fields of kprom.
	// This is usually done by franz-go, but since now we use the metrics ourselves, we need to instantiate the metrics ourselves.
	metrics.kprom.OnNewClient(client)

	offsetReader := newPartitionOffsetClient(client, topic, reg, logger)

	startOffsetsReader := newGenericOffsetReader(func(ctx context.Context) (int64, error) {
		return offsetReader.FetchPartitionStartOffset(ctx, partition)
	}, time.Second, logger)

	f, err := newConcurrentFetchers(
		ctx,
		client,
		logger,
		topic,
		partition,
		startOffset,
		concurrency,
		recordsPerFetch,
		false,
		time.Second, // same order of magnitude as the real one (defaultMinBytesMaxWaitTime), but faster for tests
		offsetReader,
		startOffsetsReader,
		&metrics,
	)
	require.NoError(t, err)
	t.Cleanup(f.Stop)

	return f
}

type waiterFunc func()

func (w waiterFunc) Wait() { w() }

type refresherFunc func()

func (r refresherFunc) ForceMetadataRefresh() { r() }
