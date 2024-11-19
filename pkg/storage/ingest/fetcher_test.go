// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestHandleKafkaFetchErr(t *testing.T) {
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
		"unknown broker": {
			err: errors.New(unknownBroker),
			lso: 5,
			fw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedFw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedBackoff:         false,
			expectedMetadataRefresh: false,
		},
		"closed broker": {
			err: errors.New(chosenBrokerDied),
			lso: 5,
			fw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedFw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedBackoff:         false,
			expectedMetadataRefresh: false,
		},
		"closed network connection": {
			err: fmt.Errorf("read tcp 10.0.227.72:37486->10.0.29.4:9092: use of closed network connection"), // this isn't exposed by the standard library so we just make one of our own
			lso: 5,
			fw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedFw: fetchWant{
				startOffset: 11,
				endOffset:   15,
			},
			expectedBackoff:         false,
			expectedMetadataRefresh: false,
		},
		"network timeout": {
			err: fmt.Errorf("read tcp 127.0.0.1:62984->127.0.0.1:9092: i/o timeout"),
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

			logs := &concurrency.SyncBuffer{}
			logger := log.NewLogfmtLogger(logs)

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
			assert.NotContains(t, logs.String(), "received an error we're not prepared to handle")
		})
	}
}

// TestFranzGoErrorStrings asserts that the strings of some errors in franz-go don't change.
// The errors themselves are not exported, but we rely on their strings not changing.
func TestFranzGoErrorStrings(t *testing.T) {
	const (
		topicName = "test-topic"
		partition = 1
	)
	_, clusterAddr := testkafka.CreateCluster(t, partition+1, topicName)
	client := newKafkaProduceClient(t, clusterAddr)

	req := kmsg.NewPtrMetadataRequest()
	_, unknownBrokerError := client.Broker(128).Request(context.Background(), req)
	assert.ErrorContains(t, unknownBrokerError, unknownBroker)
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

	t.Run("staggered production with exact multiple of concurrency and records per fetch", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		const (
			topicName       = "test-topic"
			partitionID     = 1
			concurrency     = 2
			recordsPerFetch = 3
		)

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, recordsPerFetch)

		// Produce exactly as many records as is the multiple of concurrency and records per fetch.
		// This will give each fetcher exactly as many records as they're supposed to fetch.
		const initiallyProducedRecords = concurrency * recordsPerFetch
		var producedRecordsBytes [][]byte
		for i := 0; i < initiallyProducedRecords; i++ {
			record := []byte(fmt.Sprintf("record-%d", i+1))
			produceRecord(ctx, t, client, topicName, partitionID, record)
			producedRecordsBytes = append(producedRecordsBytes, record)
		}

		// Expect that we've received all records.
		var fetchedRecordsBytes [][]byte
		for len(fetchedRecordsBytes) < initiallyProducedRecords {
			fetches, _ := fetchers.PollFetches(ctx)
			assert.NoError(t, fetches.Err())
			fetches.EachRecord(func(r *kgo.Record) {
				fetchedRecordsBytes = append(fetchedRecordsBytes, r.Value)
			})
		}

		// Produce a few more records
		const additionalRecords = 3
		for i := 0; i < additionalRecords; i++ {
			record := []byte(fmt.Sprintf("additional-record-%d", i+1))
			produceRecord(ctx, t, client, topicName, partitionID, record)
			producedRecordsBytes = append(producedRecordsBytes, record)
		}

		// Fetchers shouldn't be stalled and should continue fetching as the HWM moves forward.
		for len(fetchedRecordsBytes) < initiallyProducedRecords+additionalRecords {
			fetches, _ := fetchers.PollFetches(ctx)
			assert.NoError(t, fetches.Err())
			fetches.EachRecord(func(r *kgo.Record) {
				fetchedRecordsBytes = append(fetchedRecordsBytes, r.Value)
			})
		}

		assert.Equal(t, producedRecordsBytes, fetchedRecordsBytes)
	})

	t.Run("staggered production with one less than multiple of concurrency and records per fetch", func(t *testing.T) {
		// This test is the same as "staggered production with exact multiple of concurrency and records per fetch"
		// but covers an off-by-one error.
		t.Parallel()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		const (
			topicName       = "test-topic"
			partitionID     = 1
			concurrency     = 2
			recordsPerFetch = 3
		)

		cluster, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, recordsPerFetch)

		// Produce exactly as many records as is the multiple of concurrency and records per fetch.
		// This will give each fetcher exactly as many records as they're supposed to fetch.
		const initiallyProducedRecords = concurrency*recordsPerFetch - 1
		var producedRecordsBytes [][]byte
		for i := 0; i < initiallyProducedRecords; i++ {
			record := []byte(fmt.Sprintf("record-%d", i+1))
			produceRecord(ctx, t, client, topicName, partitionID, record)
			producedRecordsBytes = append(producedRecordsBytes, record)
		}

		// Expect that we've received all records.
		var fetchedRecordsBytes [][]byte
		for len(fetchedRecordsBytes) < initiallyProducedRecords {
			fetches, _ := fetchers.PollFetches(ctx)
			assert.NoError(t, fetches.Err())
			fetches.EachRecord(func(r *kgo.Record) {
				fetchedRecordsBytes = append(fetchedRecordsBytes, r.Value)
			})
		}

		// Produce a few more records
		const additionalRecords = 3
		for i := 0; i < additionalRecords; i++ {
			record := []byte(fmt.Sprintf("additional-record-%d", i+1))
			produceRecord(ctx, t, client, topicName, partitionID, record)
			producedRecordsBytes = append(producedRecordsBytes, record)
		}

		// Fetchers shouldn't be stalled and should continue fetching as the HWM moves forward.
		for len(fetchedRecordsBytes) < initiallyProducedRecords+additionalRecords {
			fetches, _ := fetchers.PollFetches(ctx)
			assert.NoError(t, fetches.Err())
			fetches.EachRecord(func(r *kgo.Record) {
				fetchedRecordsBytes = append(fetchedRecordsBytes, r.Value)
			})
		}

		assert.Equal(t, producedRecordsBytes, fetchedRecordsBytes)

		// Mock Kafka to fail the Fetch request.
		cluster.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()

			return nil, errors.New("mocked error"), true
		})
	})

	t.Run("fetchers do not request offset beyond high watermark", func(t *testing.T) {
		// In Warpstream fetching past the end induced more delays than MinBytesWaitTime.
		// So we avoid dispatching a fetch for past the high watermark.
		t.Parallel()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		const (
			topicName       = "test-topic"
			partitionID     = 1
			concurrency     = 2
			recordsPerFetch = 3
			initialRecords  = 8
		)

		cluster, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		fetchRequestCount := atomic.NewInt64(0)
		maxRequestedOffset := atomic.NewInt64(-1)

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, recordsPerFetch)

		// Produce initial records
		var producedRecordsBytes [][]byte
		for i := 0; i < initialRecords; i++ {
			record := []byte(fmt.Sprintf("record-%d", i+1))
			producedRecordsBytes = append(producedRecordsBytes, record)
			offset := produceRecord(ctx, t, client, topicName, partitionID, record)
			t.Log("Produced record at offset", offset)
		}

		// Fetch and verify records; this should unblock the fetchers.
		var fetchedRecordsBytes [][]byte
		for len(fetchedRecordsBytes) < initialRecords {
			fetches, _ := fetchers.PollFetches(ctx)
			assert.NoError(t, fetches.Err())
			fetches.EachRecord(func(r *kgo.Record) {
				fetchedRecordsBytes = append(fetchedRecordsBytes, r.Value)
			})
		}

		// Set up control function to monitor fetch requests
		var checkRequestOffset func(req kmsg.Request) (kmsg.Response, error, bool)
		checkRequestOffset = func(req kmsg.Request) (kmsg.Response, error, bool) {
			fetchReq := req.(*kmsg.FetchRequest)
			cluster.KeepControl()
			fetchRequestCount.Inc()
			assert.Len(t, fetchReq.Topics, 1)
			assert.Len(t, fetchReq.Topics[0].Partitions, 1)
			requestedOffset := fetchReq.Topics[0].Partitions[0].FetchOffset
			maxRequestedOffset.Store(fetchReq.Topics[0].Partitions[0].FetchOffset)
			t.Log("Received fetch request for offset", requestedOffset)

			cluster.DropControl()                                      // Let the cluster handle the request normally
			cluster.ControlKey(kmsg.Fetch.Int16(), checkRequestOffset) // But register the function again so we can inspect the next request too.

			return nil, nil, false
		}
		cluster.ControlKey(kmsg.Fetch.Int16(), checkRequestOffset)

		// Wait for a few fetch requests
		require.Eventually(t, func() bool {
			return fetchRequestCount.Load() >= 10
		}, 30*time.Second, 100*time.Millisecond, "Not enough fetch requests received")

		// Verify that the max requested offset does not exceed the number of produced records
		assert.LessOrEqualf(t, int(maxRequestedOffset.Load()), len(producedRecordsBytes),
			"Requested offset (%d) should not exceed the number of produced records (%d)", maxRequestedOffset.Load(), len(producedRecordsBytes))

		// Verify the number and content of fetched records
		assert.Equal(t, producedRecordsBytes, fetchedRecordsBytes, "Should fetch all produced records")
	})

	t.Run("fetch with random error injection", func(t *testing.T) {
		t.Parallel()
		const (
			testDuration     = 15 * time.Second
			baseInterval     = time.Millisecond
			maxJitter        = 500 * time.Microsecond
			concurrency      = 12
			recordsPerFetch  = 1000
			errorProbability = 0.1
		)

		ctx, cancel := context.WithTimeout(context.Background(), testDuration)
		defer cancel()

		cluster, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		// Track if error was injected
		errorInjected := atomic.NewBool(false)
		seed := time.Now().UnixNano()
		rand.Seed(seed)
		t.Log("Random seed", seed)

		// Set up error injection for random fetch requests
		cluster.ControlKey(int16(kmsg.Fetch), func(req kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			if rand.Float64() < errorProbability {
				errorInjected.Store(true)
				t.Log("injected an error")
				return nil, errors.New(unknownBroker), true
			}
			return nil, nil, false
		})

		fetchers := createConcurrentFetchers(context.Background(), t, client, topicName, partitionID, 0, concurrency, recordsPerFetch)

		var wg sync.WaitGroup
		producedCount := atomic.NewInt64(0)
		fetchedCount := atomic.NewInt64(0)

		// Producer goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(baseInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					// Add random jitter
					time.Sleep(time.Duration(rand.Int63n(int64(maxJitter))))
					count := producedCount.Inc()
					record := fmt.Sprintf("record-%d", count)
					produceRecord(context.Background(), t, client, topicName, partitionID, []byte(record))
				}
			}
		}()

		// Consumer goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					if fetchedCount.Load() == producedCount.Load() {
						return
					}
					// there's still some records to fetch, so we wait for them
				default:
				}
				fetches, _ := fetchers.PollFetches(ctx)
				if fetches.Err() != nil {
					t.Log("Fetch error:", fetches.Err())
				}
				fetches.EachRecord(func(r *kgo.Record) {
					assert.Equal(t, fmt.Sprintf("record-%d", fetchedCount.Inc()), string(r.Value))
				})
			}
		}()

		// Wait for test duration
		wg.Wait()

		assert.True(t, errorInjected.Load(), "Expected an error to be injected")
		assert.Equal(t, producedCount.Load(), fetchedCount.Load(), "Fetched records should match produced records")
	})
}

func createConcurrentFetchers(ctx context.Context, t *testing.T, client *kgo.Client, topic string, partition int32, startOffset int64, concurrency, recordsPerFetch int) *concurrentFetchers {
	logger := log.NewLogfmtLogger(os.Stdout)
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
