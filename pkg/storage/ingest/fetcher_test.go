// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

var (
	fastFetchBackoffConfig = backoff.Config{
		MinBackoff: 10 * time.Millisecond,
		MaxBackoff: 10 * time.Millisecond,
		MaxRetries: 0,
	}
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
		"BrokerNotAvailable": {
			err: kerr.BrokerNotAvailable,
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

type noopReaderMetricsSource struct {
}

func (n noopReaderMetricsSource) BufferedBytes() int64           { return 0 }
func (n noopReaderMetricsSource) BufferedRecords() int64         { return 0 }
func (n noopReaderMetricsSource) EstimatedBytesPerRecord() int64 { return 0 }

func TestConcurrentFetchers(t *testing.T) {
	const (
		topicName   = "test-topic"
		partitionID = 1
		concurrency = 2
	)

	waitForStableBufferedRecords := func(t *testing.T, f fetcher) {
		previousBufferedRecords := int64(0)
		assert.Eventually(t, func() bool {
			bufferedRecords := f.BufferedRecords()
			stabilized := bufferedRecords == previousBufferedRecords
			previousBufferedRecords = bufferedRecords
			return stabilized
		}, 2*time.Second, 100*time.Millisecond)
	}

	t.Run("respect context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, 0)

		// This should not block forever now
		fetches, fetchCtx := fetchers.PollFetches(ctx)

		assert.Zero(t, fetches.NumRecords())
		assert.Error(t, fetchCtx.Err(), "Expected context to be cancelled")
		assert.Zero(t, fetchers.BufferedRecords())
	})

	t.Run("cold replay", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		// Produce some records before starting the fetchers
		for i := 0; i < 5; i++ {
			produceRecord(ctx, t, client, topicName, partitionID, []byte(fmt.Sprintf("record-%d", i)))
		}

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, 0)

		fetches := longPollFetches(fetchers, 5, 2*time.Second)
		assert.Equal(t, fetches.NumRecords(), 5)

		// We expect no more records returned by PollFetches() and no buffered records.
		pollFetchesAndAssertNoRecords(t, fetchers)
	})

	t.Run("fetch records produced after startup", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, 0)

		// Produce some records after starting the fetchers
		for i := 0; i < 3; i++ {
			produceRecord(ctx, t, client, topicName, partitionID, []byte(fmt.Sprintf("record-%d", i)))
		}

		fetches := longPollFetches(fetchers, 3, 2*time.Second)
		assert.Equal(t, fetches.NumRecords(), 3)

		// We expect no more records returned by PollFetches() and no buffered records.
		pollFetchesAndAssertNoRecords(t, fetchers)
	})

	t.Run("slow processing of fetches", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, 0)

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
				consumedRecords += fetches.NumRecords()

				// Simulate slow processing.
				time.Sleep(200 * time.Millisecond)
			}
			assert.Equal(t, 10, consumedRecords)
		}()

		// Slowly produce more records while processing is slow too. This increase the chances
		// of progressive fetches done by the consumer.
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 5; i < 10; i++ {
				produceRecord(ctx, t, client, topicName, partitionID, []byte(fmt.Sprintf("record-%d", i)))
				time.Sleep(200 * time.Millisecond)
			}
		}()

		wg.Wait()

		// We expect no more records returned by PollFetches() and no buffered records.
		pollFetchesAndAssertNoRecords(t, fetchers)
	})

	t.Run("fast processing of fetches", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, 0)

		// Produce some records
		for i := 0; i < 10; i++ {
			produceRecord(ctx, t, client, topicName, partitionID, []byte(fmt.Sprintf("record-%d", i)))
		}

		// Consume all expected records.
		fetches := longPollFetches(fetchers, 10, 2*time.Second)
		consumedRecords := fetches.NumRecords()
		assert.Equal(t, 10, consumedRecords)

		// We expect no more records returned by PollFetches() and no buffered records.
		pollFetchesAndAssertNoRecords(t, fetchers)
	})

	t.Run("fetch with different concurrency levels", func(t *testing.T) {
		t.Parallel()

		for _, concurrency := range []int{1, 2, 4} {
			concurrency := concurrency

			t.Run(fmt.Sprintf("concurrency-%d", concurrency), func(t *testing.T) {
				t.Parallel()

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
				client := newKafkaProduceClient(t, clusterAddr)

				fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, 0)

				// Produce some records
				for i := 0; i < 20; i++ {
					produceRecord(ctx, t, client, topicName, partitionID, []byte(fmt.Sprintf("record-%d", i)))
				}

				fetches := longPollFetches(fetchers, 20, 2*time.Second)
				totalRecords := fetches.NumRecords()

				assert.Equal(t, 20, totalRecords)

				// We expect no more records returned by PollFetches() and no buffered records.
				pollFetchesAndAssertNoRecords(t, fetchers)
			})
		}
	})

	t.Run("start from mid-stream offset", func(t *testing.T) {
		t.Parallel()

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
		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, lastOffset-1, concurrency, 0)

		// Produce some more records
		for i := 0; i < 3; i++ {
			produceRecord(ctx, t, client, topicName, partitionID, []byte(fmt.Sprintf("new-record-%d", i)))
		}

		const expectedRecords = 5
		fetchedRecordsContents := make([]string, 0, expectedRecords)
		fetches := longPollFetches(fetchers, expectedRecords, 2*time.Second)
		fetches.EachRecord(func(r *kgo.Record) {
			fetchedRecordsContents = append(fetchedRecordsContents, string(r.Value))
		})

		assert.Equal(t, []string{
			"record-4",
			"last-initial-record",
			"new-record-0",
			"new-record-1",
			"new-record-2",
		}, fetchedRecordsContents)

		// We expect no more records returned by PollFetches() and no buffered records.
		pollFetchesAndAssertNoRecords(t, fetchers)
	})

	t.Run("synchronous produce and fetch", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, 0)

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
			fetches := longPollFetches(fetchers, recordsPerRound, 2*time.Second)
			fetches.EachRecord(func(r *kgo.Record) {
				fetchedRecords = append(fetchedRecords, string(r.Value))
				t.Log("fetched", r.Offset, string(r.Value))
			})

			// Verify fetched records
			assert.Equal(t, expectedRecords, fetchedRecords, "Fetched records in round %d do not match expected", round)

			// We expect no more records returned by PollFetches() and no buffered records.
			pollFetchesAndAssertNoRecords(t, fetchers)
		}
	})

	t.Run("concurrency can be updated", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		rec1 := []byte("record-1")
		rec2 := []byte("record-2")
		rec3 := []byte("record-3")

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)
		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, 0)

		produceRecordAndAssert := func(record []byte) {
			producedOffset := produceRecord(ctx, t, client, topicName, partitionID, record)
			// verify that the record is fetched.

			fetches := longPollFetches(fetchers, 1, 5*time.Second)

			require.Equal(t, fetches.Records()[0].Value, record)
			require.Equal(t, fetches.Records()[0].Offset, producedOffset)
		}

		// Ensure that the fetchers work with the initial concurrency.
		produceRecordAndAssert(rec1)

		// Now, update the concurrency.
		fetchers.Update(ctx, 1)

		// Ensure that the fetchers work with the updated concurrency.
		produceRecordAndAssert(rec2)

		// Update and verify again.
		fetchers.Update(ctx, 10)
		produceRecordAndAssert(rec3)

		// We expect no more records returned by PollFetches() and no buffered records.
		pollFetchesAndAssertNoRecords(t, fetchers)
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
					// Use context.Background() so that we don't race with the test context being cancelled.
					produceRecord(context.Background(), t, client, topicName, partitionID, []byte(record))
				}
			}
		}()

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, initialConcurrency, 0)

		fetchedRecords := make([]*kgo.Record, 0)

		// Initial fetch with starting concurrency
		fetches := longPollFetches(fetchers, math.MaxInt, 2*time.Second)
		initialFetched := fetches.NumRecords()

		// Update to higher concurrency
		fetchers.Update(ctx, 4)
		fetches = longPollFetches(fetchers, math.MaxInt, 3*time.Second)
		highConcurrencyFetched := fetches.NumRecords()

		// Update to lower concurrency
		fetchers.Update(ctx, 1)
		fetches = longPollFetches(fetchers, math.MaxInt, 3*time.Second)
		lowerConcurrentFetched := fetches.NumRecords()

		cancelProduce()
		// Consume everything that's left now.
		fetches = longPollFetches(fetchers, math.MaxInt, 2*time.Second)
		finalFetched := fetches.NumRecords()
		totalProduced := producedCount.Load()
		totalFetched := initialFetched + highConcurrencyFetched + lowerConcurrentFetched + finalFetched

		// Verify fetched records
		assert.True(t, totalFetched > 0, "Expected to fetch some records")
		assert.Equal(t, int64(totalFetched), totalProduced, "Should not fetch more records than produced")
		assert.True(t, highConcurrencyFetched > initialFetched, "Expected to fetch more records with higher concurrency")

		// Verify record contents
		for i, record := range fetchedRecords {
			expectedContent := fmt.Sprintf("record-%d", i+1)
			assert.Equal(t, expectedContent, string(record.Value),
				"Record %d has unexpected content: %s", i, string(record.Value))
		}

		// We expect no more records returned by PollFetches() and no buffered records.
		pollFetchesAndAssertNoRecords(t, fetchers)

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
		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, kafkaOffsetEnd, initialConcurrency, 0)

		// Immediately update concurrency
		fetchers.Update(ctx, updatedConcurrency)

		// Produce additional records
		for i := 0; i < additionalRecords; i++ {
			record := fmt.Sprintf("additional-record-%d", i+1)
			produceRecord(ctx, t, client, topicName, partitionID, []byte(record))
		}

		// Fetch records
		fetches := longPollFetches(fetchers, additionalRecords, 5*time.Second)
		fetchedRecords := fetches.Records()

		// Verify fetched records
		assert.LessOrEqual(t, len(fetchedRecords), additionalRecords,
			"Should not fetch more records than produced after start")

		// Verify record contents
		for i, record := range fetchedRecords {
			expectedContent := fmt.Sprintf("additional-record-%d", i+1)
			assert.Equal(t, expectedContent, string(record.Value),
				"Record %d has unexpected content: %s", i, string(record.Value))
		}

		// We expect no more records returned by PollFetches() and no buffered records.
		pollFetchesAndAssertNoRecords(t, fetchers)

		// Log some statistics
		t.Logf("Total records produced: %d", initialRecords+additionalRecords)
		t.Logf("Records produced after start: %d", additionalRecords)
		t.Logf("Records fetched: %d", len(fetchedRecords))
	})

	t.Run("staggered production", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		const (
			topicName   = "test-topic"
			partitionID = 1
			concurrency = 2
		)

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, 0)

		// Produce enough records to saturate each fetcher.
		const initiallyProducedRecords = concurrency * 10
		var producedRecordsBytes [][]byte
		for i := 0; i < initiallyProducedRecords; i++ {
			record := []byte(fmt.Sprintf("record-%d", i+1))
			produceRecord(ctx, t, client, topicName, partitionID, record)
			producedRecordsBytes = append(producedRecordsBytes, record)
		}

		// Expect that we've received all records.
		var fetchedRecordsBytes [][]byte
		fetches := longPollFetches(fetchers, initiallyProducedRecords, 2*time.Second)
		assert.NoError(t, fetches.Err())
		fetches.EachRecord(func(r *kgo.Record) {
			fetchedRecordsBytes = append(fetchedRecordsBytes, r.Value)
		})

		// Produce a few more records
		const additionalRecords = 3
		for i := 0; i < additionalRecords; i++ {
			record := []byte(fmt.Sprintf("additional-record-%d", i+1))
			produceRecord(ctx, t, client, topicName, partitionID, record)
			producedRecordsBytes = append(producedRecordsBytes, record)
		}

		// Fetchers shouldn't be stalled and should continue fetching as the HWM moves forward.
		fetches = longPollFetches(fetchers, additionalRecords, 2*time.Second)
		assert.NoError(t, fetches.Err())
		fetches.EachRecord(func(r *kgo.Record) {
			fetchedRecordsBytes = append(fetchedRecordsBytes, r.Value)
		})

		assert.Equal(t, producedRecordsBytes, fetchedRecordsBytes)

		// We expect no more records returned by PollFetches() and no buffered records.
		pollFetchesAndAssertNoRecords(t, fetchers)
	})

	t.Run("fetchers do not request offset beyond high watermark", func(t *testing.T) {
		// In Warpstream fetching past the end induced more delays than MinBytesWaitTime.
		// So we avoid dispatching a fetch for past the high watermark.
		t.Parallel()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		const (
			topicName      = "test-topic"
			partitionID    = 1
			concurrency    = 2
			initialRecords = 8
		)

		cluster, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		fetchRequestCount := atomic.NewInt64(0)
		maxRequestedOffset := atomic.NewInt64(-1)

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, 0)

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
		fetches := longPollFetches(fetchers, initialRecords, 2*time.Second) // Ensure no more records are fetched.
		assert.NoError(t, fetches.Err())
		fetches.EachRecord(func(r *kgo.Record) {
			fetchedRecordsBytes = append(fetchedRecordsBytes, r.Value)
		})

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

		// We expect no more records returned by PollFetches() and no buffered records.
		pollFetchesAndAssertNoRecords(t, fetchers)
	})

	t.Run("starting to run against a broken broker fails creating the fetchers", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cluster, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		mockErr := kerr.BrokerNotAvailable
		cluster.ControlKey(kmsg.Metadata.Int16(), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()

			respTopic := kmsg.NewMetadataResponseTopic()
			topicName := topicName // can't take the address of a const, so we first write it to a variable
			respTopic.Topic = &topicName
			respTopic.TopicID = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
			respTopic.ErrorCode = mockErr.Code

			resp := kmsg.NewPtrMetadataResponse()
			resp.Topics = append(resp.Topics, respTopic)
			resp.Version = 12
			return resp, nil, true
		})

		logger := log.NewNopLogger()
		reg := prometheus.NewPedanticRegistry()
		metrics := newReaderMetrics(partitionID, reg, noopReaderMetricsSource{})

		client := newKafkaProduceClient(t, clusterAddr)

		// This instantiates the fields of kprom.
		// This is usually done by franz-go, but since now we use the metrics ourselves, we need to instantiate the metrics ourselves.
		metrics.kprom.OnNewClient(client)

		offsetReader := newPartitionOffsetClient(client, topicName, reg, logger)

		startOffsetsReader := newGenericOffsetReader(func(ctx context.Context) (int64, error) {
			return offsetReader.FetchPartitionStartOffset(ctx, partitionID)
		}, time.Second, logger)

		_, err := newConcurrentFetchers(
			ctx,
			client,
			logger,
			topicName,
			partitionID,
			0,
			concurrency,
			0,
			false,
			time.Second, // same order of magnitude as the real one (defaultMinBytesMaxWaitTime), but faster for tests
			offsetReader,
			startOffsetsReader,
			fastFetchBackoffConfig,
			&metrics,
		)
		assert.ErrorContains(t, err, "failed to find topic ID")
		assert.ErrorIs(t, err, mockErr)
	})

	t.Run("should reset the buffered records count when stopping", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, 0)

		// Produce some records.
		for i := 0; i < 10; i++ {
			produceRecord(ctx, t, client, topicName, partitionID, []byte(fmt.Sprintf("record-%d", i)))
		}

		// We are not consuming the records, so we expect the count of buffered records to increase.
		// The actual number of buffered records may change due to concurrency, so we just check
		// that there are some buffered records.
		test.Poll(t, time.Second, true, func() interface{} {
			return fetchers.BufferedRecords() > 0
		})

		// Stop the fetchers.
		fetchers.Stop()

		// Even if there were some buffered records we expect the count to be reset to 0 when stopping
		// because the Stop() intentionally discard any buffered record.
		require.Zero(t, fetchers.BufferedRecords())
	})

	t.Run("respect maximum buffered bytes limit", func(t *testing.T) {
		t.Parallel()

		const (
			topicName        = "test-topic"
			partitionID      = 1
			concurrency      = 3
			maxInflightBytes = 10_000_000

			recordSizeBytes      = 100_000 // sizable records so that our lower limit of 1MB per fetch request doesn't just include all records
			totalProducedRecords = 6000    // produce a lot of records so that the client is forced to split them into multiple fetches
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		// Produce records
		recordValue := bytes.Repeat([]byte{'a'}, recordSizeBytes)
		for i := 0; i < totalProducedRecords; i++ {
			produceRecord(ctx, t, client, topicName, partitionID, recordValue)
		}

		// Create fetchers with tracking of uncompressed bytes
		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, maxInflightBytes)

		// Wait for buffered records to stabilize, we expect that they stabilize because the limit is in effect.
		waitForStableBufferedRecords(t, fetchers)

		// Assert that we don't buffer more than maxInflightBytes
		assert.LessOrEqualf(t, fetchers.BufferedBytes(), int64(maxInflightBytes), "Should not buffer more than %d bytes of records", maxInflightBytes)

		// Consume one batch of records
		fetches, _ := fetchers.PollFetches(ctx)
		totalConsumedRecords := fetches.NumRecords()
		require.Greater(t, totalConsumedRecords, 0, "Should have received some records")

		// Allow time for more fetches
		waitForStableBufferedRecords(t, fetchers)

		// Assert again that buffered bytes remain under limit
		assert.LessOrEqualf(t, fetchers.BufferedRecords(), int64(maxInflightBytes), "Should still not buffer more than %d bytes after consuming some records", maxInflightBytes)

		// Consume all remaining records and verify total
		// We produce a lot of data, give enough time so that the slow CI doesn't flake
		fetches = longPollFetches(fetchers, totalProducedRecords-totalConsumedRecords, 20*time.Second)
		totalConsumedRecords += fetches.NumRecords()

		// Allow time for more fetches
		waitForStableBufferedRecords(t, fetchers)

		pollFetchesAndAssertNoRecords(t, fetchers)
		assert.Equal(t, totalProducedRecords, totalConsumedRecords, "Should have received all records eventually")
	})

	t.Run("respect maximum buffered bytes limit with varying record sizes", func(t *testing.T) {
		// This test makes sure that the buffer doesn't become inefficient when the size estimations change (from large records we switch to small records).
		t.Parallel()

		const (
			topicName        = "test-topic"
			partitionID      = 1
			concurrency      = 30
			maxInflightBytes = 5_000_000

			largeRecordsCount = 100
			largeRecordSize   = 100_000
			smallRecordsCount = 10_000
			smallRecordSize   = 1000
		)

		require.True(t, smallRecordsCount%2 == 0, "we divide the smallRecordsCount by 2 later on, it must be divisible by 2")

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, clusterAddr := testkafka.CreateCluster(t, partitionID+1, topicName)
		client := newKafkaProduceClient(t, clusterAddr)

		// Create fetchers early to ensure we don't miss any records
		fetchers := createConcurrentFetchers(ctx, t, client, topicName, partitionID, 0, concurrency, maxInflightBytes)

		// Produce large records
		largeValue := bytes.Repeat([]byte{'a'}, largeRecordSize)
		for i := 0; i < largeRecordsCount; i++ {
			produceRecord(ctx, t, client, topicName, partitionID, largeValue)
		}

		t.Logf("Produced %d large records", largeRecordsCount)

		waitForStableBufferedRecords(t, fetchers)
		t.Log("Buffered records stabilized")

		assert.LessOrEqualf(t, fetchers.BufferedBytes(), int64(maxInflightBytes), "Should not buffer more than %d bytes of large records", maxInflightBytes)
		// Consume all large records
		fetches := longPollFetches(fetchers, largeRecordsCount, 10*time.Second)
		consumedRecords := fetches.NumRecords()

		pollFetchesAndAssertNoRecords(t, fetchers)
		t.Log("Consumed all large records")

		// Produce small records
		smallValue := bytes.Repeat([]byte{'b'}, smallRecordSize)
		for i := 0; i < smallRecordsCount; i++ {
			produceRecord(ctx, t, client, topicName, partitionID, smallValue)
		}

		t.Logf("Produced %d small records", smallRecordsCount)

		// Consume half of the small records. This should be enough to stabilize the records size estimation.
		fetches = longPollFetches(fetchers, smallRecordsCount/2, 10*time.Second)
		consumedRecords += fetches.NumRecords()
		t.Log("Consumed half of the small records")

		// Assert that the buffer is well utilized.
		waitForStableBufferedRecords(t, fetchers)
		t.Log("Buffered records stabilized")

		assert.LessOrEqualf(t, fetchers.BufferedBytes(), int64(maxInflightBytes), "Should not buffer more than %d bytes of small records", maxInflightBytes)
		assert.GreaterOrEqual(t, fetchers.BufferedBytes(), int64(maxInflightBytes/2), "Should still buffer a decent number of records")

		// Consume the rest of the small records.
		// Consume half of the small records. This should be enough to stabilize the records size estimation.
		fetches = longPollFetches(fetchers, smallRecordsCount/2, 10*time.Second)
		consumedRecords += fetches.NumRecords()
		t.Log("Consumed rest of the small records")

		// Verify we received correct number of records
		const totalProducedRecords = largeRecordsCount + smallRecordsCount
		assert.Equal(t, totalProducedRecords, consumedRecords, "Should have consumed all records")

		// Verify no more records are buffered. First wait for the buffered records to stabilize.
		waitForStableBufferedRecords(t, fetchers)

		pollFetchesAndAssertNoRecords(t, fetchers)
	})
}

func TestConcurrentFetchers_fetchSingle(t *testing.T) {
	const (
		topic       = "test-topic"
		partitionID = 1
	)

	var (
		ctx                  = context.Background()
		cluster, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topic)
		client               = newKafkaProduceClient(t, clusterAddr)
		fetchers             = createConcurrentFetchers(ctx, t, client, topic, partitionID, 0, 1, 0)
	)

	// Produce some records.
	produceRecord(ctx, t, client, topic, partitionID, []byte("record-1"))
	produceRecord(ctx, t, client, topic, partitionID, []byte("record-2"))
	produceRecord(ctx, t, client, topic, partitionID, []byte("record-3"))

	t.Run("should fetch records honoring the start offset", func(t *testing.T) {
		res := fetchers.fetchSingle(ctx, fetchWant{
			startOffset:             1,
			endOffset:               5,
			estimatedBytesPerRecord: 100,
			targetMaxBytes:          1000000,
		})

		require.NoError(t, res.Err)
		require.Len(t, res.Records, 2)
		require.Equal(t, "record-2", string(res.Records[0].Value))
		require.Equal(t, "record-3", string(res.Records[1].Value))
	})

	t.Run("should return an empty non-error response if context is canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		res := fetchers.fetchSingle(ctx, fetchWant{
			startOffset:             1,
			endOffset:               5,
			estimatedBytesPerRecord: 100,
			targetMaxBytes:          1000000,
		})

		require.NoError(t, res.Err)
		require.Len(t, res.Records, 0)
	})

	t.Run("should return an error response if the Fetch request fails", func(t *testing.T) {
		// Control only the next request, then drop it.
		cluster.ControlKey(kmsg.Fetch.Int16(), func(_ kmsg.Request) (kmsg.Response, error, bool) {
			return nil, errors.New("failed request"), true
		})

		res := fetchers.fetchSingle(ctx, fetchWant{
			startOffset:             1,
			endOffset:               5,
			estimatedBytesPerRecord: 100,
			targetMaxBytes:          1000000,
		})

		require.Error(t, res.Err)
		require.Len(t, res.Records, 0)
	})

	t.Run("should return an error response if the Fetch request contains an error", func(t *testing.T) {
		// Control only the next request, then drop it.
		cluster.ControlKey(kmsg.Fetch.Int16(), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
			req := kreq.(*kmsg.FetchRequest)

			return &kmsg.FetchResponse{
				Version: req.Version,
				Topics: []kmsg.FetchResponseTopic{{
					Topic:   req.Topics[0].Topic,
					TopicID: req.Topics[0].TopicID,
					Partitions: []kmsg.FetchResponseTopicPartition{{
						Partition: req.Topics[0].Partitions[0].Partition,
						ErrorCode: kerr.UnknownServerError.Code,
					}},
				}},
			}, nil, true
		})

		res := fetchers.fetchSingle(ctx, fetchWant{
			startOffset:             1,
			endOffset:               5,
			estimatedBytesPerRecord: 100,
			targetMaxBytes:          1000000,
		})

		require.Error(t, res.Err)
		require.ErrorContains(t, res.Err, kerr.UnknownServerError.Error())
		require.Len(t, res.Records, 0)
	})
}

func TestConcurrentFetchers_parseFetchResponse(t *testing.T) {
	const (
		topic       = "test-topic"
		partitionID = 1
	)

	var (
		ctx            = context.Background()
		_, clusterAddr = testkafka.CreateCluster(t, partitionID+1, topic)
		client         = newKafkaProduceClient(t, clusterAddr)
		fetchers       = createConcurrentFetchers(ctx, t, client, topic, partitionID, 0, 1, 0)
	)

	t.Run("should return error if the response does not contain any topic", func(t *testing.T) {
		res := fetchers.parseFetchResponse(ctx, 0, &kmsg.FetchResponse{})
		require.Error(t, res.Err)
	})

	t.Run("should return error if the response contains an error at the response level", func(t *testing.T) {
		res := fetchers.parseFetchResponse(ctx, 0, &kmsg.FetchResponse{ErrorCode: kerr.UnknownServerError.Code})
		require.Error(t, res.Err)
		require.ErrorContains(t, res.Err, "received error")
	})

	t.Run("should return error if the response contains more than 1 topic", func(t *testing.T) {
		res := fetchers.parseFetchResponse(ctx, 0, &kmsg.FetchResponse{Topics: []kmsg.FetchResponseTopic{
			{TopicID: fetchers.topicID},
			{TopicID: [16]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},
		}})

		require.Error(t, res.Err)
		require.ErrorContains(t, res.Err, "unexpected number of topics")
	})

	t.Run("should return error if the response contains 1 topic but the topic ID is not the expected one", func(t *testing.T) {
		res := fetchers.parseFetchResponse(ctx, 0, &kmsg.FetchResponse{Topics: []kmsg.FetchResponseTopic{
			{TopicID: [16]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},
		}})

		require.Error(t, res.Err)
		require.ErrorContains(t, res.Err, "unexpected topic ID")
	})

	t.Run("should return error if the response contains 1 topic with more than 1 partition", func(t *testing.T) {
		res := fetchers.parseFetchResponse(ctx, 0, &kmsg.FetchResponse{Topics: []kmsg.FetchResponseTopic{{
			TopicID: fetchers.topicID,
			Partitions: []kmsg.FetchResponseTopicPartition{
				{Partition: 0},
				{Partition: 1},
			},
		}}})

		require.Error(t, res.Err)
		require.ErrorContains(t, res.Err, "unexpected number of partitions")
	})

	t.Run("should return error if the response contains 1 topic with 1 partition but the partition is not the expected one", func(t *testing.T) {
		res := fetchers.parseFetchResponse(ctx, 0, &kmsg.FetchResponse{Topics: []kmsg.FetchResponseTopic{{
			TopicID: fetchers.topicID,
			Partitions: []kmsg.FetchResponseTopicPartition{
				{Partition: 12345},
			},
		}}})

		require.Error(t, res.Err)
		require.ErrorContains(t, res.Err, "unexpected partition ID")
	})

	t.Run("should return error if the response contains an error for the partition", func(t *testing.T) {
		res := fetchers.parseFetchResponse(ctx, 0, &kmsg.FetchResponse{Topics: []kmsg.FetchResponseTopic{{
			TopicID: fetchers.topicID,
			Partitions: []kmsg.FetchResponseTopicPartition{
				{Partition: fetchers.partitionID, ErrorCode: kerr.UnknownServerError.Code},
			},
		}}})

		require.Error(t, res.Err)
		require.ErrorContains(t, res.Err, kerr.ErrorForCode(kerr.UnknownServerError.Code).Error())
	})
}

func TestNewEmptyFetchResult(t *testing.T) {
	t.Run("should have no error set", func(t *testing.T) {
		res := newEmptyFetchResult(context.Background(), 1)
		require.Equal(t, int32(1), res.Partition)
		require.NoError(t, res.Err)
	})
}

func TestNewErrorFetchResult(t *testing.T) {
	t.Run("should have error set", func(t *testing.T) {
		err := errors.New("test error")
		res := newErrorFetchResult(context.Background(), 1, err)
		require.Equal(t, int32(1), res.Partition)
		require.Equal(t, err, res.Err)
	})

	t.Run("should panic if no error is provided", func(t *testing.T) {
		require.Panics(t, func() {
			newErrorFetchResult(context.Background(), 1, nil)
		})
	})
}

func TestFetchResult_Merge(t *testing.T) {
	t.Run("should panic if the called fetchResult has Err set", func(t *testing.T) {
		require.Panics(t, func() {
			a := fetchResult{FetchPartition: kgo.FetchPartition{Err: errors.New("test error")}}
			b := fetchResult{FetchPartition: kgo.FetchPartition{}}
			a.Merge(b)
		})
	})

	t.Run("should panic if the older fetchResult has Err set", func(t *testing.T) {
		require.Panics(t, func() {
			a := fetchResult{FetchPartition: kgo.FetchPartition{}}
			b := fetchResult{FetchPartition: kgo.FetchPartition{Err: errors.New("test error")}}
			a.Merge(b)
		})
	})
}

func createConcurrentFetchers(ctx context.Context, t *testing.T, client *kgo.Client, topic string, partition int32, startOffset int64, concurrency int, maxInflightBytes int32) *concurrentFetchers {
	logger := testingLogger.WithT(t)

	reg := prometheus.NewPedanticRegistry()
	metrics := newReaderMetrics(partition, reg, noopReaderMetricsSource{})

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
		maxInflightBytes,
		true,        // kfake uses compression and behaves similar to apache kafka
		time.Second, // same order of magnitude as the real one (defaultMinBytesMaxWaitTime), but faster for tests
		offsetReader,
		startOffsetsReader,
		fastFetchBackoffConfig,
		&metrics,
	)
	require.NoError(t, err)
	t.Cleanup(f.Stop)

	return f
}

// longPollFetches polls fetches until the timeout is reached or the number of records is at least minRecords.
func longPollFetches(fetchers *concurrentFetchers, minRecords int, timeout time.Duration) kgo.Fetches {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	allFetches := make(kgo.Fetches, 0)
	for ctx.Err() == nil && allFetches.NumRecords() < minRecords {
		fetches, _ := fetchers.PollFetches(ctx)
		if fetches.Err() != nil {
			continue
		}
		allFetches = append(allFetches, fetches...)
	}

	return allFetches
}

// pollFetchesAndAssertNoRecords ensures that PollFetches() returns 0 records and there are
// no buffered records in fetchers. Since some records are discarded in the PollFetches(),
// we may have to call it multiple times to process all buffered records that need to be
// discarded.
func pollFetchesAndAssertNoRecords(t *testing.T, fetchers *concurrentFetchers) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// If there are no buffered records, we can skip the polling at all.
	if fetchers.BufferedRecords() == 0 {
		return
	}

	for {
		fetches, returnCtx := fetchers.PollFetches(ctx)
		if errors.Is(returnCtx.Err(), context.DeadlineExceeded) {
			break
		}

		// We always expect that PollFetches() returns zero records.
		assert.Len(t, fetches.Records(), 0)

		// If there are no buffered records, we're good. We can end the assertion.
		if fetchers.BufferedRecords() == 0 {
			return
		}
	}

	// We stopped polling fetches. We have to make sure there are no buffered records.
	if !assert.Zero(t, fetchers.BufferedRecords(), "expected there aren't any buffered records") {
		fetches, _ := fetchers.PollFetches(ctx)
		t.Logf("%#v", fetches)
	}
}

type waiterFunc func()

func (w waiterFunc) Wait() { w() }

type refresherFunc func()

func (r refresherFunc) ForceMetadataRefresh() { r() }

func TestFetchWant_MaxBytes(t *testing.T) {
	testCases := map[string]struct {
		fw       fetchWant
		expected int32
	}{
		"small fetch": {
			fw: fetchWant{
				startOffset:             0,
				endOffset:               10,
				estimatedBytesPerRecord: 100,
			},
			expected: 1_000_000, // minimum fetch size
		},
		"medium fetch": {
			fw: fetchWant{
				startOffset:             0,
				endOffset:               1000,
				estimatedBytesPerRecord: 1000,
			},
			expected: 1_050_000, // 1000 * 1000 * 1.05
		},
		"huge fetch with huge bytes per record; overflow risk": {
			fw: fetchWant{
				startOffset:             0,
				endOffset:               2 << 31,
				estimatedBytesPerRecord: 2 << 30,
			},
			expected: math.MaxInt32,
		},
		"negative product due to overflow": {
			fw: fetchWant{
				startOffset:             0,
				endOffset:               math.MaxInt64,
				estimatedBytesPerRecord: math.MaxInt32,
			},
			expected: math.MaxInt32,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := tc.fw.MaxBytes()
			assert.Equal(t, tc.expected, result)
			assert.GreaterOrEqual(t, result, int32(0), "MaxBytes should never return negative values")
		})
	}
}

func TestFetchWant_UpdateBytesPerRecord(t *testing.T) {
	baseWant := fetchWant{
		startOffset:             100,
		endOffset:               200,
		estimatedBytesPerRecord: 1000,
	}

	testCases := map[string]struct {
		lastFetchBytes         int
		lastFetchRecords       int
		expectedBytesPerRecord int
	}{
		"similar to estimate": {
			lastFetchBytes:         10000,
			lastFetchRecords:       10,
			expectedBytesPerRecord: 1000,
		},
		"much larger than estimate": {
			lastFetchBytes:         100000,
			lastFetchRecords:       10,
			expectedBytesPerRecord: 2800,
		},
		"much smaller than estimate": {
			lastFetchBytes:         1000,
			lastFetchRecords:       10,
			expectedBytesPerRecord: 820,
		},
		"risk of overflow": {
			lastFetchBytes:         math.MaxInt64,
			lastFetchRecords:       1,
			expectedBytesPerRecord: math.MaxInt64/5 + int(float64(baseWant.estimatedBytesPerRecord)*0.8),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := baseWant.UpdateBytesPerRecord(tc.lastFetchBytes, tc.lastFetchRecords)

			assert.Equal(t, baseWant.startOffset, result.startOffset, "startOffset should not change")
			assert.Equal(t, baseWant.endOffset, result.endOffset, "endOffset should not change")

			// Check the new bytes per record estimation. Because of large numbers and floats we allow for 0.1% error.
			assert.InEpsilon(t, tc.expectedBytesPerRecord, result.estimatedBytesPerRecord, 0.001)

			// Verify MaxBytes() doesn't overflow or return negative values
			maxBytes := result.MaxBytes()
			assert.GreaterOrEqual(t, maxBytes, int32(0), "MaxBytes should never return negative values")
			assert.LessOrEqual(t, maxBytes, int32(math.MaxInt32), "MaxBytes should never exceed MaxInt32")
		})
	}
}

func TestFindGapsInRecords(t *testing.T) {
	tests := map[string]struct {
		records            []*kgo.Record
		lastReturnedOffset int64
		want               []offsetRange
	}{
		"no gaps": {
			records: []*kgo.Record{
				{Offset: 1},
				{Offset: 2},
				{Offset: 3},
			},
			lastReturnedOffset: 0,
			want:               nil,
		},
		"single gap": {
			records: []*kgo.Record{
				{Offset: 5},
			},
			lastReturnedOffset: 2,
			want: []offsetRange{
				{start: 3, end: 5},
			},
		},
		"multiple gaps": {
			records: []*kgo.Record{
				{Offset: 3},
				{Offset: 7},
				{Offset: 10},
			},
			lastReturnedOffset: 1,
			want: []offsetRange{
				{start: 2, end: 3},
				{start: 4, end: 7},
				{start: 8, end: 10},
			},
		},
		"empty records": {
			records:            []*kgo.Record{},
			lastReturnedOffset: 5,
			want:               nil,
		},
		"gap at start": {
			records: []*kgo.Record{
				{Offset: 10},
				{Offset: 11},
			},
			lastReturnedOffset: 5,
			want: []offsetRange{
				{start: 6, end: 10},
			},
		},
		"gap at start and middle": {
			records: []*kgo.Record{
				{Offset: 10},
				{Offset: 11},
				{Offset: 15},
				{Offset: 16},
			},
			lastReturnedOffset: 5,
			want: []offsetRange{
				{start: 6, end: 10},
				{start: 12, end: 15},
			},
		},
		"negative gap at start is ignored": {
			records: []*kgo.Record{
				{Offset: 5},
				{Offset: 6},
			},
			lastReturnedOffset: 10,
			want:               []offsetRange(nil),
		},
		"-1 start offset is ignored": {
			records: []*kgo.Record{
				{Offset: 5},
				{Offset: 6},
			},
			lastReturnedOffset: -1,
			want:               []offsetRange(nil),
		},
		"-1 start offset is ignored, but not the rest of the gaps": {
			records: []*kgo.Record{
				{Offset: 5},
				{Offset: 6},
				{Offset: 10},
			},
			lastReturnedOffset: -1,
			want: []offsetRange{
				{start: 7, end: 10},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			fetches := kgo.Fetches{{
				Topics: []kgo.FetchTopic{{
					Topic: "t1",
					Partitions: []kgo.FetchPartition{{
						Partition: 1,
						Records:   tc.records,
					}},
				}},
			}}

			got := findGapsInRecords(fetches, tc.lastReturnedOffset)
			assert.Equal(t, tc.want, got)
		})
	}
}
