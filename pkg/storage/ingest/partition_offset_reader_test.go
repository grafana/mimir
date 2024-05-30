// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestPartitionOffsetReader(t *testing.T) {
	const (
		numPartitions = 1
		topicName     = "test"
		partitionID   = int32(0)
	)

	var (
		ctx = context.Background()
	)

	t.Run("should notify waiting goroutines when stopped", func(t *testing.T) {
		var (
			_, clusterAddr = testkafka.CreateCluster(t, numPartitions, topicName)
			kafkaCfg       = createTestKafkaConfig(clusterAddr, topicName)
		)

		// Run with a very high polling interval, so that it will never run in this test.
		reader := newPartitionOffsetReader(createTestKafkaClient(t, kafkaCfg), topicName, partitionID, time.Hour, nil, log.NewNopLogger())
		require.NoError(t, services.StartAndAwaitRunning(ctx, reader))

		// Run few goroutines waiting for the last produced offset.
		wg := sync.WaitGroup{}

		for i := 0; i < 2; i++ {
			runAsync(&wg, func() {
				_, err := reader.WaitNextFetchLastProducedOffset(ctx)
				assert.Equal(t, errPartitionOffsetReaderStopped, err)
			})
		}

		// Stop the reader.
		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

		// At the point we expect the waiting goroutines to be unblocked.
		wg.Wait()

		// The next call to WaitNextFetchLastProducedOffset() should return immediately.
		_, err := reader.WaitNextFetchLastProducedOffset(ctx)
		assert.Equal(t, errPartitionOffsetReaderStopped, err)
	})
}

func TestPartitionOffsetReader_WaitNextFetchLastProducedOffset(t *testing.T) {
	const (
		numPartitions = 1
		topicName     = "test"
		partitionID   = int32(0)
		pollInterval  = time.Second
	)

	var (
		ctx    = context.Background()
		logger = log.NewNopLogger()
	)

	t.Run("should wait the result of the next request issued", func(t *testing.T) {
		var (
			cluster, clusterAddr = testkafka.CreateCluster(t, numPartitions, topicName)
			kafkaCfg             = createTestKafkaConfig(clusterAddr, topicName)
			client               = createTestKafkaClient(t, kafkaCfg)
			reader               = newPartitionOffsetReader(client, topicName, partitionID, pollInterval, nil, logger)

			lastOffset            = atomic.NewInt64(1)
			firstRequestReceived  = make(chan struct{})
			secondRequestReceived = make(chan struct{})
		)

		cluster.ControlKey(int16(kmsg.ListOffsets), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()

			switch lastOffset.Load() {
			case 1:
				close(firstRequestReceived)
			case 2:
				close(secondRequestReceived)
			}

			// Mock the response so that we can increase the offset each time.
			req := kreq.(*kmsg.ListOffsetsRequest)
			res := req.ResponseKind().(*kmsg.ListOffsetsResponse)
			res.Topics = []kmsg.ListOffsetsResponseTopic{{
				Topic: topicName,
				Partitions: []kmsg.ListOffsetsResponseTopicPartition{{
					Partition: partitionID,
					ErrorCode: 0,
					Offset:    lastOffset.Inc(),
				}},
			}}

			return res, nil, true
		})

		wg := sync.WaitGroup{}

		// The 1st WaitNextFetchLastProducedOffset() is called before the service starts.
		// The service fetches the offset once at startup, so it's expected that the first wait
		// to wait the result of the 2nd request.
		// If we don't do synchronisation, then it's also possible that we fit in the first request, but we synchronise to avoid flaky tests
		runAsyncAfter(&wg, firstRequestReceived, func() {
			actual, err := reader.WaitNextFetchLastProducedOffset(ctx)
			require.NoError(t, err)
			assert.Equal(t, int64(2), actual)
		})

		// The 2nd WaitNextFetchLastProducedOffset() is called while the 1st is running, so it's expected
		// to wait the result of the 3rd request.
		runAsyncAfter(&wg, secondRequestReceived, func() {
			actual, err := reader.WaitNextFetchLastProducedOffset(ctx)
			require.NoError(t, err)
			assert.Equal(t, int64(3), actual)
		})

		// Now we can start the service.
		require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		})

		wg.Wait()
	})

	t.Run("should immediately return if the context gets canceled", func(t *testing.T) {
		var (
			_, clusterAddr = testkafka.CreateCluster(t, numPartitions, topicName)
			kafkaCfg       = createTestKafkaConfig(clusterAddr, topicName)
			client         = createTestKafkaClient(t, kafkaCfg)
		)

		// Create the reader but do NOT start it, so that the "last produced offset" will be never fetched.
		reader := newPartitionOffsetReader(client, topicName, partitionID, pollInterval, nil, logger)

		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()

		_, err := reader.WaitNextFetchLastProducedOffset(canceledCtx)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestTopicOffsetsReader(t *testing.T) {
	const (
		numPartitions = 1
		topicName     = "test"
	)

	var (
		ctx             = context.Background()
		allPartitionIDs = func(_ context.Context) ([]int32, error) { return []int32{0}, nil }
	)

	t.Run("should notify waiting goroutines when stopped", func(t *testing.T) {
		var (
			_, clusterAddr = testkafka.CreateCluster(t, numPartitions, topicName)
			kafkaCfg       = createTestKafkaConfig(clusterAddr, topicName)
		)

		// Run with a very high polling interval, so that it will never run in this test.
		reader := NewTopicOffsetsReader(createTestKafkaClient(t, kafkaCfg), topicName, allPartitionIDs, time.Hour, nil, log.NewNopLogger())
		require.NoError(t, services.StartAndAwaitRunning(ctx, reader))

		// Run few goroutines waiting for the last produced offset.
		wg := sync.WaitGroup{}

		for i := 0; i < 2; i++ {
			runAsync(&wg, func() {
				_, err := reader.WaitNextFetchLastProducedOffset(ctx)
				assert.Equal(t, errPartitionOffsetReaderStopped, err)
			})
		}

		// Stop the reader.
		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

		// At the point we expect the waiting goroutines to be unblocked.
		wg.Wait()

		// The next call to WaitNextFetchLastProducedOffset() should return immediately.
		_, err := reader.WaitNextFetchLastProducedOffset(ctx)
		assert.Equal(t, errPartitionOffsetReaderStopped, err)
	})
}

func TestTopicOffsetsReader_WaitNextFetchLastProducedOffset(t *testing.T) {
	const (
		numPartitions = 1
		topicName     = "test"
		pollInterval  = time.Second
	)

	var (
		ctx             = context.Background()
		logger          = log.NewNopLogger()
		allPartitionIDs = func(_ context.Context) ([]int32, error) { return []int32{0}, nil }
	)

	t.Run("should wait the result of the next request issued", func(t *testing.T) {
		var (
			cluster, clusterAddr = testkafka.CreateCluster(t, numPartitions, topicName)
			kafkaCfg             = createTestKafkaConfig(clusterAddr, topicName)
			client               = createTestKafkaClient(t, kafkaCfg)
			reader               = NewTopicOffsetsReader(client, topicName, allPartitionIDs, pollInterval, nil, logger)

			lastOffset            = atomic.NewInt64(1)
			firstRequestReceived  = make(chan struct{})
			secondRequestReceived = make(chan struct{})
		)

		cluster.ControlKey(int16(kmsg.ListOffsets), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()

			switch lastOffset.Load() {
			case 1:
				close(firstRequestReceived)
			case 3:
				close(secondRequestReceived)
			}

			// Mock the response so that we can increase the offset each time.
			req := kreq.(*kmsg.ListOffsetsRequest)
			res := req.ResponseKind().(*kmsg.ListOffsetsResponse)
			res.Topics = []kmsg.ListOffsetsResponseTopic{{
				Topic: topicName,
				Partitions: []kmsg.ListOffsetsResponseTopicPartition{{
					Partition: 0,
					ErrorCode: 0,
					Offset:    lastOffset.Inc(),
				}, {
					Partition: 1,
					ErrorCode: 0,
					Offset:    lastOffset.Inc(),
				}},
			}}

			return res, nil, true
		})

		wg := sync.WaitGroup{}

		// The 1st WaitNextFetchLastProducedOffset() is called before the service starts.
		// The service fetches the offset once at startup, so it's expected that the first wait
		// to wait the result of the 2nd request.
		// If we don't do synchronisation, then it's also possible that we fit in the first request, but we synchronise to avoid flaky tests
		runAsyncAfter(&wg, firstRequestReceived, func() {
			actual, err := reader.WaitNextFetchLastProducedOffset(ctx)
			require.NoError(t, err)
			assert.Equal(t, map[int32]int64{0: int64(3), 1: int64(4)}, actual)
		})

		// The 2nd WaitNextFetchLastProducedOffset() is called while the 1st is running, so it's expected
		// to wait the result of the 3rd request.
		runAsyncAfter(&wg, secondRequestReceived, func() {
			actual, err := reader.WaitNextFetchLastProducedOffset(ctx)
			require.NoError(t, err)
			assert.Equal(t, map[int32]int64{0: int64(5), 1: int64(6)}, actual)
		})

		// Now we can start the service.
		require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		})

		wg.Wait()
	})

	t.Run("should immediately return if the context gets canceled", func(t *testing.T) {
		var (
			_, clusterAddr = testkafka.CreateCluster(t, numPartitions, topicName)
			kafkaCfg       = createTestKafkaConfig(clusterAddr, topicName)
			client         = createTestKafkaClient(t, kafkaCfg)
		)

		// Create the reader but do NOT start it, so that the "last produced offset" will be never fetched.
		reader := NewTopicOffsetsReader(client, topicName, allPartitionIDs, pollInterval, nil, logger)

		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()

		_, err := reader.WaitNextFetchLastProducedOffset(canceledCtx)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestGenericPartitionReader_Caching(t *testing.T) {
	logger := log.NewNopLogger()

	t.Run("should initialize with fetched offset", func(t *testing.T) {
		ctx := context.Background()
		mockFetch := func(context.Context) (int64, error) {
			return 42, nil
		}

		reader := newGenericOffsetReader[int64](mockFetch, time.Second, logger)
		require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		})

		offset, err := reader.CachedOffset()
		assert.NoError(t, err)
		assert.Equal(t, int64(42), offset)
	})

	t.Run("should cache error from initial fetch", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := fmt.Errorf("fetch error")
		mockFetch := func(context.Context) (int64, error) {
			return 0, expectedErr
		}

		reader := newGenericOffsetReader[int64](mockFetch, time.Second, logger)
		require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		})

		offset, err := reader.CachedOffset()
		assert.ErrorIs(t, err, expectedErr)
		assert.Equal(t, int64(0), offset)
	})

	t.Run("should update cache on poll interval", func(t *testing.T) {
		ctx := context.Background()
		fetchCount := 0
		fetchChan := make(chan struct{}, 3) // Buffer size of 3 to allow multiple fetches
		mockFetch := func(ctx context.Context) (int64, error) {
			fetchCount++
			select {
			case <-ctx.Done():
			case fetchChan <- struct{}{}:
			}
			return int64(fetchCount), nil
		}

		reader := newGenericOffsetReader[int64](mockFetch, 10*time.Millisecond, logger)
		require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		})

		// Wait for at least two fetches to complete and have their results cached.
		<-fetchChan
		<-fetchChan
		<-fetchChan

		offset, err := reader.CachedOffset()
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, offset, int64(2), "Offset should have been updated at least once")
	})

	t.Run("should handle context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		mockFetch := func(context.Context) (int64, error) {
			return 42, nil
		}

		reader := newGenericOffsetReader[int64](mockFetch, time.Second, logger)
		require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
		t.Cleanup(func() {
			cancel()
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), reader))
		})

		// The cached offset should be available
		offset, err := reader.CachedOffset()
		assert.NoError(t, err)
		assert.Equal(t, int64(42), offset)
	})

	t.Run("should handle concurrent access", func(t *testing.T) {
		ctx := context.Background()
		mockFetch := func(context.Context) (int64, error) {
			return 42, nil
		}

		reader := newGenericOffsetReader[int64](mockFetch, time.Second, logger)
		require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		})

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				offset, err := reader.CachedOffset()
				assert.NoError(t, err)
				assert.Equal(t, int64(42), offset)
			}()
		}
		wg.Wait()
	})
}
