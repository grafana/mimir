// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
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
			_, clusterAddr = createTestCluster(t, numPartitions, topicName)
			kafkaCfg       = createTestKafkaConfig(clusterAddr, topicName)
		)

		// Run with a very high polling frequency, so that it will never run in this test.
		reader := newPartitionOffsetReader(createTestKafkaClient(t, kafkaCfg), topicName, partitionID, time.Hour, nil, log.NewNopLogger())
		require.NoError(t, services.StartAndAwaitRunning(ctx, reader))

		// Run few goroutines waiting for the last produced offset.
		wg := sync.WaitGroup{}
		wg.Add(2)

		for i := 0; i < 2; i++ {
			runAsync(&wg, func() {
				_, err := reader.WaitLastProducedOffset(ctx)
				assert.Equal(t, errPartitionOffsetReaderStopped, err)
			})
		}

		// Stop the reader.
		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

		// At the point we expect the waiting goroutines to be unblocked.
		wg.Wait()

		// The next call to WaitLastProducedOffset() should return immediately.
		_, err := reader.WaitLastProducedOffset(ctx)
		assert.Equal(t, errPartitionOffsetReaderStopped, err)
	})
}

func TestPartitionOffsetReader_getLastProducedOffset(t *testing.T) {
	const (
		numPartitions = 1
		userID        = "user-1"
		topicName     = "test"
		partitionID   = int32(0)
		pollFrequency = time.Second
	)

	var (
		ctx        = context.Background()
		logger     = log.NewNopLogger()
		series1Req = &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}, Source: mimirpb.API}
		series2Req = &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_2")}, Source: mimirpb.API}
	)

	t.Run("should return the last produced offset, or -1 if the partition is empty", func(t *testing.T) {
		t.Parallel()

		var (
			_, clusterAddr = createTestCluster(t, numPartitions, topicName)
			kafkaCfg       = createTestKafkaConfig(clusterAddr, topicName)
			writer, _      = createTestWriter(t, kafkaCfg)
			client         = createTestKafkaClient(t, kafkaCfg)
			reg            = prometheus.NewPedanticRegistry()
			reader         = newPartitionOffsetReader(client, topicName, partitionID, pollFrequency, reg, logger)
		)

		offset, err := reader.getLastProducedOffset(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(-1), offset)

		// Write the 1st message.
		require.NoError(t, writer.WriteSync(ctx, partitionID, userID, series1Req))

		offset, err = reader.getLastProducedOffset(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), offset)

		// Write the 2nd message.
		require.NoError(t, writer.WriteSync(ctx, partitionID, userID, series2Req))

		offset, err = reader.getLastProducedOffset(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(1), offset)

		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_reader_last_produced_offset_failures_total Total number of failed requests to get the last produced offset.
			# TYPE cortex_ingest_storage_reader_last_produced_offset_failures_total counter
			cortex_ingest_storage_reader_last_produced_offset_failures_total{partition="0"} 0

			# HELP cortex_ingest_storage_reader_last_produced_offset_requests_total Total number of requests issued to get the last produced offset.
			# TYPE cortex_ingest_storage_reader_last_produced_offset_requests_total counter
			cortex_ingest_storage_reader_last_produced_offset_requests_total{partition="0"} 3
		`), "cortex_ingest_storage_reader_last_produced_offset_requests_total",
			"cortex_ingest_storage_reader_last_produced_offset_failures_total"))
	})

	t.Run("should honor context deadline and not fail other in-flight requests issued while the canceled one was still running", func(t *testing.T) {
		t.Parallel()

		var (
			cluster, clusterAddr = createTestCluster(t, numPartitions, topicName)
			kafkaCfg             = createTestKafkaConfig(clusterAddr, topicName)
			writer, _            = createTestWriter(t, kafkaCfg)
			client               = createTestKafkaClient(t, kafkaCfg)
			reg                  = prometheus.NewPedanticRegistry()
			reader               = newPartitionOffsetReader(client, topicName, partitionID, pollFrequency, reg, logger)

			firstRequest         = atomic.NewBool(true)
			firstRequestReceived = make(chan struct{})
			firstRequestTimeout  = time.Second
		)

		// Write some messages.
		require.NoError(t, writer.WriteSync(ctx, partitionID, userID, series1Req))
		require.NoError(t, writer.WriteSync(ctx, partitionID, userID, series2Req))
		expectedOffset := int64(1)

		// Slow down the 1st ListOffsets request.
		cluster.ControlKey(int16(kmsg.ListOffsets), func(request kmsg.Request) (kmsg.Response, error, bool) {
			if firstRequest.CompareAndSwap(true, false) {
				close(firstRequestReceived)
				time.Sleep(2 * firstRequestTimeout)
			}
			return nil, nil, false
		})

		wg := sync.WaitGroup{}
		wg.Add(2)

		// Run the 1st getLastProducedOffset() with a timeout which is expected to expire
		// before the request will succeed.
		runAsync(&wg, func() {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, firstRequestTimeout)
			defer cancel()

			_, err := reader.getLastProducedOffset(ctxWithTimeout)
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})

		// Run a 2nd getLastProducedOffset() once the 1st request is received. This request
		// is expected to succeed.
		runAsyncAfter(&wg, firstRequestReceived, func() {
			offset, err := reader.getLastProducedOffset(ctx)
			require.NoError(t, err)
			assert.Equal(t, expectedOffset, offset)
		})

		wg.Wait()
	})

	t.Run("should honor the configured retry timeout", func(t *testing.T) {
		t.Parallel()

		cluster, clusterAddr := createTestCluster(t, numPartitions, topicName)

		// Configure a short retry timeout.
		kafkaCfg := createTestKafkaConfig(clusterAddr, topicName)
		kafkaCfg.LastProducedOffsetRetryTimeout = time.Second

		client := createTestKafkaClient(t, kafkaCfg)
		reg := prometheus.NewPedanticRegistry()
		reader := newPartitionOffsetReader(client, topicName, partitionID, pollFrequency, reg, logger)

		// Make the ListOffsets request failing.
		actualTries := atomic.NewInt64(0)
		cluster.ControlKey(int16(kmsg.ListOffsets), func(request kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			actualTries.Inc()
			return nil, errors.New("mocked error"), true
		})

		startTime := time.Now()
		_, err := reader.getLastProducedOffset(ctx)
		elapsedTime := time.Since(startTime)

		require.Error(t, err)

		// Ensure the retry timeout has been honored.
		toleranceSeconds := 0.5
		assert.InDelta(t, kafkaCfg.LastProducedOffsetRetryTimeout.Seconds(), elapsedTime.Seconds(), toleranceSeconds)

		// Ensure the request was retried.
		assert.Greater(t, actualTries.Load(), int64(1))
	})
}

func TestPartitionOffsetReader_WaitLastProducedOffset(t *testing.T) {
	const (
		numPartitions = 1
		topicName     = "test"
		partitionID   = int32(0)
		pollFrequency = time.Second
	)

	var (
		ctx    = context.Background()
		logger = log.NewNopLogger()
	)

	t.Run("should wait the result of the next request issued", func(t *testing.T) {
		var (
			cluster, clusterAddr = createTestCluster(t, numPartitions, topicName)
			kafkaCfg             = createTestKafkaConfig(clusterAddr, topicName)
			client               = createTestKafkaClient(t, kafkaCfg)
			reader               = newPartitionOffsetReader(client, topicName, partitionID, pollFrequency, nil, logger)

			lastOffset           = atomic.NewInt64(1)
			firstRequestReceived = make(chan struct{})
		)

		cluster.ControlKey(int16(kmsg.ListOffsets), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()

			if lastOffset.Load() == 1 {
				close(firstRequestReceived)
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
		wg.Add(2)

		// The 1st WaitLastProducedOffset() is called before the service start so it's expected
		// to wait the result of the 1st request.
		runAsync(&wg, func() {
			actual, err := reader.WaitLastProducedOffset(ctx)
			require.NoError(t, err)
			assert.Equal(t, int64(1), actual)
		})

		// The 2nd WaitLastProducedOffset() is called while the 1st request is running, so it's expected
		// to wait the result of the 2nd request.
		runAsyncAfter(&wg, firstRequestReceived, func() {
			actual, err := reader.WaitLastProducedOffset(ctx)
			require.NoError(t, err)
			assert.Equal(t, int64(2), actual)
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
			_, clusterAddr = createTestCluster(t, numPartitions, topicName)
			kafkaCfg       = createTestKafkaConfig(clusterAddr, topicName)
			client         = createTestKafkaClient(t, kafkaCfg)
		)

		// Create the reader but do NOT start it, so that the "last produced offset" will be never fetched.
		reader := newPartitionOffsetReader(client, topicName, partitionID, pollFrequency, nil, logger)

		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()

		_, err := reader.WaitLastProducedOffset(canceledCtx)
		assert.ErrorIs(t, err, context.Canceled)

	})
}
