// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
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

		// The 1st WaitNextFetchLastProducedOffset() is called before the service start so it's expected
		// to wait the result of the 1st request.
		runAsync(&wg, func() {
			actual, err := reader.WaitNextFetchLastProducedOffset(ctx)
			require.NoError(t, err)
			assert.Equal(t, int64(1), actual)
		})

		// The 2nd WaitNextFetchLastProducedOffset() is called while the 1st request is running, so it's expected
		// to wait the result of the 2nd request.
		runAsyncAfter(&wg, firstRequestReceived, func() {
			actual, err := reader.WaitNextFetchLastProducedOffset(ctx)
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
