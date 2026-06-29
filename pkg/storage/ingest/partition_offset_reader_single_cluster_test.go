// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestSingleClusterTopicOffsetsReader(t *testing.T) {
	const (
		numPartitions = 1
		topicName     = "test"
	)

	allPartitionIDs := func(_ context.Context) ([]int32, error) { return []int32{0}, nil }

	t.Run("should notify waiting goroutines when stopped", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()

			var vnet kfake.VirtualNetwork
			_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName, testkafka.WithVirtualNetwork(&vnet))
			kafkaCfg := createTestKafkaConfig(clusterAddr, topicName)
			kafkaCfg.Dialer = vnet.DialContext

			// Run with a very high polling interval, so that it will never run in this test.
			kafkaCfg.LastProducedOffsetPollInterval = time.Hour
			reader, err := NewSingleClusterTopicOffsetsReader(kafkaCfg, topicName, allPartitionIDs, "query-frontend", prometheus.NewPedanticRegistry(), log.NewNopLogger())
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, reader))

			// Run few goroutines waiting for the last produced offset.
			wg := sync.WaitGroup{}

			for range 2 {
				runAsync(&wg, func() {
					_, err := reader.WaitNextFetchLastProducedOffset(ctx)
					assert.Equal(t, errPartitionOffsetReaderStopped, err)
				})
			}

			// Wait until both goroutines are durably blocked waiting for the next produced offset.
			synctest.Wait()

			// Stop the reader.
			require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

			// At the point we expect the waiting goroutines to be unblocked.
			wg.Wait()

			// The next call to WaitNextFetchLastProducedOffset() should return immediately.
			_, err = reader.WaitNextFetchLastProducedOffset(ctx)
			assert.Equal(t, errPartitionOffsetReaderStopped, err)
		})
	})
}

func TestSingleClusterTopicOffsetsReader_WaitNextFetchLastProducedOffset(t *testing.T) {
	const (
		numPartitions = 1
		topicName     = "test"
		pollInterval  = time.Second
	)

	var (
		logger          = log.NewNopLogger()
		allPartitionIDs = func(_ context.Context) ([]int32, error) { return []int32{0}, nil }
	)

	t.Run("should wait the result of the next request issued", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()

			var vnet kfake.VirtualNetwork
			cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName, testkafka.WithVirtualNetwork(&vnet))
			kafkaCfg := createTestKafkaConfig(clusterAddr, topicName)
			kafkaCfg.Dialer = vnet.DialContext
			kafkaCfg.LastProducedOffsetPollInterval = pollInterval
			reader, err := NewSingleClusterTopicOffsetsReader(kafkaCfg, topicName, allPartitionIDs, "query-frontend", prometheus.NewPedanticRegistry(), logger)
			require.NoError(t, err)

			var (
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
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), reader))
			})

			wg.Wait()
		})
	})

	t.Run("should immediately return if the context gets canceled", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()

			var vnet kfake.VirtualNetwork
			_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName, testkafka.WithVirtualNetwork(&vnet))
			kafkaCfg := createTestKafkaConfig(clusterAddr, topicName)
			kafkaCfg.Dialer = vnet.DialContext
			// Use a very high poll interval: the reader fetches once at startup, then never again, so the "next"
			// offset never arrives and WaitNextFetchLastProducedOffset can only return via the context.
			kafkaCfg.LastProducedOffsetPollInterval = time.Hour
			reader, err := NewSingleClusterTopicOffsetsReader(kafkaCfg, topicName, allPartitionIDs, "query-frontend", prometheus.NewPedanticRegistry(), logger)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), reader))
			})

			canceledCtx, cancel := context.WithCancel(ctx)
			cancel()

			_, err = reader.WaitNextFetchLastProducedOffset(canceledCtx)
			assert.ErrorIs(t, err, context.Canceled)
		})
	})
}
