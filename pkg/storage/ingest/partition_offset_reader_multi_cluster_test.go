// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func partitionZeroIDs(_ context.Context) ([]int32, error) { return []int32{0}, nil }

// createAdditionalTestTopic creates a topic on an already-running fake cluster (testkafka.CreateCluster
// only seeds a single topic).
func createAdditionalTestTopic(t *testing.T, clusterAddr, topic string, numPartitions int32) {
	client := createTestKafkaClient(t, createTestKafkaConfig(clusterAddr, topic))
	_, err := kadm.NewClient(client).CreateTopic(context.Background(), numPartitions, 1, nil, topic)
	require.NoError(t, err)
}

func TestMultiClusterOffsetsReader_WaitNextFetchLastProducedOffset(t *testing.T) {
	const (
		numPartitions = int32(1)
		topic0        = "rc-0"
		topic1        = "rc-1"
	)

	ctx := context.Background()
	logger := log.NewNopLogger()

	_, addr0 := testkafka.CreateCluster(t, numPartitions, topic0)
	createAdditionalTestTopic(t, addr0, topic1, numPartitions)
	_, addr1 := testkafka.CreateCluster(t, numPartitions, topic0)
	createAdditionalTestTopic(t, addr1, topic1, numPartitions)

	cfg0 := createTestKafkaConfig(addr0, topic0)
	cfg0.LastProducedOffsetPollInterval = 100 * time.Millisecond
	cfg1 := createTestKafkaConfig(addr1, topic0)
	cfg1.LastProducedOffsetPollInterval = 100 * time.Millisecond

	w0 := createTestKafkaClient(t, cfg0)
	w1 := createTestKafkaClient(t, cfg1)

	// Write compartment 0: read compartment 0 last offset 2, read compartment 1 last offset 0.
	produceRecord(ctx, t, w0, topic0, 0, []byte("a"))
	produceRecord(ctx, t, w0, topic0, 0, []byte("b"))
	produceRecord(ctx, t, w0, topic0, 0, []byte("c"))
	produceRecord(ctx, t, w0, topic1, 0, []byte("d"))
	// Write compartment 1: read compartment 0 empty (-1), read compartment 1 last offset 1.
	produceRecord(ctx, t, w1, topic1, 0, []byte("e"))
	produceRecord(ctx, t, w1, topic1, 0, []byte("f"))

	reader, err := NewMultiClusterOffsetsReader(
		[]KafkaConfig{cfg0, cfg1},
		[]string{topic0, topic1},
		[]GetPartitionIDsFunc{partitionZeroIDs, partitionZeroIDs},
		"query-frontend",
		prometheus.NewPedanticRegistry(),
		logger,
	)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, reader)) })

	offsets, err := reader.WaitNextFetchLastProducedOffset(ctx)
	require.NoError(t, err)
	assert.Equal(t, kmeta.TopicsPartitionsOffsets{
		topic0: {0: kmeta.NewMultiClusterPartitionOffsets([]int64{2, -1})},
		topic1: {0: kmeta.NewMultiClusterPartitionOffsets([]int64{0, 1})},
	}, offsets)

	// One Kafka client per write compartment, owned by the multi-cluster reader, and they are distinct.
	require.Len(t, reader.clients, 2)
	assert.NotSame(t, reader.clients[0], reader.clients[1])
}

// TestMultiClusterOffsetsReader_SingleFlight verifies that concurrent callers share a single per-poll
// fetch (one ListOffsets request per cluster), instead of each call fanning out its own requests.
func TestMultiClusterOffsetsReader_SingleFlight(t *testing.T) {
	const (
		numPartitions = int32(1)
		topic         = "rc-0"
		pollInterval  = time.Second
		numWaiters    = 20
	)

	ctx := context.Background()
	logger := log.NewNopLogger()

	cluster0, addr0 := testkafka.CreateCluster(t, numPartitions, topic)
	cluster1, addr1 := testkafka.CreateCluster(t, numPartitions, topic)

	cfg0 := createTestKafkaConfig(addr0, topic)
	cfg0.LastProducedOffsetPollInterval = pollInterval
	cfg1 := createTestKafkaConfig(addr1, topic)
	cfg1.LastProducedOffsetPollInterval = pollInterval

	w0 := createTestKafkaClient(t, cfg0)
	w1 := createTestKafkaClient(t, cfg1)
	produceRecord(ctx, t, w0, topic, 0, []byte("a")) // Cluster 0: last offset 0.
	produceRecord(ctx, t, w1, topic, 0, []byte("b")) // Cluster 1: ...
	produceRecord(ctx, t, w1, topic, 0, []byte("c")) // ... last offset 1.

	// Count the ListOffsets requests per cluster (set after producing, so only the offset fetches count),
	// and signal once each cluster has served its first (startup) fetch so the waiters below all attach to
	// the same subsequent poll.
	installCounter := func(cluster *kfake.Cluster, counter *atomic.Int64, firstReceived chan struct{}) {
		var once sync.Once
		cluster.ControlKey(int16(kmsg.ListOffsets), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			counter.Inc()
			once.Do(func() { close(firstReceived) })
			return nil, nil, false
		})
	}
	list0, list1 := atomic.NewInt64(0), atomic.NewInt64(0)
	first0, first1 := make(chan struct{}), make(chan struct{})
	installCounter(cluster0, list0, first0)
	installCounter(cluster1, list1, first1)

	reader, err := NewMultiClusterOffsetsReader(
		[]KafkaConfig{cfg0, cfg1},
		[]string{topic},
		[]GetPartitionIDsFunc{partitionZeroIDs},
		"query-frontend",
		prometheus.NewPedanticRegistry(),
		logger,
	)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, reader)) })

	// Wait for both clusters' startup fetch so the concurrent waiters share the next poll.
	<-first0
	<-first1

	results := make([]kmeta.TopicsPartitionsOffsets, numWaiters)
	errs := make([]error, numWaiters)
	var wg sync.WaitGroup
	for i := 0; i < numWaiters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = reader.WaitNextFetchLastProducedOffset(ctx)
		}(i)
	}
	wg.Wait()

	expected := kmeta.TopicsPartitionsOffsets{
		topic: {0: kmeta.NewMultiClusterPartitionOffsets([]int64{0, 1})},
	}
	for i := 0; i < numWaiters; i++ {
		require.NoError(t, errs[i])
		assert.Equal(t, expected, results[i])
	}

	// Single-flight: the many concurrent waiters shared a single fetch per poll, so each cluster issued only
	// a couple of ListOffsets requests (startup + the awaited poll), not one per waiter.
	assert.LessOrEqual(t, list0.Load(), int64(3))
	assert.LessOrEqual(t, list1.Load(), int64(3))
}

// TestMultiClusterOffsetsReader_ClosesClientsOnStop verifies that the Kafka clients the reader owns are
// closed when it stops.
func TestMultiClusterOffsetsReader_ClosesClientsOnStop(t *testing.T) {
	const (
		numPartitions = int32(1)
		topic         = "rc-0"
	)

	ctx := context.Background()
	logger := log.NewNopLogger()

	_, addr0 := testkafka.CreateCluster(t, numPartitions, topic)
	_, addr1 := testkafka.CreateCluster(t, numPartitions, topic)

	reader, err := NewMultiClusterOffsetsReader(
		[]KafkaConfig{createTestKafkaConfig(addr0, topic), createTestKafkaConfig(addr1, topic)},
		[]string{topic},
		[]GetPartitionIDsFunc{partitionZeroIDs},
		"query-frontend",
		prometheus.NewPedanticRegistry(),
		logger,
	)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, reader))

	clients := reader.clients
	require.Len(t, clients, 2)
	require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

	// Once closed, any further request on the owned clients fails.
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for _, client := range clients {
		assert.Error(t, client.Ping(pingCtx))
	}
}

func TestNewMultiClusterOffsetsReader_Validation(t *testing.T) {
	logger := log.NewNopLogger()
	cfg := createTestKafkaConfig("localhost:0", "rc-0")

	t.Run("no Kafka clusters", func(t *testing.T) {
		_, err := NewMultiClusterOffsetsReader(nil, []string{"rc-0"}, []GetPartitionIDsFunc{partitionZeroIDs}, "query-frontend", nil, logger)
		require.Error(t, err)
	})

	t.Run("mismatched topics and partition ID functions", func(t *testing.T) {
		_, err := NewMultiClusterOffsetsReader([]KafkaConfig{cfg}, []string{"rc-0", "rc-1"}, []GetPartitionIDsFunc{partitionZeroIDs}, "query-frontend", nil, logger)
		require.Error(t, err)
	})
}
