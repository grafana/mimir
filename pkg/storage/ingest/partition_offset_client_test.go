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
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestPartitionOffsetClient_FetchLastProducedOffset(t *testing.T) {
	const (
		numPartitions = 1
		topicName     = "test"
		partitionID   = int32(0)
	)

	var (
		ctx    = context.Background()
		logger = log.NewNopLogger()
	)

	t.Run("should return the last produced offset, or -1 if the partition is empty", func(t *testing.T) {
		t.Parallel()

		var (
			_, clusterAddr = testkafka.CreateCluster(t, numPartitions, topicName)
			kafkaCfg       = createTestKafkaConfig(clusterAddr, topicName)
			client         = createTestKafkaClient(t, kafkaCfg)
			reg            = prometheus.NewPedanticRegistry()
			reader         = newPartitionOffsetClient(client, topicName, reg, logger)
		)

		offset, err := reader.FetchLastProducedOffset(ctx, partitionID)
		require.NoError(t, err)
		assert.Equal(t, int64(-1), offset)

		// Write the 1st message.
		produceRecord(ctx, t, client, topicName, partitionID, []byte("message 1"))

		offset, err = reader.FetchLastProducedOffset(ctx, partitionID)
		require.NoError(t, err)
		assert.Equal(t, int64(0), offset)

		// Write the 2nd message.
		produceRecord(ctx, t, client, topicName, partitionID, []byte("message 2"))

		offset, err = reader.FetchLastProducedOffset(ctx, partitionID)
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
			cluster, clusterAddr = testkafka.CreateCluster(t, numPartitions, topicName)
			kafkaCfg             = createTestKafkaConfig(clusterAddr, topicName)
			client               = createTestKafkaClient(t, kafkaCfg)
			reg                  = prometheus.NewPedanticRegistry()
			reader               = newPartitionOffsetClient(client, topicName, reg, logger)

			firstRequest         = atomic.NewBool(true)
			firstRequestReceived = make(chan struct{})
			firstRequestTimeout  = time.Second
		)

		// Write some messages.
		produceRecord(ctx, t, client, topicName, partitionID, []byte("message 1"))
		produceRecord(ctx, t, client, topicName, partitionID, []byte("message 2"))
		expectedOffset := int64(1)

		// Slow down the 1st ListOffsets request.
		cluster.ControlKey(int16(kmsg.ListOffsets), func(kmsg.Request) (kmsg.Response, error, bool) {
			if firstRequest.CompareAndSwap(true, false) {
				close(firstRequestReceived)
				time.Sleep(2 * firstRequestTimeout)
			}
			return nil, nil, false
		})

		wg := sync.WaitGroup{}

		// Run the 1st FetchLastProducedOffset() with a timeout which is expected to expire
		// before the request will succeed.
		runAsync(&wg, func() {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, firstRequestTimeout)
			defer cancel()

			_, err := reader.FetchLastProducedOffset(ctxWithTimeout, partitionID)
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})

		// Run a 2nd FetchLastProducedOffset() once the 1st request is received. This request
		// is expected to succeed.
		runAsyncAfter(&wg, firstRequestReceived, func() {
			offset, err := reader.FetchLastProducedOffset(ctx, partitionID)
			require.NoError(t, err)
			assert.Equal(t, expectedOffset, offset)
		})

		wg.Wait()
	})

	t.Run("should honor the configured retry timeout", func(t *testing.T) {
		t.Parallel()

		cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)

		// Configure a short retry timeout.
		kafkaCfg := createTestKafkaConfig(clusterAddr, topicName)
		kafkaCfg.LastProducedOffsetRetryTimeout = time.Second

		client := createTestKafkaClient(t, kafkaCfg)
		reg := prometheus.NewPedanticRegistry()
		reader := newPartitionOffsetClient(client, topicName, reg, logger)

		// Make the ListOffsets request failing.
		actualTries := atomic.NewInt64(0)
		cluster.ControlKey(int16(kmsg.ListOffsets), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			actualTries.Inc()
			return nil, errors.New("mocked error"), true
		})

		startTime := time.Now()
		_, err := reader.FetchLastProducedOffset(ctx, partitionID)
		elapsedTime := time.Since(startTime)

		require.Error(t, err)

		// Ensure the retry timeout has been honored.
		toleranceSeconds := 0.5
		assert.InDelta(t, kafkaCfg.LastProducedOffsetRetryTimeout.Seconds(), elapsedTime.Seconds(), toleranceSeconds)

		// Ensure the request was retried.
		assert.Greater(t, actualTries.Load(), int64(1))
	})
}

func TestPartitionOffsetClient_FetchPartitionStartOffset(t *testing.T) {
	const (
		numPartitions = 1
		topicName     = "test"
		partitionID   = int32(0)
	)

	var (
		ctx    = context.Background()
		logger = log.NewNopLogger()
	)

	t.Run("should return the partition start offset", func(t *testing.T) {
		t.Parallel()

		var (
			_, clusterAddr = testkafka.CreateCluster(t, numPartitions, topicName)
			kafkaCfg       = createTestKafkaConfig(clusterAddr, topicName)
			client         = createTestKafkaClient(t, kafkaCfg)
			reg            = prometheus.NewPedanticRegistry()
			reader         = newPartitionOffsetClient(client, topicName, reg, logger)
		)

		offset, err := reader.FetchPartitionStartOffset(ctx, partitionID)
		require.NoError(t, err)
		assert.Equal(t, int64(0), offset)

		// Write the 1st record.
		produceRecord(ctx, t, client, topicName, partitionID, []byte("record 1"))

		offset, err = reader.FetchPartitionStartOffset(ctx, partitionID)
		require.NoError(t, err)
		assert.Equal(t, int64(0), offset)

		// Write the 2nd record.
		produceRecord(ctx, t, client, topicName, partitionID, []byte("record 2"))

		offset, err = reader.FetchPartitionStartOffset(ctx, partitionID)
		require.NoError(t, err)
		assert.Equal(t, int64(0), offset)

		// Delete the 1st record.
		adminClient := kadm.NewClient(client)
		advancePartitionStartTo := kadm.Offsets{}
		advancePartitionStartTo.Add(kadm.Offset{Topic: topicName, Partition: partitionID, At: 1})
		_, err = adminClient.DeleteRecords(ctx, advancePartitionStartTo)
		require.NoError(t, err)
		t.Log("advanced partition start offset to 1")

		offset, err = reader.FetchPartitionStartOffset(ctx, partitionID)
		require.NoError(t, err)
		assert.Equal(t, int64(1), offset)

		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_reader_partition_start_offset_failures_total Total number of failed requests to get the partition start offset.
			# TYPE cortex_ingest_storage_reader_partition_start_offset_failures_total counter
			cortex_ingest_storage_reader_partition_start_offset_failures_total{partition="0"} 0

			# HELP cortex_ingest_storage_reader_partition_start_offset_requests_total Total number of requests issued to get the partition start offset.
			# TYPE cortex_ingest_storage_reader_partition_start_offset_requests_total counter
			cortex_ingest_storage_reader_partition_start_offset_requests_total{partition="0"} 4
		`), "cortex_ingest_storage_reader_partition_start_offset_requests_total",
			"cortex_ingest_storage_reader_partition_start_offset_failures_total"))
	})

	t.Run("should honor context deadline and not fail other in-flight requests issued while the canceled one was still running", func(t *testing.T) {
		t.Parallel()

		var (
			cluster, clusterAddr = testkafka.CreateCluster(t, numPartitions, topicName)
			kafkaCfg             = createTestKafkaConfig(clusterAddr, topicName)
			client               = createTestKafkaClient(t, kafkaCfg)
			reg                  = prometheus.NewPedanticRegistry()
			reader               = newPartitionOffsetClient(client, topicName, reg, logger)

			firstRequest         = atomic.NewBool(true)
			firstRequestReceived = make(chan struct{})
			firstRequestTimeout  = time.Second
		)

		// Write 2 records.
		produceRecord(ctx, t, client, topicName, partitionID, []byte("record 1"))
		produceRecord(ctx, t, client, topicName, partitionID, []byte("record 2"))
		t.Log("produced 2 records")

		// Delete the 1st record.
		adminClient := kadm.NewClient(client)
		advancePartitionStartTo := kadm.Offsets{}
		advancePartitionStartTo.Add(kadm.Offset{Topic: topicName, Partition: partitionID, At: 1})
		_, err := adminClient.DeleteRecords(ctx, advancePartitionStartTo)
		require.NoError(t, err)
		t.Log("advanced partition start offset to 1")

		expectedStartOffset := int64(1)

		// Slow down the 1st ListOffsets request.
		cluster.ControlKey(int16(kmsg.ListOffsets), func(kmsg.Request) (kmsg.Response, error, bool) {
			if firstRequest.CompareAndSwap(true, false) {
				close(firstRequestReceived)
				time.Sleep(2 * firstRequestTimeout)
			}
			return nil, nil, false
		})

		wg := sync.WaitGroup{}

		// Run the 1st FetchPartitionStartOffset() with a timeout which is expected to expire
		// before the request will succeed.
		runAsync(&wg, func() {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, firstRequestTimeout)
			defer cancel()

			_, err := reader.FetchPartitionStartOffset(ctxWithTimeout, partitionID)
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})

		// Run a 2nd FetchPartitionStartOffset() once the 1st request is received. This request
		// is expected to succeed.
		runAsyncAfter(&wg, firstRequestReceived, func() {
			offset, err := reader.FetchPartitionStartOffset(ctx, partitionID)
			require.NoError(t, err)
			assert.Equal(t, expectedStartOffset, offset)
		})

		wg.Wait()
	})

	t.Run("should honor the configured retry timeout", func(t *testing.T) {
		t.Parallel()

		cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)

		// Configure a short retry timeout.
		kafkaCfg := createTestKafkaConfig(clusterAddr, topicName)
		kafkaCfg.LastProducedOffsetRetryTimeout = time.Second

		client := createTestKafkaClient(t, kafkaCfg)
		reg := prometheus.NewPedanticRegistry()
		reader := newPartitionOffsetClient(client, topicName, reg, logger)

		// Make the ListOffsets request failing.
		actualTries := atomic.NewInt64(0)
		cluster.ControlKey(int16(kmsg.ListOffsets), func(kmsg.Request) (kmsg.Response, error, bool) {
			cluster.KeepControl()
			actualTries.Inc()
			return nil, errors.New("mocked error"), true
		})

		startTime := time.Now()
		_, err := reader.FetchPartitionStartOffset(ctx, partitionID)
		elapsedTime := time.Since(startTime)

		require.Error(t, err)

		// Ensure the retry timeout has been honored.
		toleranceSeconds := 0.5
		assert.InDelta(t, kafkaCfg.LastProducedOffsetRetryTimeout.Seconds(), elapsedTime.Seconds(), toleranceSeconds)

		// Ensure the request was retried.
		assert.Greater(t, actualTries.Load(), int64(1))
	})
}
