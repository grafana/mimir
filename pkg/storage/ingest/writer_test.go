// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestWriter_WriteSync(t *testing.T) {
	const (
		topicName     = "test"
		numPartitions = 1
		partitionID   = 0
		tenantID      = "user-1"
	)

	var (
		ctx         = context.Background()
		multiSeries = []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1"), mockPreallocTimeseries("series_2")}
		series1     = []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}
		series2     = []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_2")}
		series3     = []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3")}
	)

	t.Run("should block until data has been committed to storage", func(t *testing.T) {
		t.Parallel()

		cluster, clusterAddr := createTestCluster(t, numPartitions, topicName)
		writer, reg := createTestWriter(t, clusterAddr, topicName, createTestWriterConfig())

		produceRequestProcessed := atomic.NewBool(false)

		cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
			// Add a delay, so that if WriteSync() will not wait then the test will fail.
			time.Sleep(time.Second)
			produceRequestProcessed.Store(true)

			return nil, nil, false
		})

		err := writer.WriteSync(ctx, partitionID, tenantID, multiSeries, nil, mimirpb.API)
		require.NoError(t, err)

		// Ensure it was processed before returning.
		assert.True(t, produceRequestProcessed.Load())

		// Read back from Kafka.
		consumer, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topicName: {int32(partitionID): kgo.NewOffset().AtStart()}}))
		require.NoError(t, err)
		t.Cleanup(consumer.Close)

		fetchCtx, cancel := context.WithTimeout(ctx, time.Second)
		t.Cleanup(cancel)

		fetches := consumer.PollFetches(fetchCtx)
		require.NoError(t, fetches.Err())
		require.Len(t, fetches.Records(), 1)
		assert.Equal(t, []byte(tenantID), fetches.Records()[0].Key)

		received := mimirpb.WriteRequest{}
		require.NoError(t, received.Unmarshal(fetches.Records()[0].Value))
		require.Len(t, received.Timeseries, len(multiSeries))

		for idx, expected := range multiSeries {
			assert.Equal(t, expected.Labels, received.Timeseries[idx].Labels)
			assert.Equal(t, expected.Samples, received.Timeseries[idx].Samples)
		}

		// Check metrics.
		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_ingest_storage_writer_sent_bytes_total Total number of bytes sent to the ingest storage.
			# TYPE cortex_ingest_storage_writer_sent_bytes_total counter
			cortex_ingest_storage_writer_sent_bytes_total %d
		`, len(fetches.Records()[0].Value))), "cortex_ingest_storage_writer_sent_bytes_total"))
	})

	t.Run("should interrupt the WriteSync() on context cancelled but other concurrent requests should not fail", func(t *testing.T) {
		t.Parallel()

		var (
			firstRequest         = atomic.NewBool(true)
			firstRequestReceived = make(chan struct{})

			receivedBatchesLengthMx sync.Mutex
			receivedBatchesLength   []int
		)

		cluster, clusterAddr := createTestCluster(t, numPartitions, topicName)
		writer, _ := createTestWriter(t, clusterAddr, topicName, createTestWriterConfig())

		cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
			numRecords, err := getProduceRequestRecordsCount(request.(*kmsg.ProduceRequest))
			require.NoError(t, err)

			receivedBatchesLengthMx.Lock()
			receivedBatchesLength = append(receivedBatchesLength, numRecords)
			receivedBatchesLengthMx.Unlock()

			if firstRequest.CompareAndSwap(true, false) {
				close(firstRequestReceived)

				// Introduce a delay on the 1st Produce.
				time.Sleep(time.Second)
			}

			return nil, nil, false
		})

		wg := sync.WaitGroup{}
		wg.Add(3)

		// Write the first record, which is expected to be sent immediately.
		runAsync(&wg, func() {
			require.NoError(t, writer.WriteSync(ctx, partitionID, tenantID, series1, nil, mimirpb.API))
		})

		// Once the 1st Produce request is received by the server but still processing (there's a 1s sleep),
		// issue two more requests. One with a short context timeout (expected to expire before the next Produce
		// request will be sent) and one with no timeout.
		runAsyncAfter(&wg, firstRequestReceived, func() {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()

			require.Equal(t, context.DeadlineExceeded, writer.WriteSync(ctxWithTimeout, partitionID, tenantID, series2, nil, mimirpb.API))
		})

		runAsyncAfter(&wg, firstRequestReceived, func() {
			require.NoError(t, writer.WriteSync(ctx, partitionID, tenantID, series3, nil, mimirpb.API))
		})

		wg.Wait()

		// Cancelling the context doesn't actually prevent that data from being sent to the wire.
		assert.Equal(t, []int{1, 2}, receivedBatchesLength)
	})

	t.Run("should batch multiple subsequent records together while sending the previous batches to Kafka once max in-flight Produce requests limit has been reached", func(t *testing.T) {
		t.Parallel()

		var (
			firstRequest         = atomic.NewBool(true)
			firstRequestReceived = make(chan struct{})

			receivedBatchesLengthMx sync.Mutex
			receivedBatchesLength   []int
		)

		// Allow only 1 in-flight Produce request in this test, to easily reproduce the scenario.
		writerCfg := createTestWriterConfig()
		writerCfg.kafkaMaxInflightProduceRequests = 1

		cluster, clusterAddr := createTestCluster(t, numPartitions, topicName)
		writer, _ := createTestWriter(t, clusterAddr, topicName, writerCfg)

		cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
			if firstRequest.CompareAndSwap(true, false) {
				// The produce request has been received by Kafka, so we can fire the next requests.
				close(firstRequestReceived)

				// Inject a slowdown on the 1st Produce request received by Kafka to let next produce
				// records to buffer in the batch on the client side.
				time.Sleep(time.Second)
			}

			numRecords, err := getProduceRequestRecordsCount(request.(*kmsg.ProduceRequest))
			require.NoError(t, err)

			receivedBatchesLengthMx.Lock()
			receivedBatchesLength = append(receivedBatchesLength, numRecords)
			receivedBatchesLengthMx.Unlock()

			return nil, nil, false
		})

		wg := sync.WaitGroup{}
		wg.Add(3)

		runAsync(&wg, func() {
			require.NoError(t, writer.WriteSync(ctx, partitionID, tenantID, series1, nil, mimirpb.API))
		})

		runAsyncAfter(&wg, firstRequestReceived, func() {
			require.NoError(t, writer.WriteSync(ctx, partitionID, tenantID, series2, nil, mimirpb.API))
		})

		runAsyncAfter(&wg, firstRequestReceived, func() {
			// Ensure the 3rd call to WriteSync() is issued slightly after the 2nd one,
			// otherwise records may be batched just because of concurrent append to it
			// and not because it's waiting for the 1st call to complete.
			time.Sleep(100 * time.Millisecond)

			require.NoError(t, writer.WriteSync(ctx, partitionID, tenantID, series3, nil, mimirpb.API))
		})

		wg.Wait()

		// We expect that the next 2 records have been appended to the next batch.
		assert.Equal(t, []int{1, 2}, receivedBatchesLength)
	})

	t.Run("should return error on non existing partition", func(t *testing.T) {
		t.Parallel()

		_, clusterAddr := createTestCluster(t, numPartitions, topicName)
		writer, _ := createTestWriter(t, clusterAddr, topicName, createTestWriterConfig())

		// Write to a non-existing partition.
		err := writer.WriteSync(ctx, 100, tenantID, multiSeries, nil, mimirpb.API)
		require.Error(t, err)
	})

	t.Run("should return an error and stop retrying sending a record once the write timeout expires", func(t *testing.T) {
		t.Parallel()

		cluster, clusterAddr := createTestCluster(t, numPartitions, topicName)
		writerCfg := createTestWriterConfig()
		writer, _ := createTestWriter(t, clusterAddr, topicName, writerCfg)

		cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
			// Keep failing every request.
			cluster.KeepControl()
			return nil, errors.New("mock error"), true
		})

		startTime := time.Now()
		require.Equal(t, kgo.ErrRecordTimeout, writer.WriteSync(ctx, partitionID, tenantID, series1, nil, mimirpb.API))
		elapsedTime := time.Since(startTime)

		assert.Greater(t, elapsedTime, writerCfg.KafkaWriteTimeout/2)
		assert.Less(t, elapsedTime, writerCfg.KafkaWriteTimeout*3) // High tolerance because the client does a backoff and timeout is evaluated after the backoff.
	})

	// This test documents how the Kafka client works. It's not what we ideally want, but it's how it works.
	t.Run("should fail all buffered records and close the connection on timeout while waiting for Produce response", func(t *testing.T) {
		t.Parallel()

		cluster, clusterAddr := createTestCluster(t, numPartitions, topicName)
		writerCfg := createTestWriterConfig()
		writer, _ := createTestWriter(t, clusterAddr, topicName, writerCfg)

		var (
			firstRequest         = atomic.NewBool(true)
			firstRequestReceived = make(chan struct{})
		)

		cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
			if firstRequest.CompareAndSwap(true, false) {
				// The produce request has been received by Kafka, so we can fire the next request.
				close(firstRequestReceived)

				// Inject a slowdown on the 1st Produce request received by Kafka.
				// NOTE: the slowdown is 1s longer than the client timeout.
				time.Sleep(writerCfg.KafkaWriteTimeout + writerRequestTimeoutOverhead + time.Second)
			}

			return nil, nil, false
		})

		wg := sync.WaitGroup{}
		wg.Add(2)

		// The 1st request is expected to fail because Kafka will take longer than the configured timeout.
		runAsync(&wg, func() {
			startTime := time.Now()
			require.Equal(t, kgo.ErrRecordTimeout, writer.WriteSync(ctx, partitionID, tenantID, series1, nil, mimirpb.API))
			elapsedTime := time.Since(startTime)

			// It should take nearly the client's write timeout.
			expectedElapsedTime := writerCfg.KafkaWriteTimeout + writerRequestTimeoutOverhead
			tolerance := time.Second
			assert.Greater(t, elapsedTime, expectedElapsedTime-tolerance)
			assert.Less(t, elapsedTime, expectedElapsedTime+tolerance)
		})

		// The 2nd request is fired while the 1st is still executing, but will fail anyone because the previous
		// failure causes all subsequent buffered records to fail too.
		runAsync(&wg, func() {
			<-firstRequestReceived

			// Wait 500ms less than the client timeout.
			delay := 500 * time.Millisecond
			time.Sleep(writerCfg.KafkaWriteTimeout + writerRequestTimeoutOverhead - delay)

			startTime := time.Now()
			require.Equal(t, kgo.ErrRecordTimeout, writer.WriteSync(ctx, partitionID, tenantID, series2, nil, mimirpb.API))
			elapsedTime := time.Since(startTime)

			// We expect to fail once the previous request fails, so it should take nearly the client's write timeout
			// minus the artificial delay introduced above.
			tolerance := time.Second
			assert.Less(t, elapsedTime, delay+tolerance)
		})

		wg.Wait()
	})
}

func mockPreallocTimeseries(metricName string) mimirpb.PreallocTimeseries {
	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: metricName}},
			Samples: []mimirpb.Sample{{TimestampMs: 1, Value: 2}},
		},
	}
}

func getProduceRequestRecordsCount(req *kmsg.ProduceRequest) (int, error) {
	count := 0

	for _, topic := range req.Topics {
		for _, partition := range topic.Partitions {
			b := kmsg.RecordBatch{}
			if err := b.ReadFrom(partition.Records); err != nil {
				return 0, err
			}
			count += int(b.NumRecords)
		}
	}

	return count, nil
}

func runAsync(wg *sync.WaitGroup, fn func()) {
	go func() {
		defer wg.Done()
		fn()
	}()
}

func runAsyncAfter(wg *sync.WaitGroup, waitFor chan struct{}, fn func()) {
	go func() {
		defer wg.Done()
		<-waitFor
		fn()
	}()
}

func createTestWriterConfig() WriterConfig {
	cfg := WriterConfig{}
	flagext.DefaultValues(&cfg)
	cfg.KafkaWriteTimeout = 2 * time.Second
	return cfg
}

func createTestCluster(t *testing.T, numPartitions int32, topicName string) (*kfake.Cluster, string) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(numPartitions, topicName))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	addrs := cluster.ListenAddrs()
	require.Len(t, addrs, 1)

	return cluster, addrs[0]
}

func createTestWriter(t *testing.T, clusterAddr, topicName string, writerCfg WriterConfig) (*Writer, prometheus.Gatherer) {
	reg := prometheus.NewPedanticRegistry()

	kafkaCfg := KafkaConfig{}
	flagext.DefaultValues(&kafkaCfg)
	kafkaCfg.KafkaAddress = clusterAddr
	kafkaCfg.KafkaTopic = topicName

	writer := NewWriter(kafkaCfg, writerCfg, test.NewTestingLogger(t), reg)
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), writer))
	})

	return writer, reg
}
