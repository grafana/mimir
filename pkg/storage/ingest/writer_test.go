// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestMain(m *testing.M) {
	test.VerifyNoLeakTestMain(m)
}

func TestWriter_WriteSync(t *testing.T) {
	runForEachKafkaBackend(t, testWriter_WriteSync)
}

func testWriter_WriteSync(t *testing.T, backend string) {
	const (
		topicName     = "test"
		numPartitions = 10
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

	t.Run("should block until data has been committed to storage (WriteRequest stored in a single record)", func(t *testing.T) {
		t.Parallel()

		cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		cfg := createTestKafkaConfigForBackend(backend, clusterAddr, topicName)
		writer, reg := createTestWriter(t, cfg)

		produceRequestProcessed := atomic.NewBool(false)

		cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
			// Add a delay, so that if WriteSync() will not wait then the test will fail.
			time.Sleep(time.Second)
			produceRequestProcessed.Store(true)

			return nil, nil, false
		})

		req := &mimirpb.WriteRequest{Timeseries: multiSeries, Metadata: nil, Source: mimirpb.API}
		inputSize := req.Size()
		err := writer.WriteSync(ctx, topicName, partitionID, tenantID, req)
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

		received := deserializeRecord(t, fetches.Records()[0])
		require.Len(t, received.Timeseries, len(multiSeries))

		for idx, expected := range multiSeries {
			assert.Equal(t, expected.Labels, received.Timeseries[idx].Labels)
			assert.Equal(t, expected.Samples, received.Timeseries[idx].Samples)
		}

		// Check metrics.
		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_ingest_storage_writer_input_bytes_total Total number of bytes in write requests before conversion to the Kafka record format.
			# TYPE cortex_ingest_storage_writer_input_bytes_total counter
			cortex_ingest_storage_writer_input_bytes_total %d

			# HELP cortex_ingest_storage_writer_sent_bytes_total Total number of bytes produced to the Kafka backend.
			# TYPE cortex_ingest_storage_writer_sent_bytes_total counter
			cortex_ingest_storage_writer_sent_bytes_total %d

			# HELP cortex_ingest_storage_writer_records_per_write_request The number of records a single per-partition write request has been split into.
			# TYPE cortex_ingest_storage_writer_records_per_write_request histogram
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="1"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="2"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="4"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="8"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="16"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="32"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="64"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="128"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="+Inf"} 1
			cortex_ingest_storage_writer_records_per_write_request_sum 1
			cortex_ingest_storage_writer_records_per_write_request_count 1

			# HELP cortex_ingest_storage_writer_produce_records_enqueued_total Total number of Kafka records enqueued to be sent to the Kafka backend (includes records that fail to be successfully sent to the Kafka backend).
			# TYPE cortex_ingest_storage_writer_produce_records_enqueued_total counter
			cortex_ingest_storage_writer_produce_records_enqueued_total 1
		`, inputSize, len(fetches.Records()[0].Value))),
			"cortex_ingest_storage_writer_input_bytes_total",
			"cortex_ingest_storage_writer_sent_bytes_total",
			"cortex_ingest_storage_writer_records_per_write_request",
			"cortex_ingest_storage_writer_produce_records_enqueued_total"))

		assertHistogramSampleCount(t, reg, "cortex_ingest_storage_writer_serialize_duration_seconds", 1)
	})

	t.Run("should block until data has been committed to storage (WriteRequest stored in multiple records)", func(t *testing.T) {
		t.Parallel()

		cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)

		// Customize the max record size to force splitting the WriteRequest into two records.
		expectedReq := &mimirpb.WriteRequest{Timeseries: multiSeries, Metadata: nil, Source: mimirpb.API}
		cfg := createTestKafkaConfigForBackend(backend, clusterAddr, topicName)
		cfg.ProducerMaxRecordSizeBytes = int(float64(expectedReq.Size()) * 0.8)

		writer, reg := createTestWriter(t, cfg)

		produceRequestProcessed := atomic.NewBool(false)
		cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
			// Add a delay, so that if WriteSync() will not wait then the test will fail.
			time.Sleep(time.Second)
			produceRequestProcessed.Store(true)

			return nil, nil, false
		})

		err := writer.WriteSync(ctx, topicName, partitionID, tenantID, expectedReq)
		require.NoError(t, err)

		// Ensure it was processed before returning.
		assert.True(t, produceRequestProcessed.Load())

		// Read back from Kafka.
		consumer, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topicName: {int32(partitionID): kgo.NewOffset().AtStart()}}))
		require.NoError(t, err)
		t.Cleanup(consumer.Close)

		fetchCtx, cancel := context.WithTimeout(ctx, time.Second)
		t.Cleanup(cancel)

		// Wait until we received 2 records. The timeout on fetchCtx guarantees it will not wait indefinitely.
		var records []*kgo.Record

		for len(records) < 2 {
			fetches := consumer.PollFetches(fetchCtx)
			require.NoError(t, fetches.Err())

			records = append(records, fetches.Records()...)
		}

		require.Len(t, records, 2)
		assert.Equal(t, []byte(tenantID), records[0].Key)
		assert.Equal(t, []byte(tenantID), records[1].Key)

		actualReq1 := deserializeRecord(t, records[0])
		actualReq2 := deserializeRecord(t, records[1])

		mergedTimeseries := append(actualReq1.Timeseries, actualReq2.Timeseries...)
		assert.Equal(t, expectedReq.Timeseries, mergedTimeseries)
		assert.Equal(t, expectedReq.Source, actualReq1.Source)

		// Check metrics.
		expectedSentBytes := len(records[0].Value) + len(records[1].Value)

		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_ingest_storage_writer_input_bytes_total Total number of bytes in write requests before conversion to the Kafka record format.
			# TYPE cortex_ingest_storage_writer_input_bytes_total counter
			cortex_ingest_storage_writer_input_bytes_total %d

			# HELP cortex_ingest_storage_writer_sent_bytes_total Total number of bytes produced to the Kafka backend.
			# TYPE cortex_ingest_storage_writer_sent_bytes_total counter
			cortex_ingest_storage_writer_sent_bytes_total %d

			# HELP cortex_ingest_storage_writer_records_per_write_request The number of records a single per-partition write request has been split into.
			# TYPE cortex_ingest_storage_writer_records_per_write_request histogram
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="1"} 0
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="2"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="4"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="8"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="16"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="32"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="64"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="128"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="+Inf"} 1
			cortex_ingest_storage_writer_records_per_write_request_sum 2
			cortex_ingest_storage_writer_records_per_write_request_count 1

			# HELP cortex_ingest_storage_writer_produce_records_enqueued_total Total number of Kafka records enqueued to be sent to the Kafka backend (includes records that fail to be successfully sent to the Kafka backend).
			# TYPE cortex_ingest_storage_writer_produce_records_enqueued_total counter
			cortex_ingest_storage_writer_produce_records_enqueued_total 2
		`, expectedReq.Size(), expectedSentBytes)),
			"cortex_ingest_storage_writer_input_bytes_total",
			"cortex_ingest_storage_writer_sent_bytes_total",
			"cortex_ingest_storage_writer_records_per_write_request",
			"cortex_ingest_storage_writer_produce_records_enqueued_total"))

		assertHistogramSampleCount(t, reg, "cortex_ingest_storage_writer_serialize_duration_seconds", 1)
	})

	t.Run("should write to the requested partition", func(t *testing.T) {
		t.Parallel()

		seriesPerPartition := map[int32][]mimirpb.PreallocTimeseries{
			0: series1,
			1: series2,
		}

		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		config := createTestKafkaConfigForBackend(backend, clusterAddr, topicName)
		writer, reg := createTestWriter(t, config)

		// Write to partitions.
		for partitionID, series := range seriesPerPartition {
			err := writer.WriteSync(ctx, topicName, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series, Metadata: nil, Source: mimirpb.API})
			require.NoError(t, err)
		}

		// Read back from Kafka.
		for partitionID, expectedSeries := range seriesPerPartition {
			consumer, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topicName: {partitionID: kgo.NewOffset().AtStart()}}))
			require.NoError(t, err)
			t.Cleanup(consumer.Close)

			fetchCtx, cancel := context.WithTimeout(ctx, time.Second)
			t.Cleanup(cancel)

			fetches := consumer.PollFetches(fetchCtx)
			require.NoError(t, fetches.Err())
			require.Len(t, fetches.Records(), 1)
			assert.Equal(t, []byte(tenantID), fetches.Records()[0].Key)

			received := deserializeRecord(t, fetches.Records()[0])
			require.Len(t, received.Timeseries, len(expectedSeries))

			for idx, expected := range expectedSeries {
				assert.Equal(t, expected.Labels, received.Timeseries[idx].Labels)
				assert.Equal(t, expected.Samples, received.Timeseries[idx].Samples)
			}
		}

		// Check metrics.
		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_writer_produce_records_enqueued_total Total number of Kafka records enqueued to be sent to the Kafka backend (includes records that fail to be successfully sent to the Kafka backend).
			# TYPE cortex_ingest_storage_writer_produce_records_enqueued_total counter
			cortex_ingest_storage_writer_produce_records_enqueued_total 2
		`), "cortex_ingest_storage_writer_produce_records_enqueued_total"))
	})

	t.Run("should interrupt the WriteSync() on context cancelled but other concurrent requests should not fail", func(t *testing.T) {
		t.Parallel()

		var (
			firstRequest         = atomic.NewBool(true)
			firstRequestReceived = make(chan struct{})

			receivedBatchesLengthMx sync.Mutex
			receivedBatchesLength   []int
		)

		cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		writer, _ := createTestWriter(t, createTestKafkaConfigForBackend(backend, clusterAddr, topicName))

		// Get the underlying Kafka client used by the writer.
		cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
			numRecords, err := getProduceRequestRecordsCount(request.(*kmsg.ProduceRequest))
			require.NoError(t, err)

			receivedBatchesLengthMx.Lock()
			receivedBatchesLength = append(receivedBatchesLength, numRecords)
			receivedBatchesLengthMx.Unlock()

			if firstRequest.CompareAndSwap(true, false) {
				close(firstRequestReceived)

				// Introduce a delay on the 1st Produce, keeping it in-flight long
				// enough for the test to observe the other records buffering behind it.
				time.Sleep(3 * time.Second)
			}

			return nil, nil, false
		})

		wg := sync.WaitGroup{}

		// Write the first record, which is expected to be sent immediately.
		runAsync(&wg, func() {
			assert.NoError(t, writer.WriteSync(ctx, topicName, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series1, Metadata: nil, Source: mimirpb.API}))
		})

		// Once the 1st Produce request is received by the server but still processing (there's a sleep),
		// issue two more requests. One with a short context timeout (expected to expire before the next Produce
		// request will be sent) and one with no timeout.

		runAsyncAfter(&wg, firstRequestReceived, func() {
			secondRequestCtx, cancelSecondRequest := context.WithTimeout(ctx, 10*time.Millisecond)
			t.Cleanup(cancelSecondRequest)

			assert.ErrorIs(t, writer.WriteSync(secondRequestCtx, topicName, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series2, Metadata: nil, Source: mimirpb.API}), context.DeadlineExceeded)
		})

		runAsyncAfter(&wg, firstRequestReceived, func() {
			// Wait until the 2nd request has been buffered, because we want this request to be buffered after it.
			// The nil check guards against the writer being torn down (which swaps the client to nil) if the
			// surrounding test fails and runs its cleanup while this goroutine is still polling.
			require.Eventually(t, func() bool {
				client := writer.client.Load()
				return client != nil && client.BufferedProduceRecords() == 2
			}, 2*time.Second, 10*time.Millisecond)

			assert.NoError(t, writer.WriteSync(ctx, topicName, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series3, Metadata: nil, Source: mimirpb.API}))
		})

		// Wait until all 3 requests have been buffered.
		require.Eventually(t, func() bool {
			client := writer.client.Load()
			return client != nil && client.BufferedProduceRecords() == 3
		}, 2*time.Second, 10*time.Millisecond)

		wg.Wait()

		// Cancelling the context doesn't actually prevent that data from being sent to the wire.
		require.Equal(t, []int{1, 2}, receivedBatchesLength)
	})

	t.Run("should batch multiple subsequent records together while sending the previous batches to Kafka once max in-flight Produce requests limit has been reached", func(t *testing.T) {
		t.Parallel()

		if backend == KafkaBackendWarpstream {
			t.Skipf("Warpstream client doesn't support a max in-flight Produce requests limit by design")
		}

		var (
			firstRequest         = atomic.NewBool(true)
			firstRequestReceived = make(chan struct{})

			receivedBatchesLengthMx sync.Mutex
			receivedBatchesLength   []int
		)

		cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)

		// Allow only 1 in-flight Produce request in this test, to easily reproduce the scenario.
		cfg := createTestKafkaConfigForBackend(backend, clusterAddr, topicName)
		cfg.MaxInflightProduceRequests = 1
		writer, _ := createTestWriter(t, cfg)

		cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
			if firstRequest.CompareAndSwap(true, false) {
				// The produce request has been received by Kafka, so we can fire the next requests.
				close(firstRequestReceived)

				// Inject a slowdown on the 1st Produce request received by Kafka to let next produce
				// records to buffer in the batch on the client side.
				time.Sleep(time.Second)
			}

			numRecords, err := getProduceRequestRecordsCount(request.(*kmsg.ProduceRequest))
			assert.NoError(t, err)

			receivedBatchesLengthMx.Lock()
			receivedBatchesLength = append(receivedBatchesLength, numRecords)
			receivedBatchesLengthMx.Unlock()

			return nil, nil, false
		})

		wg := sync.WaitGroup{}

		runAsync(&wg, func() {
			assert.NoError(t, writer.WriteSync(ctx, topicName, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series1, Metadata: nil, Source: mimirpb.API}))
		})

		runAsyncAfter(&wg, firstRequestReceived, func() {
			assert.NoError(t, writer.WriteSync(ctx, topicName, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series2, Metadata: nil, Source: mimirpb.API}))
		})

		runAsyncAfter(&wg, firstRequestReceived, func() {
			// Ensure the 3rd call to Write() is issued slightly after the 2nd one,
			// otherwise records may be batched just because of concurrent append to it
			// and not because it's waiting for the 1st call to complete.
			time.Sleep(100 * time.Millisecond)

			assert.NoError(t, writer.WriteSync(ctx, topicName, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series3, Metadata: nil, Source: mimirpb.API}))
		})

		wg.Wait()

		// We expect that the next 2 records have been appended to the next batch.
		require.Equal(t, []int{1, 2}, receivedBatchesLength)
	})

	t.Run("should return error on non existing partition", func(t *testing.T) {
		t.Parallel()

		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		writer, reg := createTestWriter(t, createTestKafkaConfigForBackend(backend, clusterAddr, topicName))

		// Write to a non-existing partition.
		err := writer.WriteSync(ctx, topicName, 100, tenantID, &mimirpb.WriteRequest{Timeseries: multiSeries, Metadata: nil, Source: mimirpb.API})
		require.Error(t, err)

		// Check metrics.
		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_writer_produce_records_enqueued_total Total number of Kafka records enqueued to be sent to the Kafka backend (includes records that fail to be successfully sent to the Kafka backend).
			# TYPE cortex_ingest_storage_writer_produce_records_enqueued_total counter
			cortex_ingest_storage_writer_produce_records_enqueued_total 1

			# HELP cortex_ingest_storage_writer_produce_records_failed_total Total number of Kafka records that failed to be sent to the Kafka backend.
			# TYPE cortex_ingest_storage_writer_produce_records_failed_total counter
			cortex_ingest_storage_writer_produce_records_failed_total{reason="other"} 1
		`),
			"cortex_ingest_storage_writer_produce_records_enqueued_total",
			"cortex_ingest_storage_writer_produce_records_failed_total"))
	})

	t.Run("should return an error and stop retrying sending a record once the write timeout expires", func(t *testing.T) {
		t.Parallel()

		cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		kafkaCfg := createTestKafkaConfigForBackend(backend, clusterAddr, topicName)
		writer, reg := createTestWriter(t, kafkaCfg)

		cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
			// Keep failing every request. Sleep per attempt so the retry loop
			// has something meaningful to elapse against.
			cluster.KeepControl()
			time.Sleep(kafkaCfg.WriteTimeout / 4)
			return nil, errors.New("mock error"), true
		})

		startTime := time.Now()
		require.ErrorIs(t, writer.WriteSync(ctx, topicName, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series1, Metadata: nil, Source: mimirpb.API}), kgo.ErrRecordTimeout)
		elapsedTime := time.Since(startTime)

		// Upper bound applies to both backends: WriteSync must give up within
		// a few WriteTimeouts. The lower bound only applies to the kafka
		// backend because the warpstream client's Hedger gives up as soon
		// as MaxHedgeAgents is exhausted across the candidate pool — which,
		// against this single-broker kfake cluster, is after one attempt.
		require.Less(t, elapsedTime, kafkaCfg.WriteTimeout*3)
		if backend == KafkaBackendKafka {
			require.Greater(t, elapsedTime, kafkaCfg.WriteTimeout/2)
		}

		// Check metrics.
		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_writer_produce_records_enqueued_total Total number of Kafka records enqueued to be sent to the Kafka backend (includes records that fail to be successfully sent to the Kafka backend).
			# TYPE cortex_ingest_storage_writer_produce_records_enqueued_total counter
			cortex_ingest_storage_writer_produce_records_enqueued_total 1

			# HELP cortex_ingest_storage_writer_produce_records_failed_total Total number of Kafka records that failed to be sent to the Kafka backend.
			# TYPE cortex_ingest_storage_writer_produce_records_failed_total counter
			cortex_ingest_storage_writer_produce_records_failed_total{reason="timeout"} 1
		`),
			"cortex_ingest_storage_writer_produce_records_enqueued_total",
			"cortex_ingest_storage_writer_produce_records_failed_total"))
	})

	// This test documents how the franz-go Kafka client works (cascading failures). It's not what we ideally want, but it's how it works.
	t.Run("should fail all buffered records and close the connection on timeout while waiting for Produce response (franz-go client)", func(t *testing.T) {
		t.Parallel()
		if backend != KafkaBackendKafka {
			t.Skipf("franz-go client specific behaviour")
		}

		cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		kafkaCfg := createTestKafkaConfigForBackend(backend, clusterAddr, topicName)
		writer, _ := createTestWriter(t, kafkaCfg)

		var (
			firstRequest         = atomic.NewBool(true)
			firstRequestReceived = make(chan struct{})
			wg                   = sync.WaitGroup{}
		)

		wg.Add(1)
		cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
			// Ensure the test waits for this too, since the client request will fail earlier
			// (if we don't wait, the test will end before this function and then goleak will
			// report a goroutine leak).
			defer wg.Done()

			if firstRequest.CompareAndSwap(true, false) {
				// The produce request has been received by Kafka, so we can fire the next request.
				close(firstRequestReceived)

				// Inject a slowdown on the 1st Produce request received by Kafka.
				// NOTE: the slowdown is 1s longer than the client timeout.
				time.Sleep(kafkaCfg.WriteTimeout + writerRequestTimeoutOverhead + time.Second)
			}

			return nil, nil, false
		})

		// The 1st request is expected to fail because Kafka will take longer than the configured timeout.
		runAsync(&wg, func() {
			startTime := time.Now()
			assert.ErrorIs(t, writer.WriteSync(ctx, topicName, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series1, Metadata: nil, Source: mimirpb.API}), kgo.ErrRecordTimeout)
			elapsedTime := time.Since(startTime)

			// It should take nearly the client's write timeout.
			expectedElapsedTime := kafkaCfg.WriteTimeout + writerRequestTimeoutOverhead
			assert.InDelta(t, expectedElapsedTime, elapsedTime, float64(time.Second))
		})

		// The 2nd request is fired while the 1st is still executing, but will fail anyway because the previous
		// failure causes all subsequent buffered records to fail too.
		runAsync(&wg, func() {
			<-firstRequestReceived

			// Wait 500ms less than the client timeout.
			delay := 500 * time.Millisecond
			time.Sleep(kafkaCfg.WriteTimeout + writerRequestTimeoutOverhead - delay)

			startTime := time.Now()
			assert.ErrorIs(t, writer.WriteSync(ctx, topicName, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series2, Metadata: nil, Source: mimirpb.API}), kgo.ErrRecordTimeout)
			elapsedTime := time.Since(startTime)

			// We expect to fail once the previous request fails, so it should take nearly the client's write timeout
			// minus the artificial delay introduced above.
			tolerance := time.Second
			assert.Less(t, elapsedTime, delay+tolerance)
		})

		wg.Wait()
	})

	t.Run("should return error if the WriteRequest contains a timeseries which is larger than the maximum allowed record data size", func(t *testing.T) {
		t.Parallel()

		req := &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				mockPreallocTimeseries(strings.Repeat("x", producerBatchMaxBytes)), // Huge, will fail to be written.
				mockPreallocTimeseries("series_1"),                                 // Small, will be successfully written.
			},
			Metadata: nil,
			Source:   mimirpb.API,
		}

		cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		writer, reg := createTestWriter(t, createTestKafkaConfigForBackend(backend, clusterAddr, topicName))

		produceRequestProcessed := atomic.NewBool(false)

		cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
			// Add a delay, so that if WriteSync() will not wait then the test will fail.
			time.Sleep(time.Second)
			produceRequestProcessed.Store(true)

			return nil, nil, false
		})

		err := writer.WriteSync(ctx, topicName, partitionID, tenantID, req)
		require.Equal(t, ErrWriteRequestDataItemTooLarge, err)

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

		received := deserializeRecord(t, fetches.Records()[0])

		// We expect that the small time series has been ingested, while the huge one has been discarded.
		require.Len(t, received.Timeseries, 1)
		assert.Equal(t, mockPreallocTimeseries("series_1"), received.Timeseries[0])

		// Check metrics. Since one record failed (too large), we don't track input/sent bytes
		// to keep cortex_ingest_storage_writer_sent_bytes_total and cortex_ingest_storage_writer_input_bytes_total
		// consistent with each other (both are only tracked on full success).
		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_writer_input_bytes_total Total number of bytes in write requests before conversion to the Kafka record format.
			# TYPE cortex_ingest_storage_writer_input_bytes_total counter
			cortex_ingest_storage_writer_input_bytes_total 0

			# HELP cortex_ingest_storage_writer_sent_bytes_total Total number of bytes produced to the Kafka backend.
			# TYPE cortex_ingest_storage_writer_sent_bytes_total counter
			cortex_ingest_storage_writer_sent_bytes_total 0

			# HELP cortex_ingest_storage_writer_records_per_write_request The number of records a single per-partition write request has been split into.
			# TYPE cortex_ingest_storage_writer_records_per_write_request histogram
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="1"} 0
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="2"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="4"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="8"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="16"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="32"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="64"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="128"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="+Inf"} 1
			cortex_ingest_storage_writer_records_per_write_request_sum 2
			cortex_ingest_storage_writer_records_per_write_request_count 1

			# HELP cortex_ingest_storage_writer_produce_records_enqueued_total Total number of Kafka records enqueued to be sent to the Kafka backend (includes records that fail to be successfully sent to the Kafka backend).
			# TYPE cortex_ingest_storage_writer_produce_records_enqueued_total counter
			cortex_ingest_storage_writer_produce_records_enqueued_total 2

			# HELP cortex_ingest_storage_writer_produce_records_failed_total Total number of Kafka records that failed to be sent to the Kafka backend.
			# TYPE cortex_ingest_storage_writer_produce_records_failed_total counter
			cortex_ingest_storage_writer_produce_records_failed_total{reason="record-too-large"} 1
		`),
			"cortex_ingest_storage_writer_input_bytes_total",
			"cortex_ingest_storage_writer_sent_bytes_total",
			"cortex_ingest_storage_writer_records_per_write_request",
			"cortex_ingest_storage_writer_produce_records_enqueued_total",
			"cortex_ingest_storage_writer_produce_records_failed_total"))
	})

	t.Run("should not block the WriteSync() because Kafka buffer is full", func(t *testing.T) {
		t.Parallel()

		createWriteRequest := func() *mimirpb.WriteRequest {
			return &mimirpb.WriteRequest{Timeseries: series1, Metadata: nil, Source: mimirpb.API}
		}

		// Estimate the size of each record written in this test.
		writeReq := createWriteRequest()
		serializer := RecordSerializerFromVersion(2)
		writeReqRecords, _, err := serializer.ToRecords(topicName, partitionID, tenantID, writeReq, maxProducerRecordDataBytesLimit)
		require.NoError(t, err)
		require.Len(t, writeReqRecords, 1)
		estimatedRecordSize := len(writeReqRecords[0].Value)
		t.Logf("estimated record size: %d bytes", estimatedRecordSize)

		var (
			unblockProduceRequestsOnce = sync.Once{}
			unblockProduceRequests     = make(chan struct{})
			recordsReceived            = atomic.NewInt64(0)
			goroutines                 = sync.WaitGroup{}

			writeErrsMx sync.Mutex
			writeErrs   []error
		)

		cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)

		cfg := createTestKafkaConfigForBackend(backend, clusterAddr, topicName)
		cfg.ProducerMaxBufferedBytes = int64((estimatedRecordSize * 4) - 1) // Configure the test so that we expect 3 produced records.

		// Pre-condition checks.
		assert.GreaterOrEqual(t, numPartitions, 10)

		doUnblockProduceRequests := func() {
			unblockProduceRequestsOnce.Do(func() {
				t.Log("releasing produce requests")
				close(unblockProduceRequests)
			})
		}

		// Ensure produce requests are released in case of premature test termination.
		t.Cleanup(doUnblockProduceRequests)

		// Configure Kafka to block Produce requests until the test unblocks it.
		cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
			goroutines.Add(1)
			defer goroutines.Done()

			numRecords, err := getProduceRequestRecordsCount(request.(*kmsg.ProduceRequest))
			require.NoError(t, err)
			recordsReceived.Add(int64(numRecords))

			// Block produce requests.
			<-unblockProduceRequests

			return nil, nil, false
		})

		writer, reg := createTestWriter(t, cfg)

		// Write few records, each in a different partition, so that we ensure the buffer is global and not per partition.
		for i := int32(0); i < 10; i++ {
			partition := i

			runAsync(&goroutines, func() {
				err := writer.WriteSync(ctx, topicName, partition, tenantID, createWriteRequest())
				t.Logf("WriteSync() returned with error: %v", err)

				// Keep track of the returned error (if any).
				writeErrsMx.Lock()
				writeErrs = append(writeErrs, err)
				writeErrsMx.Unlock()
			})
		}

		// We expect all WriteSync() requests to fail, either because the write timeout expired or
		// because the buffer is full.
		require.Eventually(t, func() bool {
			writeErrsMx.Lock()
			defer writeErrsMx.Unlock()
			return len(writeErrs) == 10
		}, cfg.WriteTimeout+writerRequestTimeoutOverhead+time.Second, 100*time.Millisecond)

		// Assert on the actual errors returned by WriteSync().
		actualErrRecordTimeoutCount := 0
		actualErrMaxBufferedCount := 0

		for _, writeErr := range writeErrs {
			if errors.Is(writeErr, kgo.ErrRecordTimeout) {
				actualErrRecordTimeoutCount++
			}
			if errors.Is(writeErr, kgo.ErrMaxBuffered) {
				actualErrMaxBufferedCount++
			}
		}

		assert.Equal(t, 3, actualErrRecordTimeoutCount)
		assert.Equal(t, 7, actualErrMaxBufferedCount)

		// We expect that only max buffered records have been sent to Kafka.
		assert.Equal(t, 3, int(recordsReceived.Load()))

		// Check metrics.
		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_writer_produce_records_enqueued_total Total number of Kafka records enqueued to be sent to the Kafka backend (includes records that fail to be successfully sent to the Kafka backend).
			# TYPE cortex_ingest_storage_writer_produce_records_enqueued_total counter
			cortex_ingest_storage_writer_produce_records_enqueued_total 10

			# HELP cortex_ingest_storage_writer_produce_records_failed_total Total number of Kafka records that failed to be sent to the Kafka backend.
			# TYPE cortex_ingest_storage_writer_produce_records_failed_total counter
			cortex_ingest_storage_writer_produce_records_failed_total{reason="buffer-full"} 7
			cortex_ingest_storage_writer_produce_records_failed_total{reason="timeout"} 3
		`),
			"cortex_ingest_storage_writer_produce_records_enqueued_total",
			"cortex_ingest_storage_writer_produce_records_failed_total"))

		// Unblock produce requests and wait until all goroutines are done.
		doUnblockProduceRequests()
		goroutines.Wait()

		// Now that produce requests have been unblocked, try to produce again. We expect all
		// produce to succeed.
		for i := int32(0); i < 3; i++ {
			partition := i

			runAsync(&goroutines, func() {
				require.NoError(t, writer.WriteSync(ctx, topicName, partition, tenantID, createWriteRequest()))
			})
		}

		goroutines.Wait()

		// Check metrics.
		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_writer_produce_records_enqueued_total Total number of Kafka records enqueued to be sent to the Kafka backend (includes records that fail to be successfully sent to the Kafka backend).
			# TYPE cortex_ingest_storage_writer_produce_records_enqueued_total counter
			cortex_ingest_storage_writer_produce_records_enqueued_total 13

			# HELP cortex_ingest_storage_writer_produce_records_failed_total Total number of Kafka records that failed to be sent to the Kafka backend.
			# TYPE cortex_ingest_storage_writer_produce_records_failed_total counter
			cortex_ingest_storage_writer_produce_records_failed_total{reason="buffer-full"} 7
			cortex_ingest_storage_writer_produce_records_failed_total{reason="timeout"} 3
		`),
			"cortex_ingest_storage_writer_produce_records_enqueued_total",
			"cortex_ingest_storage_writer_produce_records_failed_total"))
	})

	t.Run("should not panic if WriteSync() is called after the writer has been stopped", func(t *testing.T) {
		t.Parallel()

		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		writer, _ := createTestWriter(t, createTestKafkaConfigForBackend(backend, clusterAddr, topicName))

		err := writer.WriteSync(ctx, topicName, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: multiSeries, Metadata: nil, Source: mimirpb.API})
		require.NoError(t, err)

		require.NoError(t, services.StopAndAwaitTerminated(ctx, writer))

		err = writer.WriteSync(ctx, topicName, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: multiSeries, Metadata: nil, Source: mimirpb.API})
		require.ErrorIs(t, err, ErrWriterNotRunning)
	})
}

func TestWriter_MultiWriteSync(t *testing.T) {
	runForEachKafkaBackend(t, testWriter_MultiWriteSync)
}

func testWriter_MultiWriteSync(t *testing.T, backend string) {
	const (
		topicName     = "test"
		numPartitions = 10
		tenantID      = "user-1"
	)

	var (
		ctx     = context.Background()
		series1 = []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}
		series2 = []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_2")}
		series3 = []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3")}
	)

	t.Run("should write records to multiple partitions", func(t *testing.T) {
		t.Parallel()

		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		cfg := createTestKafkaConfigForBackend(backend, clusterAddr, topicName)
		cfg.ProducerRecordVersion = 1
		writer, reg := createTestWriter(t, cfg)

		req1 := &mimirpb.WriteRequest{Timeseries: series1, Source: mimirpb.API}
		req2 := &mimirpb.WriteRequest{Timeseries: series2, Source: mimirpb.API}
		req3 := &mimirpb.WriteRequest{Timeseries: series3, Source: mimirpb.API}
		totalInputSize := req1.Size() + req2.Size() + req3.Size()

		partitionRequests := []PartitionWriteRequest{
			{PartitionID: 0, WriteRequest: req1},
			{PartitionID: 1, WriteRequest: req2},
			{PartitionID: 2, WriteRequest: req3},
		}

		err := writer.MultiWriteSync(ctx, topicName, tenantID, partitionRequests)
		require.NoError(t, err)

		// Read back from each partition and verify.
		var totalSentBytes int
		for _, pr := range partitionRequests {
			consumer, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topicName: {pr.PartitionID: kgo.NewOffset().AtStart()},
			}))
			require.NoError(t, err)
			t.Cleanup(consumer.Close)

			fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			t.Cleanup(cancel)

			fetches := consumer.PollFetches(fetchCtx)
			require.NoError(t, fetches.Err())
			require.Len(t, fetches.Records(), 1)
			assert.Equal(t, pr.PartitionID, fetches.Records()[0].Partition)
			totalSentBytes += len(fetches.Records()[0].Value)

			received := mimirpb.WriteRequest{}
			require.NoError(t, received.Unmarshal(fetches.Records()[0].Value))
			require.Len(t, received.Timeseries, len(pr.WriteRequest.Timeseries))

			for idx, expected := range pr.WriteRequest.Timeseries {
				assert.Equal(t, expected.Labels, received.Timeseries[idx].Labels)
				assert.Equal(t, expected.Samples, received.Timeseries[idx].Samples)
			}
		}

		// Check metrics.
		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_ingest_storage_writer_input_bytes_total Total number of bytes in write requests before conversion to the Kafka record format.
			# TYPE cortex_ingest_storage_writer_input_bytes_total counter
			cortex_ingest_storage_writer_input_bytes_total %d

			# HELP cortex_ingest_storage_writer_sent_bytes_total Total number of bytes produced to the Kafka backend.
			# TYPE cortex_ingest_storage_writer_sent_bytes_total counter
			cortex_ingest_storage_writer_sent_bytes_total %d

			# HELP cortex_ingest_storage_writer_records_per_write_request The number of records a single per-partition write request has been split into.
			# TYPE cortex_ingest_storage_writer_records_per_write_request histogram
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="1"} 3
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="2"} 3
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="4"} 3
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="8"} 3
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="16"} 3
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="32"} 3
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="64"} 3
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="128"} 3
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="+Inf"} 3
			cortex_ingest_storage_writer_records_per_write_request_sum 3
			cortex_ingest_storage_writer_records_per_write_request_count 3

			# HELP cortex_ingest_storage_writer_produce_records_enqueued_total Total number of Kafka records enqueued to be sent to the Kafka backend (includes records that fail to be successfully sent to the Kafka backend).
			# TYPE cortex_ingest_storage_writer_produce_records_enqueued_total counter
			cortex_ingest_storage_writer_produce_records_enqueued_total 3
		`, totalInputSize, totalSentBytes)),
			"cortex_ingest_storage_writer_input_bytes_total",
			"cortex_ingest_storage_writer_sent_bytes_total",
			"cortex_ingest_storage_writer_records_per_write_request",
			"cortex_ingest_storage_writer_produce_records_enqueued_total"))

		assertHistogramSampleCount(t, reg, "cortex_ingest_storage_writer_serialize_duration_seconds", 1)
	})

	t.Run("should skip empty requests", func(t *testing.T) {
		t.Parallel()

		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		cfg := createTestKafkaConfigForBackend(backend, clusterAddr, topicName)
		cfg.ProducerRecordVersion = 1
		writer, reg := createTestWriter(t, cfg)

		nonEmptyReq := &mimirpb.WriteRequest{Timeseries: series1, Source: mimirpb.API}
		inputSize := nonEmptyReq.Size()

		partitionRequests := []PartitionWriteRequest{
			{PartitionID: 0, WriteRequest: &mimirpb.WriteRequest{}},
			{PartitionID: 1, WriteRequest: nonEmptyReq},
		}

		err := writer.MultiWriteSync(ctx, topicName, tenantID, partitionRequests)
		require.NoError(t, err)

		// Only partition 1 should have records.
		consumer, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topicName: {1: kgo.NewOffset().AtStart()},
		}))
		require.NoError(t, err)
		t.Cleanup(consumer.Close)

		fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		t.Cleanup(cancel)

		fetches := consumer.PollFetches(fetchCtx)
		require.NoError(t, fetches.Err())
		require.Len(t, fetches.Records(), 1)
		assert.Equal(t, int32(1), fetches.Records()[0].Partition)

		// Check metrics: only the non-empty partition should be tracked.
		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_ingest_storage_writer_input_bytes_total Total number of bytes in write requests before conversion to the Kafka record format.
			# TYPE cortex_ingest_storage_writer_input_bytes_total counter
			cortex_ingest_storage_writer_input_bytes_total %d

			# HELP cortex_ingest_storage_writer_sent_bytes_total Total number of bytes produced to the Kafka backend.
			# TYPE cortex_ingest_storage_writer_sent_bytes_total counter
			cortex_ingest_storage_writer_sent_bytes_total %d

			# HELP cortex_ingest_storage_writer_records_per_write_request The number of records a single per-partition write request has been split into.
			# TYPE cortex_ingest_storage_writer_records_per_write_request histogram
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="1"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="2"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="4"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="8"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="16"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="32"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="64"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="128"} 1
			cortex_ingest_storage_writer_records_per_write_request_bucket{le="+Inf"} 1
			cortex_ingest_storage_writer_records_per_write_request_sum 1
			cortex_ingest_storage_writer_records_per_write_request_count 1

			# HELP cortex_ingest_storage_writer_produce_records_enqueued_total Total number of Kafka records enqueued to be sent to the Kafka backend (includes records that fail to be successfully sent to the Kafka backend).
			# TYPE cortex_ingest_storage_writer_produce_records_enqueued_total counter
			cortex_ingest_storage_writer_produce_records_enqueued_total 1
		`, inputSize, len(fetches.Records()[0].Value))),
			"cortex_ingest_storage_writer_input_bytes_total",
			"cortex_ingest_storage_writer_sent_bytes_total",
			"cortex_ingest_storage_writer_records_per_write_request",
			"cortex_ingest_storage_writer_produce_records_enqueued_total"))

		assertHistogramSampleCount(t, reg, "cortex_ingest_storage_writer_serialize_duration_seconds", 1)
	})

	t.Run("should return nil when all partition requests are empty", func(t *testing.T) {
		t.Parallel()

		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		cfg := createTestKafkaConfigForBackend(backend, clusterAddr, topicName)
		cfg.ProducerRecordVersion = 1
		writer, _ := createTestWriter(t, cfg)

		partitionRequests := []PartitionWriteRequest{
			{PartitionID: 0, WriteRequest: &mimirpb.WriteRequest{}},
			{PartitionID: 1, WriteRequest: &mimirpb.WriteRequest{}},
		}

		err := writer.MultiWriteSync(ctx, topicName, tenantID, partitionRequests)
		require.NoError(t, err)
	})
}

func TestWriter_WriteSync_HighConcurrencyOnKafkaClientBufferFull(t *testing.T) {
	runForEachKafkaBackend(t, testWriter_WriteSync_HighConcurrencyOnKafkaClientBufferFull)
}

func testWriter_WriteSync_HighConcurrencyOnKafkaClientBufferFull(t *testing.T, backend string) {
	const (
		topicName     = "test"
		numPartitions = 1
		partitionID   = 0
		numWorkers    = 50
		tenantID      = "user-1"
		testDuration  = 3 * time.Second
	)

	var (
		done    = make(chan struct{})
		workers = sync.WaitGroup{}

		writeSuccessCount = atomic.NewInt64(0)
		writeFailureCount = atomic.NewInt64(0)
	)

	createRandomWriteRequest := func() *mimirpb.WriteRequest {
		// It's important that each request has a different size to reproduce the deadlock.
		metricName := strings.Repeat("x", rand.IntN(1000))

		series := []mimirpb.PreallocTimeseries{mockPreallocTimeseries(metricName)}
		return &mimirpb.WriteRequest{Timeseries: series, Metadata: nil, Source: mimirpb.API}
	}

	// If the test is successful (no WriteSync() request is in a deadlock state) then we expect the test
	// to complete shortly after the estimated test duration.
	ctx, cancel := context.WithTimeoutCause(context.Background(), 2*testDuration, errors.New("test did not complete within the expected time"))
	t.Cleanup(cancel)

	cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
	cfg := createTestKafkaConfigForBackend(backend, clusterAddr, topicName)
	cfg.ProducerMaxBufferedBytes = 10000
	cfg.WriteTimeout = testDuration * 10 // We want the Kafka client to block in case of any issue.

	// Throttle a very short (random) time to increase chances of hitting race conditions.
	cluster.ControlKey(int16(kmsg.Produce), func(_ kmsg.Request) (kmsg.Response, error, bool) {
		time.Sleep(time.Duration(rand.Int64N(int64(time.Millisecond))))

		return nil, nil, false
	})

	writer, _ := createTestWriter(t, cfg)

	// Start N workers that will concurrently write to the same partition.
	for i := 0; i < numWorkers; i++ {
		runAsync(&workers, func() {
			for {
				select {
				case <-done:
					return

				default:
					if err := writer.WriteSync(ctx, topicName, partitionID, tenantID, createRandomWriteRequest()); err == nil {
						writeSuccessCount.Inc()
					} else {
						assert.ErrorIs(t, err, kgo.ErrMaxBuffered)
						writeFailureCount.Inc()

						// Stop a worker as soon as a non-expected error occurred.
						if !errors.Is(err, kgo.ErrMaxBuffered) {
							return
						}
					}
				}
			}
		})
	}

	// Keep it running for some time.
	time.Sleep(testDuration)

	// Signal workers to stop and wait until they're done.
	close(done)
	workers.Wait()

	t.Logf("writes succeeded: %d", writeSuccessCount.Load())
	t.Logf("writes failed:    %d", writeFailureCount.Load())

	// We expect some requests to have failed.
	require.NotZero(t, writeSuccessCount.Load())
	require.NotZero(t, writeFailureCount.Load())

	// We expect the buffered bytes to get down to 0 once all write requests completed.
	require.Zero(t, writer.client.Load().bufferedBytes.Load())
}

func TestProduceResultsErr(t *testing.T) {
	t.Run("should have zero allocations on success", func(t *testing.T) {
		results := kgo.ProduceResults{
			{Record: &kgo.Record{Partition: 0}, Err: nil},
			{Record: &kgo.Record{Partition: 1}, Err: nil},
			{Record: &kgo.Record{Partition: 2}, Err: nil},
		}

		allocs := testing.AllocsPerRun(100, func() {
			err := produceResultsErr(results)
			if err != nil {
				t.Fatal("unexpected error")
			}
		})

		assert.Equal(t, float64(0), allocs)
	})

	t.Run("should return ErrWriteRequestDataItemTooLarge on MessageTooLarge", func(t *testing.T) {
		results := kgo.ProduceResults{
			{Record: &kgo.Record{Partition: 0}, Err: nil},
			{Record: &kgo.Record{Partition: 1}, Err: kerr.MessageTooLarge},
		}

		err := produceResultsErr(results)
		require.ErrorIs(t, err, ErrWriteRequestDataItemTooLarge)
	})

	t.Run("should return first error with failed partition IDs", func(t *testing.T) {
		expectedErr := errors.New("test error")
		results := kgo.ProduceResults{
			{Record: &kgo.Record{Partition: 0}, Err: nil},
			{Record: &kgo.Record{Partition: 1}, Err: expectedErr},
			{Record: &kgo.Record{Partition: 2}, Err: errors.New("another error")},
		}

		err := produceResultsErr(results)
		require.ErrorIs(t, err, expectedErr)
		assert.ErrorContains(t, err, "failed to write to partitions [1 2]")
	})

	t.Run("should deduplicate partition IDs", func(t *testing.T) {
		expectedErr := errors.New("test error")
		results := kgo.ProduceResults{
			{Record: &kgo.Record{Partition: 1}, Err: expectedErr},
			{Record: &kgo.Record{Partition: 1}, Err: expectedErr},
		}

		err := produceResultsErr(results)
		assert.ErrorContains(t, err, "failed to write to partitions [1]")
	})
}

func TestMarshalWriteRequestToRecords(t *testing.T) {
	testReq := func(t *testing.T) *mimirpb.WriteRequest {
		t.Helper()
		req := &mimirpb.WriteRequest{
			Source:              mimirpb.RULE,
			SkipLabelValidation: true,
			Timeseries: []mimirpb.PreallocTimeseries{
				mockPreallocTimeseries("series_1"),
				mockPreallocTimeseries("series_2"),
				mockPreallocTimeseries("series_3"),
			},
			Metadata: []*mimirpb.MetricMetadata{
				{Type: mimirpb.COUNTER, MetricFamilyName: "series_1", Help: "This is the first test metric."},
				{Type: mimirpb.COUNTER, MetricFamilyName: "series_2", Help: "This is the second test metric."},
				{Type: mimirpb.COUNTER, MetricFamilyName: "series_3", Help: "This is the third test metric."},
			},
		}
		// Pre-requisite check: WriteRequest fields are set to non-zero values.
		require.NotZero(t, req.Source)
		require.NotZero(t, req.SkipLabelValidation)
		require.NotZero(t, req.Timeseries)
		require.NotZero(t, req.Metadata)
		return req
	}

	testReqV2 := func(t *testing.T) *mimirpb.WriteRequest {
		t.Helper()
		rw1 := testReq(t)
		rw2, err := mimirpb.FromWriteRequestToRW2Request(rw1, V2CommonSymbols, V2RecordSymbolOffset)
		require.NoError(t, err)
		return rw2
	}

	t.Run("should return 1 record if the input WriteRequest size is less than the size limit", func(t *testing.T) {
		req := testReq(t)
		records, err := marshalWriteRequestToRecords("test", 1, "user-1", req, req.Size(), req.Size()*2, mimirpb.SplitWriteRequestByMaxMarshalSize)
		require.NoError(t, err)
		require.Len(t, records, 1)
		assert.Equal(t, "test", records[0].Topic)
		assert.Equal(t, int32(1), records[0].Partition)

		actual := &mimirpb.WriteRequest{}
		require.NoError(t, actual.Unmarshal(records[0].Value))

		actual.ClearTimeseriesUnmarshalData()
		assert.Equal(t, req, actual)
	})

	t.Run("should return 1 record if the input WriteRequest in RW2 size is less than the size limit", func(t *testing.T) {
		req := testReqV2(t)
		records, err := marshalWriteRequestToRecords("test", 1, "user-1", req, req.Size(), req.Size()*2, splitRequestVersionTwo)
		require.NoError(t, err)
		require.Len(t, records, 1)
		assert.Equal(t, "test", records[0].Topic)
		assert.Equal(t, int32(1), records[0].Partition)

		actual := &mimirpb.PreallocWriteRequest{
			UnmarshalFromRW2: true,
			RW2SymbolOffset:  V2RecordSymbolOffset,
			RW2CommonSymbols: V2CommonSymbols.GetSlice(),
		}
		require.NoError(t, actual.Unmarshal(records[0].Value))

		actual.ClearTimeseriesUnmarshalData()
		assert.Equal(t, mimirpb.RULE, actual.Source)
		assert.Equal(t, true, actual.SkipLabelValidation)
		expMetadata := []*mimirpb.MetricMetadata{
			{
				Type:             mimirpb.COUNTER,
				MetricFamilyName: "series_1",
				Help:             "This is the first test metric.",
			},
			{
				Type:             mimirpb.COUNTER,
				MetricFamilyName: "series_2",
				Help:             "This is the second test metric.",
			},
			{
				Type:             mimirpb.COUNTER,
				MetricFamilyName: "series_3",
				Help:             "This is the third test metric.",
			},
		}
		assert.ElementsMatch(t, expMetadata, actual.Metadata)
		expTimeseries := []mimirpb.PreallocTimeseries{
			{
				TimeSeries: &mimirpb.TimeSeries{
					Labels:    mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", "series_1")),
					Samples:   []mimirpb.Sample{{TimestampMs: 1, Value: 2.0}},
					Exemplars: []mimirpb.Exemplar{},
				},
			},
			{
				TimeSeries: &mimirpb.TimeSeries{
					Labels:    mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", "series_2")),
					Samples:   []mimirpb.Sample{{TimestampMs: 1, Value: 2.0}},
					Exemplars: []mimirpb.Exemplar{},
				},
			},
			{
				TimeSeries: &mimirpb.TimeSeries{
					Labels:    mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", "series_3")),
					Samples:   []mimirpb.Sample{{TimestampMs: 1, Value: 2.0}},
					Exemplars: []mimirpb.Exemplar{},
				},
			},
		}
		assert.Equal(t, expTimeseries, actual.Timeseries)
	})

	t.Run("should return multiple records if the input WriteRequest size is bigger than the size limit", func(t *testing.T) {
		const limit = 100
		req := testReq(t)

		records, err := marshalWriteRequestToRecords("test", 1, "user-1", req, req.Size(), limit, mimirpb.SplitWriteRequestByMaxMarshalSize)
		require.NoError(t, err)
		require.Len(t, records, 4)

		// Assert each record, and decode all partial WriteRequests.
		partials := make([]*mimirpb.WriteRequest, 0, len(records))

		for _, rec := range records {
			assert.Equal(t, "test", rec.Topic)
			assert.Equal(t, int32(1), rec.Partition)
			assert.Equal(t, "user-1", string(rec.Key))
			assert.Less(t, len(rec.Value), limit)

			actual := &mimirpb.WriteRequest{}
			require.NoError(t, actual.Unmarshal(rec.Value))

			actual.ClearTimeseriesUnmarshalData()
			partials = append(partials, actual)
		}

		assert.Equal(t, []*mimirpb.WriteRequest{
			{
				Source:              mimirpb.RULE,
				SkipLabelValidation: true,
				Timeseries:          []mimirpb.PreallocTimeseries{req.Timeseries[0], req.Timeseries[1]},
			}, {
				Source:              mimirpb.RULE,
				SkipLabelValidation: true,
				Timeseries:          []mimirpb.PreallocTimeseries{req.Timeseries[2]},
			}, {
				Source:              mimirpb.RULE,
				SkipLabelValidation: true,
				Metadata:            []*mimirpb.MetricMetadata{req.Metadata[0], req.Metadata[1]},
			}, {
				Source:              mimirpb.RULE,
				SkipLabelValidation: true,
				Metadata:            []*mimirpb.MetricMetadata{req.Metadata[2]},
			},
		}, partials)
	})

	t.Run("should return multiple records if the input RW2 WriteRequest size is bigger than the size limit", func(t *testing.T) {
		const limit = 100
		req := testReqV2(t)

		records, err := marshalWriteRequestToRecords("test", 1, "user-1", req, req.Size(), limit, splitRequestVersionTwo)
		require.NoError(t, err)
		require.Len(t, records, 3)

		// Assert each record, and decode all partial WriteRequests.
		partials := make([]*mimirpb.WriteRequest, 0, len(records))

		for _, rec := range records {
			assert.Equal(t, "test", rec.Topic)
			assert.Equal(t, int32(1), rec.Partition)
			assert.Equal(t, "user-1", string(rec.Key))
			assert.LessOrEqual(t, len(rec.Value), limit)

			actual := &mimirpb.PreallocWriteRequest{
				UnmarshalFromRW2: true,
				RW2SymbolOffset:  V2RecordSymbolOffset,
				RW2CommonSymbols: V2CommonSymbols.GetSlice(),
			}
			require.NoError(t, actual.Unmarshal(rec.Value))

			actual.ClearTimeseriesUnmarshalData()
			partials = append(partials, &actual.WriteRequest)
		}

		assert.Equal(t, []*mimirpb.WriteRequest{
			{
				Source:              mimirpb.RULE,
				SkipLabelValidation: true,
				Timeseries: []mimirpb.PreallocTimeseries{
					{
						TimeSeries: &mimirpb.TimeSeries{
							Labels:    mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", "series_1")),
							Samples:   []mimirpb.Sample{{TimestampMs: 1, Value: 2.0}},
							Exemplars: []mimirpb.Exemplar{},
						},
					},
				},
				Metadata: []*mimirpb.MetricMetadata{
					{
						MetricFamilyName: "series_1",
						Type:             mimirpb.COUNTER,
						Help:             "This is the first test metric.",
					},
				},
			}, {
				Source:              mimirpb.RULE,
				SkipLabelValidation: true,
				Timeseries: []mimirpb.PreallocTimeseries{
					{
						TimeSeries: &mimirpb.TimeSeries{
							Labels:    mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", "series_2")),
							Samples:   []mimirpb.Sample{{TimestampMs: 1, Value: 2.0}},
							Exemplars: []mimirpb.Exemplar{},
						},
					},
				},
				Metadata: []*mimirpb.MetricMetadata{
					{
						MetricFamilyName: "series_2",
						Type:             mimirpb.COUNTER,
						Help:             "This is the second test metric.",
					},
				},
			}, {
				Source:              mimirpb.RULE,
				SkipLabelValidation: true,
				Timeseries: []mimirpb.PreallocTimeseries{
					{
						TimeSeries: &mimirpb.TimeSeries{
							Labels:    mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", "series_3")),
							Samples:   []mimirpb.Sample{{TimestampMs: 1, Value: 2.0}},
							Exemplars: []mimirpb.Exemplar{},
						},
					},
				},
				Metadata: []*mimirpb.MetricMetadata{
					{
						MetricFamilyName: "series_3",
						Type:             mimirpb.COUNTER,
						Help:             "This is the third test metric.",
					},
				},
			},
		}, partials)
	})

	t.Run("should return multiple records, larger than the limit, if the Timeseries and Metadata entries in the WriteRequest are bigger than limit", func(t *testing.T) {
		const limit = 1
		req := testReq(t)

		records, err := marshalWriteRequestToRecords("test", 1, "user-1", req, req.Size(), limit, mimirpb.SplitWriteRequestByMaxMarshalSize)
		require.NoError(t, err)
		require.Len(t, records, 6)

		// Decode all partial WriteRequests.
		partials := make([]*mimirpb.WriteRequest, 0, len(records))
		for _, rec := range records {
			assert.Equal(t, "test", rec.Topic)
			assert.Equal(t, int32(1), rec.Partition)
			assert.Greater(t, len(rec.Value), limit)

			actual := &mimirpb.WriteRequest{}
			require.NoError(t, actual.Unmarshal(rec.Value))

			actual.ClearTimeseriesUnmarshalData()
			partials = append(partials, actual)
		}

		assert.Equal(t, []*mimirpb.WriteRequest{
			{
				Source:              mimirpb.RULE,
				SkipLabelValidation: true,
				Timeseries:          []mimirpb.PreallocTimeseries{req.Timeseries[0]},
			}, {
				Source:              mimirpb.RULE,
				SkipLabelValidation: true,
				Timeseries:          []mimirpb.PreallocTimeseries{req.Timeseries[1]},
			}, {
				Source:              mimirpb.RULE,
				SkipLabelValidation: true,
				Timeseries:          []mimirpb.PreallocTimeseries{req.Timeseries[2]},
			}, {
				Source:              mimirpb.RULE,
				SkipLabelValidation: true,
				Metadata:            []*mimirpb.MetricMetadata{req.Metadata[0]},
			}, {
				Source:              mimirpb.RULE,
				SkipLabelValidation: true,
				Metadata:            []*mimirpb.MetricMetadata{req.Metadata[1]},
			}, {
				Source:              mimirpb.RULE,
				SkipLabelValidation: true,
				Metadata:            []*mimirpb.MetricMetadata{req.Metadata[2]},
			},
		}, partials)
	})
}

func BenchmarkMarshalWriteRequestToRecords_NoSplitting(b *testing.B) {
	// This benchmark measures marshalWriteRequestToRecords() when no splitting is done
	// and compares it with the straight marshalling of the input WriteRequest. We expect
	// the two to perform the same, which means marshalWriteRequestToRecords() doesn't
	// introduce any performance penalty when a WriteRequest isn't split.

	// Generate a WriteRequest.
	req := &mimirpb.WriteRequest{Timeseries: make([]mimirpb.PreallocTimeseries, 10000)}
	for i := 0; i < len(req.Timeseries); i++ {
		req.Timeseries[i] = mockPreallocTimeseries(fmt.Sprintf("series_%d", i))
	}
	requestSplitter := mimirpb.SplitWriteRequestByMaxMarshalSize

	b.Run("marshalWriteRequestToRecords()", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			records, err := marshalWriteRequestToRecords("test", 1, "user-1", req, req.Size(), 1024*1024*1024, requestSplitter)
			if err != nil {
				b.Fatal(err)
			}
			if len(records) != 1 {
				b.Fatalf("expected 1 record but got %d", len(records))
			}
		}
	})

	b.Run("WriteRequest.Marshal()", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, err := req.Marshal()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func mockPreallocTimeseries(metricName string) mimirpb.PreallocTimeseries {
	return mockPreallocTimeseriesWithSample(metricName, 1, 2)
}

func mockPreallocTimeseriesWithSample(metricName string, ts int64, val float64) mimirpb.PreallocTimeseries {
	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels:    []mimirpb.LabelAdapter{{Name: "__name__", Value: metricName}},
			Samples:   []mimirpb.Sample{{TimestampMs: ts, Value: val}},
			Exemplars: []mimirpb.Exemplar{}, // Makes comparison with unmarshalled TimeSeries easy.
		},
	}
}

func mockPreallocTimeseriesWithExemplar(metricName string) mimirpb.PreallocTimeseries {
	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels: []mimirpb.LabelAdapter{
				{Name: "__name__", Value: metricName},
			},
			Samples: []mimirpb.Sample{{
				TimestampMs: 1,
				Value:       2,
			}},
			Exemplars: []mimirpb.Exemplar{{
				TimestampMs: 2,
				Value:       14,
				Labels: []mimirpb.LabelAdapter{
					{Name: "trace_id", Value: metricName + "_trace"},
				},
			}},
		},
	}
}

func mockPreallocTimeseriesWithAll(metricName string) mimirpb.PreallocTimeseries {
	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels: []mimirpb.LabelAdapter{
				{Name: "__name__", Value: metricName},
			},
			Samples: []mimirpb.Sample{{
				TimestampMs: 1,
				Value:       2,
			}},
			Exemplars: []mimirpb.Exemplar{{
				TimestampMs: 2,
				Value:       14,
				Labels: []mimirpb.LabelAdapter{
					{Name: "trace_id", Value: metricName + "_trace"},
				},
			}},
			Histograms: []mimirpb.Histogram{{
				Count:          &mimirpb.Histogram_CountFloat{CountFloat: 2},
				Sum:            10,
				Schema:         1,
				ZeroThreshold:  0.001,
				ZeroCount:      &mimirpb.Histogram_ZeroCountFloat{ZeroCountFloat: 0},
				NegativeSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
				NegativeCounts: []float64{1},
				PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
				PositiveCounts: []float64{1},
				ResetHint:      mimirpb.Histogram_UNKNOWN,
				Timestamp:      0,
			}},
		},
	}
}

func mockMetricMetadata(name string) *mimirpb.MetricMetadata {
	return &mimirpb.MetricMetadata{
		Type:             mimirpb.COUNTER,
		MetricFamilyName: name,
		Help:             fmt.Sprintf("Help for %s", name),
		Unit:             "seconds",
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

func getProduceRequestHighestTimestamp(req *kmsg.ProduceRequest) (time.Time, error) {
	var highestTimestamp time.Time

	for _, topic := range req.Topics {
		for _, partition := range topic.Partitions {
			batch := kmsg.RecordBatch{}
			if err := batch.ReadFrom(partition.Records); err != nil {
				return time.Time{}, err
			}

			// Read the highest timestamp from the record batch header (MaxTimestamp), which the Kafka
			// client sets to the highest record timestamp in the batch. We intentionally avoid
			// decompressing and parsing every record: doing so on the fake Kafka's single-threaded
			// control loop is expensive (especially under the race detector with incompressible
			// payloads) and makes the fake fall behind the produce rate, which distorts the latency
			// this test measures.
			batchHighestTimestamp := time.UnixMilli(batch.MaxTimestamp)
			if highestTimestamp.IsZero() || batchHighestTimestamp.After(highestTimestamp) {
				highestTimestamp = batchHighestTimestamp
			}
		}
	}

	return highestTimestamp, nil
}

func runAsync(wg *sync.WaitGroup, fn func()) {
	wg.Add(1)

	go func() {
		defer wg.Done()
		fn()
	}()
}

func runAsyncAfter(wg *sync.WaitGroup, waitFor chan struct{}, fn func()) {
	wg.Add(1)

	go func() {
		defer wg.Done()
		<-waitFor
		fn()
	}()
}

func BenchmarkWriter_WriteSync(b *testing.B) {
	const (
		topicName     = "bench"
		numPartitions = 32
		tenantID      = "user-1"
	)

	// Set up a real TracerProvider with a parent-based sampler.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1.0))),
	)
	defer func() { _ = tp.Shutdown(context.Background()) }()
	otel.SetTracerProvider(tp)

	tr := tp.Tracer("bench")

	// Build a sampled and an unsampled context.
	sampledCtx, _ := tr.Start(context.Background(), "sampled-request")
	unsampledCtx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1},
		SpanID:     trace.SpanID{1},
		TraceFlags: 0, // not sampled
		Remote:     true,
	}))

	// Create a fake Kafka cluster and a Writer.
	_, clusterAddr := testkafka.CreateCluster(b, numPartitions, topicName)
	cfg := createTestKafkaConfig(clusterAddr, topicName)
	reg := prometheus.NewPedanticRegistry()

	writer := NewWriter(cfg, test.NewTestingLogger(b), reg)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), writer))
	b.Cleanup(func() {
		require.NoError(b, services.StopAndAwaitTerminated(context.Background(), writer))
	})

	req := &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")},
		Source:     mimirpb.API,
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Run from many concurrent goroutines to simulate the distributor's
	// concurrent request handling, where each push runs in its own goroutine.
	b.RunParallel(func(pb *testing.PB) {
		n := 0
		for pb.Next() {
			// 1% sampled, 99% unsampled.
			ctx := unsampledCtx
			if n%100 == 0 {
				ctx = sampledCtx
			}
			n++

			partitionID := int32(n % numPartitions)
			if err := writer.WriteSync(ctx, topicName, partitionID, tenantID, req); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// createTestKafkaConfig returns a KafkaConfig for the default ("kafka")
// backend. Use createTestKafkaConfigForBackend to override.
func createTestKafkaConfig(clusterAddr, topicName string) KafkaConfig {
	return createTestKafkaConfigForBackend(KafkaBackendKafka, clusterAddr, topicName)
}

func createTestKafkaConfigForBackend(backend, clusterAddr, topicName string) KafkaConfig {
	cfg := KafkaConfig{}
	flagext.DefaultValues(&cfg)

	cfg.Backend = backend
	cfg.Address = flagext.StringSliceCSV{clusterAddr}
	cfg.Topic = topicName
	cfg.WriteTimeout = 5 * time.Second
	cfg.FetchConcurrencyMax = 2
	cfg.concurrentFetchersFetchBackoffConfig = fastFetchBackoffConfig

	return cfg
}

// assertHistogramSampleCount asserts that the histogram metric with the given name
// has been observed the expected number of times.
func assertHistogramSampleCount(t *testing.T, reg prometheus.Gatherer, metricName string, expected uint64) {
	t.Helper()

	mfm, err := dskit_metrics.NewMetricFamilyMapFromGatherer(reg)
	require.NoError(t, err)
	hist, err := dskit_metrics.FindHistogramWithNameAndLabels(mfm, metricName)
	require.NoError(t, err)
	assert.Equal(t, expected, hist.GetSampleCount())
}

// TestWriter_KafkaClientMetricsParity asserts that the franz-go-compatible
// writer metrics (transport, producer-state, and the extended latency
// histograms) are present and populated on both backends, so a dashboard or
// alert written against one backend keeps working on the other.
func TestWriter_KafkaClientMetricsParity(t *testing.T) {
	runForEachKafkaBackend(t, testWriter_KafkaClientMetricsParity)
}

func testWriter_KafkaClientMetricsParity(t *testing.T, backend string) {
	const (
		topicName     = "test"
		numPartitions = 1
		partitionID   = 0
		tenantID      = "user-1"
	)
	ctx := context.Background()

	_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
	cfg := createTestKafkaConfigForBackend(backend, clusterAddr, topicName)
	writer, reg := createTestWriter(t, cfg)

	req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}, Source: mimirpb.API}
	require.NoError(t, writer.WriteSync(ctx, topicName, partitionID, tenantID, req))

	mfm, err := dskit_metrics.NewMetricFamilyMapFromGatherer(reg)
	require.NoError(t, err)

	// Counters a single successful produce must increment on both backends.
	for _, name := range []string{
		"cortex_ingest_storage_writer_connects_total",
		"cortex_ingest_storage_writer_read_bytes_total",
		"cortex_ingest_storage_writer_write_bytes_total",
		"cortex_ingest_storage_writer_produce_records_total",
		"cortex_ingest_storage_writer_produce_batches_total",
		"cortex_ingest_storage_writer_produce_bytes_total",
		"cortex_ingest_storage_writer_produce_compressed_bytes_total",
	} {
		assert.Positive(t, mfm.SumCounters(name), "counter %q must be populated on backend %q", name, backend)
	}

	// Extended latency histograms (Mimir's kafka_* native histograms) a
	// successful produce must observe on both backends.
	for _, name := range []string{
		"cortex_ingest_storage_writer_kafka_read_time_seconds",
		"cortex_ingest_storage_writer_kafka_read_wait_seconds",
		"cortex_ingest_storage_writer_kafka_write_time_seconds",
		"cortex_ingest_storage_writer_kafka_write_wait_seconds",
		"cortex_ingest_storage_writer_kafka_request_duration_e2e_seconds",
	} {
		hist, err := dskit_metrics.FindHistogramWithNameAndLabels(mfm, name)
		require.NoError(t, err, "histogram %q must exist on backend %q", name, backend)
		assert.Positive(t, hist.GetSampleCount(), "histogram %q must be observed on backend %q", name, backend)
	}

	// Metrics registered on both backends that legitimately read zero after a
	// successful produce: the produce buffer has drained, the writer never
	// fetches, and the mock broker never throttles. Assert they're exposed.
	for _, name := range []string{
		"cortex_ingest_storage_writer_buffered_produce_records_total",
		"cortex_ingest_storage_writer_buffered_produce_bytes",
		"cortex_ingest_storage_writer_buffered_fetch_records_total",
		"cortex_ingest_storage_writer_buffered_fetch_bytes",
		"cortex_ingest_storage_writer_kafka_request_throttled_seconds",
	} {
		assert.Contains(t, mfm, name, "metric %q must be registered on backend %q", name, backend)
	}

	// cortex_ingest_storage_writer_{connect_errors,write_errors,read_errors}_total
	// and _disconnects_total are wired on both backends too, but they are
	// CounterVecs that emit no series until an error or disconnect occurs, so a
	// successful produce can't observe them here.
}

func TestWriter_AutoCreateTopics(t *testing.T) {
	const baseTopic = "test"

	listTopics := func(t *testing.T, clusterAddr string) map[string]struct{} {
		client, err := kgo.NewClient(commonKafkaClientOptions(createTestKafkaConfig(clusterAddr, baseTopic), nil, test.NewTestingLogger(t))...)
		require.NoError(t, err)
		t.Cleanup(client.Close)

		adm := kadm.NewClient(client)
		details, err := adm.ListTopics(context.Background())
		require.NoError(t, err)

		names := make(map[string]struct{}, len(details))
		for name := range details {
			names[name] = struct{}{}
		}
		return names
	}

	t.Run("auto-creates the configured topic when no explicit set is provided", func(t *testing.T) {
		_, clusterAddr := testkafka.CreateCluster(t, 1, baseTopic)

		cfg := createTestKafkaConfig(clusterAddr, "auto-created")
		cfg.AutoCreateTopicEnabled = true
		cfg.AutoCreateTopicDefaultPartitions = 1

		writer := NewWriter(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry())
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), writer))
		t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), writer)) })

		topics := listTopics(t, clusterAddr)
		assert.Contains(t, topics, "auto-created")
	})

	t.Run("auto-creates the explicit set of topics, not the configured topic", func(t *testing.T) {
		_, clusterAddr := testkafka.CreateCluster(t, 1, baseTopic)

		// The configured topic is a template that must never be created literally.
		cfg := createTestKafkaConfig(clusterAddr, "mimir-ingest-rc-<read-compartment-id>")
		cfg.AutoCreateTopicEnabled = true
		cfg.AutoCreateTopicDefaultPartitions = 1

		writer := NewWriter(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry(), WithAutoCreateTopics([]string{"mimir-ingest-rc-0", "mimir-ingest-rc-1"}))
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), writer))
		t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), writer)) })

		topics := listTopics(t, clusterAddr)
		assert.Contains(t, topics, "mimir-ingest-rc-0")
		assert.Contains(t, topics, "mimir-ingest-rc-1")
		assert.NotContains(t, topics, "mimir-ingest-rc-<read-compartment-id>")
	})
}

// runForEachKafkaBackend runs fn as a subtest for every supported Kafka
// producer backend. Tests that exercise the Writer should wrap their bodies
// with this helper so behavior is verified for both backends.
func runForEachKafkaBackend(t *testing.T, fn func(t *testing.T, backend string)) {
	t.Helper()
	for _, backend := range []string{KafkaBackendKafka, KafkaBackendWarpstream} {
		t.Run(backend, func(t *testing.T) {
			fn(t, backend)
		})
	}
}

func createTestWriter(t *testing.T, cfg KafkaConfig) (*Writer, prometheus.Gatherer) {
	reg := prometheus.NewPedanticRegistry()

	writer := NewWriter(cfg, test.NewTestingLogger(t), reg)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), writer))

	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), writer))
	})

	return writer, reg
}

func deserializeRecord(t *testing.T, rec *kgo.Record) mimirpb.WriteRequest {
	t.Helper()

	version := ParseRecordVersion(rec)
	var prealloc mimirpb.PreallocWriteRequest
	require.NoError(t, DeserializeRecordContent(rec.Value, &prealloc, version))
	prealloc.ClearTimeseriesUnmarshalData()
	return prealloc.WriteRequest
}

func createTestKafkaClient(t *testing.T, cfg KafkaConfig) *kgo.Client {
	metrics := kprom.NewMetrics("", kprom.Registerer(prometheus.NewPedanticRegistry()))
	opts := commonKafkaClientOptions(cfg, metrics, test.NewTestingLogger(t))

	// Use the manual partitioner because produceRecord() utility explicitly specifies
	// the partition to write to in the kgo.Record itself.
	opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))

	client, err := kgo.NewClient(opts...)
	require.NoError(t, err)

	// Automatically close it at the end of the test.
	t.Cleanup(client.Close)

	return client
}
