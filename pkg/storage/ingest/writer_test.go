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
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestMain(m *testing.M) {
	test.VerifyNoLeakTestMain(m)
}

func TestWriter_WriteSync(t *testing.T) {
	const (
		topicName     = "test"
		numPartitions = 2
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
		writer, reg := createTestWriter(t, createTestKafkaConfig(clusterAddr, topicName))

		produceRequestProcessed := atomic.NewBool(false)

		cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
			// Add a delay, so that if WriteSync() will not wait then the test will fail.
			time.Sleep(time.Second)
			produceRequestProcessed.Store(true)

			return nil, nil, false
		})

		err := writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: multiSeries, Metadata: nil, Source: mimirpb.API})
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
		`, len(fetches.Records()[0].Value))), "cortex_ingest_storage_writer_sent_bytes_total", "cortex_ingest_storage_writer_records_per_write_request"))
	})

	t.Run("should block until data has been committed to storage (WriteRequest stored in multiple records)", func(t *testing.T) {
		t.Parallel()

		cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)

		// Customize the max record size to force splitting the WriteRequest into two records.
		expectedReq := &mimirpb.WriteRequest{Timeseries: multiSeries, Metadata: nil, Source: mimirpb.API}
		cfg := createTestKafkaConfig(clusterAddr, topicName)
		cfg.ProducerMaxRecordSizeBytes = int(float64(expectedReq.Size()) * 0.8)

		writer, reg := createTestWriter(t, cfg)

		produceRequestProcessed := atomic.NewBool(false)
		cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
			// Add a delay, so that if WriteSync() will not wait then the test will fail.
			time.Sleep(time.Second)
			produceRequestProcessed.Store(true)

			return nil, nil, false
		})

		err := writer.WriteSync(ctx, partitionID, tenantID, expectedReq)
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

		actualReq1 := &mimirpb.WriteRequest{}
		actualReq2 := &mimirpb.WriteRequest{}
		require.NoError(t, actualReq1.Unmarshal(records[0].Value))
		require.NoError(t, actualReq2.Unmarshal(records[1].Value))

		actualMergedReq := *actualReq1
		actualMergedReq.Timeseries = append(actualMergedReq.Timeseries, actualReq2.Timeseries...)
		actualMergedReq.ClearTimeseriesUnmarshalData()
		assert.Equal(t, expectedReq, &actualMergedReq)

		// Check metrics.
		expectedBytes := len(records[0].Value) + len(records[1].Value)

		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_ingest_storage_writer_sent_bytes_total Total number of bytes sent to the ingest storage.
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
		`, expectedBytes)), "cortex_ingest_storage_writer_sent_bytes_total", "cortex_ingest_storage_writer_records_per_write_request"))
	})

	t.Run("should write to the requested partition", func(t *testing.T) {
		t.Parallel()

		for _, writeClients := range []int{1, 2, 10} {
			writeClients := writeClients

			t.Run(fmt.Sprintf("Write clients = %d", writeClients), func(t *testing.T) {
				t.Parallel()

				seriesPerPartition := map[int32][]mimirpb.PreallocTimeseries{
					0: series1,
					1: series2,
				}

				_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
				config := createTestKafkaConfig(clusterAddr, topicName)
				config.WriteClients = writeClients
				writer, _ := createTestWriter(t, config)

				// Write to partitions.
				for partitionID, series := range seriesPerPartition {
					err := writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series, Metadata: nil, Source: mimirpb.API})
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

					received := mimirpb.WriteRequest{}
					require.NoError(t, received.Unmarshal(fetches.Records()[0].Value))
					require.Len(t, received.Timeseries, len(expectedSeries))

					for idx, expected := range expectedSeries {
						assert.Equal(t, expected.Labels, received.Timeseries[idx].Labels)
						assert.Equal(t, expected.Samples, received.Timeseries[idx].Samples)
					}
				}
			})
		}
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
		writer, _ := createTestWriter(t, createTestKafkaConfig(clusterAddr, topicName))

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

		// Write the first record, which is expected to be sent immediately.
		runAsync(&wg, func() {
			assert.NoError(t, writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series1, Metadata: nil, Source: mimirpb.API}))
		})

		// Once the 1st Produce request is received by the server but still processing (there's a 1s sleep),
		// issue two more requests. One with a short context timeout (expected to expire before the next Produce
		// request will be sent) and one with no timeout.
		runAsyncAfter(&wg, firstRequestReceived, func() {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()

			assert.Equal(t, context.DeadlineExceeded, writer.WriteSync(ctxWithTimeout, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series2, Metadata: nil, Source: mimirpb.API}))
		})

		runAsyncAfter(&wg, firstRequestReceived, func() {
			assert.NoError(t, writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series3, Metadata: nil, Source: mimirpb.API}))
		})

		wg.Wait()

		// Cancelling the context doesn't actually prevent that data from being sent to the wire.
		require.Equal(t, []int{1, 2}, receivedBatchesLength)
	})

	t.Run("should batch multiple subsequent records together while sending the previous batches to Kafka once max in-flight Produce requests limit has been reached", func(t *testing.T) {
		t.Parallel()

		var (
			firstRequest         = atomic.NewBool(true)
			firstRequestReceived = make(chan struct{})

			receivedBatchesLengthMx sync.Mutex
			receivedBatchesLength   []int
		)

		cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		writer, _ := createTestWriter(t, createTestKafkaConfig(clusterAddr, topicName))

		// Allow only 1 in-flight Produce request in this test, to easily reproduce the scenario.
		writer.maxInflightProduceRequests = 1

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
			assert.NoError(t, writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series1, Metadata: nil, Source: mimirpb.API}))
		})

		runAsyncAfter(&wg, firstRequestReceived, func() {
			assert.NoError(t, writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series2, Metadata: nil, Source: mimirpb.API}))
		})

		runAsyncAfter(&wg, firstRequestReceived, func() {
			// Ensure the 3rd call to Write() is issued slightly after the 2nd one,
			// otherwise records may be batched just because of concurrent append to it
			// and not because it's waiting for the 1st call to complete.
			time.Sleep(100 * time.Millisecond)

			assert.NoError(t, writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series3, Metadata: nil, Source: mimirpb.API}))
		})

		wg.Wait()

		// We expect that the next 2 records have been appended to the next batch.
		require.Equal(t, []int{1, 2}, receivedBatchesLength)
	})

	t.Run("should return error on non existing partition", func(t *testing.T) {
		t.Parallel()

		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		writer, _ := createTestWriter(t, createTestKafkaConfig(clusterAddr, topicName))

		// Write to a non-existing partition.
		err := writer.WriteSync(ctx, 100, tenantID, &mimirpb.WriteRequest{Timeseries: multiSeries, Metadata: nil, Source: mimirpb.API})
		require.Error(t, err)
	})

	t.Run("should return an error and stop retrying sending a record once the write timeout expires", func(t *testing.T) {
		t.Parallel()

		cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		kafkaCfg := createTestKafkaConfig(clusterAddr, topicName)
		writer, _ := createTestWriter(t, kafkaCfg)

		cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
			// Keep failing every request.
			cluster.KeepControl()
			return nil, errors.New("mock error"), true
		})

		startTime := time.Now()
		require.Equal(t, kgo.ErrRecordTimeout, writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series1, Metadata: nil, Source: mimirpb.API}))
		elapsedTime := time.Since(startTime)

		require.Greater(t, elapsedTime, kafkaCfg.WriteTimeout/2)
		require.Less(t, elapsedTime, kafkaCfg.WriteTimeout*3) // High tolerance because the client does a backoff and timeout is evaluated after the backoff.
	})

	// This test documents how the Kafka client works. It's not what we ideally want, but it's how it works.
	t.Run("should fail all buffered records and close the connection on timeout while waiting for Produce response", func(t *testing.T) {
		t.Parallel()

		cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
		kafkaCfg := createTestKafkaConfig(clusterAddr, topicName)
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
			assert.Equal(t, kgo.ErrRecordTimeout, writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series1, Metadata: nil, Source: mimirpb.API}))
			elapsedTime := time.Since(startTime)

			// It should take nearly the client's write timeout.
			expectedElapsedTime := kafkaCfg.WriteTimeout + writerRequestTimeoutOverhead
			assert.InDelta(t, expectedElapsedTime, elapsedTime, float64(time.Second))
		})

		// The 2nd request is fired while the 1st is still executing, but will fail anyone because the previous
		// failure causes all subsequent buffered records to fail too.
		runAsync(&wg, func() {
			<-firstRequestReceived

			// Wait 500ms less than the client timeout.
			delay := 500 * time.Millisecond
			time.Sleep(kafkaCfg.WriteTimeout + writerRequestTimeoutOverhead - delay)

			startTime := time.Now()
			assert.Equal(t, kgo.ErrRecordTimeout, writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series2, Metadata: nil, Source: mimirpb.API}))
			elapsedTime := time.Since(startTime)

			// We expect to fail once the previous request fails, so it should take nearly the client's write timeout
			// minus the artificial delay introduced above.
			tolerance := time.Second
			assert.Less(t, elapsedTime, delay+tolerance)
		})

		wg.Wait()
	})
}

func TestMarshalWriteRequestToRecords(t *testing.T) {
	req := &mimirpb.WriteRequest{
		Source:                  mimirpb.RULE,
		SkipLabelNameValidation: true,
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
	require.NotZero(t, req.SkipLabelNameValidation)
	require.NotZero(t, req.Timeseries)
	require.NotZero(t, req.Metadata)

	t.Run("should return 1 record if the input WriteRequest size is less than the size limit", func(t *testing.T) {
		records, size, err := marshalWriteRequestToRecords(1, "user-1", req, req.Size()*2)
		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, len(records[0].Value), size)

		actual := &mimirpb.WriteRequest{}
		require.NoError(t, actual.Unmarshal(records[0].Value))

		actual.ClearTimeseriesUnmarshalData()
		assert.Equal(t, req, actual)
	})

	t.Run("should return multiple records if the input WriteRequest size is bigger than the size limit", func(t *testing.T) {
		const limit = 100

		records, size, err := marshalWriteRequestToRecords(1, "user-1", req, limit)
		require.NoError(t, err)
		require.Len(t, records, 4)

		// Assert each record, and decode all partial WriteRequests.
		expectedSize := 0
		partials := make([]*mimirpb.WriteRequest, 0, len(records))

		for _, rec := range records {
			assert.Equal(t, int32(1), rec.Partition)
			assert.Equal(t, "user-1", string(rec.Key))
			assert.Less(t, len(rec.Value), limit)

			expectedSize += len(rec.Value)

			actual := &mimirpb.WriteRequest{}
			require.NoError(t, actual.Unmarshal(rec.Value))

			actual.ClearTimeseriesUnmarshalData()
			partials = append(partials, actual)
		}

		assert.Equal(t, expectedSize, size)
		assert.Equal(t, []*mimirpb.WriteRequest{
			{
				Source:                  mimirpb.RULE,
				SkipLabelNameValidation: true,
				Timeseries:              []mimirpb.PreallocTimeseries{req.Timeseries[0], req.Timeseries[1]},
			}, {
				Source:                  mimirpb.RULE,
				SkipLabelNameValidation: true,
				Timeseries:              []mimirpb.PreallocTimeseries{req.Timeseries[2]},
			}, {
				Source:                  mimirpb.RULE,
				SkipLabelNameValidation: true,
				Metadata:                []*mimirpb.MetricMetadata{req.Metadata[0], req.Metadata[1]},
			}, {
				Source:                  mimirpb.RULE,
				SkipLabelNameValidation: true,
				Metadata:                []*mimirpb.MetricMetadata{req.Metadata[2]},
			},
		}, partials)
	})

	t.Run("should return multiple records, larger than the limit, if the Timeseries and Metadata entries in the WriteRequest are bigger than limit", func(t *testing.T) {
		const limit = 1

		records, _, err := marshalWriteRequestToRecords(1, "user-1", req, limit)
		require.NoError(t, err)
		require.Len(t, records, 6)

		// Decode all partial WriteRequests.
		partials := make([]*mimirpb.WriteRequest, 0, len(records))
		for _, rec := range records {
			assert.Greater(t, len(rec.Value), limit)

			actual := &mimirpb.WriteRequest{}
			require.NoError(t, actual.Unmarshal(rec.Value))

			actual.ClearTimeseriesUnmarshalData()
			partials = append(partials, actual)
		}

		assert.Equal(t, []*mimirpb.WriteRequest{
			{
				Source:                  mimirpb.RULE,
				SkipLabelNameValidation: true,
				Timeseries:              []mimirpb.PreallocTimeseries{req.Timeseries[0]},
			}, {
				Source:                  mimirpb.RULE,
				SkipLabelNameValidation: true,
				Timeseries:              []mimirpb.PreallocTimeseries{req.Timeseries[1]},
			}, {
				Source:                  mimirpb.RULE,
				SkipLabelNameValidation: true,
				Timeseries:              []mimirpb.PreallocTimeseries{req.Timeseries[2]},
			}, {
				Source:                  mimirpb.RULE,
				SkipLabelNameValidation: true,
				Metadata:                []*mimirpb.MetricMetadata{req.Metadata[0]},
			}, {
				Source:                  mimirpb.RULE,
				SkipLabelNameValidation: true,
				Metadata:                []*mimirpb.MetricMetadata{req.Metadata[1]},
			}, {
				Source:                  mimirpb.RULE,
				SkipLabelNameValidation: true,
				Metadata:                []*mimirpb.MetricMetadata{req.Metadata[2]},
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

	b.Run("marshalWriteRequestToRecords()", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			records, _, err := marshalWriteRequestToRecords(1, "user-1", req, 1024*1024*1024)
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
	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels:    []mimirpb.LabelAdapter{{Name: "__name__", Value: metricName}},
			Samples:   []mimirpb.Sample{{TimestampMs: 1, Value: 2}},
			Exemplars: []mimirpb.Exemplar{}, // Makes comparison with unmarshalled TimeSeries easy.
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

func createTestKafkaConfig(clusterAddr, topicName string) KafkaConfig {
	cfg := KafkaConfig{}
	flagext.DefaultValues(&cfg)

	cfg.Address = clusterAddr
	cfg.Topic = topicName
	cfg.WriteTimeout = 2 * time.Second

	return cfg
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
