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

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest/ingestpb"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestMain(m *testing.M) {
	test.VerifyNoLeakTestMain(m)
}

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

		wAgents := createTestWriteAgentSelector()
		writer, reg := createTestWriter(t, wAgents)

		produceRequestProcessed := atomic.NewBool(false)

		wAgents.onWrite(func(*ingestpb.WriteRequest) (*ingestpb.WriteResponse, error) {
			// Add a delay, so that if WriteSync() will not wait then the test will fail.
			time.Sleep(time.Second)
			produceRequestProcessed.Store(true)

			return &ingestpb.WriteResponse{}, nil
		})

		err := writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: multiSeries, Metadata: nil, Source: mimirpb.API})
		require.NoError(t, err)

		// Ensure it was processed before returning.
		assert.True(t, produceRequestProcessed.Load())

		require.Len(t, wAgents.receivedWrites, 1)
		received := wAgents.receivedWrites[0].Piece.WriteRequest
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
		`, wAgents.receivedWrites[0].Size())), "cortex_ingest_storage_writer_sent_bytes_total"))
	})

	t.Run("should interrupt the WriteSync() on context cancelled but other concurrent requests should not fail", func(t *testing.T) {
		t.Skip("this test confirms franz-go behavior, which is not used in grafana streams")
		t.Parallel()

		var (
			firstRequest         = atomic.NewBool(true)
			firstRequestReceived = make(chan struct{})

			receivedBatchesLengthMx sync.Mutex
			receivedBatchesLength   []int
		)
		wAgents := createTestWriteAgentSelector()
		writer, _ := createTestWriter(t, wAgents)

		wAgents.onWrite(func(request *ingestpb.WriteRequest) (*ingestpb.WriteResponse, error) {
			numRecords := 1

			receivedBatchesLengthMx.Lock()
			receivedBatchesLength = append(receivedBatchesLength, numRecords)
			receivedBatchesLengthMx.Unlock()

			if firstRequest.CompareAndSwap(true, false) {
				close(firstRequestReceived)

				// Introduce a delay on the 1st Produce.
				time.Sleep(time.Second)
			}

			return nil, nil
		})

		wg := sync.WaitGroup{}

		// Write the first record, which is expected to be sent immediately.
		runAsync(&wg, func() {
			require.NoError(t, writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series1, Metadata: nil, Source: mimirpb.API}))
		})

		// Once the 1st Produce request is received by the server but still processing (there's a 1s sleep),
		// issue two more requests. One with a short context timeout (expected to expire before the next Produce
		// request will be sent) and one with no timeout.
		runAsyncAfter(&wg, firstRequestReceived, func() {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()

			require.Equal(t, context.DeadlineExceeded, writer.WriteSync(ctxWithTimeout, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series2, Metadata: nil, Source: mimirpb.API}))
		})

		runAsyncAfter(&wg, firstRequestReceived, func() {
			require.NoError(t, writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series3, Metadata: nil, Source: mimirpb.API}))
		})

		wg.Wait()

		// Cancelling the context doesn't actually prevent that data from being sent to the wire.
		assert.Equal(t, []int{1, 2}, receivedBatchesLength)
	})

	t.Run("should batch multiple subsequent records together while sending the previous batches to Kafka once max in-flight Produce requests limit has been reached", func(t *testing.T) {
		t.Skip("there's no limit for the number of inflight requests when using grafana streams")
		t.Parallel()

		var (
			firstRequest         = atomic.NewBool(true)
			firstRequestReceived = make(chan struct{})

			receivedBatchesLengthMx sync.Mutex
			receivedBatchesLength   []int
		)

		cluster, _ := testkafka.CreateCluster(t, numPartitions, topicName)
		writer, _ := createTestWriter(t, createTestWriteAgentSelector())

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
			require.NoError(t, err)

			receivedBatchesLengthMx.Lock()
			receivedBatchesLength = append(receivedBatchesLength, numRecords)
			receivedBatchesLengthMx.Unlock()

			return nil, nil, false
		})

		wg := sync.WaitGroup{}

		runAsync(&wg, func() {
			require.NoError(t, writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series1, Metadata: nil, Source: mimirpb.API}))
		})

		runAsyncAfter(&wg, firstRequestReceived, func() {
			require.NoError(t, writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series2, Metadata: nil, Source: mimirpb.API}))
		})

		runAsyncAfter(&wg, firstRequestReceived, func() {
			// Ensure the 3rd call to WriteSync() is issued slightly after the 2nd one,
			// otherwise records may be batched just because of concurrent append to it
			// and not because it's waiting for the 1st call to complete.
			time.Sleep(100 * time.Millisecond)

			require.NoError(t, writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series3, Metadata: nil, Source: mimirpb.API}))
		})

		wg.Wait()

		// We expect that the next 2 records have been appended to the next batch.
		assert.Equal(t, []int{1, 2}, receivedBatchesLength)
	})

	t.Run("should return error on non existing partition", func(t *testing.T) {
		t.Skip("grafana streams write agents support all partitions")
		t.Parallel()

		_, _ = testkafka.CreateCluster(t, numPartitions, topicName)
		writer, _ := createTestWriter(t, createTestWriteAgentSelector())

		// Write to a non-existing partition.
		err := writer.WriteSync(ctx, 100, tenantID, &mimirpb.WriteRequest{Timeseries: multiSeries, Metadata: nil, Source: mimirpb.API})
		require.Error(t, err)
	})

	t.Run("should return an error and stop retrying sending a record once the write timeout expires", func(t *testing.T) {
		t.Skip("there's no connection timeout when sending to grafana streams")
		t.Parallel()

		cluster, _ := testkafka.CreateCluster(t, numPartitions, topicName)
		kafkaCfg := createTestWriteAgentSelector()
		writer, _ := createTestWriter(t, kafkaCfg)

		cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
			// Keep failing every request.
			cluster.KeepControl()
			return nil, errors.New("mock error"), true
		})

		//startTime := time.Now()
		require.Equal(t, kgo.ErrRecordTimeout, writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series1, Metadata: nil, Source: mimirpb.API}))
		//elapsedTime := time.Since(startTime)

		//assert.Greater(t, elapsedTime, kafkaCfg.WriteTimeout/2)
		//assert.Less(t, elapsedTime, kafkaCfg.WriteTimeout*3) // High tolerance because the client does a backoff and timeout is evaluated after the backoff.
	})

	// This test documents how the Kafka client works. It's not what we ideally want, but it's how it works.
	t.Run("should fail all buffered records and close the connection on timeout while waiting for Produce response", func(t *testing.T) {
		t.Skip("there's not connection timeout when sending to grafana streams")
		t.Parallel()

		cluster, _ := testkafka.CreateCluster(t, numPartitions, topicName)
		kafkaCfg := createTestWriteAgentSelector()
		writer, _ := createTestWriter(t, kafkaCfg)

		var (
			firstRequest         = atomic.NewBool(true)
			firstRequestReceived = make(chan struct{})
			wg                   = sync.WaitGroup{}
		)

		wg.Add(1)
		cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
			// Ensure the test waits for this too, since the client request will fail earlier
			// (if we don't wait, the test will end before this function and then goleak will
			// report a goroutine leak).
			defer wg.Done()

			if firstRequest.CompareAndSwap(true, false) {
				// The produce request has been received by Kafka, so we can fire the next request.
				close(firstRequestReceived)

				// Inject a slowdown on the 1st Produce request received by Kafka.
				// NOTE: the slowdown is 1s longer than the client timeout.
				//time.Sleep(kafkaCfg.WriteTimeout + writerRequestTimeoutOverhead + time.Second)
			}

			return nil, nil, false
		})

		// The 1st request is expected to fail because Kafka will take longer than the configured timeout.
		runAsync(&wg, func() {
			//startTime := time.Now()
			require.Equal(t, kgo.ErrRecordTimeout, writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series1, Metadata: nil, Source: mimirpb.API}))
			//elapsedTime := time.Since(startTime)

			// It should take nearly the client's write timeout.
			//expectedElapsedTime := kafkaCfg.WriteTimeout + writerRequestTimeoutOverhead
			//assert.InDelta(t, expectedElapsedTime, elapsedTime, float64(time.Second))
		})

		// The 2nd request is fired while the 1st is still executing, but will fail anyone because the previous
		// failure causes all subsequent buffered records to fail too.
		runAsync(&wg, func() {
			<-firstRequestReceived

			// Wait 500ms less than the client timeout.
			delay := 500 * time.Millisecond
			//time.Sleep(kafkaCfg.WriteTimeout + writerRequestTimeoutOverhead - delay)

			startTime := time.Now()
			require.Equal(t, kgo.ErrRecordTimeout, writer.WriteSync(ctx, partitionID, tenantID, &mimirpb.WriteRequest{Timeseries: series2, Metadata: nil, Source: mimirpb.API}))
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
			Labels:    []mimirpb.LabelAdapter{{Name: "__name__", Value: metricName}},
			Samples:   []mimirpb.Sample{{TimestampMs: 1, Value: 2}},
			Exemplars: make([]mimirpb.Exemplar, 0),
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

func createTestContextWithTimeout(t *testing.T, timeout time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}

type mockWriteAgentSelector struct {
	*services.BasicService

	doOnWrite      func(*ingestpb.WriteRequest) (*ingestpb.WriteResponse, error)
	receivedWrites []*ingestpb.WriteRequest
}

func (m *mockWriteAgentSelector) PickServer(partitionID int32) (ingestpb.WriteAgentClient, error) {
	return m, nil
}

func (m *mockWriteAgentSelector) Write(ctx context.Context, in *ingestpb.WriteRequest, opts ...grpc.CallOption) (*ingestpb.WriteResponse, error) {
	m.receivedWrites = append(m.receivedWrites, in)
	return m.doOnWrite(in)
}

func (m *mockWriteAgentSelector) onWrite(f func(*ingestpb.WriteRequest) (*ingestpb.WriteResponse, error)) {
	m.doOnWrite = f
}

func createTestWriteAgentSelector() *mockWriteAgentSelector {
	noopWrite := func(*ingestpb.WriteRequest) (*ingestpb.WriteResponse, error) {
		return &ingestpb.WriteResponse{}, nil
	}
	return &mockWriteAgentSelector{BasicService: services.NewIdleService(nil, nil), doOnWrite: noopWrite}
}

func createTestWriter(t *testing.T, waSelector writeAgentSelector) (*Writer, prometheus.Gatherer) {
	reg := prometheus.NewPedanticRegistry()

	writer, err := newWriter(waSelector, test.NewTestingLogger(t), reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), writer))

	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), writer))
	})

	return writer, reg
}

// TODO remove
func createTestKafkaClient(t *testing.T, cfg KafkaConfig) *kgo.Client {
	metrics := kprom.NewMetrics("", kprom.Registerer(prometheus.NewPedanticRegistry()))

	client, err := kgo.NewClient(commonKafkaClientOptions(cfg, metrics, log.NewNopLogger())...)
	require.NoError(t, err)

	// Automatically close it at the end of the test.
	t.Cleanup(client.Close)

	return client
}
