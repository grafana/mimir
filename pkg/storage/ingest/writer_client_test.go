// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"crypto/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestNewKafkaWriterClient_ShouldSupportSASLPlainAuthentication(t *testing.T) {
	const (
		topicName     = "test"
		numPartitions = 1
		username      = "mimir"
		password      = "supersecret"
	)

	_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName, testkafka.WithSASLPlain(username, password))

	t.Run("should fail if the provided auth is wrong", func(t *testing.T) {
		t.Parallel()

		cfg := createTestKafkaConfig(clusterAddr, topicName)
		cfg.SASL.Username = username
		require.NoError(t, cfg.SASL.Password.Set("wrong"))

		client, err := NewKafkaWriterClient(cfg, 1, log.NewNopLogger(), prometheus.NewPedanticRegistry())
		require.NoError(t, err)
		t.Cleanup(client.Close)

		require.Error(t, client.Ping(context.Background()))
	})

	t.Run("should succeed if the provided auth is good", func(t *testing.T) {
		t.Parallel()

		cfg := createTestKafkaConfig(clusterAddr, topicName)
		cfg.SASL.Username = username
		require.NoError(t, cfg.SASL.Password.Set(password))

		client, err := NewKafkaWriterClient(cfg, 1, log.NewNopLogger(), prometheus.NewPedanticRegistry())
		require.NoError(t, err)
		t.Cleanup(client.Close)

		require.NoError(t, client.Ping(context.Background()))
	})
}

func TestNewKafkaWriterClient_ShouldTrackExtendedKafkaClientMetrics(t *testing.T) {
	const (
		topicName     = "test"
		numPartitions = 1
	)

	_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)

	cfg := createTestKafkaConfig(clusterAddr, topicName)
	reg := prometheus.NewPedanticRegistry()
	prefixedReg := prometheus.WrapRegistererWithPrefix(writerMetricsPrefix, reg)

	client, err := NewKafkaWriterClient(cfg, 1, log.NewNopLogger(), prefixedReg)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	res := client.ProduceSync(context.Background(), &kgo.Record{Key: []byte("test"), Value: []byte("message 1")})
	require.NoError(t, res.FirstErr())

	metrics, err := dskit_metrics.NewMetricFamilyMapFromGatherer(reg)
	require.NoError(t, err)

	expectedHistograms := []string{
		"cortex_ingest_storage_writer_kafka_write_wait_seconds",
		"cortex_ingest_storage_writer_kafka_write_time_seconds",
		"cortex_ingest_storage_writer_kafka_read_wait_seconds",
		"cortex_ingest_storage_writer_kafka_read_time_seconds",
		"cortex_ingest_storage_writer_kafka_request_duration_e2e_seconds",
	}

	for _, expectedHistogram := range expectedHistograms {
		t.Run(expectedHistogram, func(t *testing.T) {
			actualHistogram, err := dskit_metrics.FindHistogramWithNameAndLabels(metrics, expectedHistogram)
			require.NoError(t, err)

			// Metrics are tracked for each Kafka message. Even if we produced only 1 record,
			// the Kafka client actually issues other messages too (e.g. Metadata).
			require.GreaterOrEqual(t, *actualHistogram.SampleCount, uint64(1))
		})
	}
}

func TestKafkaProducer_ShouldExposeBufferedBytesLimit(t *testing.T) {
	const (
		numPartitions = 1
		topicName     = "test"
	)

	_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
	cfg := createTestKafkaConfig(clusterAddr, topicName)
	cfg.ProducerMaxBufferedBytes = 1024 * 1024

	reg := prometheus.NewPedanticRegistry()
	prefixedReg := prometheus.WrapRegistererWithPrefix(writerMetricsPrefix, reg)

	client, err := NewKafkaWriterClient(cfg, 1, log.NewNopLogger(), prefixedReg)
	require.NoError(t, err)

	producer := NewKafkaProducer(client, cfg.ProducerMaxBufferedBytes, prefixedReg)
	t.Cleanup(producer.Close)

	assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingest_storage_writer_buffered_produce_bytes_limit The bytes limit on buffered produce records. Produce requests fail once this limit is reached.
		# TYPE cortex_ingest_storage_writer_buffered_produce_bytes_limit gauge
		cortex_ingest_storage_writer_buffered_produce_bytes_limit 1.048576e+06
	`), "cortex_ingest_storage_writer_buffered_produce_bytes_limit"))
}

func TestKafkaProducer_ProduceSync_ShouldTrackBufferedProduceBytes(t *testing.T) {
	const (
		numPartitions = 1
		topicName     = "test"
	)

	cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)

	// Configure Kafka to block on Produce requests until the test unblocks it.
	unblockProduceRequests := make(chan struct{})
	cluster.ControlKey(int16(kmsg.Produce), func(_ kmsg.Request) (kmsg.Response, error, bool) {
		<-unblockProduceRequests
		return nil, nil, false
	})

	ctx := context.Background()
	cfg := createTestKafkaConfig(clusterAddr, topicName)
	reg := prometheus.NewPedanticRegistry()
	prefixedReg := prometheus.WrapRegistererWithPrefix(writerMetricsPrefix, reg)

	client, err := NewKafkaWriterClient(cfg, 1, log.NewNopLogger(), prefixedReg)
	require.NoError(t, err)

	producer := NewKafkaProducer(client, cfg.ProducerMaxBufferedBytes, prefixedReg)
	t.Cleanup(producer.Close)

	wg := sync.WaitGroup{}

	// At the beginning, the buffered produced bytes metric should be 0.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.InDelta(collect, 0, getSummaryQuantileValue(collect, reg, "cortex_ingest_storage_writer_buffered_produce_bytes_distribution", 1), 0.0001)
	}, time.Second, 100*time.Millisecond)

	// Produce a 1st record.
	wg.Add(1)
	client.Produce(ctx, &kgo.Record{Key: []byte("test"), Value: []byte("message 1")}, func(_ *kgo.Record, err error) {
		defer wg.Done()
		require.NoError(t, err)
	})

	// Get the expected record size directly from Kafka client.
	expectedRecordSize := client.BufferedProduceBytes()
	require.Greater(t, expectedRecordSize, int64(0))
	t.Logf("expected record size bytes: %d", expectedRecordSize)

	// At this point, the buffered produced bytes metric should have tracked 1 record.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.InDelta(collect, expectedRecordSize, getSummaryQuantileValue(collect, reg, "cortex_ingest_storage_writer_buffered_produce_bytes_distribution", 1), 0.0001)
	}, time.Second, 100*time.Millisecond)

	// Produce a 2nd record, while the 1st is still in-flight (because in this test Produce requests are blocked on Kafka side).
	wg.Add(1)
	client.Produce(ctx, &kgo.Record{Key: []byte("test"), Value: []byte("message 1")}, func(_ *kgo.Record, err error) {
		defer wg.Done()
		require.NoError(t, err)
	})

	// At this point, the buffered produced bytes metric should have tracked 2 records.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.InDelta(collect, 2*expectedRecordSize, getSummaryQuantileValue(collect, reg, "cortex_ingest_storage_writer_buffered_produce_bytes_distribution", 1), 0.0001)
	}, time.Second, 100*time.Millisecond)

	// Release Produce requests and wait until done.
	close(unblockProduceRequests)
	wg.Wait()
}

func TestKafkaProducer_ProduceSync_ShouldCircuitBreakIfContextIsDone(t *testing.T) {
	const (
		numPartitions = 1
		topicName     = "test"
	)

	_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)

	ctx := context.Background()
	cfg := createTestKafkaConfig(clusterAddr, topicName)
	reg := prometheus.NewPedanticRegistry()
	prefixedReg := prometheus.WrapRegistererWithPrefix(writerMetricsPrefix, reg)

	client, err := NewKafkaWriterClient(cfg, defaultMaxInflightProduceRequests, log.NewNopLogger(), prefixedReg)
	require.NoError(t, err)

	producer := NewKafkaProducer(client, cfg.ProducerMaxBufferedBytes, prefixedReg)
	t.Cleanup(producer.Close)

	// Create a context with an expired deadline.
	expiredCtx, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	t.Cleanup(cancel)

	res := producer.ProduceSync(expiredCtx, []*kgo.Record{{Key: []byte("test"), Value: []byte("message 1")}})
	require.ErrorIs(t, res.FirstErr(), context.DeadlineExceeded)

	// We expect no records buffered in the Kafka client, because of the circuit breaker.
	require.Equal(t, int64(0), producer.BufferedProduceRecords())

	// Even if no record was actually enqueued, we expect the metric was tracked anyway to keep it consistent
	// (we don't want to lose track of such records that, if context wasn't already done, they would have been produced).
	assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingest_storage_writer_produce_records_enqueued_total Total number of Kafka records enqueued to be sent to the Kafka backend (includes records that fail to be successfully sent to the Kafka backend).
		# TYPE cortex_ingest_storage_writer_produce_records_enqueued_total counter
		cortex_ingest_storage_writer_produce_records_enqueued_total{} 1

		# HELP cortex_ingest_storage_writer_produce_records_failed_total Total number of Kafka records that failed to be sent to the Kafka backend.
		# TYPE cortex_ingest_storage_writer_produce_records_failed_total counter
		cortex_ingest_storage_writer_produce_records_failed_total{reason="cancelled-before-producing"} 1
	`),
		"cortex_ingest_storage_writer_produce_records_enqueued_total",
		"cortex_ingest_storage_writer_produce_records_failed_total"))

	// We expect the circuit breaker triggers after we track the remaining context deadline (that in this case is 0).
	metrics, err := dskit_metrics.NewMetricFamilyMapFromGatherer(reg)
	require.NoError(t, err)
	histogram, err := dskit_metrics.FindHistogramWithNameAndLabels(metrics, "cortex_ingest_storage_writer_produce_remaining_deadline_seconds")
	require.NoError(t, err)
	require.Equal(t, uint64(1), *histogram.SampleCount)
	require.Equal(t, float64(0), *histogram.SampleSum)
}

func TestKafkaProducer_ProduceSync_LatencyShouldBeDrivenByKafkaProduceLatency(t *testing.T) {
	t.Parallel()

	const (
		topicName                     = "test"
		tenantID                      = "user-1"
		numPartitions                 = 100
		produceRecordsInterval        = 2 * time.Millisecond
		produceRecordsDuration        = 10 * time.Second
		produceRecordsTotal           = produceRecordsDuration / produceRecordsInterval
		produceRecordsThroughputBytes = 50_000_000
		produceRecordSizeBytes        = produceRecordsThroughputBytes / int(time.Second/produceRecordsInterval)
		produceLatency                = 2 * time.Second
	)

	// Set a max execution time for this test.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	cluster, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)

	// Simulate high latency on Produce requests.
	cluster.ControlKey(int16(kmsg.Produce), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.ProduceRequest)

		numRecords, err := getProduceRequestRecordsCount(req)
		require.NoError(t, err)
		highestTimestamp, err := getProduceRequestHighestTimestamp(req)
		require.NoError(t, err)

		// The fake Kafka server we use in unit tests have a single thread control loop. This means that when we
		// add latency to a request, other requests pipelined on the same connections are not evaluated until
		// the previous requests are processed.
		//
		// In this test we want to simulate the case each Produce takes X time to execute since when it was written
		// on the wire. We don't have the timestamp when it was sent on the network, so we use the highest record timestamp
		// as an approximation.
		simulatedLatency := max(0, produceLatency-time.Since(highestTimestamp))
		t.Log(time.Now().String(), "Produce - num records:", numRecords, "highestTimestamp:", highestTimestamp.String(), "simulatedLatency:", simulatedLatency)

		cluster.KeepControl()
		cluster.SleepControl(func() {
			select {
			case <-ctx.Done():
			case <-time.After(simulatedLatency):
			}
		})

		return nil, nil, false
	})

	cfg := createTestKafkaConfig(clusterAddr, topicName)
	reg := prometheus.NewPedanticRegistry()
	client, err := NewKafkaWriterClient(cfg, defaultMaxInflightProduceRequests, log.NewNopLogger(), reg)
	require.NoError(t, err)

	producer := NewKafkaProducer(client, cfg.ProducerMaxBufferedBytes, reg)
	t.Cleanup(producer.Close)

	// Generate a random payload to reduce its compression ratio.
	recordPayload, err := generateRandomBytes(produceRecordSizeBytes)
	require.NoError(t, err)

	// Produce records at fixed interval.
	produceLatencySecondsSum := atomic.NewFloat64(0)
	producerWg := sync.WaitGroup{}
	producerWg.Add(1)

	go func() {
		recordsAcked := atomic.NewInt32(0)

		for recordID := 0; recordID < int(produceRecordsTotal); recordID++ {
			producer.Produce(ctx, &kgo.Record{
				Key:       []byte(tenantID),
				Value:     recordPayload,
				Partition: int32(recordID % numPartitions), // Round robin across partitions.
				Timestamp: time.Now(),
			}, func(record *kgo.Record, err error) {
				// Keep track of the latency.
				produceLatencySecondsSum.Add(time.Since(record.Timestamp).Seconds())

				// We're done once we've got the ACK for all records we produced.
				if recordsAcked.Inc() >= int32(produceRecordsTotal) {
					producerWg.Done()
				}
			})

			// Throttle.
			select {
			case <-ctx.Done():
				// The test timed out. Let's unblock the main test goroutine.
				producerWg.Done()
				return

			case <-time.After(produceRecordsInterval):
			}
		}
	}()

	// Wait until all records have been produced.
	producerWg.Wait()

	// Ensure the test didn't timed out.
	require.NoError(t, ctx.Err())

	// We expect the average produce latency to be close to the simulated Kafka produce latency.
	averageProduceLatencySeconds := produceLatencySecondsSum.Load() / float64(produceRecordsTotal)
	t.Logf("average produce latency seconds: %.2f", averageProduceLatencySeconds)
	require.InDelta(t, produceLatency.Seconds(), averageProduceLatencySeconds, 1.)
}

func getSummaryQuantileValue(t require.TestingT, reg prometheus.Gatherer, metricName string, quantile float64) float64 {
	const delta = 0.0001

	metrics, err := reg.Gather()
	require.NoError(t, err)

	for _, family := range metrics {
		if family.GetName() != metricName || family.GetType() != dto.MetricType_SUMMARY {
			continue
		}

		require.Len(t, family.Metric, 1)
		require.NotNil(t, family.Metric[0].Summary)

		for _, q := range family.Metric[0].Summary.Quantile {
			if q.GetQuantile() > quantile-delta && q.GetQuantile() < quantile+delta {
				return q.GetValue()
			}
		}
	}

	require.Failf(t, "summary metric %s or quantile %f not found", metricName, quantile)
	return 0
}

func generateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	return b, err
}
