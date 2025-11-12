// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.uber.org/atomic"
)

// NewKafkaWriterClient returns the kgo.Client that should be used by the Writer.
//
// The input prometheus.Registerer must be wrapped with a prefix (the names of metrics
// registered don't have a prefix).
func NewKafkaWriterClient(kafkaCfg KafkaConfig, maxInflightProduceRequests int, logger log.Logger, reg prometheus.Registerer) (*kgo.Client, error) {
	// Do not export the client ID, because we use it to specify options to the backend.
	metrics := kprom.NewMetrics(
		"", // No prefix. We expect the input prometheus.Registered to be wrapped with a prefix.
		kprom.Registerer(reg),
		kprom.FetchAndProduceDetail(kprom.Batches, kprom.Records, kprom.CompressedBytes, kprom.UncompressedBytes))

	// Allow to disable linger in tests.
	linger := 50 * time.Millisecond
	if kafkaCfg.disableLinger {
		linger = 0
	}

	opts := append(
		commonKafkaClientOptions(kafkaCfg, metrics, logger),

		// Hook our custom Kafka client metrics for the writer client, in order to have a deeper observability
		// when we produce records. We expect the input prometheus.Registered to be wrapped with a prefix.
		kgo.WithHooks(NewKafkaClientExtendedMetrics(reg)),

		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DefaultProduceTopic(kafkaCfg.Topic),

		// We set the partition field in each record.
		kgo.RecordPartitioner(kgo.ManualPartitioner()),

		// Set the upper bounds the size of a record batch.
		kgo.ProducerBatchMaxBytes(producerBatchMaxBytes),

		// By default, the Kafka client allows 1 Produce in-flight request per broker. Disabling write idempotency
		// (which we don't need), we can increase the max number of in-flight Produce requests per broker. A higher
		// number of in-flight requests, in addition to short buffering ("linger") in client side before firing the
		// next Produce request allows us to reduce the end-to-end latency.
		//
		// The result of the multiplication of producer linger and max in-flight requests should match the maximum
		// Produce latency expected by the Kafka backend in a steady state. For example, 50ms * 20 requests = 1s,
		// which means the Kafka client will keep issuing a Produce request every 50ms as far as the Kafka backend
		// doesn't take longer than 1s to process them (if it takes longer, the client will buffer data and stop
		// issuing new Produce requests until some previous ones complete).
		kgo.DisableIdempotentWrite(),
		kgo.ProducerLinger(linger),
		kgo.MaxProduceRequestsInflightPerBroker(maxInflightProduceRequests),

		// Unlimited number of Produce retries but a deadline on the max time a record can take to be delivered.
		// With the default config it would retry infinitely.
		//
		// Details of the involved timeouts:
		// - RecordDeliveryTimeout: how long a Kafka client Produce() call can take for a given record. The overhead
		//   timeout is NOT applied.
		// - ProduceRequestTimeout: how long to wait for the response to the Produce request (the Kafka protocol message)
		//   after being sent on the network. The actual timeout is increased by the configured overhead.
		//
		// When a Produce request to Kafka fail, the client will retry up until the RecordDeliveryTimeout is reached.
		// Once the timeout is reached, the Produce request will fail and all other buffered requests in the client
		// (for the same partition) will fail too. See kgo.RecordDeliveryTimeout() documentation for more info.
		kgo.RecordRetries(math.MaxInt64),
		kgo.RecordDeliveryTimeout(kafkaCfg.WriteTimeout),
		kgo.ProduceRequestTimeout(kafkaCfg.WriteTimeout),
		kgo.RequestTimeoutOverhead(writerRequestTimeoutOverhead),

		// Unlimited number of buffered records because we limit on bytes in Writer. The reason why we don't use
		// kgo.MaxBufferedBytes() is because it suffers a deadlock issue:
		// https://github.com/twmb/franz-go/issues/777
		kgo.MaxBufferedRecords(math.MaxInt), // Use a high value to set it as unlimited, because the client doesn't support "0 as unlimited".
		kgo.MaxBufferedBytes(0),
	)

	return kgo.NewClient(opts...)
}

// KafkaProducer is a kgo.Client wrapper exposing some higher level features and metrics useful for producers.
type KafkaProducer struct {
	*kgo.Client

	closeOnce *sync.Once
	closed    chan struct{}

	// Keep track of Kafka records size (bytes) currently in-flight in the Kafka client.
	// This counter is used to implement a limit on the max buffered bytes.
	bufferedBytes *atomic.Int64

	// The max buffered bytes allowed. Once this limit is reached, produce requests fail.
	maxBufferedBytes int64

	// Custom metrics.
	bufferedProduceBytes          prometheus.Summary
	bufferedProduceBytesLimit     prometheus.Gauge
	produceRecordsEnqueuedTotal   prometheus.Counter
	produceRecordsFailedTotal     *prometheus.CounterVec
	produceRecordsEnqueueDuration prometheus.Histogram
	produceRemainingDeadline      prometheus.Histogram
}

// NewKafkaProducer returns a new KafkaProducer.
//
// The input prometheus.Registerer must be wrapped with a prefix (the names of metrics
// registered don't have a prefix).
func NewKafkaProducer(client *kgo.Client, maxBufferedBytes int64, reg prometheus.Registerer) *KafkaProducer {
	producer := &KafkaProducer{
		Client:           client,
		closeOnce:        &sync.Once{},
		closed:           make(chan struct{}),
		bufferedBytes:    atomic.NewInt64(0),
		maxBufferedBytes: maxBufferedBytes,

		// Metrics.
		bufferedProduceBytes: promauto.With(reg).NewSummary(
			// The franz-go library exposes "buffered_produce_bytes" metric which is a gauge. Gauges export a snapshot
			// of the buffer at the time of scraping, but for our use case (alert on 100th percentile) we need to have
			// higher accuracy, so we also track this metric that gets updated with a high frequency.
			prometheus.SummaryOpts{
				Name:       "buffered_produce_bytes_distribution",
				Help:       "The buffered produce records in bytes. Quantile buckets keep track of buffered records size over the last 60s.",
				Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001, 1: 0.001},
				MaxAge:     time.Minute,
				AgeBuckets: 6,
			}),
		bufferedProduceBytesLimit: promauto.With(reg).NewGauge(
			prometheus.GaugeOpts{
				Name: "buffered_produce_bytes_limit",
				Help: "The bytes limit on buffered produce records. Produce requests fail once this limit is reached.",
			}),
		produceRecordsEnqueuedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "produce_records_enqueued_total",
			Help: "Total number of Kafka records enqueued to be sent to the Kafka backend (includes records that fail to be successfully sent to the Kafka backend).",
		}),
		produceRecordsFailedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "produce_records_failed_total",
			Help: "Total number of Kafka records that failed to be sent to the Kafka backend.",
		}, []string{"reason"}),
		produceRecordsEnqueueDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "produce_records_enqueue_duration_seconds",
			Help:                            "How long it takes to enqueue produced Kafka records in the client, appending them to the batches that will sent to the Kafka backend (in seconds).",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}),
		produceRemainingDeadline: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "produce_remaining_deadline_seconds",
			Help:                            "The remaining deadline (in seconds) when records are requested to be produced.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}),
	}

	producer.bufferedProduceBytesLimit.Set(float64(maxBufferedBytes))

	go producer.updateMetricsLoop()

	return producer
}

func (c *KafkaProducer) Close() {
	c.closeOnce.Do(func() {
		close(c.closed)
	})

	c.Client.Close()
}

func (c *KafkaProducer) updateMetricsLoop() {
	// We observe buffered produce bytes and at regular intervals, to have a good
	// approximation of the peak value reached over the observation period.
	ticker := time.NewTicker(250 * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			c.bufferedProduceBytes.Observe(float64(c.BufferedProduceBytes()))

		case <-c.closed:
			return
		}
	}
}

// ProduceSync produces records to Kafka and returns once all records have been successfully committed,
// or an error occurred.
//
// This function honors the configure max buffered bytes and refuse to produce a record, returnin kgo.ErrMaxBuffered,
// if the configured limit is reached.
func (c *KafkaProducer) ProduceSync(ctx context.Context, records []*kgo.Record) kgo.ProduceResults {
	var (
		remaining = atomic.NewInt64(int64(len(records)))
		done      = make(chan struct{})
		resMx     sync.Mutex
		res       = make(kgo.ProduceResults, 0, len(records))
	)

	// Keep track of the remaining deadline before producing records.
	// This could be useful for troubleshooting.
	if deadline, ok := ctx.Deadline(); ok {
		c.produceRemainingDeadline.Observe(max(0, time.Until(deadline).Seconds()))
	}

	// As a safety mechanism, we want to make sure that the context is not already canceled or its deadline exceeded.
	// The reason is that once we buffer records to the Kafka client (later in this function), these records will be
	// sent to the Kafka backend regardless the context is canceled or not. There's no way, in the Kafka client, to
	// pull out records from a batch buffer. So, if the context is canceled, we circuit break instead of buffering
	// records that may be sent to the Kafka backend, but that the caller will not know about because context is done.
	if err := ctx.Err(); err != nil {
		recordsCount := float64(len(records))

		c.produceRecordsEnqueuedTotal.Add(recordsCount)
		c.produceRecordsFailedTotal.WithLabelValues("cancelled-before-producing").Add(recordsCount)

		// We wrap the error to make it cristal clear where the context canceled/timeout comes from.
		return kgo.ProduceResults{{Err: errors.Wrap(context.Cause(ctx), "skipped producing Kafka records because context is already done")}}
	}

	onProduceDone := func(r *kgo.Record, err error) {
		if c.maxBufferedBytes > 0 {
			c.bufferedBytes.Add(-int64(len(r.Value)))
		}

		resMx.Lock()
		res = append(res, kgo.ProduceResult{Record: r, Err: err})
		resMx.Unlock()

		if err != nil {
			c.produceRecordsFailedTotal.WithLabelValues(produceErrReason(err)).Inc()
		}

		// In case of error we'll wait for all responses anyway before returning from produceSync().
		// It allows us to keep code easier, given we don't expect this function to be frequently
		// called with multiple records.
		if remaining.Dec() == 0 {
			close(done)
		}
	}

	// Produce all records. We expect this to be quick, because records are just buffered in the Kafka client.
	// We keep track of how long it takes to "enqueue" all records in the client buffers, in order to have a
	// data point when we troubleshoot high production latencies.
	{
		enqueueStartTime := time.Now()
		c.produceRecordsEnqueuedTotal.Add(float64(len(records)))

		for _, record := range records {
			// Fast fail if the Kafka client buffer is full. Buffered bytes counter is decreased onProducerDone().
			if c.maxBufferedBytes > 0 && c.bufferedBytes.Add(int64(len(record.Value))) > c.maxBufferedBytes {
				onProduceDone(record, kgo.ErrMaxBuffered)
				continue
			}

			// We use a new context to avoid that other Produce() may be cancelled when this call's context is
			// canceled. It's important to note that cancelling the context passed to Produce() doesn't actually
			// prevent the data to be sent over the wire (because it's never removed from the buffer) but in some
			// cases may cause all requests to fail with context cancelled.
			//
			// Produce() may theoretically block if the buffer is full, but we configure the Kafka client with
			// unlimited buffer because we implement the buffer limit ourselves (see maxBufferedBytes). This means
			// Produce() should never block for us in practice.
			c.Produce(context.WithoutCancel(ctx), record, onProduceDone)
		}

		c.produceRecordsEnqueueDuration.Observe(time.Since(enqueueStartTime).Seconds())
	}

	// Wait for a response or until the context has done.
	select {
	case <-ctx.Done():
		// We wrap the error to make it cristal clear where the context canceled/timeout comes from.
		return kgo.ProduceResults{{Err: errors.Wrap(context.Cause(ctx), "waiting for Kafka records to be produced and acknowledged")}}
	case <-done:
		// Once we're done, it's guaranteed that no more results will be appended, so we can safely return it.
		return res
	}
}

func produceErrReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kgo.ErrRecordTimeout) {
		return "timeout"
	}
	if errors.Is(err, kgo.ErrMaxBuffered) {
		return "buffer-full"
	}
	if errors.Is(err, kerr.MessageTooLarge) {
		return "record-too-large"
	}
	if errors.Is(err, context.Canceled) {
		// This should never happen because we don't cancel produce requests, however we
		// check this error anyway to detect if something unexpected happened.
		return "canceled"
	}
	return "other"
}
