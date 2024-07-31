// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/go-kit/log"
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

	opts := append(
		commonKafkaClientOptions(kafkaCfg, metrics, logger),
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
		kgo.ProducerLinger(50*time.Millisecond),
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
	bufferedProduceBytes      prometheus.Summary
	bufferedProduceBytesLimit prometheus.Gauge
	produceRequestsTotal      prometheus.Counter
	produceFailuresTotal      *prometheus.CounterVec
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
			prometheus.SummaryOpts{
				Name:       "buffered_produce_bytes",
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
		produceRequestsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "produce_requests_total",
			Help: "Total number of produce requests issued to Kafka.",
		}),
		produceFailuresTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "produce_failures_total",
			Help: "Total number of failed produce requests issued to Kafka.",
		}, []string{"reason"}),
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
			c.bufferedProduceBytes.Observe(float64(c.Client.BufferedProduceBytes()))

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

	c.produceRequestsTotal.Add(float64(len(records)))

	onProduceDone := func(r *kgo.Record, err error) {
		if c.maxBufferedBytes > 0 {
			c.bufferedBytes.Add(-int64(len(r.Value)))
		}

		resMx.Lock()
		res = append(res, kgo.ProduceResult{Record: r, Err: err})
		resMx.Unlock()

		if err != nil {
			c.produceFailuresTotal.WithLabelValues(produceErrReason(err)).Inc()
		}

		// In case of error we'll wait for all responses anyway before returning from produceSync().
		// It allows us to keep code easier, given we don't expect this function to be frequently
		// called with multiple records.
		if remaining.Dec() == 0 {
			close(done)
		}
	}

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
		c.Client.Produce(context.WithoutCancel(ctx), record, onProduceDone)
	}

	// Wait for a response or until the context has done.
	select {
	case <-ctx.Done():
		return kgo.ProduceResults{{Err: context.Cause(ctx)}}
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
