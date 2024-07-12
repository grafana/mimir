// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
)

type kafkaWriterClient struct {
	*kgo.Client

	closeOnce *sync.Once
	closed    chan struct{}

	// Custom metrics.
	bufferedProduceBytes prometheus.Summary
}

func newKafkaWriterClient(clientID int, kafkaCfg KafkaConfig, maxInflightProduceRequests int, logger log.Logger, reg prometheus.Registerer) (*kafkaWriterClient, error) {
	logger = log.With(logger, "client_id", clientID)
	reg = prometheus.WrapRegistererWith(prometheus.Labels{"client_id": strconv.Itoa(clientID)}, reg)

	// Do not export the client ID, because we use it to specify options to the backend.
	metrics := kprom.NewMetrics("cortex_ingest_storage_writer",
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

	kafkaClient, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	customClient := &kafkaWriterClient{
		Client:    kafkaClient,
		closeOnce: &sync.Once{},
		closed:    make(chan struct{}),

		bufferedProduceBytes: promauto.With(reg).NewSummary(
			prometheus.SummaryOpts{
				Name:       "cortex_ingest_storage_writer_buffered_produce_bytes",
				Help:       "The buffered produce records in bytes. Quantile buckets keep track of buffered records size over the last 60s.",
				Objectives: map[float64]float64{0.5: 0.05, 0.99: 0.001, 1: 0.001},
				MaxAge:     time.Minute,
				AgeBuckets: 6,
			}),
	}

	go customClient.updateMetricsLoop()

	return customClient, nil
}

func (c *kafkaWriterClient) Close() {
	c.closeOnce.Do(func() {
		close(c.closed)
	})

	c.Client.Close()
}

func (c *kafkaWriterClient) updateMetricsLoop() {
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
