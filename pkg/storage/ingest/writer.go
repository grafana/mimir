// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"

	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	// writerRequestTimeoutOverhead is the overhead applied by the Writer to every Kafka timeout.
	// You can think about this overhead as an extra time for requests sitting in the client's buffer
	// before being sent on the wire and the actual time it takes to send it over the network and
	// start being processed by Kafka.
	writerRequestTimeoutOverhead = 2 * time.Second
)

// Writer is responsible to write incoming data to the ingest storage.
type Writer struct {
	services.Service

	kafkaCfg   KafkaConfig
	logger     log.Logger
	registerer prometheus.Registerer

	// We create 1 writer per partition to better parallelize the workload.
	writersMx sync.RWMutex
	writers   map[int32]*kgo.Client

	// Metrics.
	writeLatency    prometheus.Summary
	writeBytesTotal prometheus.Counter

	// The following settings can only be overridden in tests.
	maxInflightProduceRequests int
}

func NewWriter(kafkaCfg KafkaConfig, logger log.Logger, reg prometheus.Registerer) *Writer {
	w := &Writer{
		kafkaCfg:                   kafkaCfg,
		logger:                     logger,
		registerer:                 reg,
		writers:                    map[int32]*kgo.Client{},
		maxInflightProduceRequests: 20,

		// Metrics.
		writeLatency: promauto.With(reg).NewSummary(prometheus.SummaryOpts{
			Name: "cortex_ingest_storage_writer_latency_seconds",
			Help: "Latency to write an incoming request to the ingest storage.",
			Objectives: map[float64]float64{
				0.5:   0.05,
				0.99:  0.001,
				0.999: 0.001,
				1:     0.001,
			},
			MaxAge:     time.Minute,
			AgeBuckets: 10,
		}),
		writeBytesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_writer_sent_bytes_total",
			Help: "Total number of bytes sent to the ingest storage.",
		}),
	}

	w.Service = services.NewIdleService(nil, w.stopping)

	return w
}

func (w *Writer) stopping(_ error) error {
	w.writersMx.Lock()
	defer w.writersMx.Unlock()

	for partitionID, client := range w.writers {
		client.Close()
		delete(w.writers, partitionID)
	}

	return nil
}

// WriteSync the input data to the ingest storage. The function blocks until the data has been successfully committed,
// or an error occurred.
func (w *Writer) WriteSync(ctx context.Context, partitionID int32, userID string, timeseries []mimirpb.PreallocTimeseries, metadata []*mimirpb.MetricMetadata, source mimirpb.WriteRequest_SourceEnum) error {
	startTime := time.Now()

	// Nothing to do if the input data is empty.
	if len(timeseries) == 0 && len(metadata) == 0 {
		return nil
	}

	// Serialise the input data.
	entry := &mimirpb.WriteRequest{
		Timeseries: timeseries,
		Metadata:   metadata,
		Source:     source,
	}

	data, err := entry.Marshal()
	if err != nil {
		return errors.Wrap(err, "failed to serialise data")
	}

	// Prepare the record to write.
	record := &kgo.Record{
		Key:   []byte(userID), // We don't partition based on the key, so the value here doesn't make any difference.
		Value: data,
	}

	// Write to backend.
	writer, err := w.getKafkaWriterForPartition(partitionID)
	if err != nil {
		return err
	}

	err = w.produceSync(ctx, writer, record)
	if err != nil {
		return err
	}

	// Track latency and payload size only for successful requests.
	w.writeLatency.Observe(time.Since(startTime).Seconds())
	w.writeBytesTotal.Add(float64(len(data)))

	return nil
}

func (w *Writer) produceSync(ctx context.Context, client *kgo.Client, record *kgo.Record) error {
	errCh := make(chan error, 1)

	// We use a new context to avoid that other Produce() may be cancelled when this call's context is
	// canceled. It's important to note that cancelling the context passed to Produce() doesn't actually
	// prevent the data to be sent over the wire (because it's never removed from the buffer) but in some
	// cases may cause all requests to fail with context cancelled.
	client.Produce(context.Background(), record, func(_ *kgo.Record, err error) {
		errCh <- err
	})

	// Wait for a response or until the context has done.
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case err := <-errCh:
		return err
	}
}

func (w *Writer) getKafkaWriterForPartition(partitionID int32) (*kgo.Client, error) {
	// Check if the writer has already been created.
	w.writersMx.RLock()
	writer := w.writers[partitionID]
	w.writersMx.RUnlock()

	if writer != nil {
		return writer, nil
	}

	w.writersMx.Lock()
	defer w.writersMx.Unlock()

	// Ensure a new writer wasn't created in the meanwhile. If so, use it.
	writer = w.writers[partitionID]
	if writer != nil {
		return writer, nil
	}
	newWriter, err := w.newKafkaWriter(partitionID)
	if err != nil {
		return nil, err
	}
	w.writers[partitionID] = newWriter
	return newWriter, nil
}

// newKafkaWriter creates a new Kafka client used to write to a specific partition.
func (w *Writer) newKafkaWriter(partitionID int32) (*kgo.Client, error) {
	logger := log.With(w.logger, "partition", partitionID)

	// Do not export the client ID, because we use it to specify options to the backend.
	metrics := kprom.NewMetrics("cortex_ingest_storage_writer",
		kprom.Registerer(prometheus.WrapRegistererWith(prometheus.Labels{"partition": strconv.Itoa(int(partitionID))}, w.registerer)),
		kprom.FetchAndProduceDetail(kprom.Batches, kprom.Records, kprom.CompressedBytes, kprom.UncompressedBytes))

	return kgo.NewClient(
		kgo.ClientID(w.kafkaCfg.ClientID),
		kgo.SeedBrokers(w.kafkaCfg.Address),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
		kgo.DefaultProduceTopic(w.kafkaCfg.Topic),
		kgo.DialTimeout(w.kafkaCfg.DialTimeout),
		kgo.WithHooks(metrics),
		kgo.WithLogger(newKafkaLogger(logger)),

		// Use a static partitioner because we want to be in control of the partition.
		kgo.RecordPartitioner(newKafkaStaticPartitioner(int(partitionID))),

		// Set the upper bounds the size of a record batch.
		kgo.ProducerBatchMaxBytes(16_000_000),

		// A cluster metadata update is a request sent to a broker and getting back the map of partitions and
		// the leader broker for each partition. The cluster metadata can be updated (a) periodically or
		// (b) when some events occur (e.g. backoff due to errors).
		//
		// MetadataMinAge() sets the minimum time between two cluster metadata updates due to events.
		// MetadataMaxAge() sets how frequently the periodic update should occur.
		//
		// We configure the client to frequently refresh the cluster metadata. This allows us to quickly react in case
		// some brokers are unhealthy.
		//
		// The side effect of frequently refreshing the cluster metadata is that if the backend returns each time a
		// different authoritative owner for a partition, then each time cluster metadata is updated the Kafka client
		// will create a new connection for each partition, leading to a high connections churn rate.
		kgo.MetadataMaxAge(10*time.Second),

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
		kgo.MaxProduceRequestsInflightPerBroker(w.maxInflightProduceRequests),

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
		kgo.RecordDeliveryTimeout(w.kafkaCfg.WriteTimeout),
		kgo.ProduceRequestTimeout(w.kafkaCfg.WriteTimeout),
		kgo.RequestTimeoutOverhead(writerRequestTimeoutOverhead),
	)
}

type kafkaStaticPartitioner struct {
	partitionID int
}

func newKafkaStaticPartitioner(partitionID int) *kafkaStaticPartitioner {
	return &kafkaStaticPartitioner{
		partitionID: partitionID,
	}
}

// ForTopic implements kgo.Partitioner.
func (p *kafkaStaticPartitioner) ForTopic(string) kgo.TopicPartitioner {
	return p
}

// RequiresConsistency implements kgo.TopicPartitioner.
func (p *kafkaStaticPartitioner) RequiresConsistency(_ *kgo.Record) bool {
	// Never let Kafka client to write the record to another partition
	// if the partition is down.
	return true
}

// Partition implements kgo.TopicPartitioner.
func (p *kafkaStaticPartitioner) Partition(_ *kgo.Record, _ int) int {
	return p.partitionID
}
