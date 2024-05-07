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
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	// writerRequestTimeoutOverhead is the overhead applied by the Writer to every Kafka timeout.
	// You can think about this overhead as an extra time for requests sitting in the client's buffer
	// before being sent on the wire and the actual time it takes to send it over the network and
	// start being processed by Kafka.
	writerRequestTimeoutOverhead = 2 * time.Second

	// producerBatchMaxBytes is the max allowed size of a batch of Kafka records.
	producerBatchMaxBytes = 16_000_000

	// producerRecordDataMaxBytes is the max allowed size of a single record data. Given we have a limit
	// on the max batch size, a Kafka record data can't be bigger than the batch size minus some overhead
	// required to serialise the batch and the record itself. We use 16KB as such overhead in the worst
	// case scenario, which is expected to be way above the actual one.
	producerRecordDataMaxBytes = producerBatchMaxBytes - 16384
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
	writeLatency      prometheus.Histogram
	writeBytesTotal   prometheus.Counter
	recordsPerRequest prometheus.Histogram

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
		writeLatency: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "cortex_ingest_storage_writer_latency_seconds",
			Help:                            "Latency to write an incoming request to the ingest storage.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			NativeHistogramMaxBucketNumber:  100,
			Buckets:                         prometheus.DefBuckets,
		}),
		writeBytesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_writer_sent_bytes_total",
			Help: "Total number of bytes sent to the ingest storage.",
		}),
		recordsPerRequest: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingest_storage_writer_records_per_request",
			Help:    "The number of records a single per-partition write request has been split into.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 8),
		}),
	}

	w.Service = services.NewIdleService(w.starting, w.stopping)

	return w
}

func (w *Writer) starting(_ context.Context) error {
	if w.kafkaCfg.AutoCreateTopicEnabled {
		setDefaultNumberOfPartitionsForAutocreatedTopics(w.kafkaCfg, w.logger)
	}
	return nil
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
func (w *Writer) WriteSync(ctx context.Context, partitionID int32, userID string, req *mimirpb.WriteRequest) error {
	startTime := time.Now()

	// Nothing to do if the input data is empty.
	if req.IsEmpty() {
		return nil
	}

	// Create records out of the write request.
	records, recordsSizeBytes, err := marshalWriteRequestToRecords(userID, req, producerRecordDataMaxBytes)
	if err != nil {
		return err
	}

	// Write to backend.
	writer, err := w.getKafkaWriterForPartition(partitionID)
	if err != nil {
		return err
	}

	// Track the number of records the given WriteRequest has been split into.
	// Track this before sending records to Kafka so that we track it also for failures (e.g. we want to have
	// visibility over this metric if records are rejected by Kafka because of MESSAGE_TOO_LARGE).
	// TODO unit test
	w.recordsPerRequest.Observe(float64(len(records)))

	err = w.produceSync(ctx, writer, records)
	if err != nil {
		return err
	}

	// Track latency and payload size only for successful requests.
	w.writeLatency.Observe(time.Since(startTime).Seconds())
	w.writeBytesTotal.Add(float64(recordsSizeBytes))

	return nil
}

// produceSync produces records to Kafka and returns once all records have been successfully committed,
// or an error occurred.
// TODO unit test: multiple records
func (w *Writer) produceSync(ctx context.Context, client *kgo.Client, records []*kgo.Record) error {
	var (
		remaining  = atomic.NewInt64(int64(len(records)))
		done       = make(chan struct{})
		firstErrMx sync.Mutex
		firstErr   error
	)

	for _, record := range records {
		// We use a new context to avoid that other Produce() may be cancelled when this call's context is
		// canceled. It's important to note that cancelling the context passed to Produce() doesn't actually
		// prevent the data to be sent over the wire (because it's never removed from the buffer) but in some
		// cases may cause all requests to fail with context cancelled.
		client.Produce(context.WithoutCancel(ctx), record, func(_ *kgo.Record, err error) {
			if err != nil {
				// Keep track of the first error. In case of error we'll wait for all responses anyway
				// before returning from this function. It allows us to keep code easier, given we don't
				// expect this function to be frequently called with multiple records.
				firstErrMx.Lock()
				if firstErr == nil {
					firstErr = err
				}
				firstErrMx.Unlock()
			}

			if remaining.Dec() == 0 {
				close(done)
			}
		})
	}

	// Wait for a response or until the context has done.
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-done:
		return firstErr
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

	opts := append(
		commonKafkaClientOptions(w.kafkaCfg, metrics, logger),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DefaultProduceTopic(w.kafkaCfg.Topic),

		// Use a static partitioner because we want to be in control of the partition.
		kgo.RecordPartitioner(newKafkaStaticPartitioner(int(partitionID))),

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
	return kgo.NewClient(opts...)
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

// marshalWriteRequestToRecords marshals a mimirpb.WriteRequest to one or more Kafka records.
// The request may be split to multiple records to get that each single Kafka record
// data size is not bigger than maxDataLength.
//
// This function is a best-effort. The returned Kafka records are not strictly guaranteed to
// have their data size limited to maxDataLength. The reason is that the WriteRequest is split
// by each individual Timeseries and Metadata: if a single Timeseries or Metadata is bigger than
// maxDataLength, than the resulting record will be bigger than the limit as well.
// TODO unit test
func marshalWriteRequestToRecords(tenantID string, req *mimirpb.WriteRequest, maxDataLength int) ([]*kgo.Record, int, error) {
	return marshalWriteRequestsToRecords(tenantID, req.SplitByMaxMarshalSize(maxDataLength))
}

func marshalWriteRequestsToRecords(tenantID string, reqs []*mimirpb.WriteRequest) ([]*kgo.Record, int, error) {
	records := make([]*kgo.Record, 0, len(reqs))
	recordsDataSizeBytes := 0

	for _, req := range reqs {
		data, err := req.Marshal()
		if err != nil {
			return nil, 0, errors.Wrap(err, "failed to serialise data")
		}

		records = append(records, &kgo.Record{
			Key:   []byte(tenantID), // We don't partition based on the key, so the value here doesn't make any difference.
			Value: data,
		})
		recordsDataSizeBytes += len(data)
	}

	return records, recordsDataSizeBytes, nil
}
