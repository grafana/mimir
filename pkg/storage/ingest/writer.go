// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

const (
	// writerRequestTimeoutOverhead is the overhead applied by the Writer to every Kafka timeout.
	// You can think about this overhead as an extra time for requests sitting in the client's buffer
	// before being sent on the wire and the actual time it takes to send it over the network and
	// start being processed by Kafka.
	writerRequestTimeoutOverhead = 2 * time.Second

	// producerBatchMaxBytes is the max allowed size of a batch of Kafka records.
	producerBatchMaxBytes = 16_000_000

	// maxProducerRecordDataBytesLimit is the max allowed size of a single record data. Given we have a limit
	// on the max batch size (producerBatchMaxBytes), a Kafka record data can't be bigger than the batch size
	// minus some overhead required to serialise the batch and the record itself. We use 16KB as such overhead
	// in the worst case scenario, which is expected to be way above the actual one.
	maxProducerRecordDataBytesLimit = producerBatchMaxBytes - 16384
	minProducerRecordDataBytesLimit = 1024 * 1024
)

var (
	// ErrWriteRequestDataItemTooLarge is the error returned when a split WriteRequest failed to be written to
	// a partition because it's larger than the max allowed record size. The size reported here is always
	// maxProducerRecordDataBytesLimit, regardless of the configured max record data size, because the ingestion
	// fails only if bigger than the upper limit.
	ErrWriteRequestDataItemTooLarge = errors.New(globalerror.DistributorMaxWriteRequestDataItemSize.Message(
		fmt.Sprintf("the write request contains a timeseries or metadata item which is larger that the maximum allowed size of %d bytes", maxProducerRecordDataBytesLimit)))
)

// Writer is responsible to write incoming data to the ingest storage.
type Writer struct {
	services.Service

	kafkaCfg   KafkaConfig
	logger     log.Logger
	registerer prometheus.Registerer

	// We support multiple Kafka clients to better parallelize the workload. The number of
	// clients is fixed during the Writer lifecycle, but they're initialised lazily.
	writersMx sync.RWMutex
	writers   []*kgo.Client

	// Metrics.
	writeRequestsTotal prometheus.Counter
	writeFailuresTotal *prometheus.CounterVec
	writeLatency       prometheus.Histogram
	writeBytesTotal    prometheus.Counter
	recordsPerRequest  prometheus.Histogram

	// The following settings can only be overridden in tests.
	maxInflightProduceRequests int
}

func NewWriter(kafkaCfg KafkaConfig, logger log.Logger, reg prometheus.Registerer) *Writer {
	w := &Writer{
		kafkaCfg:                   kafkaCfg,
		logger:                     logger,
		registerer:                 reg,
		writers:                    make([]*kgo.Client, kafkaCfg.WriteClients),
		maxInflightProduceRequests: 20,

		// Metrics.
		writeRequestsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_writer_produce_requests_total",
			Help: "Total number of produce requests issued to Kafka.",
		}),
		writeFailuresTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_writer_produce_failures_total",
			Help: "Total number of failed produce requests issued to Kafka.",
		}, []string{"reason"}),
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
			Name:    "cortex_ingest_storage_writer_records_per_write_request",
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

	for idx, client := range w.writers {
		if client == nil {
			continue
		}

		client.Close()
		w.writers[idx] = nil
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
	records, err := marshalWriteRequestToRecords(partitionID, userID, req, w.kafkaCfg.ProducerMaxRecordSizeBytes)
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
	w.recordsPerRequest.Observe(float64(len(records)))

	res := w.produceSync(ctx, writer, records)

	// Track latency only for successfully written records.
	if count, sizeBytes := successfulProduceRecordsStats(res); count > 0 {
		w.writeLatency.Observe(time.Since(startTime).Seconds())
		w.writeBytesTotal.Add(float64(sizeBytes))
	}

	if err := res.FirstErr(); err != nil {
		if errors.Is(err, kerr.MessageTooLarge) {
			return ErrWriteRequestDataItemTooLarge
		}

		return err
	}

	return nil
}

// produceSync produces records to Kafka and returns once all records have been successfully committed,
// or an error occurred.
func (w *Writer) produceSync(ctx context.Context, client *kgo.Client, records []*kgo.Record) kgo.ProduceResults {
	var (
		remaining = atomic.NewInt64(int64(len(records)))
		done      = make(chan struct{})
		resMx     sync.Mutex
		res       kgo.ProduceResults
	)

	w.writeRequestsTotal.Add(float64(len(records)))

	for _, record := range records {
		// We use a new context to avoid that other TryProduce() may be cancelled when this call's context is
		// canceled. It's important to note that cancelling the context passed to TryProduce() doesn't actually
		// prevent the data to be sent over the wire (because it's never removed from the buffer) but in some
		// cases may cause all requests to fail with context cancelled.
		//
		// We use TryProduce() instead of Produce() so that it will fast fail if the produce buffer is full.
		// If we would use Produce(), the Produce() function call will block until the buffer can accept the record.
		client.TryProduce(context.WithoutCancel(ctx), record, func(r *kgo.Record, err error) {
			resMx.Lock()
			res = append(res, kgo.ProduceResult{Record: r, Err: err})
			resMx.Unlock()

			if err != nil {
				w.writeFailuresTotal.WithLabelValues(produceErrReason(err)).Inc()
			}

			// In case of error we'll wait for all responses anyway before returning from produceSync().
			// It allows us to keep code easier, given we don't expect this function to be frequently
			// called with multiple records.
			if remaining.Dec() == 0 {
				close(done)
			}
		})
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

func (w *Writer) getKafkaWriterForPartition(partitionID int32) (*kgo.Client, error) {
	// Check if the writer has already been created.
	w.writersMx.RLock()
	clientID := int(partitionID) % len(w.writers)
	writer := w.writers[clientID]
	w.writersMx.RUnlock()

	if writer != nil {
		return writer, nil
	}

	w.writersMx.Lock()
	defer w.writersMx.Unlock()

	// Ensure a new writer wasn't created in the meanwhile. If so, use it.
	writer = w.writers[clientID]
	if writer != nil {
		return writer, nil
	}
	newWriter, err := w.newKafkaWriter(clientID)
	if err != nil {
		return nil, err
	}
	w.writers[clientID] = newWriter
	return newWriter, nil
}

// newKafkaWriter creates a new Kafka client.
func (w *Writer) newKafkaWriter(clientID int) (*kgo.Client, error) {
	logger := log.With(w.logger, "client_id", clientID)

	// Do not export the client ID, because we use it to specify options to the backend.
	metrics := kprom.NewMetrics("cortex_ingest_storage_writer",
		kprom.Registerer(prometheus.WrapRegistererWith(prometheus.Labels{"client_id": strconv.Itoa(clientID)}, w.registerer)),
		kprom.FetchAndProduceDetail(kprom.Batches, kprom.Records, kprom.CompressedBytes, kprom.UncompressedBytes))

	// Kafka client doesn't support unlimited buffered records, so we "simulate" it by setting a very high value.
	maxBufferedRecords := w.kafkaCfg.ProducerMaxBufferedRecords
	if maxBufferedRecords <= 0 {
		maxBufferedRecords = math.MaxInt
	}

	opts := append(
		commonKafkaClientOptions(w.kafkaCfg, metrics, logger),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DefaultProduceTopic(w.kafkaCfg.Topic),

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

		// Override the produce buffer max size (both in terms of number of records and size in bytes).
		kgo.MaxBufferedRecords(maxBufferedRecords),
		kgo.MaxBufferedBytes(w.kafkaCfg.ProducerMaxBufferedBytes),
	)
	return kgo.NewClient(opts...)
}

// marshalWriteRequestToRecords marshals a mimirpb.WriteRequest to one or more Kafka records.
// The request may be split to multiple records to get that each single Kafka record
// data size is not bigger than maxSize.
//
// This function is a best-effort. The returned Kafka records are not strictly guaranteed to
// have their data size limited to maxSize. The reason is that the WriteRequest is split
// by each individual Timeseries and Metadata: if a single Timeseries or Metadata is bigger than
// maxSize, than the resulting record will be bigger than the limit as well.
func marshalWriteRequestToRecords(partitionID int32, tenantID string, req *mimirpb.WriteRequest, maxSize int) ([]*kgo.Record, error) {
	reqSize := req.Size()

	if reqSize <= maxSize {
		// No need to split the request. We can take a fast path.
		rec, err := marshalWriteRequestToRecord(partitionID, tenantID, req, reqSize)
		if err != nil {
			return nil, err
		}

		return []*kgo.Record{rec}, nil
	}

	return marshalWriteRequestsToRecords(partitionID, tenantID, mimirpb.SplitWriteRequestByMaxMarshalSize(req, reqSize, maxSize))
}

func marshalWriteRequestsToRecords(partitionID int32, tenantID string, reqs []*mimirpb.WriteRequest) ([]*kgo.Record, error) {
	records := make([]*kgo.Record, 0, len(reqs))

	for _, req := range reqs {
		rec, err := marshalWriteRequestToRecord(partitionID, tenantID, req, req.Size())
		if err != nil {
			return nil, err
		}

		records = append(records, rec)
	}

	return records, nil
}

func marshalWriteRequestToRecord(partitionID int32, tenantID string, req *mimirpb.WriteRequest, reqSize int) (*kgo.Record, error) {
	// Marshal the request.
	data := make([]byte, reqSize)
	n, err := req.MarshalToSizedBuffer(data[:reqSize])
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialise write request")
	}
	data = data[:n]

	return &kgo.Record{
		Key:       []byte(tenantID), // We don't partition based on the key, so the value here doesn't make any difference.
		Value:     data,
		Partition: partitionID,
	}, nil
}

func successfulProduceRecordsStats(results kgo.ProduceResults) (count, sizeBytes int) {
	for _, res := range results {
		if res.Err == nil && res.Record != nil {
			count++
			sizeBytes += len(res.Record.Value)
		}
	}

	return
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
