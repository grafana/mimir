// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
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
	writers   []*kafkaWriterClient

	// Keep track of Kafka records size (bytes) currently in-flight in the Kafka client.
	// This counter is used to implement a limit on the max buffered bytes.
	writersBufferedBytes *atomic.Int64

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
		writers:                    make([]*kafkaWriterClient, kafkaCfg.WriteClients),
		maxInflightProduceRequests: 20,
		writersBufferedBytes:       atomic.NewInt64(0),

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
func (w *Writer) produceSync(ctx context.Context, client *kafkaWriterClient, records []*kgo.Record) kgo.ProduceResults {
	var (
		remaining        = atomic.NewInt64(int64(len(records)))
		done             = make(chan struct{})
		resMx            sync.Mutex
		res              = make(kgo.ProduceResults, 0, len(records))
		maxBufferedBytes = int64(w.kafkaCfg.ProducerMaxBufferedBytes)
	)

	w.writeRequestsTotal.Add(float64(len(records)))

	onProduceDone := func(r *kgo.Record, err error) {
		if maxBufferedBytes > 0 {
			w.writersBufferedBytes.Add(-int64(len(r.Value)))
		}

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
	}

	for _, record := range records {
		// Fast fail if the Kafka client buffer is full. Buffered bytes counter is decreased onProducerDone().
		if maxBufferedBytes > 0 && w.writersBufferedBytes.Add(int64(len(record.Value))) > maxBufferedBytes {
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
		client.Produce(context.WithoutCancel(ctx), record, onProduceDone)
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

func (w *Writer) getKafkaWriterForPartition(partitionID int32) (*kafkaWriterClient, error) {
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
	newWriter, err := newKafkaWriterClient(clientID, w.kafkaCfg, w.maxInflightProduceRequests, w.logger, w.registerer)
	if err != nil {
		return nil, err
	}
	w.writers[clientID] = newWriter
	return newWriter, nil
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
