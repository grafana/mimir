// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
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

	writerMetricsPrefix = "cortex_ingest_storage_writer_"
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
	writers   []*KafkaProducer

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
		writers:                    make([]*KafkaProducer, kafkaCfg.WriteClients),
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
		return createTopic(w.kafkaCfg, w.logger)
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

	res := writer.ProduceSync(ctx, records)

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

func (w *Writer) getKafkaWriterForPartition(partitionID int32) (*KafkaProducer, error) {
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

	// Add the client ID to metrics so that they don't clash when the Writer is configured
	// to run with multiple Kafka clients.
	clientReg := prometheus.WrapRegistererWithPrefix(writerMetricsPrefix,
		prometheus.WrapRegistererWith(prometheus.Labels{"client_id": strconv.Itoa(clientID)}, w.registerer))

	// Add the client ID to logger so that we can easily distinguish Kafka clients in logs.
	clientLogger := log.With(w.logger, "client_id", clientID)

	newClient, err := NewKafkaWriterClient(w.kafkaCfg, w.maxInflightProduceRequests, clientLogger, clientReg)
	if err != nil {
		return nil, err
	}

	newWriter := NewKafkaProducer(newClient, w.kafkaCfg.ProducerMaxBufferedBytes, clientReg)
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
