// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
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

	defaultMaxInflightProduceRequests = 20

	writerMetricsPrefix = "cortex_ingest_storage_writer_"
)

var (
	// ErrWriteRequestDataItemTooLarge is the error returned when a split WriteRequest failed to be written to
	// a partition because it's larger than the max allowed record size. The size reported here is always
	// maxProducerRecordDataBytesLimit, regardless of the configured max record data size, because the ingestion
	// fails only if bigger than the upper limit.
	ErrWriteRequestDataItemTooLarge = errors.New(globalerror.DistributorMaxWriteRequestDataItemSize.Message(
		fmt.Sprintf("the write request contains a timeseries or metadata item which is larger that the maximum allowed size of %d bytes", maxProducerRecordDataBytesLimit)))

	// ErrWriterNotRunning is the error returned if someone tries to use Writer but the service is not
	// in the running state.
	ErrWriterNotRunning = errors.New("the Kafka writer client is not in the running state")
)

// Writer is responsible to write incoming data to the ingest storage.
type Writer struct {
	services.Service

	kafkaCfg   KafkaConfig
	logger     log.Logger
	registerer prometheus.Registerer

	client     atomic.Pointer[KafkaProducer]
	serializer recordSerializer

	// Metrics.
	writeSuccessLatency prometheus.Observer
	writeFailureLatency prometheus.Observer
	writeBytesTotal     prometheus.Counter
	inputBytesTotal     prometheus.Counter
	recordsPerRequest   prometheus.Histogram
}

func NewWriter(kafkaCfg KafkaConfig, logger log.Logger, reg prometheus.Registerer) *Writer {
	writeLatency := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "cortex_ingest_storage_writer_latency_seconds",
		Help:                            "Latency to write an incoming request to Kafka partitions.",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMinResetDuration: 1 * time.Hour,
		NativeHistogramMaxBucketNumber:  100,
		Buckets:                         prometheus.DefBuckets,
	}, []string{"outcome"})

	w := &Writer{
		kafkaCfg:   kafkaCfg,
		logger:     logger,
		registerer: reg,
		serializer: recordSerializerFromCfg(kafkaCfg),

		// Metrics.
		writeSuccessLatency: writeLatency.WithLabelValues("success"),
		writeFailureLatency: writeLatency.WithLabelValues("failure"),
		writeBytesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_writer_sent_bytes_total",
			Help: "Total number of bytes produced to the Kafka backend.",
		}),
		inputBytesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_writer_input_bytes_total",
			Help: "Total number of bytes in write requests before conversion to the Kafka record format.",
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
		if err := CreateTopic(w.kafkaCfg, w.logger); err != nil {
			return err
		}
	}

	clientReg := prometheus.WrapRegistererWithPrefix(writerMetricsPrefix, w.registerer)

	maxInflightProduceRequests := w.kafkaCfg.MaxInflightProduceRequests
	if maxInflightProduceRequests == 0 {
		maxInflightProduceRequests = defaultMaxInflightProduceRequests
	}

	client, err := NewKafkaWriterClient(w.kafkaCfg, maxInflightProduceRequests, w.logger, clientReg)
	if err != nil {
		return err
	}

	w.client.Store(NewKafkaProducer(client, w.kafkaCfg.ProducerMaxBufferedBytes, clientReg))
	return nil
}

func (w *Writer) stopping(_ error) error {
	if client := w.client.Swap(nil); client != nil {
		client.Close()
	}

	return nil
}

// PartitionWriteRequest holds a write request targeted to a specific partition.
type PartitionWriteRequest struct {
	PartitionID  int32
	WriteRequest *mimirpb.WriteRequest
}

// WriteSync the input data to the ingest storage. The function blocks until the data has been successfully committed,
// or an error occurred.
func (w *Writer) WriteSync(ctx context.Context, partitionID int32, userID string, req *mimirpb.WriteRequest) error {
	return w.MultiWriteSync(ctx, userID, []PartitionWriteRequest{{PartitionID: partitionID, WriteRequest: req}})
}

// MultiWriteSync writes data for multiple partitions to the ingest storage in a single ProduceSync call.
// The function blocks until all data has been successfully committed, or an error occurred.
func (w *Writer) MultiWriteSync(ctx context.Context, userID string, partitionRequests []PartitionWriteRequest) error {
	client := w.client.Load()
	if client == nil {
		return ErrWriterNotRunning
	}

	startTime := time.Now()

	// Serialize all partition requests into a single records slice.
	var (
		allRecords       []*kgo.Record
		requestSizeBytes int
	)
	for _, pr := range partitionRequests {
		// Nothing to do if the input data is empty.
		if pr.WriteRequest.IsEmpty() {
			continue
		}

		records, reqSizeBytes, err := w.serializer.ToRecords(pr.PartitionID, userID, pr.WriteRequest, w.kafkaCfg.ProducerMaxRecordSizeBytes)
		if err != nil {
			return err
		}

		// Track the number of records the given WriteRequest has been split into.
		// Track this before sending records to Kafka so that we track it also for failures (e.g. we want to have
		// visibility over this metric if records are rejected by Kafka because of MESSAGE_TOO_LARGE).
		w.recordsPerRequest.Observe(float64(len(records)))

		allRecords = append(allRecords, records...)
		requestSizeBytes += reqSizeBytes
	}

	// Nothing to do if all requests were empty.
	if len(allRecords) == 0 {
		return nil
	}

	// Write to backend. The partition field is already set on each record by ToRecords,
	// so the Kafka client routes records to the correct partition.
	res := client.ProduceSync(ctx, allRecords)

	// We track the latency both in case of success and failure (but with a different label), to avoid misunderstandings
	// when we look at it in case Kafka Produce requests time out (if latency wasn't tracked on error, we would see low
	// latency but in practice many are very high latency and timing out).
	//
	// Since MultiWriteSync batches records for multiple partitions into a single ProduceSync call,
	// we only track success metrics when all records succeeded. This keeps cortex_ingest_storage_writer_sent_bytes_total
	// and cortex_ingest_storage_writer_input_bytes_total consistent with each other, avoiding inflation on partial failures.
	if count, recordsSizeBytes := successfulProduceRecordsStats(res); count == len(allRecords) {
		w.writeSuccessLatency.Observe(time.Since(startTime).Seconds())
		w.writeBytesTotal.Add(float64(recordsSizeBytes))
		w.inputBytesTotal.Add(float64(requestSizeBytes))
	} else {
		w.writeFailureLatency.Observe(time.Since(startTime).Seconds())
	}

	return produceResultsErr(res)
}

type requestSplitter func(req *mimirpb.WriteRequest, reqSize, maxSize int) []*mimirpb.WriteRequest

// marshalWriteRequestToRecords marshals a mimirpb.WriteRequest to one or more Kafka records.
// The request may be split to multiple records to get that each single Kafka record
// data size is not bigger than maxSize.
//
// This function is a best-effort. The returned Kafka records are not strictly guaranteed to
// have their data size limited to maxSize. The reason is that the WriteRequest is split
// by each individual Timeseries and Metadata: if a single Timeseries or Metadata is bigger than
// maxSize, than the resulting record will be bigger than the limit as well.
func marshalWriteRequestToRecords(partitionID int32, tenantID string, req *mimirpb.WriteRequest, reqSize, maxSize int, split requestSplitter) ([]*kgo.Record, error) {
	if reqSize <= maxSize {
		// No need to split the request. We can take a fast path.
		rec, err := marshalWriteRequestToRecord(partitionID, tenantID, req, reqSize)
		if err != nil {
			return nil, err
		}

		return []*kgo.Record{rec}, nil
	}

	return marshalWriteRequestsToRecords(partitionID, tenantID, split(req, reqSize, maxSize))
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

// produceResultsErr inspects the produce results and returns an appropriate error.
// If any record failed with MessageTooLarge, ErrWriteRequestDataItemTooLarge is returned (higher priority).
// Otherwise, the first error is decorated with the list of partitions that failed.
func produceResultsErr(results kgo.ProduceResults) error {
	var (
		firstErr            error
		failedPartitions    []int32
		failedPartitionsMap map[int32]struct{}
	)

	for _, res := range results {
		if res.Err == nil {
			continue
		}

		if errors.Is(res.Err, kerr.MessageTooLarge) {
			return ErrWriteRequestDataItemTooLarge
		}

		if firstErr == nil {
			firstErr = res.Err
			failedPartitionsMap = map[int32]struct{}{}
		}

		if res.Record != nil {
			if _, seen := failedPartitionsMap[res.Record.Partition]; !seen {
				failedPartitionsMap[res.Record.Partition] = struct{}{}
				failedPartitions = append(failedPartitions, res.Record.Partition)
			}
		}
	}

	if firstErr == nil {
		return nil
	}

	return fmt.Errorf("failed to write to partitions %v: %w", failedPartitions, firstErr)
}
