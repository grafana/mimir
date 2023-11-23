package ingest

import (
	"context"
	"fmt"
	"regexp"
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

// Writer is responsible to write incoming data to the ingest storage.
// TODO on the consumer side, may be more performant if we group a tenant's data together
type Writer struct {
	services.Service

	kafkaAddress string
	kafkaTopic   string
	logger       log.Logger
	registerer   prometheus.Registerer

	// We create 1 writer per partition to better parallelize the workload.
	// TODO close idle writers
	writersMx sync.RWMutex
	writers   map[int]*kgo.Client

	// Metrics.
	writeLatency    prometheus.Summary
	writeBytesTotal prometheus.Counter
}

func NewWriter(kafkaAddress, kafkaTopic string, logger log.Logger, reg prometheus.Registerer) *Writer {
	w := &Writer{
		kafkaAddress: kafkaAddress,
		kafkaTopic:   kafkaTopic,
		logger:       logger,
		registerer:   reg,
		writers:      map[int]*kgo.Client{},

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

	w.Service = services.NewIdleService(nil, nil)

	return w
}

// WriteSync the input data to the ingest storage. The function blocks until the data has been successfully committed,
// or an error occurred.
func (w *Writer) WriteSync(ctx context.Context, partitionID int, userID string, timeseries []mimirpb.PreallocTimeseries, metadata []*mimirpb.MetricMetadata, source mimirpb.WriteRequest_SourceEnum) error {
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

	res := writer.ProduceSync(ctx, record)
	if err := res.FirstErr(); err != nil {
		return err
	}

	// Track latency and payload size only for successful requests.
	w.writeLatency.Observe(time.Since(startTime).Seconds())
	w.writeBytesTotal.Add(float64(len(data)))

	return nil
}

func (w *Writer) getKafkaWriterForPartition(partitionID int) (*kgo.Client, error) {
	// Check if the writer has already been created.
	w.writersMx.RLock()
	writer := w.writers[partitionID]
	w.writersMx.RUnlock()

	if writer != nil {
		return writer, nil
	}

	// Optimistically create the new writer without taking the lock.
	newWriter, err := w.newKafkaWriter(partitionID)
	if err != nil {
		return nil, err
	}

	// Ensure a new writer wasn't created in the meanwhile. If so, cache it.
	w.writersMx.Lock()
	writer = w.writers[partitionID]
	if writer == nil {
		w.writers[partitionID] = newWriter
		writer = newWriter
	}
	w.writersMx.Unlock()

	// If the new writer wasn't used, then just close it.
	if writer != newWriter {
		newWriter.Close()
	}

	return writer, nil
}

// newKafkaWriter creates a new Kafka client used to write to a specific partition.
func (w *Writer) newKafkaWriter(partitionID int) (*kgo.Client, error) {
	logger := log.With(w.logger, "partition", partitionID)

	metrics := kprom.NewMetrics("cortex_ingest_storage_writer",
		kprom.Registerer(w.registerer),
		kprom.WithClientLabel(),
		kprom.FetchAndProduceDetail(kprom.Batches, kprom.Records, kprom.CompressedBytes, kprom.UncompressedBytes))

	// TODO tune timeouts
	return kgo.NewClient(
		kgo.ClientID(fmt.Sprintf("partition-%d", partitionID)),
		kgo.SeedBrokers(w.kafkaAddress),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerBatchMaxBytes(16_000_000), // TODO I think we also need to set the linger
		kgo.DefaultProduceTopic(w.kafkaTopic),
		kgo.MetadataMaxAge(time.Minute),
		kgo.WithHooks(metrics),
		kgo.WithLogger(newKafkaLogger(logger, kgo.LogLevelInfo)), // TODO pass the log level configured in  Mimir
		kgo.DisableIdempotentWrite(),

		// Use a static partitioner because we want to be in control of the partition.
		kgo.RecordPartitioner(newKafkaStaticPartitioner(partitionID)),
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

// Regular expression used to parse the ingester numeric ID.
var ingesterIDRegexp = regexp.MustCompile(".*([0-9]+)$")

func IngesterPartition(ingesterID string) (int, error) {
	// TODO  This is a very hacky way to assign partitions, because it doesn't work when scaling in/out ingesters,
	// 		and also because it assumes a specific naming (but that assumption is also made in the spread minimizing tokens generator).
	match := ingesterIDRegexp.FindStringSubmatch(ingesterID)
	if len(match) == 0 {
		return 0, fmt.Errorf("unable to get the partition ID for %s", ingesterID)
	}

	partitionID, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, fmt.Errorf("unable to get the partition ID for %s", ingesterID)
	}
	return partitionID, nil
}
