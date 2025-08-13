package continuoustest

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type IngestStorageRecordTestConfig struct {
	Kafka                    ingest.KafkaConfig `yaml:"-"`
	ConsumerGroup            string             `yaml:"consumer_group"`
	MaxJumpLimitPerPartition int                `yaml:"max_jump_size"`
	MaxRecordsPerRun         int                `yaml:"max_records_per_run"`
	RecordsProcessedPercent  int                `yaml:"records_processed_percent"`
}

func (cfg *IngestStorageRecordTestConfig) RegisterFlags(f *flag.FlagSet) {
	// cfg.Kafka.RegisterFlagsWithPrefix("ingest-storage.kafka.", f)
	f.StringVar(&cfg.ConsumerGroup, "tests.ingest-storage-record.consumer-group", "ingest-storage-record", "The Kafka consumer group used for getting/setting commmitted offsets.")
	f.IntVar(&cfg.MaxJumpLimitPerPartition, "tests.ingest-storage-record.max-jump-size", 100000000, "If a partition increases by this many offsets in a run, we skip processing it, to protect against downloading unexpectedly huge batches.")
	f.IntVar(&cfg.MaxRecordsPerRun, "tests.ingest-storage-record.max-records-per-run", 200000, "Limit on the number of total records to be processed in a run, to keep memory bounded in large cells. ")
	f.IntVar(&cfg.RecordsProcessedPercent, "tests.ingest-storage-record.records-processed-percent", 5, "The approximate percent of records to actually fetch and compare.")
}

type IngestStorageRecordTestMetrics struct {
	recordsProcessedTotal               *prometheus.CounterVec
	recordsWithMetadataProcessedTotal   *prometheus.CounterVec
	recordsWithTimeseriesProcessedTotal *prometheus.CounterVec
}

func NewIngestStorageRecordTestMetrics(reg prometheus.Registerer) *IngestStorageRecordTestMetrics {
	return &IngestStorageRecordTestMetrics{
		recordsProcessedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mimir_continuous_test_ingest_storage_records_processed_total",
			Help: "Number of records analyzed by the tool per tenant.",
		}, []string{"user"}),
		recordsWithMetadataProcessedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mimir_continuous_test_ingest_storage_record_metadata_processed_total",
			Help: "Number of records containing metadata analyzed by the tool per tenant.",
		}, []string{"user"}),
		recordsWithTimeseriesProcessedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mimir_continuous_test_ingest_storage_record_timeseries_processed_total",
			Help: "Number of records containing timeseries analyzed by the tool per tenant.",
		}, []string{"user"}),
	}
}

type IngestStorageRecordTest struct {
	name        string
	cfg         IngestStorageRecordTestConfig
	client      *kgo.Client
	adminClient *kadm.Client
	metrics     *IngestStorageRecordTestMetrics
	logger      log.Logger
	reg         prometheus.Registerer
}

func NewIngestStorageRecordTest(cfg IngestStorageRecordTestConfig, logger log.Logger, reg prometheus.Registerer) *IngestStorageRecordTest {
	const name = "ingest-storage-record"

	return &IngestStorageRecordTest{
		name:    name,
		cfg:     cfg,
		metrics: NewIngestStorageRecordTestMetrics(reg),
		logger:  logger,
		reg:     reg,
	}
}

// Name implements Test.
func (t *IngestStorageRecordTest) Name() string {
	return t.name
}

// Init implements Test.
func (t *IngestStorageRecordTest) Init(ctx context.Context, now time.Time) error {
	level.Info(t.logger).Log("msg", "starting kafka client")

	kc, err := ingest.NewKafkaReaderClient(
		t.cfg.Kafka,
		ingest.NewKafkaReaderClientMetrics(ingest.ReaderMetricsPrefix, "record-continuous-test", t.reg),
		t.logger)
	if err != nil {
		return fmt.Errorf("creating kafka reader: %w", err)
	}

	t.client = kc
	t.adminClient = kadm.NewClient(kc)
	return nil
}

// Run implements Test.
func (t *IngestStorageRecordTest) Run(ctx context.Context, now time.Time) error {
	topics, err := t.adminClient.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping kafka: %w", err)
	}
	for _, to := range topics {
		level.Info(t.logger).Log("msg", "detected topic", "topic", to.Topic)
	}

	offResp, err := t.adminClient.FetchOffsetsForTopics(ctx, t.cfg.ConsumerGroup, "ingest")
	if err != nil {
		if errors.Is(err, kerr.GroupIDNotFound) {
			// ignore.
		}
		return fmt.Errorf("fetch offsets error: %w", err)
	}
	if err := offResp.Error(); err != nil {
		return fmt.Errorf("fetch offsets response error: %w", err)
	}
	offsets := offResp.Offsets()

	endOffsetsResp, err := t.adminClient.ListEndOffsets(ctx, "ingest")
	if err != nil {
		return fmt.Errorf("fetch end offsets error: %w", err)
	}
	if endOffsetsResp.Error() != nil {
		return fmt.Errorf("fetch end offsets response error: %w", err)
	}
	allPartitionEndOffsets := endOffsetsResp.Offsets()
	endOffsets := allPartitionEndOffsets["ingest"]

	totalOffsetDiff := int64(0)
	for partition, endOffset := range endOffsets {
		startOffset, ok := offsets["ingest"][partition]
		if !ok {
			continue
		}

		diff := endOffset.At - startOffset.At
		if diff > int64(t.cfg.MaxJumpLimitPerPartition) {
			level.Warn(t.logger).Log(
				"msg", "skipping partition because it jumped by an amount greater than the limit per run",
				"partition", partition,
				"limit", t.cfg.MaxJumpLimitPerPartition,
				"actual", diff,
			)
			continue
		}
		totalOffsetDiff += diff
	}

	recordsRemainingInBatch := (totalOffsetDiff / 100) * int64(t.cfg.RecordsProcessedPercent)
	if recordsRemainingInBatch > int64(t.cfg.MaxRecordsPerRun) {
		level.Info(t.logger).Log("msg", "the expected number of records is larger than the limit", "expectedRecordsToProcess", recordsRemainingInBatch, "limit", t.cfg.MaxRecordsPerRun)
		recordsRemainingInBatch = int64(t.cfg.MaxRecordsPerRun)
	}

	startOffsets := map[string]map[int32]kgo.Offset{"ingest": {}}
	for _, partitionOffsets := range offsets {
		for partition, offset := range partitionOffsets {
			startOffsets["ingest"][partition] = kgo.NewOffset().At(offset.At)
		}
	}

	t.client.AddConsumePartitions(startOffsets)

	recordsProcessedThisBatch := 0
	for recordsRemainingInBatch > 0 {
		fetches := t.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			level.Error(t.logger).Log("fetch errors", "errs", errs)
			break
		}

		recordsRemainingInBatch -= int64(fetches.NumRecords())
		recordsProcessedThisBatch += fetches.NumRecords()

		err = t.testBatch(fetches)
		if err != nil {
			// Log errors, but don't fail them, this is experimental and we don't want to fail the actual continuous tester yet.
			level.Error(t.logger).Log("msg", "test failed", "reason", err)
			err = nil
		}
	}

	level.Info(t.logger).Log("msg", "run complete", "size", recordsProcessedThisBatch, "totalOffsetsIncrease", totalOffsetDiff)

	// Update to the end.
	t.adminClient.CommitOffsets(ctx, t.cfg.ConsumerGroup, allPartitionEndOffsets)

	return nil
}

func (t *IngestStorageRecordTest) testBatch(fetches kgo.Fetches) error {
	var batchErr error
	fetches.EachRecord(func(rec *kgo.Record) {
		// Only log the first failure in a batch, to keep the logs from being spammed.
		if batchErr != nil {
			return
		}

		tenantID := string(rec.Key)
		t.metrics.recordsProcessedTotal.WithLabelValues(tenantID).Inc()
		err := t.testRec(rec)
		if err != nil {
			level.Error(t.logger).Log("msg", "a record failed the test", "user", tenantID, "err", err)
			batchErr = err
		}
	})

	return batchErr
}

func (t *IngestStorageRecordTest) testRec(rec *kgo.Record) error {
	tenantID := string(rec.Key)
	req := mimirpb.PreallocWriteRequest{}
	defer mimirpb.ReuseSlice(req.Timeseries)

	version := ingest.ParseRecordVersion(rec)
	if version > ingest.LatestRecordVersion {
		return fmt.Errorf("received a record with an unsupported version: %d, max supported version: %d", version, ingest.LatestRecordVersion)
	}

	err := ingest.DeserializeRecordContent(rec.Value, &req, version)
	if err != nil {
		return fmt.Errorf("failed to unmarshal record from ingest topic: %w", err)
	}

	ser := ingest.VersionTwoRecordSerializer{}
	v2Records, err := ser.ToRecords(rec.Partition, string(rec.Key), &req.WriteRequest, t.cfg.Kafka.ProducerMaxRecordSizeBytes)
	if len(v2Records) == 0 {
		return fmt.Errorf("no records returned after v2 conversion")
	}
	if len(v2Records) > 1 {
		return fmt.Errorf("A V1 record was split when converted to its smaller V2 counterpart. This is highly unusual")
	}
	v2Rec := v2Records[0]

	if string(rec.Key) != string(v2Rec.Key) {
		return fmt.Errorf("Key did not match, got: %s, expected: %s.", string(v2Rec.Key), string(rec.Key))
	}

	if version != 2 {
		if len(v2Rec.Value) > len(rec.Value) {
			level.Warn(t.logger).Log("msg", "a v2 record was larger than its v1 counterpart", "user", string(rec.Key), "v1size", len(rec.Value), "v2size", len(v2Rec.Value))
		}
	}

	v2Req := mimirpb.PreallocWriteRequest{}
	defer mimirpb.ReuseSlice(v2Req.Timeseries)
	err = ingest.DeserializeRecordContent(v2Rec.Value, &v2Req, 2)
	if err != nil {
		return fmt.Errorf("failed to unmarshal V2 record: %w", err)
	}

	if v2Req.SkipLabelValidation != req.SkipLabelValidation {
		return fmt.Errorf("SkipLabelValidation did not match, original: %t, v2: %t", req.SkipLabelValidation, v2Req.SkipLabelValidation)
	}
	if v2Req.SkipLabelCountValidation != req.SkipLabelCountValidation {
		return fmt.Errorf("SkipLabelCountValidation did not match, original: %t, v2: %t", req.SkipLabelCountValidation, v2Req.SkipLabelCountValidation)
	}
	if v2Req.Source != req.Source {
		return fmt.Errorf("Source did not match, original: %d, v2: %d", req.Source, v2Req.Source)
	}
	if v2Req.SymbolsRW2 != nil {
		return fmt.Errorf("v2 record had a SymbolsRW2 unmarshalling field left populated")
	}
	if v2Req.TimeseriesRW2 != nil {
		return fmt.Errorf("v2 record had a TimeseriesRW2 unmarshalling field left populated")
	}

	if len(req.Metadata) != 0 || len(v2Req.Metadata) != 0 {
		t.metrics.recordsWithMetadataProcessedTotal.WithLabelValues(tenantID).Inc()
		sortMetadata := cmpopts.SortSlices(func(m1, m2 *mimirpb.MetricMetadata) bool {
			return m1.MetricFamilyName < m2.MetricFamilyName
		})
		if !cmp.Equal(req.Metadata, v2Req.Metadata, sortMetadata) {
			diff := cmp.Diff(req.Metadata, v2Req.Metadata, sortMetadata)
			return fmt.Errorf("Metadata did not match (adjusting for ordering). Diff: %s", diff)
		}
	}

	req.ClearTimeseriesUnmarshalData() // We do not want to match on gRPC buffers used only in an optimization.
	labelsComparer := cmp.Comparer(func(x, y mimirpb.LabelAdapter) bool {
		return string(x.Name) == string(y.Name) && string(x.Value) == string(y.Value)
	})
	timeseriesComparer := cmp.Comparer(func(x, y *mimirpb.TimeSeries) bool {
		return cmp.Equal(*x, *y, labelsComparer)
	})
	if len(req.Timeseries) != 0 || len(v2Req.Timeseries) != 0 {
		t.metrics.recordsWithTimeseriesProcessedTotal.WithLabelValues(tenantID).Inc()
		if !cmp.Equal(req.Timeseries, v2Req.Timeseries, labelsComparer, timeseriesComparer) {
			diff := cmp.Diff(req.Timeseries, v2Req.Timeseries, labelsComparer, timeseriesComparer)
			return fmt.Errorf("Timeseries did not match. Diff: %s", diff)
		}
	}

	return nil
}
