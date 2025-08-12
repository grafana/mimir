package continuoustest

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type IngestStorageRecordTestConfig struct {
	Kafka                    ingest.KafkaConfig `yaml:"-"`
	ConsumerGroup            string             `yaml:"consumer_group"`
	MaxJumpLimitPerPartition int                `yaml:"max_jump_size"`
	RecordsProcessedPercent  int                `yaml:"records_processed_percent"`
}

func (cfg *IngestStorageRecordTestConfig) RegisterFlags(f *flag.FlagSet) {
	// cfg.Kafka.RegisterFlagsWithPrefix("ingest-storage.kafka.", f)
	f.StringVar(&cfg.ConsumerGroup, "tests.ingest-storage-record.consumer-group", "ingest-storage-record", "The Kafka consumer group used for getting/setting commmitted offsets.")
	f.IntVar(&cfg.MaxJumpLimitPerPartition, "tests.ingest-storage-record.max-jump-size", 100000000, "If a partition increases by this many offsets in a run, we skip processing it, to protect against downloading unexpectedly huge batches.")
	f.IntVar(&cfg.RecordsProcessedPercent, "tests.ingest-storage-record.records-processed-percent", 5, "The approximate percent of records to actually fetch and compare.")
}

type IngestStorageRecordTest struct {
	name        string
	cfg         IngestStorageRecordTestConfig
	client      *kgo.Client
	adminClient *kadm.Client
	logger      log.Logger
	reg         prometheus.Registerer
}

func NewIngestStorageRecordTest(cfg IngestStorageRecordTestConfig, logger log.Logger, reg prometheus.Registerer) *IngestStorageRecordTest {
	const name = "ingest-storage-record"

	return &IngestStorageRecordTest{
		name:   name,
		cfg:    cfg,
		logger: logger,
		reg:    reg,
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
	level.Info(t.logger).Log("msg", "test loop")
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
	var errs []error
	fetches.EachRecord(func(rec *kgo.Record) {
		req := mimirpb.PreallocWriteRequest{}
		defer mimirpb.ReuseSlice(req.Timeseries)

		version := ingest.ParseRecordVersion(rec)
		if version > ingest.LatestRecordVersion {
			errs = append(errs, fmt.Errorf("received a record with an unsupported version: %d, max supported version: %d", version, ingest.LatestRecordVersion))
			return
		}

		err := ingest.DeserializeRecordContent(rec.Value, &req, version)
		if err != nil {
			errs = append(errs, fmt.Errorf("unmarshal record key %s: %w", rec.Key, err))
			return
		}
	})

	return errors.Join(errs...)
}
