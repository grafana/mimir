package continuoustest

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

type IngestStorageRecordTestConfig struct {
	Kafka         ingest.KafkaConfig `yaml:"-"`
	ConsumerGroup string             `yaml:"consumer_group"`
}

func (cfg *IngestStorageRecordTestConfig) RegisterFlags(f *flag.FlagSet) {
	// cfg.Kafka.RegisterFlagsWithPrefix("ingest-storage.kafka.", f)
	f.StringVar(&cfg.ConsumerGroup, "tests.ingest-storage-record.consumer-group", "ingest-storage-record", "The Kafka consumer group used for getting/setting commmitted offsets.")
}

type IngestStorageRecordTest struct {
	name        string
	cfg         IngestStorageRecordTestConfig
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
	endOffsets := endOffsetsResp.Offsets()

	// Calculate the amount of jump per partition.
	diffMap := make(map[int32]int64)
	for topic, partOffsets := range offsets {
		for partition, startOffset := range partOffsets {
			if endPartOffset, ok := endOffsets[topic]; ok {
				if endOffset, ok := endPartOffset[partition]; ok {
					diffMap[partition] = endOffset.At - startOffset.At
				} else {
					diffMap[partition] = -1
				}
			}
		}
	}
	js, err := json.Marshal(diffMap)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}
	level.Info(t.logger).Log("msg", "grabbed batch", "diffsPerPartition", string(js))

	// Update to the end.
	t.adminClient.CommitOffsets(ctx, t.cfg.ConsumerGroup, endOffsets)

	return nil
}
