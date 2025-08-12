package continuoustest

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
)

type IngestStorageRecordTestConfig struct {
	Kafka ingest.KafkaConfig `yaml:"-"`
}

func (cfg *IngestStorageRecordTestConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.Kafka.RegisterFlagsWithPrefix("ingest-storage.kafka.", f)
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
	return nil
}
