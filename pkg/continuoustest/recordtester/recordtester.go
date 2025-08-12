package recordtester

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
)

type Config struct {
	ConsumeInterval time.Duration `yaml:"consume_interval"`

	Kafka ingest.KafkaConfig `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.ConsumeInterval, "record-continuous-test.consume-interval", 1*time.Minute, "")
}

type RecordContinuousTester struct {
	services.Service
	cfg         Config
	adminClient *kadm.Client

	logger log.Logger
	reg    prometheus.Registerer
}

func NewRecordContinuousTester(cfg Config, reg prometheus.Registerer, logger log.Logger) (*RecordContinuousTester, error) {
	s := &RecordContinuousTester{
		cfg:    cfg,
		reg:    reg,
		logger: logger,
	}
	s.Service = services.NewBasicService(s.starting, s.running, s.stopping)
	return s, nil
}

func (s *RecordContinuousTester) starting(context.Context) error {
	level.Info(s.logger).Log("msg", "starting kafka client")

	kc, err := ingest.NewKafkaReaderClient(
		s.cfg.Kafka,
		ingest.NewKafkaReaderClientMetrics(ingest.ReaderMetricsPrefix, "record-continuous-test", s.reg),
		s.logger)
	if err != nil {
		return fmt.Errorf("creating kafka reader: %w", err)
	}

	s.adminClient = kadm.NewClient(kc)
	return nil
}

func (s *RecordContinuousTester) stopping(error) error {
	s.adminClient.Close()
	return nil
}

func (s *RecordContinuousTester) running(ctx context.Context) error {
	level.Info(s.logger).Log("msg", "running recordtester")

	updateTick := time.NewTicker(s.cfg.ConsumeInterval)
	defer updateTick.Stop()
	for {
		select {
		case <-updateTick.C:
			level.Info(s.logger).Log("msg", "tick")
		case <-ctx.Done():
			return nil
		}
	}
}
