package blockbuilderscheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

type BlockBuilderScheduler struct {
	services.Service

	kafkaClient *kgo.Client
	cfg         Config
	logger      log.Logger
	register    prometheus.Registerer
}

func NewScheduler(
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
) (*BlockBuilderScheduler, error) {
	s := &BlockBuilderScheduler{
		cfg:      cfg,
		logger:   logger,
		register: reg,
	}
	s.Service = services.NewBasicService(s.starting, s.running, s.stopping)
	return s, nil
}

func (s *BlockBuilderScheduler) starting(context.Context) (err error) {
	s.kafkaClient, err = ingest.NewKafkaReaderClient(
		s.cfg.Kafka,
		ingest.NewKafkaReaderClientMetrics("block-builder-scheduler", s.register),
		s.logger,
	)
	if err != nil {
		return fmt.Errorf("creating kafka reader: %w", err)
	}
	return nil
}

func (s *BlockBuilderScheduler) stopping(_ error) error {
	s.kafkaClient.Close()
	return nil
}

func (s *BlockBuilderScheduler) running(ctx context.Context) error {
	monitorTick := time.NewTicker(s.cfg.KafkaMonitorInterval)
	defer monitorTick.Stop()
	for {
		select {
		case <-monitorTick.C:
			s.monitorPartitions(ctx)
		case <-ctx.Done():
			return nil
		}
	}
}

// monitorPartitions updates knowledge of all active partitions.
func (s *BlockBuilderScheduler) monitorPartitions(ctx context.Context) {
	// We're interested in:
	// * the lag of the block builder consumer group for each partition.
	// * each partition's start and end offsets.
	//admin := kadm.NewClient(s.kafkaClient)
}
