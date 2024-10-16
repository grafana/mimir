package blockbuilderscheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type BlockBuilderScheduler struct {
	services.Service

	kafkaClient *kgo.Client
	cfg         Config
	logger      log.Logger
	register    prometheus.Registerer
	metrics     schedulerMetrics
}

func New(
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
) (*BlockBuilderScheduler, error) {
	s := &BlockBuilderScheduler{
		cfg:      cfg,
		logger:   logger,
		register: reg,
		metrics:  newSchedulerMetrics(reg),
	}
	s.Service = services.NewBasicService(s.starting, s.running, s.stopping)
	return s, nil
}

func (s *BlockBuilderScheduler) starting(context.Context) (err error) {
	s.kafkaClient, err = ingest.NewKafkaReaderClient(
		s.cfg.Kafka,
		ingest.NewKafkaReaderClientMetrics("block-builder-scheduler", s.register),
		s.logger,
		kgo.DisableAutoCommit(),
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
	startTime := time.Now()
	// Eventually this will also include job computation. But for now, collect partition data.
	admin := kadm.NewClient(s.kafkaClient)

	startOffsets, err := admin.ListStartOffsets(ctx, s.cfg.Kafka.Topic)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to list start offsets", "err", err)
	}
	endOffsets, err := admin.ListEndOffsets(ctx, s.cfg.Kafka.Topic)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to list end offsets", "err", err)
	}

	s.metrics.monitorPartitionsDuration.Observe(time.Since(startTime).Seconds())

	startOffsets.Each(func(o kadm.ListedOffset) {
		s.metrics.partitionStartOffsets.WithLabelValues(fmt.Sprint(o.Partition)).Set(float64(o.Offset))
	})
	endOffsets.Each(func(o kadm.ListedOffset) {
		s.metrics.partitionEndOffsets.WithLabelValues(fmt.Sprint(o.Partition)).Set(float64(o.Offset))
	})
}
