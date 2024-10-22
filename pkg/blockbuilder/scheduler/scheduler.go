// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/storage/ingest"
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

func (s *BlockBuilderScheduler) starting(context.Context) error {
	kc, err := ingest.NewKafkaReaderClient(
		s.cfg.Kafka,
		ingest.NewKafkaReaderClientMetrics("block-builder-scheduler", s.register),
		s.logger,
		kgo.ConsumerGroup(s.cfg.SchedulerConsumerGroup),
		// The scheduler simply monitors partitions. We don't want it committing offsets.
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return fmt.Errorf("creating kafka reader: %w", err)
	}
	s.kafkaClient = kc
	return nil
}

func (s *BlockBuilderScheduler) stopping(_ error) error {
	s.kafkaClient.Close()
	return nil
}

func (s *BlockBuilderScheduler) running(ctx context.Context) error {
	updateTick := time.NewTicker(s.cfg.SchedulingInterval)
	defer updateTick.Stop()
	for {
		select {
		case <-updateTick.C:
			s.updateSchedule(ctx)
		case <-ctx.Done():
			return nil
		}
	}
}

func (s *BlockBuilderScheduler) updateSchedule(ctx context.Context) {
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

	s.metrics.updateScheduleDuration.Observe(time.Since(startTime).Seconds())

	startOffsets.Each(func(o kadm.ListedOffset) {
		s.metrics.partitionStartOffsets.WithLabelValues(fmt.Sprint(o.Partition)).Set(float64(o.Offset))
	})
	endOffsets.Each(func(o kadm.ListedOffset) {
		s.metrics.partitionEndOffsets.WithLabelValues(fmt.Sprint(o.Partition)).Set(float64(o.Offset))
	})
}
