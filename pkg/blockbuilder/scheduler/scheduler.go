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

	"github.com/grafana/mimir/pkg/blockbuilder"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

type BlockBuilderScheduler struct {
	services.Service

	adminClient *kadm.Client
	schedule    *schedule
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
		schedule: newSchedule(cfg.JobLeaseTime),
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
	)
	if err != nil {
		return fmt.Errorf("creating kafka reader: %w", err)
	}

	s.adminClient = kadm.NewClient(kc)
	return nil
}

func (s *BlockBuilderScheduler) stopping(_ error) error {
	s.adminClient.Close()
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
	defer func() {
		s.metrics.updateScheduleDuration.Observe(time.Since(startTime).Seconds())
	}()

	lag, err := blockbuilder.GetGroupLag(ctx, s.adminClient, s.cfg.Kafka.Topic, s.cfg.BuilderConsumerGroup, 0)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to get group lag", "err", err)
		return
	}

	if ps, ok := lag[s.cfg.Kafka.Topic]; ok {
		for part, gl := range ps {
			partStr := fmt.Sprint(part)
			s.metrics.partitionStartOffset.WithLabelValues(partStr).Set(float64(gl.Start.Offset))
			s.metrics.partitionEndOffset.WithLabelValues(partStr).Set(float64(gl.End.Offset))
			s.metrics.partitionCommittedOffset.WithLabelValues(partStr).Set(float64(gl.Commit.At))
		}
	}

	// See if the group-committed offset per partition is behind our offset that is considered "old."
	oldTime := time.Now().Add(-s.cfg.ConsumeInterval)
	offs, err := s.adminClient.ListOffsetsAfterMilli(ctx, oldTime.UnixMilli(), s.cfg.Kafka.Topic)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to list offsets after milli", "err", err)
		return
	}

	readyPartitions := []int32{}
	offs.Each(func(o kadm.ListedOffset) {
		if l, ok := lag.Lookup(o.Topic, o.Partition); ok {
			if l.Commit.At < o.Offset {
				readyPartitions = append(readyPartitions, o.Partition)
				level.Info(s.logger).Log("msg", "partition ready", "p", o.Partition)
			}
		}
	})
}
