// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/alecthomas/units"
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
	adminClient *kadm.Client
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
		kgo.ConsumerGroup(s.cfg.SchedulerConsumerGroup),
		// Scheduler is just an observer; we don't want to it committing any offsets.
		kgo.DisableAutoCommit(),

		// Dial these back as we're only fetching single records.
		kgo.FetchMaxBytes(100*int32(units.KiB)),
		kgo.FetchMaxPartitionBytes(16*int32(units.KiB)),
	)
	if err != nil {
		return fmt.Errorf("creating kafka reader: %w", err)
	}
	s.adminClient = kadm.NewClient(s.kafkaClient)
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
	defer func() {
		s.metrics.monitorPartitionsDuration.Observe(time.Since(startTime).Seconds())
	}()

	startOffsets, err := s.adminClient.ListStartOffsets(ctx, s.cfg.Kafka.Topic)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to list start offsets", "err", err)
	}

	endOffsets, err := s.adminClient.ListEndOffsets(ctx, s.cfg.Kafka.Topic)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to list end offsets", "err", err)
	}

	startOffsets.Each(func(o kadm.ListedOffset) {
		s.metrics.partitionStartOffsets.WithLabelValues(fmt.Sprint(o.Partition)).Set(float64(o.Offset))
	})

	endOffsets.Each(func(o kadm.ListedOffset) {
		s.metrics.partitionEndOffsets.WithLabelValues(fmt.Sprint(o.Partition)).Set(float64(o.Offset))
	})

	// Any errors from here on are ones that cause us to bail from this monitoring iteration.

	committedOffsets, err := s.adminClient.ListCommittedOffsets(ctx, s.cfg.Kafka.Topic)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to list committed offsets", "err", err)
		return
	}

	committedOffsets.Each(func(o kadm.ListedOffset) {
		s.metrics.partitionCommittedOffsets.WithLabelValues(fmt.Sprint(o.Partition)).Set(float64(o.Offset))
	})

	consumedOffsets, err := s.adminClient.FetchOffsets(ctx, s.cfg.BuilderConsumerGroup)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to list consumed offsets", "err", err)
		return
	}

	backlogTime, err := s.computeBacklogTime(ctx, consumedOffsets.KOffsets(), committedOffsets.KOffsets())
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to compute backlog time", "err", err)
		return
	}

	for p, t := range backlogTime {
		s.metrics.partitionBacklogTime.WithLabelValues(fmt.Sprint(p)).Set(t.Seconds())
	}
}

func (s *BlockBuilderScheduler) computeBacklogTime(ctx context.Context,
	consumedOffsets, committedOffsets map[string]map[int32]kgo.Offset,
) (map[int32]time.Duration, error) {
	// fetch the (consumed, committed) records for each partition and store the
	// difference in the returned map.

	loRecords, err := s.fetchSingleRecords(ctx, consumedOffsets)
	if err != nil {
		return nil, fmt.Errorf("fetch consumed: %w", err)
	}
	hiRecords, err := s.fetchSingleRecords(ctx, committedOffsets)
	if err != nil {
		return nil, fmt.Errorf("fetch end records: %w", err)
	}

	backlogTime := make(map[int32]time.Duration, len(consumedOffsets))
	for p, loRecord := range loRecords {
		if hiRecord, ok := hiRecords[p]; ok {
			backlogTime[p] = hiRecord.Timestamp.Sub(loRecord.Timestamp)
		}
	}
	return backlogTime, nil
}

// fetchSingleRecords fetches the first record from each partition starting at the given offsets.
func (s *BlockBuilderScheduler) fetchSingleRecords(ctx context.Context, offsets map[string]map[int32]kgo.Offset) (map[int32]*kgo.Record, error) {
	s.kafkaClient.AddConsumePartitions(offsets)
	defer s.kafkaClient.PurgeTopicsFromConsuming(s.cfg.Kafka.Topic)
	out := make(map[int32]*kgo.Record)
	f := s.kafkaClient.PollFetches(ctx)
	f.EachError(func(_ string, _ int32, err error) {
		if !errors.Is(err, context.Canceled) {
			level.Error(s.logger).Log("msg", "failed to fetch records", "err", err)
		}
	})
	f.EachPartition(func(tp kgo.FetchTopicPartition) {
		if _, seen := out[tp.Partition]; seen {
			return
		}
		if len(tp.Records) > 0 {
			// We're only interested in the first record per partition.
			out[tp.Partition] = tp.Records[0]
		}
	})
	return out, nil
}
