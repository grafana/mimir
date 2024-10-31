// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	jobs        *jobQueue
	cfg         Config
	logger      log.Logger
	register    prometheus.Registerer
	metrics     schedulerMetrics

	mu        sync.Mutex
	committed []int64
	dirty     bool
}

func New(
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
) (*BlockBuilderScheduler, error) {
	s := &BlockBuilderScheduler{
		jobs:     newJobQueue(cfg.JobLeaseTime, logger),
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
			s.jobs.clearExpiredLeases()
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

	// TODO: Flush committed offsets to Kafka.

	lag, err := blockbuilder.GetGroupLag(ctx, s.adminClient, s.cfg.Kafka.Topic, s.cfg.BuilderConsumerGroup, 0)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to get group lag", "err", err)
		return
	}

	if ps, ok := lag[s.cfg.Kafka.Topic]; ok {
		s.ensurePartitionCount(len(ps))

		for part, gl := range ps {
			partStr := fmt.Sprint(part)
			s.metrics.partitionStartOffset.WithLabelValues(partStr).Set(float64(gl.Start.Offset))
			s.metrics.partitionEndOffset.WithLabelValues(partStr).Set(float64(gl.End.Offset))
			s.metrics.partitionCommittedOffset.WithLabelValues(partStr).Set(float64(gl.Commit.At))
		}
	}

	oldTime := time.Now().Add(-s.cfg.ConsumeInterval)
	oldOffsets, err := s.adminClient.ListOffsetsAfterMilli(ctx, oldTime.UnixMilli(), s.cfg.Kafka.Topic)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to obtain old offsets", "err", err)
		return
	}

	// See if the group-committed offset per partition is behind our "old" offsets.

	oldOffsets.Each(func(o kadm.ListedOffset) {
		if l, ok := lag.Lookup(o.Topic, o.Partition); ok {
			if l.Commit.At < o.Offset {
				level.Info(s.logger).Log("msg", "partition ready", "p", o.Partition)
				// The job is uniquely identified by {topic, partition, consumption start offset}.
				jobID := fmt.Sprintf("%s/%d/%d", o.Topic, o.Partition, l.Commit.At)
				t := time.Time{} // TODO: this should be the time of the last commit from the lag info.
				s.jobs.addOrUpdate(jobID, t, jobSpec{
					topic:       o.Topic,
					partition:   o.Partition,
					startOffset: l.Commit.At,
					endOffset:   l.End.Offset,
					// TODO: populate these from the unmarshaled lag metadata.
					commitRecTs:    t,
					lastSeenOffset: 0,
					lastBlockEndTs: t,
				})
			}
		}
	})
}

func (s *BlockBuilderScheduler) ensurePartitionCount(ps int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.committed) < ps {
		s.committed = append(s.committed, make([]int64, ps-len(s.committed))...)
	}
}

func (s *BlockBuilderScheduler) getAssignedJob(workerID string) (string, jobSpec, error) {
	j, err := s.jobs.assign(workerID)
	if err != nil {
		return "", jobSpec{}, err
	}
	return j.id, j.spec, nil
}

// updateJob takes a job update from the client and
// (This is a temporary method for unit tests until we have RPCs.)
func (s *BlockBuilderScheduler) updateJob(jobID, workerID string, complete bool, j jobSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if p := j.partition; p < 0 || int(p) >= len(s.committed) {
		return fmt.Errorf("partition %d out of range", p)
	}

	if j.endOffset <= s.committed[j.partition] {
		// History. Ignore.
		return nil
	}

	if complete {
		if err := s.jobs.completeJob(jobID, workerID); err != nil {
			// job not found is fine, as clients will be re-informing us.
			if !errors.Is(err, errJobNotFound) {
				return fmt.Errorf("complete job: %w", err)
			}
		}
		s.committed[j.partition] = j.endOffset
		s.dirty = true
	} else {
		// It's an in-progress job whose lease we need to renew.
		if err := s.jobs.renewLease(jobID, workerID); err != nil {
			return fmt.Errorf("renew lease: %w", err)
		}
	}
	return nil
}
