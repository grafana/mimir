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
	committed kadm.Offsets
	dirty     bool
}

func New(
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
) (*BlockBuilderScheduler, error) {
	s := &BlockBuilderScheduler{
		jobs:      newJobQueue(cfg.JobLeaseTime, logger),
		cfg:       cfg,
		logger:    logger,
		register:  reg,
		metrics:   newSchedulerMetrics(reg),
		committed: make(kadm.Offsets),
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

const minMinute = 15

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

	// This is an interim solution to avoid scheduling jobs in the first 15 minutes of the hour.
	if time.Now().Minute() < minMinute {
		return
	}

	// TODO: s.committed will be *at least* what is returned as committed from Kafka.
	// We'll want to adjust the returned Kafka committed offsets to be the max of what we've seen.

	parts, ok := lag[s.cfg.Kafka.Topic]
	if !ok {
		level.Warn(s.logger).Log("msg", "no partitions found for topic")
		return
	}

	for p, lag := range parts {
		if lag.Lag <= 0 {
			level.Info(s.logger).Log(
				"msg", "nothing to consume in partition",
				"partition", p,
				"commit_offset", lag.Commit.At,
				"start_offset", lag.Start.Offset,
				"end_offset", lag.End.Offset,
				"lag", lag.Lag,
			)
			continue
		}

		// The job is uniquely identified by {topic, partition, consumption start offset}.
		jobID := fmt.Sprintf("%s/%d/%d", s.cfg.Kafka.Topic, p, lag.Commit.At)
		partState := blockbuilder.PartitionStateFromLag(s.logger, lag, 0)
		s.jobs.addOrUpdate(jobID, jobSpec{
			topic:          s.cfg.Kafka.Topic,
			partition:      p,
			startOffset:    lag.Commit.At,
			endOffset:      lag.End.Offset,                  // scan window offset
			commitRecTs:    partState.CommitRecordTimestamp, // On commit, this comes from worker as the last consumeds to disk offset ts.
			lastSeenOffset: partState.LastSeenOffset,        // endOffset from n-1 iteration
			lastBlockEndTs: partState.LastBlockEnd,          // Use the BB code.
			currBlockEndTs: time.Now().Truncate(s.cfg.ConsumeInterval),
		})
	}
}

// assignJob returns an assigned job for the given workerID.
// (This is a temporary method for unit tests until we have RPCs.)
func (s *BlockBuilderScheduler) assignJob(workerID string) (string, jobSpec, error) {
	return s.jobs.assign(workerID)
}

// updateJob takes a job update from the client and records it, if necessary.
// (This is a temporary method for unit tests until we have RPCs.)
func (s *BlockBuilderScheduler) updateJob(jobID, workerID string, complete bool, j jobSpec) error {

	/*
		Today worker computes a whole bunch of stuff at the end of consumption
		and commits that directly to Kafka.  Instead, we need to have the worker
		send that to the scheduler, which will then (eventually) commit it to
		Kafka.

		It'll be all of this info:

		commit := kadm.Offset{
			Topic:       commitRec.Topic,
			Partition:   commitRec.Partition,
			At:          commitRec.Offset + 1, // offset+1 means everything up to (including) the offset was processed
			LeaderEpoch: commitRec.LeaderEpoch,
			Metadata:    marshallCommitMeta(commitRec.Timestamp.UnixMilli(), lastSeenOffset, lastBlockEnd.UnixMilli()),
		}
		newState := PartitionState{
			Commit:                commit,
			CommitRecordTimestamp: commitRec.Timestamp,
			LastSeenOffset:        lastSeenOffset,
			LastBlockEnd:          lastBlockEnd,
		}
	*/
	s.mu.Lock()
	defer s.mu.Unlock()

	if c, ok := s.committed.Lookup(s.cfg.Kafka.Topic, j.partition); ok {
		if j.startOffset <= c.At {
			// Update of a completed/committed job. Ignore.
			return nil
		}
	}

	if complete {
		if err := s.jobs.completeJob(jobID, workerID); err != nil {
			// job not found is fine, as clients will be re-informing us.
			if !errors.Is(err, errJobNotFound) {
				return fmt.Errorf("complete job: %w", err)
			}
		}
		// TODO: Move forward P's offset info in s.committed.
	} else {
		// It's an in-progress job whose lease we need to renew.
		if err := s.jobs.renewLease(jobID, workerID); err != nil {
			return fmt.Errorf("renew lease: %w", err)
		}
	}
	return nil
}
