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
	"github.com/gogo/status"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"google.golang.org/grpc/codes"

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

	mu                  sync.Mutex
	committed           kadm.Offsets
	dirty               bool
	observations        map[string]*observation
	observationComplete bool
}

func New(
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
) (*BlockBuilderScheduler, error) {
	s := &BlockBuilderScheduler{
		jobs:     newJobQueue(cfg.JobLeaseExpiry, logger),
		cfg:      cfg,
		logger:   logger,
		register: reg,
		metrics:  newSchedulerMetrics(reg),

		committed:    make(kadm.Offsets),
		observations: make(map[string]*observation),
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
	// The first thing we do when starting up is to complete the startup process
	//  where we allow both of these to complete:
	// 1. perform an initial update of the schedule
	// 2. listen to worker updates for a while to learn the state of the world

	time.Sleep(s.cfg.StartupObserveTime)
	s.completeObservationMode()

	// Now that that's done, we can start the main loop.

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

func (s *BlockBuilderScheduler) completeObservationMode() {
	s.mu.Lock()
	s.committed, s.jobs = s.computeStateFromObservations(s.observations, s.committed)
	s.observationComplete = true
	s.mu.Unlock()
}

func (s *BlockBuilderScheduler) updateSchedule(ctx context.Context) {
	startTime := time.Now()
	defer func() {
		s.metrics.updateScheduleDuration.Observe(time.Since(startTime).Seconds())
	}()

	// TODO: Commit the offsets back to Kafka if dirty.

	lag, err := blockbuilder.GetGroupLag(ctx, s.adminClient, s.cfg.Kafka.Topic, s.cfg.ConsumerGroup, 0)
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
				partState := blockbuilder.PartitionStateFromLag(s.logger, l, 0)
				s.jobs.addOrUpdate(jobID, jobSpec{
					topic:          o.Topic,
					partition:      o.Partition,
					startOffset:    l.Commit.At,
					endOffset:      l.End.Offset,
					commitRecTs:    partState.CommitRecordTimestamp,
					lastSeenOffset: partState.LastSeenOffset,
					lastBlockEndTs: partState.LastBlockEnd,
				})
			}
		}
	})
}

// assignJob returns an assigned job for the given workerID.
// (This is a temporary method for unit tests until we have RPCs.)
func (s *BlockBuilderScheduler) assignJob(workerID string) (jobKey, jobSpec, error) {
	s.mu.Lock()
	doneObserving := s.observationComplete
	s.mu.Unlock()

	if !doneObserving {
		return jobKey{}, jobSpec{}, status.Error(codes.Unavailable, "observation period not complete")
	}

	return s.jobs.assign(workerID)
}

// updateJob takes a job update from the client and records it, if necessary.
// (This is a temporary method for unit tests until we have RPCs.)
func (s *BlockBuilderScheduler) updateJob(key jobKey, workerID string, complete bool, j jobSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.observationComplete {
		if err := s.observeUpdate(key, workerID, complete, j); err != nil {
			return err
		}

		s.logger.Log("msg", "recovered job", "key", key, "worker", workerID)
		return nil
	}

	if c, ok := s.committed.Lookup(s.cfg.Kafka.Topic, j.partition); ok {
		if j.startOffset <= c.At {
			// Update of a completed/committed job. Ignore.
			s.logger.Log("msg", "ignored historical job", "key", key, "worker", workerID)
			return nil
		}
	}

	if complete {
		if err := s.jobs.completeJob(key, workerID); err != nil {
			// job not found is fine, as clients will be re-informing us.
			if !errors.Is(err, errJobNotFound) {
				return fmt.Errorf("complete job: %w", err)
			}
		}
		s.committed.AddOffset(s.cfg.Kafka.Topic, j.partition, j.endOffset, -1)
		s.dirty = true
		s.logger.Log("msg", "completed job", "key", key, "worker", workerID)
	} else {
		// It's an in-progress job whose lease we need to renew.
		if err := s.jobs.renewLease(key, workerID); err != nil {
			return fmt.Errorf("renew lease: %w", err)
		}
		s.logger.Log("msg", "renewed lease", "key", key, "worker", workerID)
	}
	return nil
}

type observation struct {
	key      jobKey
	spec     jobSpec
	workerID string
	complete bool
}

func (s *BlockBuilderScheduler) observeUpdate(key jobKey, workerID string, complete bool, j jobSpec) error {
	rj, ok := s.observations[key.id]
	if !ok {
		s.observations[key.id] = &observation{
			key:      key,
			spec:     j,
			workerID: workerID,
			complete: complete,
		}
		return nil
	}

	// Otherwise, we've seen it before. Higher epochs win, and cause earlier ones to fail.

	if key.epoch < rj.key.epoch {
		return errBadEpoch
	}

	rj.key = key
	rj.spec = j
	rj.workerID = workerID
	rj.complete = complete
	return nil
}

// completeObservationMode considers the observations and offsets from Kafka, rectifying them into
// the starting state of the scheduler's normal operation.
func (s *BlockBuilderScheduler) computeStateFromObservations(observations map[string]*observation, kafkaOffsets kadm.Offsets) (kadm.Offsets, *jobQueue) {
	/*
		Inputs:
			- the observations
			- the offsets we've learned from Kafka

			Note that the observations for in-progress jobs should be strictly less than whatever
			we've learned from Kafka.
			Some of the completed job observations can exceed the offsets from Kafka, and should move them forward.
		The output from this is:
			- Updated Kafka committed offsets.
			- a jobQueue that's ready for action.
	*/

	committed := make(map[int32]int64)
	jobs := newJobQueue(s.cfg.JobLeaseExpiry, s.logger)

	for _, rj := range observations {
		if rj.complete {
			if o, ok := committed[rj.spec.partition]; !ok || o < rj.spec.endOffset {
				committed[rj.spec.partition] = rj.spec.endOffset
			}
		} else {
			// In progress.
			if err := jobs.importJob(rj.key, rj.workerID, rj.spec); err != nil {
				level.Warn(s.logger).Log("msg", "failed to recover job", "err", err)
			}
		}
	}

	c := make(kadm.Offsets)

	// Now we go back through the committed offsets and rig up the K offsets.
	for part, off := range committed {
		c.AddOffset(s.cfg.Kafka.Topic, part, off, -1)
	}

	return c, jobs
}
