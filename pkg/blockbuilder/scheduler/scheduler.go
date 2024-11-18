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
	"github.com/grafana/dskit/backoff"
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

	mu sync.Mutex
	// committed is our local notion of the committed offsets.
	// It is learned from Kafka at startup, but only updated by the completion of jobs.
	committed           kadm.Offsets
	observations        obsMap
	observationComplete bool
}

func New(
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
) (*BlockBuilderScheduler, error) {
	s := &BlockBuilderScheduler{
		jobs:     nil,
		cfg:      cfg,
		logger:   logger,
		register: reg,
		metrics:  newSchedulerMetrics(reg),

		committed:    make(kadm.Offsets),
		observations: make(obsMap),
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
	// The first thing we do when starting up is to complete the startup process where we:
	//  1. obtain an initial set of offset info from Kafka
	//  2. listen to worker updates for a while to learn the state of the world
	// When both of those are complete, we transition from observation mode to normal operation.

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		lag, err := s.fetchLag(ctx)
		if err != nil {
			panic(err)
		}
		s.committed = commitOffsetsFromLag(lag)
	}()
	go func() {
		defer wg.Done()
		time.Sleep(s.cfg.StartupObserveTime)
	}()

	wg.Wait()

	if err := ctx.Err(); err != nil {
		return err
	}

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
	defer s.mu.Unlock()

	if s.observationComplete {
		return
	}

	s.jobs = newJobQueue(s.cfg.JobLeaseExpiry, s.logger)

	if err := s.finalizeObservations(); err != nil {
		level.Warn(s.logger).Log("msg", "failed to compute state from observations", "err", err)
		// (what to do here?)
	}

	s.observations = nil
	s.observationComplete = true
}

func (s *BlockBuilderScheduler) updateSchedule(ctx context.Context) {
	startTime := time.Now()
	defer func() {
		s.metrics.updateScheduleDuration.Observe(time.Since(startTime).Seconds())
	}()

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
		if o.Err != nil {
			level.Warn(s.logger).Log("msg", "failed to get old offset", "partition", o.Partition, "err", o.Err)
			return
		}
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

func (s *BlockBuilderScheduler) fetchLag(ctx context.Context) (kadm.GroupLag, error) {
	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		MaxRetries: 10,
	})
	var lastErr error
	for boff.Ongoing() {
		groupLag, err := blockbuilder.GetGroupLag(ctx, s.adminClient, s.cfg.Kafka.Topic, s.cfg.ConsumerGroup, 0)
		if err != nil {
			lastErr = fmt.Errorf("lag: %w", err)
			boff.Wait()
			continue
		}

		return groupLag, nil
	}

	return kadm.GroupLag{}, lastErr
}

// commitOffsetsFromLag computes the committed offset info from the given lag.
func commitOffsetsFromLag(lag kadm.GroupLag) kadm.Offsets {
	offsets := make(kadm.Offsets)
	for _, ps := range lag {
		for _, gl := range ps {
			offsets.Add(gl.Commit)
		}
	}
	return offsets
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
		if err := s.updateObservation(key, workerID, complete, j); err != nil {
			return fmt.Errorf("observe update: %w", err)
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

		// TODO: Push forward the local notion of the committed offset.

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

func (s *BlockBuilderScheduler) updateObservation(key jobKey, workerID string, complete bool, j jobSpec) error {
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

// finalizeObservations considers the observations and offsets from Kafka, rectifying them into
// the starting state of the scheduler's normal operation.
func (s *BlockBuilderScheduler) finalizeObservations() error {
	for _, rj := range s.observations {
		if rj.complete {
			// Completed.
			if o, ok := s.committed.Lookup(rj.spec.topic, rj.spec.partition); ok {
				if rj.spec.endOffset > o.At {
					// Completed jobs can push forward the offsets we've learned from Kafka.
					o.At = rj.spec.endOffset
					o.Metadata = "{}" // TODO: take the new meta from the completion message.
					s.committed[rj.spec.topic][rj.spec.partition] = o
				}
			} else {
				s.committed.Add(kadm.Offset{
					Topic:     rj.spec.topic,
					Partition: rj.spec.partition,
					At:        rj.spec.endOffset,
					Metadata:  "{}", // TODO: take the new meta from the completion message.
				})
			}
		} else {
			// An in-progress job.
			// These don't affect offsets (yet), they just get added to the job queue.
			if err := s.jobs.importJob(rj.key, rj.workerID, rj.spec); err != nil {
				return fmt.Errorf("import job: %w", err)
			}
		}
	}

	return nil
}

type obsMap map[string]*observation

type observation struct {
	key      jobKey
	spec     jobSpec
	workerID string
	complete bool
}
