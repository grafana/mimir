// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"slices"
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
	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

type BlockBuilderScheduler struct {
	services.Service

	adminClient *kadm.Client
	jobs        *jobQueue[schedulerpb.JobSpec]
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

func (s *BlockBuilderScheduler) starting(ctx context.Context) error {
	kc, err := ingest.NewKafkaReaderClient(
		s.cfg.Kafka,
		ingest.NewKafkaReaderClientMetrics(ingest.ReaderMetricsPrefix, "block-builder-scheduler", s.register),
		s.logger,
	)
	if err != nil {
		return fmt.Errorf("creating kafka reader: %w", err)
	}

	s.adminClient = kadm.NewClient(kc)

	// The startup process for block-builder-scheduler entails learning the state of the world:
	//  1. obtain an initial set of offset info from Kafka
	//  2. listen to worker updates for a while to learn what the previous scheduler knew
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
		select {
		case <-time.After(s.cfg.StartupObserveTime):
		case <-ctx.Done():
		}
	}()

	wg.Wait()

	if err := ctx.Err(); err != nil {
		return err
	}

	s.completeObservationMode()
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
			// These tasks are not prerequisites to updating the schedule, but
			// we do them here rather than creating a ton of update tickers.
			s.jobs.clearExpiredLeases()

			if err := s.flushOffsetsToKafka(context.WithoutCancel(ctx)); err != nil {
				level.Error(s.logger).Log("msg", "failed to flush offsets to Kafka", "err", err)
				s.metrics.flushFailed.Inc()
			}

			s.updateSchedule(ctx)
		case <-ctx.Done():
			return nil
		}
	}
}

// completeObservationMode transitions the scheduler from observation mode to normal operation.
func (s *BlockBuilderScheduler) completeObservationMode() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.observationComplete {
		return
	}

	s.jobs = newJobQueue(s.cfg.JobLeaseExpiry, s.logger, specLessThan)
	s.finalizeObservations()
	s.observations = nil
	s.observationComplete = true
}

// updateSchedule examines the state of the Kafka topic and updates the
// schedule, creating consumption jobs if appropriate.
func (s *BlockBuilderScheduler) updateSchedule(ctx context.Context) {
	startTime := time.Now()
	defer func() {
		s.metrics.updateScheduleDuration.Observe(time.Since(startTime).Seconds())
	}()

	jobs, err := s.computeJobs(ctx)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to discover jobs", "err", err)
		return
	}

	for _, job := range jobs {
		jobID := fmt.Sprintf("%s/%d/%d", job.Topic, job.Partition, job.StartOffset)
		s.jobs.addOrUpdate(jobID, *job)
	}
}

func (s *BlockBuilderScheduler) computeJobs(ctx context.Context) ([]*schedulerpb.JobSpec, error) {
	lag, err := blockbuilder.GetGroupLag(ctx, s.adminClient, s.cfg.Kafka.Topic, s.cfg.ConsumerGroup, 0)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to get group lag", "err", err)
		return nil, err
	}

	// We choose a boundary time that is the current time rounded down to the nearest hour.
	// We want to create jobs that will consume everything up but not including this boundary.
	// We want to consume nothing after this boundary. A later run will create a new job for the next hour.

	boundary := time.Now().Truncate(time.Hour)
	of := newOffsetFinder(s.adminClient)
	jobs := []*schedulerpb.JobSpec{}

	if ps, ok := lag[s.cfg.Kafka.Topic]; ok {
		for part, gl := range ps {
			partStr := fmt.Sprint(part)
			s.metrics.partitionStartOffset.WithLabelValues(partStr).Set(float64(gl.Start.Offset))
			s.metrics.partitionEndOffset.WithLabelValues(partStr).Set(float64(gl.End.Offset))
			s.metrics.partitionCommittedOffset.WithLabelValues(partStr).Set(float64(gl.Commit.At))

			ranges, err := consumptionRanges(ctx, of, s.cfg.Kafka.Topic, part, gl.Commit.At, gl.End.Offset, time.Hour, boundary)
			if err != nil {
				level.Warn(s.logger).Log("msg", "failed to get consumption ranges", "err", err)
				continue
			}

			for _, r := range ranges {
				jobs = append(jobs, &schedulerpb.JobSpec{
					Topic:       s.cfg.Kafka.Topic,
					Partition:   part,
					StartOffset: r.start,
					EndOffset:   r.end,
				})
			}
		}
	}

	return jobs, nil
}

type offsetStore interface {
	offsetAfterTime(context.Context, string, int32, time.Time) (int64, error)
}

type offsetFinder struct {
	offsets     map[time.Time]kadm.ListedOffsets
	adminClient *kadm.Client
}

func newOffsetFinder(adminClient *kadm.Client) *offsetFinder {
	return &offsetFinder{
		offsets:     make(map[time.Time]kadm.ListedOffsets),
		adminClient: adminClient,
	}
}

type offsetRange struct {
	start int64 // the first offset to consume
	end   int64 // the first offset not to consume
}

// offsetAfterTime is a cached version of adminClient.ListOffsetsAfterMilli that
// makes use of the fact that we're always asking about the same points in time.
func (o *offsetFinder) offsetAfterTime(ctx context.Context, topic string, partition int32, t time.Time) (int64, error) {
	offs, ok := o.offsets[t]
	if !ok {
		offs, err := o.adminClient.ListOffsetsAfterMilli(ctx, t.UnixMilli(), topic)
		if err != nil {
			return 0, err
		}
		o.offsets[t] = offs
	}

	l, ok := offs.Lookup(topic, partition)
	if !ok {
		return 0, fmt.Errorf("no offset found for partition %d at time %s", partition, t)
	}
	if l.Err != nil {
		return 0, fmt.Errorf("failed to get offset for partition %d at time %s: %w", partition, t, l.Err)
	}
	return l.Offset, nil
}

var _ offsetStore = (*offsetFinder)(nil)

// consumptionRanges returns the ranges of offsets that should be consumed.
func consumptionRanges(ctx context.Context, offs offsetStore, topic string, partition int32, committed int64, partEnd int64, width time.Duration, boundaryTime time.Time) ([]offsetRange, error) {
	if committed >= partEnd {
		// No new data to consume.
		return []offsetRange{}, nil
	}

	// boundaryTime is intended to be exclusive. (i.e., consume up to but not including 11:00 AM.)
	// Subtract a millisecond to make this work correctly with the Kafka offset APIs.
	boundaryTime = boundaryTime.Add(-1 * time.Millisecond)
	boundaryOffset, err := offs.offsetAfterTime(ctx, topic, partition, boundaryTime)
	if err != nil {
		return nil, err
	}

	if committed >= boundaryOffset {
		// No new data to consume.
		return []offsetRange{}, nil
	}

	starts := []int64{}

	for pb := boundaryTime.Add(-width); true; pb = pb.Add(-width) {
		off, err := offs.offsetAfterTime(ctx, topic, partition, pb)
		if err != nil {
			return nil, err
		}
		if off < committed {
			break
		}
		if len(starts) == 0 || off != starts[len(starts)-1] {
			starts = append(starts, off)
		}
	}

	// We have a slice of start offsets. Put them in increasing order, then transform them into (startInclusive, endExclusive) pairs.

	slices.Reverse(starts)
	ranges := make([]offsetRange, len(starts))

	for i, s := range starts {
		ranges[i].start = s
		if i < len(starts)-1 {
			ranges[i].end = starts[i+1]
		} else {
			ranges[i].end = boundaryOffset
		}
	}

	return ranges, nil
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

func (s *BlockBuilderScheduler) snapCommitted() kadm.Offsets {
	cp := make(kadm.Offsets)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.committed.Each(func(o kadm.Offset) {
		cp.Add(o)
	})
	return cp
}

func (s *BlockBuilderScheduler) flushOffsetsToKafka(ctx context.Context) error {
	offsets := s.snapCommitted()
	return s.adminClient.CommitAllOffsets(ctx, s.cfg.ConsumerGroup, offsets)
}

// advanceCommittedOffset advances the committed offset for the given topic/partition.
// It is a no-op if the new offset is not greater than the current committed offset.
// Assumes the lock is held.
func (s *BlockBuilderScheduler) advanceCommittedOffset(topic string, partition int32, newOffset int64) {
	if o, ok := s.committed.Lookup(topic, partition); ok {
		if newOffset > o.At {
			o.At = newOffset
			s.committed[topic][partition] = o
		}
	} else {
		s.committed.Add(kadm.Offset{
			Topic:     topic,
			Partition: partition,
			At:        newOffset,
		})
	}
}

// AssignJob assigns and returns a job, if one is available.
func (s *BlockBuilderScheduler) AssignJob(_ context.Context, req *schedulerpb.AssignJobRequest) (*schedulerpb.AssignJobResponse, error) {
	key, spec, err := s.assignJob(req.WorkerId)
	if err != nil {
		return nil, err
	}

	return &schedulerpb.AssignJobResponse{
		Key: &schedulerpb.JobKey{
			Id:    key.id,
			Epoch: key.epoch,
		},
		Spec: &spec,
	}, err
}

// assignJob returns an assigned job for the given workerID, if one is available.
func (s *BlockBuilderScheduler) assignJob(workerID string) (jobKey, schedulerpb.JobSpec, error) {
	s.mu.Lock()
	doneObserving := s.observationComplete
	s.mu.Unlock()

	if !doneObserving {
		var empty schedulerpb.JobSpec
		return jobKey{}, empty, status.Error(codes.Unavailable, "observation period not complete")
	}

	return s.jobs.assign(workerID)
}

// UpdateJob takes a job update from the client and records it, if necessary.
func (s *BlockBuilderScheduler) UpdateJob(_ context.Context, req *schedulerpb.UpdateJobRequest) (*schedulerpb.UpdateJobResponse, error) {
	k := jobKey{
		id:    req.Key.Id,
		epoch: req.Key.Epoch,
	}
	if err := s.updateJob(k, req.WorkerId, req.Complete, *req.Spec); err != nil {
		return nil, err
	}
	return &schedulerpb.UpdateJobResponse{}, nil
}

func (s *BlockBuilderScheduler) updateJob(key jobKey, workerID string, complete bool, j schedulerpb.JobSpec) error {
	logger := log.With(s.logger, "job_id", key.id, "epoch", key.epoch, "worker", workerID)

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.observationComplete {
		// We're still in observation mode. Record the observation.
		if err := s.updateObservation(key, workerID, complete, j); err != nil {
			return fmt.Errorf("observe update: %w", err)
		}

		logger.Log("msg", "recovered job")
		return nil
	}

	if c, ok := s.committed.Lookup(s.cfg.Kafka.Topic, j.Partition); ok {
		if j.EndOffset <= c.At {
			// Update of a completed/committed job. Ignore.
			level.Debug(logger).Log("msg", "ignored historical job")
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

		// EndOffset is inclusive, so we need to commit EndOffset+1.
		s.advanceCommittedOffset(j.Topic, j.Partition, j.EndOffset+1)
		logger.Log("msg", "completed job")
	} else {
		// It's an in-progress job whose lease we need to renew.
		if err := s.jobs.renewLease(key, workerID); err != nil {
			return fmt.Errorf("renew lease: %w", err)
		}
		logger.Log("msg", "renewed lease")
	}
	return nil
}

func (s *BlockBuilderScheduler) updateObservation(key jobKey, workerID string, complete bool, j schedulerpb.JobSpec) error {
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
func (s *BlockBuilderScheduler) finalizeObservations() {
	for _, rj := range s.observations {
		if rj.complete {
			// Completed.
			s.advanceCommittedOffset(rj.spec.Topic, rj.spec.Partition, rj.spec.EndOffset)
		} else {
			// An in-progress job.
			// These don't affect offsets, they just get added to the job queue.
			if err := s.jobs.importJob(rj.key, rj.workerID, rj.spec); err != nil {
				level.Warn(s.logger).Log("msg", "failed to import job", "job_id", rj.key.id, "epoch", rj.key.epoch, "worker", rj.workerID, "err", err)
			}
		}
	}
}

type obsMap map[string]*observation

type observation struct {
	key      jobKey
	spec     schedulerpb.JobSpec
	workerID string
	complete bool
}

var _ schedulerpb.BlockBuilderSchedulerServer = (*BlockBuilderScheduler)(nil)

// specLessThan determines whether spec a should come before b in job scheduling.
func specLessThan(a, b schedulerpb.JobSpec) bool {
	return a.CommitRecTs.Before(b.CommitRecTs)
}
