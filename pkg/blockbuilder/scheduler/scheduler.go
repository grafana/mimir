// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"cmp"
	"container/list"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/status"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"google.golang.org/grpc/codes"

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
	partState           map[int32]*partitionState
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
		partState:    make(map[int32]*partitionState),
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
	return nil
}

func (s *BlockBuilderScheduler) stopping(_ error) error {
	if err := s.flushOffsetsToKafka(context.Background()); err != nil {
		level.Error(s.logger).Log("msg", "failed to flush offsets at shutdown", "err", err)
	}

	s.adminClient.Close()
	return nil
}

func (s *BlockBuilderScheduler) running(ctx context.Context) error {
	level.Info(s.logger).Log("msg", "entering observation mode")

	observeComplete := time.After(s.cfg.StartupObserveTime)

	c, err := s.fetchCommittedOffsets(ctx)
	if err != nil {
		level.Error(s.logger).Log("msg", "failed to fetch committed offsets", "err", err)
		s.metrics.fetchOffsetsFailed.Inc()
		return fmt.Errorf("fetch committed offsets: %w", err)
	}
	level.Debug(s.logger).Log("msg", "loaded initial committed offsets", "offsets", offsetsStr(c))
	s.mu.Lock()
	s.committed = c
	s.mu.Unlock()

	// Wait for StartupObserveTime to pass.
	select {
	case <-observeComplete:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Now we can transition to normal operation.

	s.completeObservationMode(ctx)
	go s.enqueuePendingJobsWorker(ctx)

	level.Info(s.logger).Log("msg", "entering normal operation")

	s.metrics.outstandingJobs.Set(float64(s.jobs.count()))
	s.metrics.assignedJobs.Set(float64(s.jobs.assigned()))

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
func (s *BlockBuilderScheduler) completeObservationMode(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.observationComplete {
		return
	}

	var policy jobCreationPolicy[schedulerpb.JobSpec]

	if s.cfg.MaxJobsPerPartition > 0 {
		policy = limitPerPartitionJobCreationPolicy{partitionLimit: s.cfg.MaxJobsPerPartition}
	} else {
		policy = noOpJobCreationPolicy[schedulerpb.JobSpec]{}
	}

	s.jobs = newJobQueue(s.cfg.JobLeaseExpiry, s.logger, policy)
	s.finalizeObservations()
	s.populateInitialJobs(ctx)
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

	now := time.Now()
	endOffsets, err := s.adminClient.ListEndOffsets(ctx, s.cfg.Kafka.Topic)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to list end offsets", "err", err)
		return
	}
	if endOffsets.Error() != nil {
		level.Warn(s.logger).Log("msg", "failed to list end offsets", "err", endOffsets.Error())
		return
	}

	endOffsets.Each(func(o kadm.ListedOffset) {
		partStr := fmt.Sprint(o.Partition)
		s.metrics.partitionEndOffset.WithLabelValues(partStr).Set(float64(o.Offset))

		s.mu.Lock()
		ps := s.getPartitionState(o.Partition)
		job, err := ps.updateEndOffset(o.Offset, now, s.cfg.JobSize)
		s.mu.Unlock()
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to observe end offset", "err", err)
			return
		}
		if job != nil {
			ps.addPendingJob(job)
		}
	})

	s.metrics.outstandingJobs.Set(float64(s.jobs.count()))
	s.metrics.assignedJobs.Set(float64(s.jobs.assigned()))
}

type partitionState struct {
	offset    int64
	jobBucket time.Time

	pendingJobs *list.List
}

const (
	bucketBefore = -1
	bucketSame   = 0
	bucketAfter  = 1
)

type offsetRange struct {
	start, end int64
}

// updateEndOffset processes an end offset and returns a consumption job spec if
// one is ready. This is expected to be called with monotonically increasing
// end offsets, and called frequently, even in the absence of new data.
func (s *partitionState) updateEndOffset(end int64, ts time.Time, jobSize time.Duration) (*offsetRange, error) {
	newJobBucket := ts.Truncate(jobSize)

	if s.jobBucket.IsZero() {
		s.offset = end
		s.jobBucket = newJobBucket
		return nil, nil
	}

	switch newJobBucket.Compare(s.jobBucket) {
	case bucketBefore:
		// New bucket is before our current one. This should only happen if our
		// Kafka's end offsets aren't monotonically increasing.
		return nil, fmt.Errorf("time went backwards: %s < %s (%d, %d)", s.jobBucket, newJobBucket, s.offset, end)
	case bucketSame:
		// Observation is in the currently tracked bucket. No action needed.
	case bucketAfter:
		// We've entered a new job bucket. Emit a job for the current
		// bucket if it has data and start a new one.

		var job *offsetRange
		if s.offset < end {
			job = &offsetRange{
				start: s.offset,
				end:   end,
			}
		}
		s.offset = end
		s.jobBucket = newJobBucket
		return job, nil
	}

	return nil, nil
}

func (s *partitionState) addPendingJob(job *offsetRange) {
	s.pendingJobs.PushBack(job)
}

// enqueuePendingJobsWorker is a worker method that enqueues pending jobs at a regular interval.
func (s *BlockBuilderScheduler) enqueuePendingJobsWorker(ctx context.Context) {
	enqueueTick := time.NewTicker(s.cfg.EnqueueInterval)
	defer enqueueTick.Stop()

	for {
		select {
		case <-enqueueTick.C:
			s.enqueuePendingJobs()

		case <-ctx.Done():
			return
		}
	}
}

// enqueuePendingJobs moves per-partition pending jobs to the active job queue
// for assignment to workers, subject to the job creation policy.
func (s *BlockBuilderScheduler) enqueuePendingJobs() {
	// For each partition, attempt to enqueue jobs until we run into a rejection
	// from the job creation policy. Pending jobs are created in order of their
	// offsets, therefore pulling from the front achieves the same.

	pending := make(map[int32]int, len(s.partState))

	s.mu.Lock()

	for partition, ps := range s.partState {
		pending[partition] = ps.pendingJobs.Len()

		for ps.pendingJobs.Len() > 0 {
			e := ps.pendingJobs.Front()
			or := e.Value.(*offsetRange)

			// The job discovery process happens concurrently with ongoing job
			// completions. Now that we have the lock, ignore this job if it's
			// older than our committed offset.
			if c, ok := s.committed.Lookup(s.cfg.Kafka.Topic, partition); ok && or.end <= c.At {
				level.Info(s.logger).Log("msg", "ignoring pending job as it's behind the committed offset (expected at startup)",
					"partition", partition, "start", or.start, "end", or.end, "committed", c.At)
				ps.pendingJobs.Remove(e)
				continue
			}

			jobID := fmt.Sprintf("%s/%d/%d", s.cfg.Kafka.Topic, partition, or.start)
			spec := schedulerpb.JobSpec{
				Topic:       s.cfg.Kafka.Topic,
				Partition:   partition,
				StartOffset: or.start,
				EndOffset:   or.end,
			}
			if err := s.jobs.add(jobID, spec); err != nil {
				if errors.Is(err, errJobCreationDisallowed) || errors.Is(err, errJobAlreadyExists) {
					// We've hit the limit for this partition.
				} else {
					level.Warn(s.logger).Log("msg", "failed to enqueue job", "partition", partition, "job_id", jobID, "err", err)
				}
				// Move onto the next partition.
				break
			}
			// Otherwise, it was successful.
			ps.pendingJobs.Remove(e)
		}
	}

	s.mu.Unlock()

	// And update the pending jobs metric.

	for partition, count := range pending {
		s.metrics.pendingJobs.WithLabelValues(fmt.Sprint(partition)).Set(float64(count))
	}
}

func (s *BlockBuilderScheduler) populateInitialJobs(ctx context.Context) {
	// Note that the lock is already held because we're in startup mode.
	consumeOffs, err := s.consumptionOffsets(ctx, s.cfg.Kafka.Topic, s.committed, time.Now().Add(-s.cfg.LookbackOnNoCommit))
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to get consumption offsets", "err", err)
		return
	}

	offFinder := newOffsetFinder(s.adminClient, s.logger)

	for _, off := range consumeOffs {
		o, err := probeInitialJobOffsets(ctx, offFinder, off.topic, off.partition,
			off.start, off.resume, off.end, time.Now(), s.cfg.JobSize,
			time.Now().Add(-s.cfg.MaxScanAge), s.logger)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to get consumption ranges", "err", err)
			continue
		}
		if len(o) == 0 {
			// No new data to consume, so skip.
			continue
		}

		ps := s.getPartitionState(off.partition)

		for _, io := range o {
			if job, err := ps.updateEndOffset(io.offset, io.time, s.cfg.JobSize); err != nil {
				level.Warn(s.logger).Log("msg", "failed to observe end offset", "err", err)
			} else if job != nil {
				ps.addPendingJob(job)
			}
		}
	}
}

func (s *BlockBuilderScheduler) getPartitionState(partition int32) *partitionState {
	if ps, ok := s.partState[partition]; ok {
		return ps
	}

	ps := &partitionState{
		pendingJobs: list.New(),
	}
	s.partState[partition] = ps
	return ps
}

type offsetTime struct {
	offset int64
	time   time.Time
}

// computeInitialJobs computes an initial set of consumption jobs that exist
// between each partition's committed offset and the end of the partition. This
// is used to bootstrap the job queue when the scheduler starts up. After these
// jobs are created, the scheduler is only concerned with keeping track of each
// partition's end offsets.
func probeInitialJobOffsets(ctx context.Context, offs offsetStore, topic string, partition int32, start, resume, end int64,
	endTime time.Time, jobSize time.Duration, minScanTime time.Time, logger log.Logger) ([]*offsetTime, error) {

	// The general idea is that we know the commit offset, but we don't know
	// that offset's timestamp. We have an API to get offsets given a timestamp,
	// so we iteratively call that API until we reach the committed offset.

	if resume >= end || start >= end {
		// No new data to consume.
		return []*offsetTime{}, nil
	}

	sentinels := []*offsetTime{}

	// Pick a more high-resolution interval to scan for the sentinel offsets.
	scanStep := jobSize / 4

	// Iterate backwards from the boundary time by job size, stopping when we've
	// either crossed the committed offset or the min scan time.
	for pb := endTime; minScanTime.Before(pb); pb = pb.Add(-scanStep) {
		off, t, err := offs.offsetAfterTime(ctx, topic, partition, pb)
		if err != nil {
			return nil, err
		}
		level.Debug(logger).Log("msg", "found next boundary offset", "ts", pb,
			"topic", topic, "partition", partition, "offset", off)

		// Don't want to create jobs that are before the resume offset.
		off = max(off, resume)

		if len(sentinels) == 0 || off != sentinels[len(sentinels)-1].offset {
			sentinels = append(sentinels, &offsetTime{offset: off, time: t})
		}

		if off == resume {
			// We've reached the resumption offset, so we're done.
			break
		}
	}

	// Return them in increasing order of offset.
	slices.SortFunc(sentinels, func(a, b *offsetTime) int {
		return cmp.Compare(a.offset, b.offset)
	})

	return sentinels, nil
}

type partitionOffsets struct {
	topic              string
	partition          int32
	start, resume, end int64
}

// consumptionOffsets returns the resumption and end offsets for each partition, falling back to the
// fallbackTime if there is no committed offset for a partition.
func (s *BlockBuilderScheduler) consumptionOffsets(ctx context.Context, topic string, committed kadm.Offsets, fallbackTime time.Time) ([]partitionOffsets, error) {
	startOffsets, err := s.adminClient.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets: %w", err)
	}
	if startOffsets.Error() != nil {
		return nil, fmt.Errorf("list start offsets: %w", startOffsets.Error())
	}
	endOffsets, err := s.adminClient.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}
	if endOffsets.Error() != nil {
		return nil, fmt.Errorf("list end offsets: %w", endOffsets.Error())
	}
	fallbackOffsets, err := s.adminClient.ListOffsetsAfterMilli(ctx, fallbackTime.UnixMilli(), topic)
	if err != nil {
		return nil, fmt.Errorf("list fallback offsets: %w", err)
	}
	if fallbackOffsets.Error() != nil {
		return nil, fmt.Errorf("list fallback offsets: %w", fallbackOffsets.Error())
	}

	so := startOffsets.Offsets()
	offs := make([]partitionOffsets, 0, len(so))

	for t, pt := range so {
		if t != topic {
			continue
		}
		for partition, startOffset := range pt {
			partStr := fmt.Sprint(partition)

			var consumeOffset int64

			if committedOff, ok := committed.Lookup(t, partition); ok {
				s.metrics.partitionCommittedOffset.WithLabelValues(partStr).Set(float64(committedOff.At))

				consumeOffset = committedOff.At
			} else {
				// Nothing committed for this partition. Rewind to fallback offset instead.
				o, ok := fallbackOffsets.Lookup(t, partition)
				if !ok {
					return nil, fmt.Errorf("partition %d not found in fallback offsets for topic %s", partition, t)
				}

				level.Debug(s.logger).Log("msg", "no commit; falling back to max of startOffset and fallbackOffset",
					"topic", t, "partition", partition, "startOffset", startOffset.At, "fallbackOffset", o.Offset)

				consumeOffset = max(startOffset.At, o.Offset)
			}

			end, ok := endOffsets.Lookup(t, partition)
			if !ok {
				return nil, fmt.Errorf("partition %d not found in end offsets for topic %s", partition, t)
			}

			level.Debug(s.logger).Log("msg", "consumptionOffsets", "topic", t, "partition", partition,
				"start", startOffset.At, "end", end.Offset, "consumeOffset", consumeOffset)

			s.metrics.partitionStartOffset.WithLabelValues(partStr).Set(float64(startOffset.At))
			s.metrics.partitionEndOffset.WithLabelValues(partStr).Set(float64(end.Offset))

			offs = append(offs, partitionOffsets{
				topic:     t,
				partition: partition,
				start:     startOffset.At,
				resume:    consumeOffset,
				end:       end.Offset,
			})
		}
	}

	return offs, nil
}

type offsetStore interface {
	offsetAfterTime(context.Context, string, int32, time.Time) (int64, time.Time, error)
}

type offsetFinder struct {
	offsets     map[time.Time]kadm.ListedOffsets
	adminClient *kadm.Client
	logger      log.Logger
}

func newOffsetFinder(adminClient *kadm.Client, logger log.Logger) *offsetFinder {
	return &offsetFinder{
		offsets:     make(map[time.Time]kadm.ListedOffsets),
		adminClient: adminClient,
		logger:      logger,
	}
}

// offsetAfterTime is a cached version of adminClient.ListOffsetsAfterMilli that
// makes use of the fact that we want to ask about the same times for all partitions.
func (o *offsetFinder) offsetAfterTime(ctx context.Context, topic string, partition int32, t time.Time) (int64, time.Time, error) {
	offs, ok := o.offsets[t]
	if !ok {
		var err error
		offs, err = o.adminClient.ListOffsetsAfterMilli(ctx, t.UnixMilli(), topic)
		if err != nil {
			return 0, time.Time{}, err
		}
		if offs.Error() != nil {
			return 0, time.Time{}, offs.Error()
		}

		o.offsets[t] = offs
	}

	po, ok := offs.Lookup(topic, partition)
	if !ok {
		return 0, time.Time{}, fmt.Errorf("failed to get offset for partition %d at time %s: not present", partition, t)
	}
	if po.Err != nil {
		return 0, time.Time{}, fmt.Errorf("failed to get offset for partition %d at time %s: %w", partition, t, po.Err)
	}

	return po.Offset, time.UnixMilli(po.Timestamp), nil
}

var _ offsetStore = (*offsetFinder)(nil)

// fetchCommittedOffsets fetches the committed offsets for the scheduler's consumer group.
// It returns empty offsets if the consumer group is not found.
func (s *BlockBuilderScheduler) fetchCommittedOffsets(ctx context.Context) (kadm.Offsets, error) {
	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 5 * time.Second,
		MaxRetries: 20,
	})
	var lastErr error

	for boff.Ongoing() {
		offs, err := s.adminClient.FetchOffsets(ctx, s.cfg.ConsumerGroup)
		if err != nil {
			if !errors.Is(err, kerr.GroupIDNotFound) {
				lastErr = fmt.Errorf("fetch offsets: %w", err)
				boff.Wait()
				continue
			}
		}

		if err := offs.Error(); err != nil {
			lastErr = fmt.Errorf("fetch offsets got error in response: %w", err)
			boff.Wait()
			continue
		}

		committed := make(kadm.Offsets)

		for _, ps := range offs {
			for _, o := range ps {
				committed.Add(kadm.Offset{
					Topic:       o.Topic,
					Partition:   o.Partition,
					At:          o.At,
					LeaderEpoch: o.LeaderEpoch,
				})
			}
		}

		return committed, nil
	}

	return kadm.Offsets{}, lastErr
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

// flushOffsetsToKafka flushes the committed offsets to Kafka and updates relevant metrics.
func (s *BlockBuilderScheduler) flushOffsetsToKafka(ctx context.Context) error {
	// TODO: only flush if dirty.
	offsets := s.snapCommitted()

	offsets.Each(func(o kadm.Offset) {
		s.metrics.partitionCommittedOffset.WithLabelValues(fmt.Sprint(o.Partition)).Set(float64(o.At))
	})

	err := s.adminClient.CommitAllOffsets(ctx, s.cfg.ConsumerGroup, offsets)
	if err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}

	level.Debug(s.logger).Log("msg", "flushed offsets to Kafka", "offsets", offsetsStr(offsets))

	return nil
}

// offsetsStr returns a string representation of the given offsets.
func offsetsStr(offsets kadm.Offsets) string {
	var b strings.Builder
	offsets.Each(func(o kadm.Offset) {
		if o.At == 0 {
			return
		}
		if b.Len() > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s/%d=%d", o.Topic, o.Partition, o.At)
	})
	offsetsStr := b.String()
	if offsetsStr == "" {
		offsetsStr = "<none>"
	}
	return offsetsStr
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
	defer s.mu.Unlock()
	doneObserving := s.observationComplete

	if !doneObserving {
		var empty schedulerpb.JobSpec
		return jobKey{}, empty, status.Error(codes.FailedPrecondition, "observation period not complete")
	}

	for {
		k, spec, err := s.jobs.assign(workerID)
		if err != nil {
			return k, spec, err
		}

		if c, ok := s.committed.Lookup(spec.Topic, spec.Partition); ok && spec.StartOffset < c.At {
			// Job is before the committed offset. Remove it.
			s.jobs.removeJob(k)
			continue
		}

		return k, spec, nil
	}
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
	logger := log.With(s.logger, "job_id", key.id, "epoch", key.epoch,
		"worker", workerID, "start_offset", j.StartOffset, "end_offset", j.EndOffset)

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.observationComplete {
		// We're still in observation mode. Record the observation.
		if err := s.updateObservation(key, workerID, complete, j); err != nil {
			return fmt.Errorf("observe update: %w", err)
		}

		level.Info(logger).Log("msg", "recovered job")
		return nil
	}

	if complete {
		if err := s.jobs.completeJob(key, workerID); err != nil {
			// job not found is fine, as clients will be re-informing us.
			if !errors.Is(err, errJobNotFound) {
				return fmt.Errorf("complete job: %w", err)
			}
		}

		// EndOffset is exclusive. It is the next offset to consume.
		s.advanceCommittedOffset(j.Topic, j.Partition, j.EndOffset)
		level.Info(logger).Log("msg", "completed job")
	} else {
		// It's an in-progress job whose lease we need to renew.

		if c, ok := s.committed.Lookup(s.cfg.Kafka.Topic, j.Partition); ok {
			if j.EndOffset <= c.At {
				// Update of a completed/committed job. Ignore.
				level.Debug(logger).Log("msg", "ignored historical job")
				return nil
			}
		}

		if err := s.jobs.renewLease(key, workerID); err != nil {
			return fmt.Errorf("renew lease: %w", err)
		}
		level.Info(logger).Log("msg", "renewed lease")
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
		level.Warn(s.logger).Log("msg", "bad epoch", "job_id", key.id,
			"epoch", key.epoch, "existing_epoch", rj.key.epoch, "existing_worker", rj.workerID)
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

type limitPerPartitionJobCreationPolicy struct {
	partitionLimit int
}

// canCreateJob allows at most $partitionLimit jobs per partition.
// TODO(davidgrant): add an error return to explain the reason for rejection.
func (p limitPerPartitionJobCreationPolicy) canCreateJob(_ jobKey, spec *schedulerpb.JobSpec, existingJobs []*schedulerpb.JobSpec) bool {
	remaining := p.partitionLimit - 1 // -1: we're about to add one.

	for _, existing := range existingJobs {
		if existing.Topic == spec.Topic && existing.Partition == spec.Partition {
			remaining--
			if remaining < 0 {
				return false
			}
		}
	}

	return true
}

var _ jobCreationPolicy[schedulerpb.JobSpec] = (*limitPerPartitionJobCreationPolicy)(nil)
