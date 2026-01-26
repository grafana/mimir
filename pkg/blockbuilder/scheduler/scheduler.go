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

	mu                  sync.Mutex
	observations        obsMap
	observationComplete bool
	partState           map[int32]*partitionState

	// for synchronizing tests.
	onScheduleUpdated func()
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

		observations: make(obsMap),
		partState:    make(map[int32]*partitionState),

		onScheduleUpdated: func() {},
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
	level.Info(s.logger).Log("msg", "loaded initial committed offsets", "offsets", offsetsStr(c))
	s.mu.Lock()
	c.Each(func(o kadm.Offset) {
		ps := s.getPartitionState(o.Topic, o.Partition)
		ps.initCommit(o.At)
	})
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
			s.onScheduleUpdated()

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

	s.jobs = newJobQueue(s.cfg.JobLeaseExpiry, policy, s.cfg.JobFailuresAllowed, s.metrics, s.logger)
	s.finalizeObservations()

	consumeOffs, err := s.consumptionOffsets(ctx, s.cfg.Kafka.Topic, time.Now().Add(-s.cfg.LookbackOnNoCommit))
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to get consumption offsets", "err", err)
		return
	}

	s.populateInitialJobs(ctx, consumeOffs, newOffsetFinder(s.adminClient, s.logger), time.Now())
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
		defer s.mu.Unlock()

		ps := s.getPartitionState(o.Topic, o.Partition)
		job, err := ps.updateEndOffset(o.Offset, now, s.cfg.JobSize)

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
	topic     string
	partition int32

	offset    int64
	jobBucket time.Time

	committed *advancingOffset
	planned   *advancingOffset

	// pendingJobs are jobs that are waiting to be enqueued. The job creation policy is what allows them to advance to the plannedJobs list.
	pendingJobs *list.List
	// plannedJobs are jobs that are either ready to be assigned, in-progress, or completed.
	plannedJobs *list.List
	// plannedJobsMap is a map of jobID to jobState for quick lookup.
	plannedJobsMap map[string]*jobState
}

type jobState struct {
	jobID    string
	spec     schedulerpb.JobSpec
	complete bool
}

const (
	bucketBefore = -1
	bucketSame   = 0
	bucketAfter  = 1
)

// updateEndOffset processes an end offset and returns a consumption job spec if
// one is ready. This is expected to be called with monotonically increasing
// end offsets, and called frequently, even in the absence of new data.
func (s *partitionState) updateEndOffset(end int64, ts time.Time, jobSize time.Duration) (*schedulerpb.JobSpec, error) {
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
		return nil, fmt.Errorf("time went backwards: %s < %s (%d, %d)", newJobBucket, s.jobBucket, s.offset, end)
	case bucketSame:
		// Observation is in the currently tracked bucket. No action needed.
	case bucketAfter:
		// We've entered a new job bucket. Emit a job for the current
		// bucket if it has data and start a new one.

		var job *schedulerpb.JobSpec
		if s.offset < end {
			job = &schedulerpb.JobSpec{
				Topic:       s.topic,
				Partition:   s.partition,
				StartOffset: s.offset,
				EndOffset:   end,
			}
		}
		s.offset = end
		s.jobBucket = newJobBucket
		return job, nil
	}

	return nil, nil
}

func (s *partitionState) initCommit(commit int64) {
	s.committed.set(commit)
	// Initially, the planned offset is the committed offset.
	s.planned.set(commit)
}

func (s *partitionState) addPendingJob(job *schedulerpb.JobSpec) {
	s.pendingJobs.PushBack(job)
}

func (s *partitionState) addPlannedJob(id string, spec schedulerpb.JobSpec) {
	if s.planned.beyondSpec(spec) {
		// This shouldn't happen. All callers of addPlannedJob must do so in
		// increasing offset order.
		panic(fmt.Sprintf("given spec %d [%d, %d) is behind the current planned offset %d",
			spec.Partition, spec.StartOffset, spec.EndOffset, s.planned.offset()))
	}

	js := &jobState{jobID: id, spec: spec, complete: false}
	s.plannedJobs.PushBack(js)
	s.plannedJobsMap[js.jobID] = js
	s.planned.advance(id, spec)
}

func (s *partitionState) completeJob(jobID string) error {
	if j, ok := s.plannedJobsMap[jobID]; !ok {
		return errJobNotFound
	} else {
		j.complete = true
	}

	// Now we both advance the committed offset and garbage collect completed
	// jobs. As the active jobs list knows about all active jobs for this
	// partition and its order is maintained, we can advance the committed
	// offset and GC any completed job(s) at the front of this list.

	for elem := s.plannedJobs.Front(); elem != nil; elem = s.plannedJobs.Front() {
		js := elem.Value.(*jobState)
		if !js.complete {
			break
		}
		s.plannedJobs.Remove(elem)
		delete(s.plannedJobsMap, js.jobID)
		s.committed.advance(js.jobID, js.spec)
	}
	return nil
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
			spec := e.Value.(*schedulerpb.JobSpec)

			// The job discovery process happens concurrently with ongoing job
			// completions. Now that we have the lock, ignore this job if it's
			// older than our committed offset.
			if ps.committed.beyondSpec(*spec) {
				level.Info(s.logger).Log("msg", "ignoring pending job as it's behind the committed offset (expected at startup)",
					"partition", partition, "start", spec.StartOffset, "end", spec.EndOffset, "committed", ps.committed.off)
				ps.pendingJobs.Remove(e)
				continue
			}

			jobID := fmt.Sprintf("%s/%d/%d", s.cfg.Kafka.Topic, partition, spec.StartOffset)
			if err := s.jobs.add(jobID, *spec); err != nil {
				if errors.Is(err, errJobCreationDisallowed) || errors.Is(err, errJobAlreadyExists) {
					// We've hit the limit for this partition.
				} else {
					level.Warn(s.logger).Log("msg", "failed to enqueue job", "partition", partition, "job_id", jobID, "err", err)
				}
				// Move onto the next partition.
				break
			}

			// Otherwise, it was successful. Move it to the planned jobs list.
			ps.pendingJobs.Remove(e)
			ps.addPlannedJob(jobID, *spec)
		}
	}

	s.mu.Unlock()

	// And update the pending jobs metric.

	for partition, count := range pending {
		s.metrics.pendingJobs.WithLabelValues(fmt.Sprint(partition)).Set(float64(count))
	}
}

func (s *BlockBuilderScheduler) populateInitialJobs(ctx context.Context, consumeOffs []partitionOffsets, offStore offsetStore, endTime time.Time) {
	// (Note that the lock is already held because we're in startup mode.)

	// While during normal operation we are periodically asking about every
	// partition's end offset, during startup we need to compute a set of
	// ~correctly-sized jobs that may exist between the partition's commit and
	// end offsets.
	// We do that by performing a one-time probe of <offset, time> pairs between
	// those two offsets and seeding the schedule by calling updateEndOffset for
	// each of them- just like we do during normal operation.

	minScanTime := endTime.Add(-s.cfg.MaxScanAge)

	for _, off := range consumeOffs {
		o, err := probeInitialOffsets(ctx, offStore, off.topic, off.partition,
			off.start, off.resume, off.end, endTime, s.cfg.JobSize, minScanTime, s.logger)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to probe initial offsets", "err", err)
			continue
		}
		if len(o) == 0 {
			// No new data to consume, so skip.
			continue
		}

		ps := s.getPartitionState(off.topic, off.partition)

		for _, io := range o {
			if job, err := ps.updateEndOffset(io.offset, io.time, s.cfg.JobSize); err != nil {
				level.Warn(s.logger).Log("msg", "failed to observe end offset", "err", err)
			} else if job != nil {
				ps.addPendingJob(job)
			}
		}
	}
}

func (s *BlockBuilderScheduler) getPartitionState(topic string, partition int32) *partitionState {
	if ps, ok := s.partState[partition]; ok {
		return ps
	}

	ps := &partitionState{
		topic:          topic,
		partition:      partition,
		pendingJobs:    list.New(),
		plannedJobs:    list.New(),
		plannedJobsMap: make(map[string]*jobState),
		planned: &advancingOffset{
			name:    offsetNamePlanned,
			off:     offsetEmpty,
			metrics: &s.metrics,
			logger:  s.logger,
		},
		committed: &advancingOffset{
			name:    offsetNameCommitted,
			off:     offsetEmpty,
			metrics: &s.metrics,
			logger:  s.logger,
		},
	}
	s.partState[partition] = ps
	return ps
}

type offsetTime struct {
	offset int64
	time   time.Time
}

// probeInitialOffsets computes an initial set of <offset, time> pairs that
// exist between this partition's commit and end offsets. These pairs can be
// used to seed a bunch of end offset observations to start the scheduler.
func probeInitialOffsets(ctx context.Context, offs offsetStore, topic string, partition int32, start, commit, end int64,
	endTime time.Time, jobSize time.Duration, minScanTime time.Time, logger log.Logger) ([]*offsetTime, error) {

	if commit >= end || start >= end {
		// No new data to consume. Return the single end offset so it is initially registered.
		return []*offsetTime{{offset: end, time: endTime}}, nil
	}

	// Pick a more high-resolution interval to scan for the sentinel offsets.
	scanStep := jobSize / 4
	reachedCommit := false
	sentinels := []*offsetTime{}

	// The general idea is that we know the commit offset, but we don't know
	// that offset's timestamp. We have an API (offsetAfterTime) to get offsets
	// given a timestamp, so we iteratively call that API until we reach either
	// the commit offset or the min scan time.
	for pb := endTime; minScanTime.Before(pb); pb = pb.Add(-scanStep) {
		off, t, err := offs.offsetAfterTime(ctx, topic, partition, pb)
		if err != nil {
			return nil, err
		}
		level.Debug(logger).Log("msg", "found next boundary offset", "ts", pb,
			"topic", topic, "partition", partition, "offset", off)

		// Don't want to probe for offsets before the commit.
		off = max(off, commit)

		if len(sentinels) == 0 || off != sentinels[len(sentinels)-1].offset {
			sentinels = append(sentinels, &offsetTime{offset: off, time: t})
		}

		if off == commit {
			// We've reached the commit offset, so we're done.
			reachedCommit = true
			break
		}
	}

	if !reachedCommit {
		lastOffset := int64(-1)
		if len(sentinels) > 0 {
			lastOffset = sentinels[len(sentinels)-1].offset
		}
		level.Warn(logger).Log("msg", "probe offsets: probe did not reach commit offset due to limited scan age", "lastOffset", lastOffset, "commitOffset", commit)
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
// fallbackTime if there is no planned offset for a partition.
func (s *BlockBuilderScheduler) consumptionOffsets(ctx context.Context, topic string, fallbackTime time.Time) ([]partitionOffsets, error) {
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
			ps := s.getPartitionState(t, partition)

			// Where to resume from? The partition's lowest planned offset, if
			// available. Otherwise, we choose the higher of the partition's
			// fallback and start offset.

			var resumeOffset int64

			if !ps.planned.empty() {
				planned := ps.planned.offset()
				s.metrics.partitionPlannedOffset.WithLabelValues(partStr).Set(float64(planned))
				resumeOffset = planned
			} else {
				// Nothing planned offset for this partition. Resume from fallback offset instead.
				o, ok := fallbackOffsets.Lookup(t, partition)
				if !ok {
					return nil, fmt.Errorf("partition %d not found in fallback offsets for topic %s", partition, t)
				}

				level.Debug(s.logger).Log("msg", "no planned offset; falling back to max of startOffset and fallbackOffset",
					"topic", t, "partition", partition, "startOffset", startOffset.At, "fallbackOffset", o.Offset)

				resumeOffset = max(startOffset.At, o.Offset)
			}

			end, ok := endOffsets.Lookup(t, partition)
			if !ok {
				return nil, fmt.Errorf("partition %d not found in end offsets for topic %s", partition, t)
			}

			level.Debug(s.logger).Log("msg", "consumptionOffsets", "topic", t, "partition", partition,
				"start", startOffset.At, "end", end.Offset, "consumeOffset", resumeOffset)

			s.metrics.partitionStartOffset.WithLabelValues(partStr).Set(float64(startOffset.At))
			s.metrics.partitionEndOffset.WithLabelValues(partStr).Set(float64(end.Offset))

			offs = append(offs, partitionOffsets{
				topic:     t,
				partition: partition,
				start:     startOffset.At,
				resume:    resumeOffset,
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

// snapOffsets returns a snapshot of the committed and planned offsets for all partitions.
func (s *BlockBuilderScheduler) snapOffsets() (kadm.Offsets, kadm.Offsets) {
	cp := make(kadm.Offsets)
	pp := make(kadm.Offsets)

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, ps := range s.partState {
		if !ps.committed.empty() {
			cp.AddOffset(ps.topic, ps.partition, ps.committed.offset(), 0)
		}
		if !ps.planned.empty() {
			pp.AddOffset(ps.topic, ps.partition, ps.planned.offset(), 0)
		}
	}

	return cp, pp
}

// flushOffsetsToKafka flushes the committed offsets to Kafka and updates relevant metrics.
func (s *BlockBuilderScheduler) flushOffsetsToKafka(ctx context.Context) error {
	// TODO: only flush if dirty.
	committed, planned := s.snapOffsets()

	committed.Each(func(o kadm.Offset) {
		s.metrics.partitionCommittedOffset.WithLabelValues(fmt.Sprint(o.Partition)).Set(float64(o.At))
	})
	planned.Each(func(o kadm.Offset) {
		s.metrics.partitionPlannedOffset.WithLabelValues(fmt.Sprint(o.Partition)).Set(float64(o.At))
	})

	err := s.adminClient.CommitAllOffsets(ctx, s.cfg.ConsumerGroup, committed)
	if err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}

	level.Debug(s.logger).Log("msg", "flushed offsets to Kafka", "offsets", offsetsStr(committed))
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

		if ps := s.getPartitionState(spec.Topic, spec.Partition); ps.committed.beyondSpec(spec) {
			// Job is before the committed offset. Remove it.
			level.Info(s.logger).Log("msg", "removing job as it's behind the committed offset (expected at startup)",
				"job_id", k.id, "epoch", k.epoch, "partition", spec.Partition,
				"start_offset", spec.StartOffset, "end_offset", spec.EndOffset,
				"committed", ps.committed.offset())
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

	ps := s.getPartitionState(j.Topic, j.Partition)

	if complete {
		if err := s.jobs.completeJob(key, workerID); err != nil {
			if errors.Is(err, errJobNotFound) {
				// job not found is fine, as clients will be re-informing us.
				return nil
			}
			return fmt.Errorf("complete job: %w", err)
		}

		if err := ps.completeJob(key.id); err != nil {
			return fmt.Errorf("complete scheduler job: %w", err)
		}
		level.Info(logger).Log("msg", "completed job")
	} else {
		// It's an in-progress job whose lease we need to renew.

		if ps.committed.beyondSpec(j) {
			// Update of a completed/committed job. Ignore.
			level.Debug(logger).Log("msg", "ignored historical job")
			return nil
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

	// Group observations by partition for gap analysis
	partitionObservations := make(map[int32][]*observation)

	for _, rj := range s.observations {
		partitionObservations[rj.spec.Partition] = append(partitionObservations[rj.spec.Partition], rj)
	}

	maxEpoch := int64(0)

	for partition, observations := range partitionObservations {
		ps := s.getPartitionState(s.cfg.Kafka.Topic, partition)
		contiguous := true

		if len(observations) == 0 {
			// No observations, keep latest planned offset at committed offset
			continue
		}

		// Sort observations by start offset
		slices.SortFunc(observations, func(a, b *observation) int {
			return cmp.Compare(a.spec.StartOffset, b.spec.StartOffset)
		})

		// Find the highest contiguous coverage by processing jobs in order.
		// Stop importing jobs if we find a gap. The last continuous job defines
		// our latest planned offset which will be where we resume job planning.
		for _, obs := range observations {
			maxEpoch = max(maxEpoch, obs.key.epoch)

			if !contiguous {
				// We found a gap earlier. Skip and warn.
				level.Warn(s.logger).Log("msg", "startup: skipping job import due to offset gap",
					"partition", partition, "job_id", obs.key.id, "epoch", obs.key.epoch,
					"start_offset", obs.spec.StartOffset, "end_offset", obs.spec.EndOffset)
				continue
			}

			if ps.planned.beyondSpec(obs.spec) {
				// This job is wholly before the latest planned offset. Skip.
				level.Warn(s.logger).Log("msg", "startup: skipping job before commit",
					"partition", partition, "job_id", obs.key.id, "epoch", obs.key.epoch,
					"start_offset", obs.spec.StartOffset, "end_offset", obs.spec.EndOffset)
				continue
			}

			if !ps.planned.validNextSpec(obs.spec) {
				// Found a gap, can't continue the contiguous range
				contiguous = false
				level.Warn(s.logger).Log("msg", "startup: skipping job due to detected offset gap",
					"partition", partition, "job_id", obs.key.id, "start_offset", obs.spec.StartOffset,
					"end_offset", obs.spec.EndOffset, "latest_planned_offset", ps.planned.offset())
				continue
			}

			if obs.complete {
				// Completed. Add it to the plan and mark it as such.
				ps.addPlannedJob(obs.key.id, obs.spec)
				if err := ps.completeJob(obs.key.id); err != nil {
					panic(fmt.Sprintf("unable to complete previously planned job %s, %s", obs.key.id, err.Error()))
				}
			} else {
				// An in-progress job that's part of our continuous coverage.
				if err := s.jobs.importJob(obs.key, obs.workerID, obs.spec); err != nil {
					level.Warn(s.logger).Log("msg", "failed to import job", "job_id", obs.key.id,
						"epoch", obs.key.epoch, "worker", obs.workerID, "err", err)
					contiguous = false
					continue
				}
				ps.addPlannedJob(obs.key.id, obs.spec)
			}
		}
	}

	s.jobs.setEpoch(maxEpoch + 1)
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

// advancingOffset keeps track of an offset that is expected to advance
// monotonically based on job progression.
type advancingOffset struct {
	off     int64
	name    string
	metrics *schedulerMetrics
	logger  log.Logger
}

const offsetEmpty int64 = -1

const (
	offsetNamePlanned   = "planned"
	offsetNameCommitted = "committed"
)

// advance moves the offset forward by the given job spec. Advancements are
// expected to be monotonically increasing and contiguous. Advance will not
// allow backwards movement. If a gap is detected, a warning is logged and a
// metric is incremented.
func (o *advancingOffset) advance(jobID string, spec schedulerpb.JobSpec) {
	if o.beyondSpec(spec) {
		// Frequent, and expected.
		level.Debug(o.logger).Log("msg", "ignoring historical job", "offset_name", o.name, "job_id", jobID,
			"partition", spec.Partition, "start_offset", spec.StartOffset, "end_offset", spec.EndOffset, "committed", o.off)
		return
	}

	if !o.validNextSpec(spec) {
		// Gap detected.
		level.Warn(o.logger).Log("msg", "gap detected in offset advancement", "offset_name", o.name, "job_id", jobID,
			"partition", spec.Partition, "start_offset", spec.StartOffset, "end_offset", spec.EndOffset, "committed", o.off)
		o.metrics.jobGapDetected.WithLabelValues(o.name).Inc()
	}

	o.off = spec.EndOffset
}

func (o *advancingOffset) offset() int64 {
	return o.off
}

func (o *advancingOffset) set(offset int64) {
	o.off = offset
}

// empty returns true if the offset is empty and uninitialized.
func (o *advancingOffset) empty() bool {
	return o.off == offsetEmpty
}

// validNextSpec returns true if the given job spec is valid to be added to the
// offset. It is valid if the start offset is the same as the current offset.
// We also allow transitioning out of an empty offset without calling it a gap.
func (o *advancingOffset) validNextSpec(spec schedulerpb.JobSpec) bool {
	return o.off == spec.StartOffset || o.empty()
}

// beyondSpec returns true if the offset is beyond the given job spec.
func (o *advancingOffset) beyondSpec(spec schedulerpb.JobSpec) bool {
	return !o.empty() && spec.EndOffset <= o.off
}
