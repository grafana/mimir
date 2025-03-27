// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
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

	if err := s.observe(ctx); err != nil {
		level.Error(s.logger).Log("msg", "failed to observe updates", "err", err)
		return fmt.Errorf("observe: %w", err)
	}

	s.completeObservationMode()
	level.Info(s.logger).Log("msg", "entering normal operation")

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

// observe carries out the startup process for block-builder-scheduler which entails learning the state of the world:
//  1. obtain an initial set of offset info from Kafka
//  2. listen to worker updates for a while to learn what the previous scheduler knew
func (s *BlockBuilderScheduler) observe(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		c, err := s.fetchCommittedOffsets(ctx)
		if err != nil {
			panic(err)
		}

		level.Debug(s.logger).Log("msg", "loaded initial committed offsets", "offsets", offsetsStr(c))

		s.mu.Lock()
		s.committed = c
		s.mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		select {
		case <-time.After(s.cfg.StartupObserveTime):
		case <-ctx.Done():
		}
	}()

	wg.Wait()
	return ctx.Err()
}

// completeObservationMode transitions the scheduler from observation mode to normal operation.
func (s *BlockBuilderScheduler) completeObservationMode() {
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

	s.jobs = newJobQueue(s.cfg.JobLeaseExpiry, s.logger, specLessThan, policy)
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

	// job computation was based on a snapshot of the committed offsets that may have changed.
	// Nix any jobs that are now before the committed offsets.
	s.mu.Lock()
	defer s.mu.Unlock()

	jobs = slices.DeleteFunc(jobs, func(job *schedulerpb.JobSpec) bool {
		committedOff, ok := s.committed.Lookup(job.Topic, job.Partition)
		if !ok {
			// create the job even if there is no committed offset.
			return false
		}
		return job.StartOffset < committedOff.At
	})

	for _, job := range jobs {
		jobID := fmt.Sprintf("%s/%d/%d", job.Topic, job.Partition, job.StartOffset)
		s.jobs.addOrUpdate(jobID, *job)
	}
}

type partitionOffsets struct {
	topic      string
	partition  int32
	start, end int64
}

// consumptionOffsets returns the resumption and end offsets for each partition, falling back to the
// fallbackTime if there is no committed offset for a partition.
func (s *BlockBuilderScheduler) consumptionOffsets(ctx context.Context, topic string, committed kadm.Offsets, fallbackTime time.Time) ([]partitionOffsets, error) {
	startOffsets, err := s.adminClient.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets: %w", err)
	}
	endOffsets, err := s.adminClient.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}
	fallbackOffsets, err := s.adminClient.ListOffsetsAfterMilli(ctx, fallbackTime.UnixMilli(), topic)
	if err != nil {
		return nil, fmt.Errorf("list fallback offsets: %w", err)
	}

	offs := []partitionOffsets{}

	for t, pt := range startOffsets.Offsets() {
		if t != topic {
			continue
		}
		for partition, startOffset := range pt {
			partStr := fmt.Sprint(partition)

			var consumeOffset int64
			committedOff, ok := committed.Lookup(t, partition)
			if ok {
				s.metrics.partitionCommittedOffset.WithLabelValues(partStr).Set(float64(committedOff.At))

				consumeOffset = committedOff.At
				level.Debug(s.logger).Log("msg", "found commit offset", "topic", t, "partition", partition, "offset", consumeOffset)
			} else {
				// Nothing committed for this partition. Rewind to fallback offset instead.
				o, ok := fallbackOffsets.Lookup(t, partition)
				if !ok {
					return nil, fmt.Errorf("partition %d not found in fallback offsets for topic %s", partition, t)
				}

				level.Debug(s.logger).Log("msg", "no commit, so falling back to max of startOffset and fallbackOffset",
					"topic", t, "partition", partition, "startOffset", startOffset.At, "fallbackOffset", o.Offset)

				consumeOffset = max(startOffset.At, o.Offset)
			}

			end, ok := endOffsets.Lookup(t, partition)
			if !ok {
				return nil, fmt.Errorf("partition %d not found in end offsets for topic %s", partition, t)
			}

			level.Debug(s.logger).Log("msg", "consumptionOffsets", "topic", t, "partition", partition, "start", startOffset.At, "end", end.Offset, "consumeOffset", consumeOffset)

			s.metrics.partitionStartOffset.WithLabelValues(partStr).Set(float64(startOffset.At))
			s.metrics.partitionEndOffset.WithLabelValues(partStr).Set(float64(end.Offset))

			if consumeOffset < end.Offset {
				offs = append(offs, partitionOffsets{
					topic:     t,
					partition: partition,
					start:     consumeOffset,
					end:       end.Offset,
				})
			}
		}
	}

	return offs, nil
}

func (s *BlockBuilderScheduler) computeJobs(ctx context.Context) ([]*schedulerpb.JobSpec, error) {
	consumeOffs, err := s.consumptionOffsets(ctx, s.cfg.Kafka.Topic, s.snapCommitted(), time.Now().Add(-s.cfg.LookbackOnNoCommit))
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to get consumption offsets", "err", err)
		return nil, err
	}

	// We choose a boundary time that is the current time rounded down to the job size.
	// We want to create jobs that will consume everything up but not including this boundary.
	// A later run will create job(s) for the boundary and beyond.

	boundary := time.Now().Truncate(s.cfg.JobSize)
	of := newOffsetFinder(s.adminClient, s.logger)
	minScanTime := time.Now().Add(-s.cfg.MaxScanAge)
	jobs := []*schedulerpb.JobSpec{}

	// randomize partition order until we have a fair ordering mechanism.
	rand.Shuffle(len(consumeOffs), func(i, j int) {
		consumeOffs[i], consumeOffs[j] = consumeOffs[j], consumeOffs[i]
	})

	for _, off := range consumeOffs {
		level.Debug(s.logger).Log("msg", "computing jobs for partition", "topic", off.topic, "partition", off.partition, "start", off.start, "end", off.end, "boundary", boundary)

		pj, err := computePartitionJobs(ctx, of, off.topic, off.partition,
			off.start, off.end, boundary, s.cfg.JobSize, minScanTime)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to get consumption ranges", "err", err)
			continue
		}

		// Log details of each job for debugging purposes
		for _, job := range pj {
			level.Debug(s.logger).Log(
				"msg", "computed job",
				"topic", job.Topic,
				"partition", job.Partition,
				"startOffset", job.StartOffset,
				"endOffset", job.EndOffset,
			)
		}

		jobs = append(jobs, pj...)
	}

	return jobs, nil
}

type offsetStore interface {
	offsetAfterTime(context.Context, string, int32, time.Time) (int64, error)
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
func (o *offsetFinder) offsetAfterTime(ctx context.Context, topic string, partition int32, t time.Time) (int64, error) {
	offs, ok := o.offsets[t]
	if !ok {
		var err error
		offs, err = o.adminClient.ListOffsetsAfterMilli(ctx, t.UnixMilli(), topic)
		if err != nil {
			return 0, err
		}
		if offs.Error() != nil {
			return 0, offs.Error()
		}

		for _, p := range offs.Offsets() {
			for _, oo := range p {
				if oo.Partition == partition {
					level.Debug(o.logger).Log("msg", "Offset found for ts", "ts", t, "topic", oo.Topic, "partition", oo.Partition, "offset", oo.At)
				}
			}
		}

		o.offsets[t] = offs
	}

	po, ok := offs.Lookup(topic, partition)
	if !ok {
		return 0, nil
	}
	if po.Err != nil {
		return 0, fmt.Errorf("failed to get offset for partition %d at time %s: %w", partition, t, po.Err)
	}
	return po.Offset, nil
}

var _ offsetStore = (*offsetFinder)(nil)

// computePartitionJobs returns the ranges of offsets that should be consumed
// for the given topic and partition.
func computePartitionJobs(ctx context.Context, offs offsetStore, topic string, partition int32,
	committed int64, partEnd int64, boundaryTime time.Time, jobSize time.Duration, minScanTime time.Time) ([]*schedulerpb.JobSpec, error) {

	if committed >= partEnd {
		// No new data to consume.
		return []*schedulerpb.JobSpec{}, nil
	}

	// boundaryTime is intended to be exclusive. (i.e., consume up to but not including 11:00 AM.)
	// Subtract 1ms to turn it into an inclusive boundary for fetching start offsets.

	boundaryTime = boundaryTime.Add(-1 * time.Millisecond)
	windowEndOffset, err := offs.offsetAfterTime(ctx, topic, partition, boundaryTime)
	if err != nil {
		return nil, err
	}

	if committed >= windowEndOffset {
		// No new data to consume.
		return []*schedulerpb.JobSpec{}, nil
	}

	jobs := []*schedulerpb.JobSpec{}

	// Iterate backwards from the boundary time by job size, stopping when we've
	// either crossed the committed offset or the min scan time.
	for pb := boundaryTime.Add(-jobSize); minScanTime.Before(pb); pb = pb.Add(-jobSize) {
		off, err := offs.offsetAfterTime(ctx, topic, partition, pb)
		if err != nil {
			return nil, err
		}
		if off < committed {
			// We're now before the committed offset, so we're done.
			break
		}
		if off >= windowEndOffset {
			// Found an offset exceeding our window. Ignore and keep looking.
			continue
		}

		if len(jobs) == 0 || off != jobs[len(jobs)-1].StartOffset {
			jobs = append(jobs, &schedulerpb.JobSpec{
				Topic:       topic,
				Partition:   partition,
				StartOffset: off,
			})
		}

		if off == committed {
			// We've reached the committed offset, so we're done.
			break
		}
	}

	// We have a slice of jobs with start offsets. Put them in increasing order,
	// then fill in the exclusive end offsets.

	slices.Reverse(jobs)

	for i := range jobs {
		if i < len(jobs)-1 {
			jobs[i].EndOffset = jobs[i+1].StartOffset
		} else {
			jobs[i].EndOffset = windowEndOffset
		}
	}

	return jobs, nil
}

// fetchCommittedOffsets fetches the committed offsets for the given consumer group.
// It returns empty offsets if the consumer group is not found.
func (s *BlockBuilderScheduler) fetchCommittedOffsets(ctx context.Context) (kadm.Offsets, error) {
	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: time.Second,
		MaxRetries: 10,
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

func (s *BlockBuilderScheduler) flushOffsetsToKafka(ctx context.Context) error {
	// TODO: only flush if dirty.
	offsets := s.snapCommitted()
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
	doneObserving := s.observationComplete
	s.mu.Unlock()

	if !doneObserving {
		var empty schedulerpb.JobSpec
		return jobKey{}, empty, status.Error(codes.FailedPrecondition, "observation period not complete")
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

// specLessThan determines whether spec a should come before b in job scheduling.
func specLessThan(a, b schedulerpb.JobSpec) bool {
	// Don't have an ordering criteria for now.
	return true
}

type limitPerPartitionJobCreationPolicy struct {
	partitionLimit int
}

// canCreateJob allows at most $partitionLimit jobs per partition.
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
