// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"iter"
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

	partitionStates map[int32]*partitionState

	// for synchronizing tests.
	onScheduleUpdated func()
}

// Compartment support is not wired up yet; the scheduler runs a single cluster per
// partition. These will be replaced by config once compartments are enabled.
const (
	compartmentsEnabled = false
	numClusters         = 1
)

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
		metrics:  newSchedulerMetrics(reg, compartmentsEnabled, numClusters),

		observations:    make(obsMap),
		partitionStates: make(map[int32]*partitionState),

		onScheduleUpdated: func() {},
	}
	s.Service = services.NewBasicService(s.starting, s.running, s.stopping)
	return s, nil
}

func (s *BlockBuilderScheduler) starting(_ context.Context) error {
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
	// Throughout this function we map ctx.Done/context.Canceled to nil, as this
	// is a normal shutdown and we don't want the service framework to interpret
	// it as an error.

	level.Info(s.logger).Log("msg", "entering observation mode")

	observeComplete := time.After(s.cfg.StartupObserveTime)

	if err := s.loadInitialCommittedOffsets(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	// Wait for StartupObserveTime to pass.
	select {
	case <-observeComplete:
	case <-ctx.Done():
		return nil
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

// loadInitialCommittedOffsets seeds each partition's committed offset from the committed
// offsets, so startup recovery resumes from where the previous scheduler left off.
func (s *BlockBuilderScheduler) loadInitialCommittedOffsets(ctx context.Context) error {
	c, err := fetchCommittedOffsets(ctx, s.adminClient, s.cfg.ConsumerGroup, s.cfg.Kafka.Topic)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		err = fmt.Errorf("fetch committed offsets: %w", err)
		level.Error(s.logger).Log("msg", "failed to load initial committed offsets", "err", err)
		s.metrics.fetchOffsetsFailed.Inc()
		return err
	}
	level.Info(s.logger).Log("msg", "loaded initial committed offsets", "offsets", offsetsStr(c))
	s.mu.Lock()
	c.Each(func(o kadm.Offset) {
		ps := s.getPartitionState(o.Topic, o.Partition)
		ps.initCommit(0, o.At)
	})
	s.mu.Unlock()
	return nil
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

	offsetsByPartition, err := s.initConsumptionOffsets(ctx, time.Now().Add(-s.cfg.LookbackOnNoCommit))
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed to get consumption offsets", "err", err)
		return
	}

	scanner := newOffsetScanner([]offsetStore{newOffsetFinder(s.adminClient)}, s.cfg.Kafka.Topic, time.Now(), s.cfg.JobSize, s.cfg.MaxScanAge, s.metrics.probeRecordTimeDelta)
	s.populateInitialJobs(ctx, offsetsByPartition, scanner)
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

	touched := make(map[int32]struct{})
	s.recordEndOffsets(endOffsets, touched)

	// Advance the time bucket of each touched partition, enqueuing any job that rolled over.
	s.mu.Lock()
	defer s.mu.Unlock()
	for partition := range touched {
		ps := s.getPartitionState(s.cfg.Kafka.Topic, partition)
		job, err := ps.updateTime(now, s.cfg.JobSize)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to update partition time", "err", err)
			continue
		}
		if job != nil {
			ps.addPendingJob(job)
		}
	}

	s.metrics.outstandingJobs.Set(float64(s.jobs.count()))
	s.metrics.assignedJobs.Set(float64(s.jobs.assigned()))
}

// recordEndOffsets records the end offsets into each partition's state, marking every partition
// it touched so the caller can advance them once all offsets are recorded.
func (s *BlockBuilderScheduler) recordEndOffsets(endOffsets kadm.ListedOffsets, touched map[int32]struct{}) {
	endOffsets.Each(func(o kadm.ListedOffset) {
		s.metrics.perClusterMetrics[0].endOffset.WithLabelValues(fmt.Sprint(o.Partition)).Set(float64(o.Offset))

		s.mu.Lock()
		defer s.mu.Unlock()

		ps := s.getPartitionState(o.Topic, o.Partition)
		ps.updateEndOffset(0, o.Offset)
		touched[o.Partition] = struct{}{}
	})
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
	s.mu.Lock()
	pending := make(map[int32]int, len(s.partitionStates))

	defer func() {
		// Unlock the scheduler before populating metrics. Unblocks the concurrent goroutines a tiny bit faster.
		s.mu.Unlock()

		for partition, count := range pending {
			s.metrics.pendingJobs.WithLabelValues(fmt.Sprint(partition)).Set(float64(count))
		}
	}()

	// For each partition, attempt to enqueue jobs until we run into a rejection
	// from the job creation policy. Pending jobs are created in order of their
	// offsets, therefore pulling from the front achieves the same.
	for partition, ps := range s.partitionStates {
		pending[partition] = ps.pendingJobs.Len()

		for ps.pendingJobs.Len() > 0 {
			e := ps.pendingJobs.Front()
			spec := e.Value.(*schedulerpb.JobSpec)

			// The job discovery process happens concurrently with ongoing job
			// completions. Now that we have the lock, ignore this job if it's
			// older than our committed offset.
			if ps.committedBeyondSpec(*spec) {
				level.Info(s.logger).Log("msg", "ignoring pending job as it's behind the committed offset (expected at startup)",
					"partition", partition, "job", spec.RangesString(), "offsets", ps.offsetsSummary(true, false))
				ps.pendingJobs.Remove(e)
				continue
			}

			jobID := jobIDForSpec(compartmentsEnabled, spec)
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
}

func (s *BlockBuilderScheduler) populateInitialJobs(ctx context.Context, offsetsByPartition map[int32][]*partitionOffsets, scanner *offsetScanner) {
	// (Note that the lock is already held because we're in startup mode.)

	// While during normal operation we are periodically asking about every
	// partition's end offset, during startup we need to compute a set of
	// ~correctly-sized jobs that may exist between the partition's commit and
	// end offsets.
	// We do that by performing a one-time probe of <offset, time> pairs between
	// those two offsets and seeding the schedule by calling updateEndOffset and
	// updateTime for each of them- just like we do during normal operation.

	// Each partition's clusters are probed and replayed together (clusterOffsets is indexed by
	// cluster ID; a nil entry means the partition has no offsets on that cluster) so jobs are cut
	// at boundaries aligned across all of the partition's clusters.
	for partition, clusterOffsets := range offsetsByPartition {
		perCluster, err := scanner.probeInitialPartitionOffsets(ctx, clusterOffsets, s.logger)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to probe initial offsets", "partition", partition, "err", err)
			continue
		}
		ps := s.getPartitionState(s.cfg.Kafka.Topic, partition)
		ps.replayOffsetsAtStartup(perCluster, s.cfg.JobSize, s.logger)
	}
}

func (s *BlockBuilderScheduler) getPartitionState(topic string, partition int32) *partitionState {
	if ps, ok := s.partitionStates[partition]; ok {
		return ps
	}

	ps := newPartitionState(topic, partition, numClusters, compartmentsEnabled, &s.metrics, s.logger)
	s.partitionStates[partition] = ps
	return ps
}

type partitionOffsets struct {
	partition          int32
	start, resume, end int64
}

// initConsumptionOffsets probes the consumption offsets and groups them by partition. The
// returned slice for each partition is indexed by cluster ID, currently a single cluster.
func (s *BlockBuilderScheduler) initConsumptionOffsets(ctx context.Context, fallbackTime time.Time) (map[int32][]*partitionOffsets, error) {
	consumeOffs, err := s.initSingleClusterConsumptionOffsets(ctx, s.cfg.Kafka.Topic, fallbackTime)
	if err != nil {
		return nil, err
	}
	offsetsByPartition := make(map[int32][]*partitionOffsets, len(consumeOffs))
	for i := range consumeOffs {
		po := &consumeOffs[i]
		offsetsByPartition[po.partition] = []*partitionOffsets{po}
	}
	return offsetsByPartition, nil
}

// initSingleClusterConsumptionOffsets returns the resumption and end offsets for each partition,
// falling back to the fallbackTime if there is no planned offset for a partition.
func (s *BlockBuilderScheduler) initSingleClusterConsumptionOffsets(ctx context.Context, topic string, fallbackTime time.Time) ([]partitionOffsets, error) {
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

			if !ps.plannedEmpty(0) {
				planned := ps.plannedOffset(0)
				s.metrics.perClusterMetrics[0].plannedOffset.WithLabelValues(partStr).Set(float64(planned))
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

			level.Debug(s.logger).Log("msg", "initSingleClusterConsumptionOffsets", "topic", t, "partition", partition,
				"start", startOffset.At, "end", end.Offset, "consumeOffset", resumeOffset)

			s.metrics.perClusterMetrics[0].startOffset.WithLabelValues(partStr).Set(float64(startOffset.At))
			s.metrics.perClusterMetrics[0].endOffset.WithLabelValues(partStr).Set(float64(end.Offset))

			offs = append(offs, partitionOffsets{
				partition: partition,
				start:     startOffset.At,
				resume:    resumeOffset,
				end:       end.Offset,
			})
		}
	}

	return offs, nil
}

// fetchCommittedOffsets fetches the topic's committed offsets for the given consumer group on
// the given cluster's Kafka. It returns empty offsets if the consumer group is not found.
func fetchCommittedOffsets(ctx context.Context, adminClient *kadm.Client, consumerGroup, topic string) (kadm.Offsets, error) {
	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 5 * time.Second,
		MaxRetries: 20,
	})
	var lastErr error

	for boff.Ongoing() {
		offs, err := adminClient.FetchOffsets(ctx, consumerGroup)
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
				// A write compartment's cluster hosts every read compartment's topic. The
				// per-read-compartment consumer group should keep commits separate, but filter
				// by topic too so another read compartment's commits can't seed our partition state.
				if o.Topic != topic {
					continue
				}
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

// snapshotOffsets returns a snapshot of the committed and planned offsets for all partitions.
func (s *BlockBuilderScheduler) snapshotOffsets() (kadm.Offsets, kadm.Offsets) {
	cp := make(kadm.Offsets)
	pp := make(kadm.Offsets)

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, ps := range s.partitionStates {
		if !ps.committedEmpty(0) {
			cp.AddOffset(ps.topic, ps.partition, ps.committedOffset(0), 0)
		}
		if !ps.plannedEmpty(0) {
			pp.AddOffset(ps.topic, ps.partition, ps.plannedOffset(0), 0)
		}
	}

	return cp, pp
}

// flushOffsetsToKafka flushes the committed offsets to Kafka and updates relevant metrics.
func (s *BlockBuilderScheduler) flushOffsetsToKafka(ctx context.Context) error {
	// TODO: only flush if dirty.
	committed, planned := s.snapshotOffsets()

	committed.Each(func(o kadm.Offset) {
		s.metrics.perClusterMetrics[0].committedOffset.WithLabelValues(fmt.Sprint(o.Partition)).Set(float64(o.At))
	})
	planned.Each(func(o kadm.Offset) {
		s.metrics.perClusterMetrics[0].plannedOffset.WithLabelValues(fmt.Sprint(o.Partition)).Set(float64(o.At))
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

		if ps := s.getPartitionState(spec.Topic, spec.Partition); ps.committedBeyondSpec(spec) {
			// Job is before the committed offset. Remove it.
			level.Info(s.logger).Log("msg", "removing job as it's behind the committed offset (expected at startup)",
				"job_id", k.id, "epoch", k.epoch, "partition", spec.Partition,
				"job", spec.RangesString(), "offsets", ps.offsetsSummary(true, false))
			s.jobs.removeJob(k)
			continue
		}

		return k, spec, nil
	}
}

// UpdateJob takes a job update from the client and records it, if necessary.
func (s *BlockBuilderScheduler) UpdateJob(_ context.Context, req *schedulerpb.UpdateJobRequest) (*schedulerpb.UpdateJobResponse, error) {
	if err := req.Spec.Validate(compartmentsEnabled, numClusters); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
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
		"worker", workerID, "job", j.RangesString())

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

		if ps.committedBeyondSpec(j) {
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

		// Track the highest epoch across all observations so setEpoch resumes
		// assigning epochs above every recovered job's epoch. Done before the
		// skip check below so a partition we choose not to recover still advances
		// the counter past its jobs.
		for _, obs := range observations {
			maxEpoch = max(maxEpoch, obs.key.epoch)
		}

		ordered, err := orderObservationsForImport(observations, len(ps.offsets))
		if err != nil {
			level.Error(s.logger).Log("msg", "startup: skipping partition recovery",
				"partition", partition, "observations", len(observations), "err", err)
			continue
		}

		// Find the highest contiguous coverage by processing jobs in order.
		// Stop importing jobs if we find a gap. The last continuous job defines
		// our latest planned offset which will be where we resume job planning.
		for _, obs := range ordered {
			if !contiguous {
				// We found a gap earlier. Skip and warn.
				level.Warn(s.logger).Log("msg", "startup: skipping job import due to offset gap",
					"partition", partition, "job_id", obs.key.id, "epoch", obs.key.epoch,
					"job", obs.spec.RangesString())
				continue
			}

			if ps.plannedBeyondSpec(obs.spec) {
				// This job is wholly before the latest planned offset. Skip.
				level.Warn(s.logger).Log("msg", "startup: skipping job before commit",
					"partition", partition, "job_id", obs.key.id, "epoch", obs.key.epoch,
					"job", obs.spec.RangesString())
				continue
			}

			if !ps.plannedValidNextSpec(obs.spec) {
				// Found a gap, can't continue the contiguous range
				contiguous = false
				level.Warn(s.logger).Log("msg", "startup: skipping job due to detected offset gap",
					"partition", partition, "job_id", obs.key.id, "job", obs.spec.RangesString(),
					"offsets", ps.offsetsSummary(false, true))
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
func (p limitPerPartitionJobCreationPolicy) canCreateJob(_ jobKey, spec *schedulerpb.JobSpec, existing iter.Seq[*schedulerpb.JobSpec]) error {
	remaining := p.partitionLimit - 1 // -1: we're about to add one.

	for existingSpec := range existing {
		if existingSpec.Topic != spec.Topic || existingSpec.Partition != spec.Partition {
			continue
		}
		if remaining--; remaining < 0 {
			return fmt.Errorf("partition %d already has %d in-flight jobs (limit %d)",
				spec.Partition, p.partitionLimit, p.partitionLimit)
		}
	}

	return nil
}

var _ jobCreationPolicy[schedulerpb.JobSpec] = (*limitPerPartitionJobCreationPolicy)(nil)
