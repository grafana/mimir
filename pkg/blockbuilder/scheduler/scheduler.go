// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strconv"
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

	adminClients []*kadm.Client // one per write WC; index is the WC ID.
	jobs         *jobQueue[schedulerpb.JobSpec]
	cfg          Config
	logger       log.Logger
	register     prometheus.Registerer
	metrics      schedulerMetrics

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

func (s *BlockBuilderScheduler) starting(_ context.Context) error {
	if !s.cfg.Compartments.Enabled {
		kc, err := ingest.NewKafkaReaderClient(
			s.cfg.Kafka,
			ingest.NewKafkaReaderClientMetrics(ingest.ReaderMetricsPrefix, "block-builder-scheduler", s.register),
			s.logger,
		)
		if err != nil {
			return fmt.Errorf("creating kafka reader: %w", err)
		}
		s.adminClients = []*kadm.Client{kadm.NewClient(kc)}
		return nil
	}

	out := make([]ingest.KafkaConfig, s.cfg.Compartments.Write.NumCompartments)
	for wc := range out {
		k := s.cfg.Kafka.WriteCompartmentConfig(wc)
		k.Topic = s.cfg.Kafka.Topic
		out[wc] = k
	}

	configs := s.wcKafkaConfigs()
	clients := make([]*kadm.Client, 0, len(configs))
	for wc, kcfg := range configs {
		reg := prometheus.WrapRegistererWith(prometheus.Labels{"write_compartment": strconv.Itoa(wc)}, s.register)
		kc, err := ingest.NewKafkaReaderClient(
			kcfg,
			ingest.NewKafkaReaderClientMetrics(ingest.ReaderMetricsPrefix, "block-builder-scheduler", reg),
			s.logger,
		)
		if err != nil {
			return fmt.Errorf("creating kafka reader for clusterID %d: %w", wc, err)
		}
		clients = append(clients, kadm.NewClient(kc))
	}
	s.adminClients = clients
	return nil
}

func (s *BlockBuilderScheduler) stopping(_ error) error {
	if err := s.flushOffsetsToKafka(context.Background()); err != nil {
		level.Error(s.logger).Log("msg", "failed to flush offsets at shutdown", "err", err)
	}

	for _, ac := range s.adminClients {
		ac.Close()
	}
	return nil
}

func (s *BlockBuilderScheduler) running(ctx context.Context) error {
	// Throughout this function we map ctx.Done/context.Canceled to nil, as this
	// is a normal shutdown and we don't want the service framework to interpret
	// it as an error.

	level.Info(s.logger).Log("msg", "entering observation mode")

	observeComplete := time.After(s.cfg.StartupObserveTime)

	// Recover each cluster's committed offsets from its own cluster and seed
	// that cluster's offset tracker, so consumption resumes where it left off.
	for cluster := range s.adminClients {
		c, err := s.fetchCommittedOffsets(ctx, cluster)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			level.Error(s.logger).Log("msg", "failed to fetch committed offsets", "err", err)
			s.metrics.fetchOffsetsFailed.Inc()
			return fmt.Errorf("fetch committed offsets: %w", err)
		}
		level.Info(s.logger).Log("msg", "loaded initial committed offsets", "offsets", offsetsStr(c))
		s.mu.Lock()
		c.Each(func(o kadm.Offset) {
			ps := s.getPartitionState(o.Topic, o.Partition)
			ps.initCommit(cluster, o.At)
		})
		s.mu.Unlock()
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

	// Probe every write WC for the read topic and group the resulting offsets by
	// partition, so populateInitialJobs can bundle all of a partition's WCs into
	// one job.
	offsByPartition := make(map[int32][]partitionOffsets)
	fallbackTime := time.Now().Add(-s.cfg.LookbackOnNoCommit)
	for cluster := range s.adminClients {
		wcOffs, err := s.consumptionOffsets(ctx, s.cfg.Kafka.Topic, fallbackTime, cluster)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to get consumption offsets", "err", err)
			return
		}
		for _, off := range wcOffs {
			offsByPartition[off.partition] = append(offsByPartition[off.partition], off)
		}
	}

	finders := make([]offsetStore, len(s.adminClients))
	for wc, ac := range s.adminClients {
		finders[wc] = newOffsetFinder(ac, s.logger)
	}

	s.populateInitialJobs(ctx, offsByPartition, finders, time.Now())
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

	// Gather the end offset of each partition across every cluster in a clusterID -> endOffset map.
	// If a cluster doesn't report a partition it's skipped from the map.
	offsetsByPartition := make(map[int32]map[int]int64)
	for clusterID, ac := range s.adminClients {
		endOffsets, err := ac.ListEndOffsets(ctx, s.cfg.Kafka.Topic)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to list end offsets", "err", err)
			return
		}
		if endOffsets.Error() != nil {
			level.Warn(s.logger).Log("msg", "failed to list end offsets", "err", endOffsets.Error())
			return
		}
		endOffsets.Each(func(o kadm.ListedOffset) {
			if offsetsByPartition[o.Partition] == nil {
				offsetsByPartition[o.Partition] = make(map[int]int64)
			}
			offsetsByPartition[o.Partition][clusterID] = o.Offset
			s.metrics.partitionEndOffset.WithLabelValues(fmt.Sprint(o.Partition)).Set(float64(o.Offset))
		})
	}

	for partition, endByCluster := range offsetsByPartition {
		s.mu.Lock()
		ps := s.getPartitionState(s.cfg.Kafka.Topic, partition)
		for clusterID, end := range endByCluster {
			ps.updateEndOffset(clusterID, end)
		}
		job, err := ps.updateTime(now, s.cfg.JobSize)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to update partition time", "partition", partition, "err", err)
		} else if job != nil {
			ps.addPendingJob(job)
		}
		s.mu.Unlock()
	}

	s.metrics.outstandingJobs.Set(float64(s.jobs.count()))
	s.metrics.assignedJobs.Set(float64(s.jobs.assigned()))
}

// wcKafkaConfigs returns the per-WC Kafka configs the scheduler must monitor.
// In non-compartment mode it returns a single entry for the base topic; with
// compartments it returns one entry per write compartment, all reading the
// scheduler's read compartment topic (s.cfg.Kafka.Topic) from that write compartment's
// cluster. This mirrors how the ingester builds its per-write-compartment readers.
func (s *BlockBuilderScheduler) wcKafkaConfigs() []ingest.KafkaConfig {
	if !s.cfg.Compartments.Enabled {
		return []ingest.KafkaConfig{s.cfg.Kafka}
	}
	out := make([]ingest.KafkaConfig, s.cfg.Compartments.Write.NumCompartments)
	for wc := range out {
		k := s.cfg.Kafka.WriteCompartmentConfig(wc)
		k.Topic = s.cfg.Kafka.Topic
		out[wc] = k
	}
	return out
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
	pending := make(map[int32]int, len(s.partState))

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
	for partition, ps := range s.partState {
		pending[partition] = ps.pendingJobs.Len()

		for ps.pendingJobs.Len() > 0 {
			e := ps.pendingJobs.Front()
			spec := e.Value.(*schedulerpb.JobSpec)

			// The job discovery process happens concurrently with ongoing job
			// completions. Now that we have the lock, ignore this job if all of
			// its WC ranges are already behind their committed offsets.
			if ps.committedBeyondSpec(*spec) {
				level.Info(s.logger).Log("msg", "ignoring pending job as it's behind the committed offset (expected at startup)",
					"partition", partition, "job_id", jobIDForSpec(spec))
				ps.pendingJobs.Remove(e)
				continue
			}

			jobID := jobIDForSpec(spec)
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

func (s *BlockBuilderScheduler) populateInitialJobs(ctx context.Context, offsByPartition map[int32][]partitionOffsets, offStores []offsetStore, endTime time.Time) {
	// (Note that the lock is already held because we're in startup mode.)

	// While during normal operation we are periodically asking about every
	// partition's end offset, during startup we need to compute a set of
	// ~correctly-sized jobs that may exist between the partition's commit and
	// end offsets.
	// We do that by performing a one-time probe of <offset, time> pairs between
	// those two offsets and seeding the schedule by calling updateEndOffset and
	// updateTime for each of them- just like we do during normal operation.
	minScanTime := endTime.Add(-s.cfg.MaxScanAge)

	for partition, offs := range offsByPartition {
		observations, err := probeInitialOffsets(ctx, offs, offStores, s.cfg.Kafka.Topic, partition, endTime, s.cfg.JobSize, minScanTime, s.logger)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to probe initial offsets", "partition", partition, "err", err)
			continue
		}

		ps := s.getPartitionState(s.cfg.Kafka.Topic, partition)

		for _, obs := range observations {
			for clusterID, end := range obs.offsets {
				ps.updateEndOffset(clusterID, end)
			}
			if job, err := ps.updateTime(obs.time, s.cfg.JobSize); err != nil {
				level.Warn(s.logger).Log("msg", "failed to update partition time", "partition", partition, "err", err)
			} else if job != nil {
				ps.addPendingJob(job)
			}
		}
	}
}

// probeObservations is a bundled view of the clusters whose end offset changed at a
// single probe time, bucketed at the earliest boundary-record time across them when
// replayed through updateEndOffset.
type probeObservations struct {
	time    time.Time     // earliest boundary-record time across the WCs.
	offsets map[int]int64 // WC ID -> end offset as of time.
}

func (s *BlockBuilderScheduler) getPartitionState(topic string, partition int32) *partitionState {
	if ps, ok := s.partState[partition]; ok {
		return ps
	}

	numClusters := 1
	if s.cfg.Compartments.Enabled {
		numClusters = s.cfg.Compartments.Write.NumCompartments
	}
	ps := newPartitionState(topic, partition, numClusters, s.cfg.Compartments.Enabled, &s.metrics, s.logger)
	s.partState[partition] = ps
	return ps
}

type offsetTime struct {
	offset int64
	time   time.Time
}

// probeInitialOffsets reconstructs, for one partition, the cross-cluster end-offset
// observations to replay through updateEndOffset, recovering the jobs between each
// WC's resume offset and the partition's current end offset.
func probeInitialOffsets(ctx context.Context, offs []partitionOffsets, offStores []offsetStore, topic string, partition int32,
	endTime time.Time, jobSize time.Duration, minScanTime time.Time, logger log.Logger) ([]probeObservations, error) {

	// Probe at a higher resolution than the job size so we don't skip buckets.
	scanStep := jobSize / 4

	// Once a cluster has scanned back to its resume offset there's no older data to find
	// for it, so we stop the walk once every cluster has.
	reached := make(map[int]bool, len(offs))
	// The last offset we recorded per cluster, so we only emit a cluster when its offset steps down.
	lastOffset := make(map[int]int64, len(offs))

	// We walk a shared grid backwards from endTime, probing every cluster at the same
	// times so their observations bucket together when replayed.
	var observations []probeObservations
	for pb := endTime; minScanTime.Before(pb); pb = pb.Add(-scanStep) {
		if len(reached) == len(offs) {
			break
		}
		offsets := make(map[int]int64, len(offs))
		var minRecord time.Time
		for _, off := range offs {
			if reached[off.clusterID] {
				continue
			}
			o, rec, err := offStores[off.clusterID].offsetAfterTime(ctx, topic, off.partition, pb)
			if err != nil {
				return nil, err
			}
			level.Debug(logger).Log("msg", "found next boundary offset", "ts", pb,
				"topic", topic, "partition", off.partition, "offset", o)

			// Never go below resume: we've already consumed up to there.
			if o <= off.resume {
				o = off.resume
				reached[off.clusterID] = true
			}

			// Record a WC only when its offset changes from the previous probe. Walking
			// backwards, the first probe that resolves to a given offset is the one closest
			// to that record's real time (e.g. if probes at T20 and T10 both return offset
			// 300, it belongs at T20), so we keep that sighting and skip later repeats.
			if last, ok := lastOffset[off.clusterID]; ok && o == last {
				continue
			}

			lastOffset[off.clusterID] = o
			offsets[off.clusterID] = o

			// Track the earliest real boundary-record time across the WCs that changed; this
			// is the time the observation is replayed at through updateEndOffset.
			//
			// rec.UnixMilli() <= 0 means offsetAfterTime found no record at/after this probe
			// time and returned the high-watermark sentinel (offset = the next offset that
			// will be produced, timestamp = -1), so there's no real boundary record to key on.
			if rec.UnixMilli() > 0 && (minRecord.IsZero() || rec.Before(minRecord)) {
				minRecord = rec
			}
		}

		if len(offsets) == 0 {
			continue
		}

		if minRecord.IsZero() {
			// Every WC that changed here is at its high watermark, so there's no boundary
			// record to anchor the observation in time. This is the still-open current
			// bucket (in practice the first probe step, at endTime); we leave it for live
			// operation to cut once it rolls over rather than seeding a job now.
			continue
		}

		observations = append(observations, probeObservations{time: minRecord, offsets: offsets})
	}

	if len(reached) < len(offs) {
		level.Warn(logger).Log("msg", "probe did not reach resume offset for every WC within max scan age",
			"partition", partition, "reached", len(reached), "write_compartments", len(offs))
	}

	// updateTime replays these in order, buckets on observation time, and errors if a
	// bucket goes backwards, so sort by the time that will be passed into updateTime.
	slices.SortStableFunc(observations, func(a, b probeObservations) int {
		return a.time.Compare(b.time)
	})

	// Offsets and record times are not necessarily co-monotonic, so sorting by time can
	// leave a cluster's offsets out of order. updateEndOffset requires monotonically increasing
	// end offsets per cluster (it panics otherwise), so drop any sighting that would move a cluster's
	// offset backwards from one already kept earlier in time.
	maxByCluster := make(map[int]int64, len(offs))
	for i := range observations {
		for clusterID, o := range observations[i].offsets {
			if last, ok := maxByCluster[clusterID]; ok && o < last {
				delete(observations[i].offsets, clusterID)
				continue
			}
			maxByCluster[clusterID] = o
		}
	}

	// A cluster with no observation is either caught up (no records between its resume offset
	// and endTime) or has a backlog entirely older than maxScanAge. Either way, seed it at
	// its end offset (the high watermark, from ListEndOffsets) so updateEndOffset resumes
	// it there rather than re-consuming from 0.
	// FIXME: this could drop the lowest offset for a cluster
	// We should instead sort by offsets and skip the timestamps which go down
	// https://github.com/grafana/mimir/pull/15855 will bring in a refactoring that will help (skip time update but still update offset)
	observed := make(map[int]bool, len(offs))
	for _, obs := range observations {
		for clusterID := range obs.offsets {
			observed[clusterID] = true
		}
	}
	var seed map[int]int64
	for _, off := range offs {
		if !observed[off.clusterID] {
			if seed == nil {
				seed = make(map[int]int64)
			}
			seed[off.clusterID] = off.end
		}
	}
	if len(seed) > 0 {
		// Replay the seeds at endTime so each seeded cluster's start offset is set there. endTime
		// is later than every real observation, so this only advances the shared job bucket to
		// "now": for an active partition it's a no-op (endTime shares the last record's bucket),
		// and for a quiet partition it just cuts the last (already-complete) backlog bucket now
		// instead of on the first updateSchedule after switching to running mode.
		// TODO: updateEndOffset now records an offset separately from updateTime advancing the
		// time bucket, so the seeded WCs' offsets could be set directly via updateEndOffset
		// instead of this synthetic observation.
		observations = append(observations, probeObservations{time: endTime, offsets: seed})
	}

	return observations, nil
}

type partitionOffsets struct {
	topic              string
	partition          int32
	clusterID          int
	start, resume, end int64
}

// consumptionOffsets returns the resumption and end offsets for each partition
// of the given read compartment topic on the given cluster, falling
// back to the fallbackTime if there is no planned offset for a partition.
func (s *BlockBuilderScheduler) consumptionOffsets(ctx context.Context, topic string, fallbackTime time.Time, clusterID int) ([]partitionOffsets, error) {
	ac := s.adminClients[clusterID]
	startOffsets, err := ac.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets: %w", err)
	}
	if startOffsets.Error() != nil {
		return nil, fmt.Errorf("list start offsets: %w", startOffsets.Error())
	}
	endOffsets, err := ac.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}
	if endOffsets.Error() != nil {
		return nil, fmt.Errorf("list end offsets: %w", endOffsets.Error())
	}
	fallbackOffsets, err := ac.ListOffsetsAfterMilli(ctx, fallbackTime.UnixMilli(), topic)
	if err != nil {
		return nil, fmt.Errorf("list fallback offsets: %w", err)
	}
	if fallbackOffsets.Error() != nil {
		return nil, fmt.Errorf("list fallback offsets: %w", fallbackOffsets.Error())
	}

	var offs []partitionOffsets
	for t, pt := range startOffsets.Offsets() {
		if t != topic {
			continue
		}
		for partition, startOffset := range pt {
			partStr := fmt.Sprint(partition)
			ps := s.getPartitionState(t, partition)

			// Where to resume from? This WC's lowest planned offset, if
			// available. Otherwise, we choose the higher of the partition's
			// fallback and start offset.

			var resumeOffset int64

			if !ps.plannedEmpty(clusterID) {
				planned := ps.plannedOffset(clusterID)
				s.metrics.partitionPlannedOffset.WithLabelValues(partStr).Set(float64(planned))
				resumeOffset = planned
			} else {
				// No planned offset for this WC of the partition. Resume from fallback offset instead.
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
				clusterID: clusterID,
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

// fetchCommittedOffsets fetches the committed offsets for the scheduler's
// consumer group from the given write WC's cluster. It returns empty offsets if
// the consumer group is not found. Offsets for any topic other than this
// scheduler's read topic are ignored: this is defensive, since the consumer
// group is per read compartment, but a group shared on a cluster must never
// bleed another compartment's offsets into our (partition-keyed) state.
func (s *BlockBuilderScheduler) fetchCommittedOffsets(ctx context.Context, wc int) (kadm.Offsets, error) {
	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 5 * time.Second,
		MaxRetries: 20,
	})
	var lastErr error

	for boff.Ongoing() {
		offs, err := s.adminClients[wc].FetchOffsets(ctx, s.cfg.ConsumerGroup)
		if err != nil {
			if !errors.Is(err, kerr.GroupIDNotFound) {
				lastErr = fmt.Errorf("fetch offsets for clusterID %d: %w", wc, err)
				boff.Wait()
				continue
			}
		}

		if err := offs.Error(); err != nil {
			lastErr = fmt.Errorf("fetch offsets for clusterID %d got error in response: %w", wc, err)
			boff.Wait()
			continue
		}

		committed := make(kadm.Offsets)
		for _, ps := range offs {
			for _, o := range ps {
				if o.Topic != s.cfg.Kafka.Topic {
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

	return nil, lastErr
}

// snapOffsets returns a snapshot of the given write WC's committed and planned
// offsets for all partitions. Each WC's offsets are committed to that WC's own
// cluster.
func (s *BlockBuilderScheduler) snapOffsets(wc int) (committed, planned kadm.Offsets) {
	committed = make(kadm.Offsets)
	planned = make(kadm.Offsets)

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, ps := range s.partState {
		if !ps.committedEmpty(wc) {
			committed.AddOffset(ps.topic, ps.partition, ps.committedOffset(wc), 0)
		}
		if !ps.plannedEmpty(wc) {
			planned.AddOffset(ps.topic, ps.partition, ps.plannedOffset(wc), 0)
		}
	}

	return committed, planned
}

// flushOffsetsToKafka flushes each cluster's committed offsets to that WC's
// updates relevant metrics.
func (s *BlockBuilderScheduler) flushOffsetsToKafka(ctx context.Context) error {
	// TODO: only flush if dirty.
	for wc := range s.adminClients {
		committed, planned := s.snapOffsets(wc)

		planned.Each(func(o kadm.Offset) {
			s.metrics.partitionPlannedOffset.WithLabelValues(fmt.Sprint(o.Partition)).Set(float64(o.At))
		})

		if len(committed) == 0 {
			continue
		}

		committed.Each(func(o kadm.Offset) {
			s.metrics.partitionCommittedOffset.WithLabelValues(fmt.Sprint(o.Partition)).Set(float64(o.At))
		})

		if err := s.adminClients[wc].CommitAllOffsets(ctx, s.cfg.ConsumerGroup, committed); err != nil {
			return fmt.Errorf("commit offsets for clusterID %d: %w", wc, err)
		}

		level.Debug(s.logger).Log("msg", "flushed offsets to Kafka", "offsets", offsetsStr(committed))
	}

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
				"job_id", k.id, "epoch", k.epoch, "partition", spec.Partition)
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
	logger := log.With(s.logger, "job_id", key.id, "epoch", key.epoch, "worker", workerID, "partition", j.Partition)

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(j.Ranges()) == 0 {
		// A spec with no WC entries carries no work; don't record an observation
		// or advance any offset off it.
		level.Warn(logger).Log("msg", "job update has no WC entries; ignoring")
		return nil
	}

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
		if len(rj.spec.Ranges()) == 0 {
			// A spec with no WC entries carries no work; nothing to recover from it.
			continue
		}
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

		// Track the highest epoch across all observations so setEpoch
		// resumes assigning epochs above every recovered job's epoch.
		// Done before the skip check below so a partition
		// we choose not to recover still advances the counter past its jobs.
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
					"partition", partition, "job_id", obs.key.id, "epoch", obs.key.epoch)
				continue
			}

			if ps.plannedBeyondSpec(obs.spec) {
				// This job is wholly before the latest planned offset. Skip.
				level.Warn(s.logger).Log("msg", "startup: skipping job before commit",
					"partition", partition, "job_id", obs.key.id, "epoch", obs.key.epoch)
				continue
			}

			if !ps.plannedValidNextSpec(obs.spec) {
				// Found a gap in at least one WC, can't continue the contiguous range.
				contiguous = false
				level.Warn(s.logger).Log("msg", "startup: skipping job due to detected offset gap",
					"partition", partition, "job_id", obs.key.id, "epoch", obs.key.epoch)
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

// orderObservationsForImport returns observations ordered so that, within each
// cluster, jobs appear in ascending start-offset order, and a job that
// spans multiple clusters appears only after every job that precedes it in any of its
// clusters. Specs are sparse (a job lists only the clusters that had new data) and
// consecutive jobs can cover disjoint cluster sets, so there's no single offset to
// sort by; this merges the per-cluster entries instead.
//
// A cycle should be impossible: a job's ranges all come from one wall-clock
// bucket, so an earlier bucket's job has a start offset <= a later one's in every
// cluster they share. That invariant lives in job creation though, so we detect a
// cycle defensively and return errObservationOffsetCycle rather than trusting it
// blindly; the caller then skips the partition. On success the returned slice is
// a permutation of observations; gap detection is left to the caller's
// contiguity scan.
func orderObservationsForImport(observations []*observation, numClusters int) (orderedObservations []*observation, err error) {
	if numClusters <= 1 {
		// Single cluster (compartments disabled, or one write compartment): every spec
		// has just cluster 0, so a plain sort by start offset is the whole ordering.
		slices.SortFunc(observations, func(a, b *observation) int {
			return cmp.Compare(a.spec.Ranges()[0].StartOffset, b.spec.Ranges()[0].StartOffset)
		})
		return observations, nil
	}

	entriesByCluster := groupByClusterID(observations, numClusters)
	// Merge cursor: nextUnemittedByCluster[clusterID] is the index of the next not-yet-emitted
	// entry in entriesByCluster[clusterID]. A job is emitted once it's at the cursor of every
	// WC it belongs to; emitting it advances each of those cursors past it.
	nextUnemittedByCluster := make([]int, numClusters)

	orderedObservations = make([]*observation, 0, len(observations))
	for len(orderedObservations) < len(observations) {
		emittedThisPass := false
		for clusterID := 0; clusterID < numClusters; clusterID++ {
			entries := entriesByCluster[clusterID]
			if nextUnemittedByCluster[clusterID] >= len(entries) {
				continue // This WC's list is fully emitted.
			}
			candidate := entries[nextUnemittedByCluster[clusterID]].obs
			if !isNextInAllCluster(candidate, entriesByCluster, nextUnemittedByCluster) {
				continue
			}
			orderedObservations = append(orderedObservations, candidate)
			// Emitting the candidate advances the next pointer of every WC it belongs
			// to; since it was next in each of those, no WC is left pointing at an
			// already-emitted job.
			for wc := range candidate.spec.Ranges() {
				nextUnemittedByCluster[wc]++
			}
			emittedThisPass = true
		}
		if !emittedThisPass {
			// No job is ready yet some remain: the per-WC precedence graph has a
			// cycle, which violates the one-bucket-per-job invariant. Rather than
			// import an arbitrary (and possibly wrong) order, signal failure so the
			// caller skips this partition.
			return nil, errObservationOffsetCycle
		}
	}
	return orderedObservations, nil
}

var errObservationOffsetCycle = errors.New("observation offsets form a cycle across clusters")

// clusterEntry is one job's appearance in a cluster's offset-ordered list,
type clusterEntry struct {
	obs         *observation
	startOffset int64
}

// groupByClusterID groups observations into one list per cluster (indexed
// by ID), each sorted ascending by that cluster's start offset.
func groupByClusterID(observations []*observation, numWCs int) [][]clusterEntry {
	entriesByWC := make([][]clusterEntry, numWCs)
	for _, obs := range observations {
		for wc, wcRange := range obs.spec.Ranges() {
			entriesByWC[wc] = append(entriesByWC[wc],
				clusterEntry{obs: obs, startOffset: wcRange.StartOffset})
		}
	}
	for wc := range entriesByWC {
		slices.SortFunc(entriesByWC[wc], func(a, b clusterEntry) int {
			return cmp.Compare(a.startOffset, b.startOffset)
		})
	}
	return entriesByWC
}

// isNextInAllCluster reports whether candidate is the next unemitted entry in every
// write compartment it belongs to, i.e. all of its per-WC predecessors have
// already been emitted.
func isNextInAllCluster(candidate *observation, entriesByCluster [][]clusterEntry, nextUnemittedByCluster []int) bool {
	for clusterID := range candidate.spec.Ranges() {
		entries := entriesByCluster[clusterID]
		next := nextUnemittedByCluster[clusterID]
		if next >= len(entries) || entries[next].obs != candidate {
			return false
		}
	}
	return true
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
