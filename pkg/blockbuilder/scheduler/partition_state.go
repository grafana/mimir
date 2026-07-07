// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"container/list"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
)

type partitionState struct {
	topic     string
	partition int32

	compartmentsEnabled bool

	jobBucket time.Time

	// offsets is indexed by cluster ID.
	offsets []singleClusterOffsets

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

func newPartitionState(topic string, partition int32, numClusters int, compartmentsEnabled bool, metrics *schedulerMetrics, logger log.Logger) *partitionState {
	if !compartmentsEnabled && numClusters != 1 {
		panic(fmt.Sprintf("compartments disabled but numClusters=%d (expected 1)", numClusters))
	}
	ps := &partitionState{
		topic:               topic,
		partition:           partition,
		compartmentsEnabled: compartmentsEnabled,
		offsets:             make([]singleClusterOffsets, numClusters),
		pendingJobs:         list.New(),
		plannedJobs:         list.New(),
		plannedJobsMap:      make(map[string]*jobState),
	}
	for clusterID := range ps.offsets {
		clusterLogger := log.With(logger, "partition", partition)
		if compartmentsEnabled {
			clusterLogger = log.With(clusterLogger, "write_compartment", clusterID)
		}
		ps.offsets[clusterID] = newSingleClusterOffsets(metrics, clusterLogger)
	}
	return ps
}

// updateEndOffset records the latest observed end offset for the partition/cluster. It
// is expected to be called with monotonically increasing end offsets.
func (s *partitionState) updateEndOffset(clusterID int, end int64) {
	s.offsets[clusterID].updateEndOffset(end)
}

// updateTime advances the partition's time bucket to the bucket containing ts
// and returns a consumption job spec if one is ready. It is expected to be
// called frequently with monotonically increasing timestamps, even in the
// absence of new data.
func (s *partitionState) updateTime(ts time.Time, jobSize time.Duration) (*schedulerpb.JobSpec, error) {
	newJobBucket := ts.Truncate(jobSize)

	if s.jobBucket.IsZero() {
		s.jobBucket = newJobBucket
		return nil, nil
	}

	switch newJobBucket.Compare(s.jobBucket) {
	case bucketBefore:
		// New bucket is before our current one. This should only happen if our
		// Kafka's end offsets aren't monotonically increasing.
		return nil, fmt.Errorf("time went backwards: %s < %s (offsets: %s)", newJobBucket, s.jobBucket, s.offsetsSummary(false, false))

	case bucketSame:
		// Observation is in the currently tracked bucket. No action needed.

	case bucketAfter:
		// We've entered a new job bucket. Emit one job bundling every cluster that has new
		// data in [startOffset, endOffset), then start the next bucket.
		ranges := make(map[int32]schedulerpb.OffsetRange, len(s.offsets))
		for clusterID := range s.offsets {
			// Skip a cluster we've never sampled: we don't know its start offset yet, and we
			// don't want to emit a job from offset 0.
			if s.offsets[clusterID].startOffset == offsetEmpty {
				continue
			}
			if s.offsets[clusterID].startOffset < s.offsets[clusterID].endOffset {
				ranges[int32(clusterID)] = schedulerpb.OffsetRange{
					StartOffset: s.offsets[clusterID].startOffset,
					EndOffset:   s.offsets[clusterID].endOffset,
				}
			}
			s.offsets[clusterID].startOffset = s.offsets[clusterID].endOffset
		}
		s.jobBucket = newJobBucket
		if len(ranges) == 0 {
			return nil, nil
		}
		if !s.compartmentsEnabled {
			r := ranges[0]
			return &schedulerpb.JobSpec{Topic: s.topic, Partition: s.partition, StartOffset: r.StartOffset, EndOffset: r.EndOffset}, nil
		}
		return &schedulerpb.JobSpec{Topic: s.topic, Partition: s.partition, OffsetRanges: ranges}, nil
	}

	return nil, nil
}

// replayOffsetsAtStartup replays each cluster's probed offsets into the partition, cutting a
// job at every jobSize boundary. perCluster[i] must hold cluster i's offsets in ascending order.
//
// Within a cluster, offsets are replayed in input order. Their timestamps decide
// which job bucket each one lands in, but timestamps are not guaranteed to be monotonic, so
// the walk only ever moves forward through boundaries: an offset whose timestamp is earlier
// than one already replayed folds into the current bucket rather than reopening an earlier
// one.
func (s *partitionState) replayOffsetsAtStartup(perCluster [][]*offsetTime, jobSize time.Duration, logger log.Logger) {
	// Special casing for the single cluster path. We don't need to worry about aligning the offsets by time between
	// clusters for better job alignment, so we can replay the offsets and their recorded time.
	if len(perCluster) == 1 {
		for _, offset := range perCluster[0] {
			s.updateEndOffset(0, offset.offset)
			if job, err := s.updateTime(offset.time, jobSize); err != nil {
				level.Warn(logger).Log("msg", "failed to update partition time", "partition", s.partition, "err", err)
			} else if job != nil {
				s.addPendingJob(job)
			}
		}
		return
	}

	// Start the boundary walk at the earliest offset's timestamp across clusters.
	var firstTime time.Time
	for _, offsets := range perCluster {
		if len(offsets) > 0 && (firstTime.IsZero() || offsets[0].time.Before(firstTime)) {
			firstTime = offsets[0].time
		}
	}
	if firstTime.IsZero() {
		return // No offsets to replay.
	}

	cursors := make([]int, len(perCluster))
	for boundary := firstTime.Truncate(jobSize); ; {
		// Advancing to the next boundary each iteration closes the bucket we just filled, but
		// updateTime only returns a job when that bucket actually had data (an empty bucket, or
		// the first call that just seeds the starting bucket, returns nil).
		if job, err := s.updateTime(boundary, jobSize); err != nil {
			level.Warn(logger).Log("msg", "failed to update partition time", "partition", s.partition, "err", err)
		} else if job != nil {
			s.addPendingJob(job)
		}

		next := boundary.Add(jobSize)
		remaining := false
		for clusterID := range perCluster {
			offsets := perCluster[clusterID]
			cursor := cursors[clusterID]
			for cursor < len(offsets) && offsets[cursor].time.Before(next) {
				s.updateEndOffset(clusterID, offsets[cursor].offset)
				cursor++
			}
			cursors[clusterID] = cursor
			// End offsets are exclusive, so this bucket ends at the first offset at or after the
			// next boundary. That offset is also real data belonging to a later bucket, so we record
			// it as the end but don't increment the cursor so we can consume it when it lands in its
			// proper bucket.
			if cursor < len(offsets) {
				s.updateEndOffset(clusterID, offsets[cursor].offset)
				remaining = true
			}
		}

		if !remaining {
			// Leave the final (current) bucket open; it'll
			// cut later once live updates roll it over.
			return
		}
		boundary = next
	}
}

// offsetsSummary returns a per-cluster offset summary for debugging.
func (s *partitionState) offsetsSummary(includeCommitted, includePlanned bool) string {
	var b strings.Builder
	for clusterID := range s.offsets {
		if clusterID > 0 {
			b.WriteByte(' ')
		}
		if s.compartmentsEnabled {
			fmt.Fprintf(&b, "%d:", clusterID)
		}
		fmt.Fprintf(&b, "[%d,%d)", s.offsets[clusterID].startOffset, s.offsets[clusterID].endOffset)
		if includeCommitted {
			fmt.Fprintf(&b, " committed=%d", s.offsets[clusterID].committed.offset())
		}
		if includePlanned {
			fmt.Fprintf(&b, " planned=%d", s.offsets[clusterID].planned.offset())
		}
	}
	return b.String()
}

func (s *partitionState) initCommit(clusterID int, commit int64) {
	s.offsets[clusterID].committed.set(commit)
	// Initially, the planned offset is the committed offset.
	s.offsets[clusterID].planned.set(commit)
}

func (s *partitionState) addPendingJob(job *schedulerpb.JobSpec) {
	s.pendingJobs.PushBack(job)
}

func (s *partitionState) addPlannedJob(id string, spec schedulerpb.JobSpec) {
	for clusterID, offsetRange := range spec.Ranges() {
		// This shouldn't happen. All callers of addPlannedJob must do so in
		// increasing offset order (for all clusters).
		if s.offsets[clusterID].planned.beyondOffsetRange(offsetRange) {
			panic(fmt.Sprintf("planned job %q for partition %d is behind the current planned offset: job=%v current=%s", id, s.partition, spec.Ranges(), s.offsetsSummary(false, true)))
		}
	}

	js := &jobState{jobID: id, spec: spec, complete: false}
	s.plannedJobs.PushBack(js)
	s.plannedJobsMap[js.jobID] = js
	for clusterID, offsetRange := range spec.Ranges() {
		s.offsets[clusterID].planned.advance(id, offsetRange)
	}
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
		// Advance every cluster of the job together, never partially, while the caller holds the
		// scheduler lock. This keeps the committed offset from landing partway through a job's
		// ranges (a beyondSome state), which assignJob/updateJob/enqueuePendingJobs reject as a
		// fatal invariant break.
		for clusterID, offsetRange := range js.spec.Ranges() {
			s.offsets[clusterID].committed.advance(js.jobID, offsetRange)
		}
	}
	return nil
}

func (s *partitionState) committedOffset(clusterID int) int64 {
	return s.offsets[clusterID].committed.offset()
}

func (s *partitionState) committedEmpty(clusterID int) bool {
	return s.offsets[clusterID].committed.empty()
}

// specBeyond classifies a job spec's per-cluster ranges against one offset kind
// (committed or planned).
type specBeyond int

const (
	// beyondNone means no cluster's offset is at or past its range end.
	beyondNone specBeyond = iota
	// beyondSome means some clusters' offsets are at or past their range ends, others' are not.
	// Only expressible with more than one cluster in the spec.
	beyondSome
	// beyondAll means every cluster's offset is at or past its range end.
	beyondAll
)

// classifyBeyondSpec classifies spec's cluster ranges against the offsets selected by
// offsetFor. A cluster whose offset is still empty is never beyond its range.
func classifyBeyondSpec(spec schedulerpb.JobSpec, offsetFor func(clusterID int32) *advancingOffset) specBeyond {
	beyond, needed := false, false
	for clusterID, offsetRange := range spec.Ranges() {
		if offsetFor(clusterID).beyondOffsetRange(offsetRange) {
			beyond = true
		} else {
			needed = true
		}
	}
	switch {
	case beyond && needed:
		return beyondSome
	case beyond:
		return beyondAll
	default:
		return beyondNone
	}
}

// committedBeyondSpec classifies spec's cluster ranges against each cluster's committed
// offset (beyondAll means the whole job is already consumed).
func (s *partitionState) committedBeyondSpec(spec schedulerpb.JobSpec) specBeyond {
	return classifyBeyondSpec(spec, func(clusterID int32) *advancingOffset {
		return s.offsets[clusterID].committed
	})
}

func (s *partitionState) plannedOffset(clusterID int) int64 {
	return s.offsets[clusterID].planned.offset()
}

func (s *partitionState) plannedEmpty(clusterID int) bool {
	return s.offsets[clusterID].planned.empty()
}

// plannedBeyondSpec classifies spec's cluster ranges against each cluster's planned
// offset (beyondAll means the whole job has already been planned).
func (s *partitionState) plannedBeyondSpec(spec schedulerpb.JobSpec) specBeyond {
	return classifyBeyondSpec(spec, func(clusterID int32) *advancingOffset {
		return s.offsets[clusterID].planned
	})
}

// plannedValidNextSpec reports whether spec is the contiguous next job for
// every cluster, i.e. each cluster entry's start offset equals that cluster's
// planned offset (or that cluster's planned offset is still empty). A gap in any
// cluster makes it false.
func (s *partitionState) plannedValidNextSpec(spec schedulerpb.JobSpec) bool {
	for clusterID, offsetRange := range spec.Ranges() {
		if !s.offsets[clusterID].planned.validNextOffsetRange(offsetRange) {
			return false
		}
	}
	return true
}

// singleClusterOffsets tracks the offsets for a single Kafka cluster.
type singleClusterOffsets struct {
	// startOffset is the start offset of the next job to emit.
	// It is offsetEmpty until the first end offset is observed.
	startOffset int64
	// endOffset is the most recently observed end offset.
	endOffset int64

	committed *advancingOffset
	planned   *advancingOffset
}

func newSingleClusterOffsets(metrics *schedulerMetrics, logger log.Logger) singleClusterOffsets {
	return singleClusterOffsets{
		startOffset: offsetEmpty,
		endOffset:   offsetEmpty,
		committed:   newAdvancingOffset(offsetNameCommitted, metrics, logger),
		planned:     newAdvancingOffset(offsetNamePlanned, metrics, logger),
	}
}

// updateEndOffset records the latest observed end offset for the cluster. The
// first observed offset also seeds the start offset of the first job. It is
// expected to be called with monotonically increasing end offsets.
func (o *singleClusterOffsets) updateEndOffset(end int64) {
	if o.endOffset != offsetEmpty && end < o.endOffset {
		panic(fmt.Sprintf("end offset went backwards: %d < %d", end, o.endOffset))
	}
	if o.startOffset == offsetEmpty {
		o.startOffset = end
	}
	o.endOffset = end
}

// advancingOffset keeps track of an offset that is expected to advance
// monotonically based on job progression.
type advancingOffset struct {
	off     int64
	name    string
	metrics *schedulerMetrics
	logger  log.Logger
}

func newAdvancingOffset(name string, metrics *schedulerMetrics, logger log.Logger) *advancingOffset {
	return &advancingOffset{
		name:    name,
		off:     offsetEmpty,
		metrics: metrics,
		logger:  logger,
	}
}

const offsetEmpty int64 = -1

const (
	offsetNamePlanned   = "planned"
	offsetNameCommitted = "committed"
)

// advance moves the offset forward by a single cluster's range. Advancements are
// expected to be monotonically increasing and contiguous. Advance will not
// allow backwards movement. If a gap is detected, a warning is logged and a
// metric is incremented.
func (o *advancingOffset) advance(jobID string, offsetRange schedulerpb.OffsetRange) {
	if o.beyondOffsetRange(offsetRange) {
		// Frequent, and expected.
		level.Debug(o.logger).Log("msg", "ignoring historical job", "offset_name", o.name, "job_id", jobID,
			"start_offset", offsetRange.StartOffset, "end_offset", offsetRange.EndOffset, "current_offset", o.off)
		return
	}

	if !o.validNextOffsetRange(offsetRange) {
		// Gap detected.
		level.Warn(o.logger).Log("msg", "gap detected in offset advancement", "offset_name", o.name, "job_id", jobID,
			"start_offset", offsetRange.StartOffset, "end_offset", offsetRange.EndOffset, "current_offset", o.off)
		o.metrics.jobGapDetected.WithLabelValues(o.name).Inc()
	}

	o.off = offsetRange.EndOffset
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

// validNextOffsetRange returns true if a single cluster's range is valid to be
// added to the offset. It is valid if the start offset is the same as the
// current offset. We also allow transitioning out of an empty offset without
// calling it a gap.
func (o *advancingOffset) validNextOffsetRange(offsetRange schedulerpb.OffsetRange) bool {
	return o.off == offsetRange.StartOffset || o.empty()
}

// beyondOffsetRange returns true if the offset is beyond a single cluster's range.
func (o *advancingOffset) beyondOffsetRange(offsetRange schedulerpb.OffsetRange) bool {
	return !o.empty() && offsetRange.EndOffset <= o.off
}
