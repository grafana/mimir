// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"container/list"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
)

type partitionState struct {
	topic     string
	partition int32

	compartmentsEnabled bool // non-compartment mode emits a top-level range; compartment mode emits OffsetRanges.

	jobBucket time.Time // shared wall-clock bucket across WCs.

	offsets []singleClusterOffsets // indexed by WC ID; always has at least one entry.

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

func newPartitionState(topic string, partition int32, numWCs int, compartmentsEnabled bool, metrics *schedulerMetrics, logger log.Logger) *partitionState {
	if numWCs < 1 {
		panic(fmt.Sprintf("partition state for %s/%d needs at least one write WC, got %d", topic, partition, numWCs))
	}
	ps := &partitionState{
		topic:               topic,
		partition:           partition,
		compartmentsEnabled: compartmentsEnabled,
		offsets:             make([]singleClusterOffsets, numWCs),
		pendingJobs:         list.New(),
		plannedJobs:         list.New(),
		plannedJobsMap:      make(map[string]*jobState),
	}
	for wc := range ps.offsets {
		ps.offsets[wc] = newSingleClusterOffsets(metrics, logger)
	}
	return ps
}

// updateEndOffset records the latest observed end offset for the partition/wc. It
// is expected to be called with monotonically increasing end offsets
func (s *partitionState) updateEndOffset(wc int, end int64) {
	s.offsets[wc].updateEndOffset(end)
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
		return nil, fmt.Errorf("time went backwards: %s < %s", newJobBucket, s.jobBucket)

	case bucketSame:
		// Observation is in the currently tracked bucket. No action needed.
		return nil, nil

	case bucketAfter:
		// We've entered a new job bucket. Emit one job bundling every WC that has new
		// data in [startOffset, endOffset), then start the next bucket.
		wcs := make([]schedulerpb.WCOffsetRange, 0, len(s.offsets))
		for wc := range s.offsets {
			o := &s.offsets[wc]
			// Skip a WC we've never sampled: we don't know its start offset yet, and we
			// don't want to emit a job from offset 0.
			if o.startOffset == offsetEmpty {
				continue
			}
			if o.endOffset > o.startOffset {
				wcs = append(wcs, schedulerpb.WCOffsetRange{
					WcId:        int32(wc),
					StartOffset: o.startOffset,
					EndOffset:   o.endOffset,
				})
			}
			o.startOffset = o.endOffset
		}
		s.jobBucket = newJobBucket
		if len(wcs) == 0 {
			return nil, nil
		}
		if !s.compartmentsEnabled {
			return &schedulerpb.JobSpec{Topic: s.topic, Partition: s.partition, StartOffset: wcs[0].StartOffset, EndOffset: wcs[0].EndOffset}, nil
		}
		return &schedulerpb.JobSpec{Topic: s.topic, Partition: s.partition, OffsetRanges: wcs}, nil
	}

	return nil, nil
}

// initCommitWC seeds a single WC's committed (and initial planned) offset from
// the offset recovered from that WC's cluster at startup.
func (s *partitionState) initCommitWC(wc int, commit int64) {
	s.offsets[wc].committed.set(commit)
	// Initially, the planned offset is the committed offset.
	s.offsets[wc].planned.set(commit)
}

func (s *partitionState) addPendingJob(job *schedulerpb.JobSpec) {
	s.pendingJobs.PushBack(job)
}

func (s *partitionState) addPlannedJob(id string, spec schedulerpb.JobSpec) {
	if s.plannedBeyondSpec(spec) {
		// This shouldn't happen. All callers of addPlannedJob must do so in
		// increasing offset order. The job ID encodes each WC's start offset.
		panic(fmt.Sprintf("planned job %q for partition %d is behind the current planned offset", id, s.partition))
	}

	js := &jobState{jobID: id, spec: spec, complete: false}
	s.plannedJobs.PushBack(js)
	s.plannedJobsMap[js.jobID] = js
	for _, rng := range spec.Ranges() {
		s.offsets[rng.WcId].planned.advance(id, rng)
	}
}

func (s *partitionState) committedOffset(wc int) int64 {
	return s.offsets[wc].committed.offset()
}

func (s *partitionState) committedEmpty(wc int) bool {
	return s.offsets[wc].committed.empty()
}

// committedBeyondSpec reports whether every WC range in spec is already at or
// under that WC's committed offset (i.e. the whole job is already consumed).
func (s *partitionState) committedBeyondSpec(spec schedulerpb.JobSpec) bool {
	if len(spec.Ranges()) == 0 {
		return false
	}
	for _, rng := range spec.Ranges() {
		if !s.offsets[rng.WcId].committed.beyondSpec(rng) {
			return false
		}
	}
	return true
}

func (s *partitionState) plannedOffset(wc int) int64 {
	return s.offsets[wc].planned.offset()
}

func (s *partitionState) plannedEmpty(wc int) bool {
	return s.offsets[wc].planned.empty()
}

func (s *partitionState) plannedBeyondSpec(spec schedulerpb.JobSpec) bool {
	if len(spec.Ranges()) == 0 {
		return false
	}
	for _, rng := range spec.Ranges() {
		if !s.offsets[rng.WcId].planned.beyondSpec(rng) {
			return false
		}
	}
	return true
}

// plannedValidNextSpec reports whether spec is the contiguous next job for
// every WC, i.e. each WC entry's start offset equals that WC's planned offset
// (or that WC's planned offset is still empty). A gap in any WC makes it false.
func (s *partitionState) plannedValidNextSpec(spec schedulerpb.JobSpec) bool {
	if len(spec.Ranges()) == 0 {
		return false
	}
	for _, rng := range spec.Ranges() {
		if !s.offsets[rng.WcId].planned.validNextSpec(rng) {
			return false
		}
	}
	return true
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
		for _, rng := range js.spec.Ranges() {
			s.offsets[rng.WcId].committed.advance(js.jobID, rng)
		}
	}
	return nil
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

// advance moves the offset forward by a single WC's range. Advancements are
// expected to be monotonically increasing and contiguous. Advance will not
// allow backwards movement. If a gap is detected, a warning is logged and a
// metric is incremented.
func (o *advancingOffset) advance(jobID string, rng schedulerpb.WCOffsetRange) {
	if o.beyondSpec(rng) {
		// Frequent, and expected.
		level.Debug(o.logger).Log("msg", "ignoring historical job", "offset_name", o.name, "job_id", jobID,
			"start_offset", rng.StartOffset, "end_offset", rng.EndOffset, "committed", o.off)
		return
	}

	if !o.validNextSpec(rng) {
		// Gap detected.
		level.Warn(o.logger).Log("msg", "gap detected in offset advancement", "offset_name", o.name, "job_id", jobID,
			"start_offset", rng.StartOffset, "end_offset", rng.EndOffset, "committed", o.off)
		o.metrics.jobGapDetected.WithLabelValues(o.name).Inc()
	}

	o.off = rng.EndOffset
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

// validNextSpec returns true if a single WC's range is valid to be added to the
// offset. It is valid if the start offset is the same as the current offset. We
// also allow transitioning out of an empty offset without calling it a gap.
func (o *advancingOffset) validNextSpec(rng schedulerpb.WCOffsetRange) bool {
	return o.off == rng.StartOffset || o.empty()
}

// beyondSpec returns true if the offset is beyond a single WC's range.
func (o *advancingOffset) beyondSpec(rng schedulerpb.WCOffsetRange) bool {
	return !o.empty() && rng.EndOffset <= o.off
}
