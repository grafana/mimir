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

func newPartitionState(topic string, partition int32, metrics *schedulerMetrics, logger log.Logger) *partitionState {
	return &partitionState{
		topic:          topic,
		partition:      partition,
		pendingJobs:    list.New(),
		plannedJobs:    list.New(),
		plannedJobsMap: make(map[string]*jobState),
		planned: &advancingOffset{
			name:    offsetNamePlanned,
			off:     offsetEmpty,
			metrics: metrics,
			logger:  logger,
		},
		committed: &advancingOffset{
			name:    offsetNameCommitted,
			off:     offsetEmpty,
			metrics: metrics,
			logger:  logger,
		},
	}
}

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

func (s *partitionState) committedOffset() int64 {
	return s.committed.offset()
}

func (s *partitionState) committedEmpty() bool {
	return s.committed.empty()
}

func (s *partitionState) committedBeyondSpec(spec schedulerpb.JobSpec) bool {
	return s.committed.beyondSpec(spec)
}

func (s *partitionState) plannedOffset() int64 {
	return s.planned.offset()
}

func (s *partitionState) plannedEmpty() bool {
	return s.planned.empty()
}

func (s *partitionState) plannedBeyondSpec(spec schedulerpb.JobSpec) bool {
	return s.planned.beyondSpec(spec)
}

func (s *partitionState) plannedValidNextSpec(spec schedulerpb.JobSpec) bool {
	return s.planned.validNextSpec(spec)
}

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
