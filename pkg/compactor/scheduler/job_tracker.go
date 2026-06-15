// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"cmp"
	"container/list"
	"fmt"
	"slices"
	"sync"
	"time"
	"unsafe"

	"github.com/benbjohnson/clock"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

// JobTracker tracks pending, active, and (temporarily) complete jobs for tenants.
//
// Pending jobs are either plan jobs scheduled during Maintenance, or compaction jobs offered
// via OfferCompactionJobs. At most one plan job exists at a time. Lease moves a selected
// job from pending to active.
//
// Pending jobs are partitioned into per-lane queues according to the lanePolicy.
//
// Active jobs share a single list ordered by oldest. An active job returns to a pending lane if its lease expires
// (via Maintenance) or is canceled within the attempt limit.
//
// Plan jobs complete via OfferCompactionJobs. Compaction jobs complete via Remove. Compaction jobs that complete
// while a plan job is active are temporarily retained for conflict detection.
type JobTracker struct {
	persister  JobPersister
	tenant     string
	clock      clock.Clock
	logger     log.Logger
	lanePolicy lanePolicy // determines how to organize pending jobs into lanes

	maxLeases                      int // maximum lease attempts per job where 0 (infiniteLeases) means unlimited. Plan jobs ignore this.
	repeatedFailureReportThreshold int // number of failures before a repeated failure is recorded. 0 (infiniteLeases) means unlimited.
	metrics                        *trackerMetrics

	mtx                    sync.Mutex
	pending                map[lane]*list.List
	pendingCount           int
	active                 *list.List              // ordered by oldest lease first
	isPlanJobLeased        bool                    // used to decide whether to retain completed compaction jobs
	incompleteJobs         map[string]jobLocation  // all incomplete jobs, located in one pending lane or in active
	completePlanTime       time.Time               // time of the last completed plan job. Zero time if planning has never completed or a plan job is currently incomplete (pending or active).
	completeCompactionJobs []*TrackedCompactionJob // tracked in order to reject jobs that may be from a stale planning view.
}

// jobLocation records where an incomplete job lives. When leased, element is in active; otherwise it
// is in pending[lane].
type jobLocation struct {
	element *list.Element
	lane    lane // meaningful only when not leased
	leased  bool
}

func NewJobTracker(jobPersister JobPersister, tenant string, clock clock.Clock, lanePolicy lanePolicy, maxLeases int, repeatedFailureReportThreshold int, metrics *trackerMetrics, logger log.Logger) *JobTracker {
	pending := make(map[lane]*list.List)
	for _, l := range lanePolicy.AllLanes() {
		pending[l] = list.New()
	}

	jt := &JobTracker{
		persister:                      jobPersister,
		tenant:                         tenant,
		clock:                          clock,
		logger:                         log.With(logger, "user", tenant),
		lanePolicy:                     lanePolicy,
		maxLeases:                      maxLeases,
		repeatedFailureReportThreshold: repeatedFailureReportThreshold,
		metrics:                        metrics,
		mtx:                            sync.Mutex{},
		pending:                        pending,
		active:                         list.New(),
		isPlanJobLeased:                false,
		incompleteJobs:                 make(map[string]jobLocation),
		completeCompactionJobs:         make([]*TrackedCompactionJob, 0),
	}
	return jt
}

// toPendingBack adds a job to the back of its lane's queue.
func (jt *JobTracker) toPendingBack(j TrackedJob) {
	l := jt.lanePolicy.LaneForJob(j)
	jt.incompleteJobs[j.ID()] = jobLocation{element: jt.pending[l].PushBack(j), lane: l}
	jt.pendingCount++
}

// toPendingFront adds a job to the front of its lane's queue, reporting whether that
// lane was empty beforehand.
func (jt *JobTracker) toPendingFront(j TrackedJob) (lane, bool) {
	l := jt.lanePolicy.LaneForJob(j)
	wasEmpty := jt.isLanePendingEmpty(l)
	jt.incompleteJobs[j.ID()] = jobLocation{element: jt.pending[l].PushFront(j), lane: l}
	jt.pendingCount++
	return l, wasEmpty
}

func (jt *JobTracker) recoverFrom(compactionJobs []*TrackedCompactionJob, planJob *TrackedPlanJob) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	leased := make([]TrackedJob, 0, len(compactionJobs)+1)
	pending := make([]TrackedJob, 0, len(compactionJobs)+1)

	if planJob != nil {
		if planJob.IsLeased() {
			leased = append(leased, planJob)
			jt.isPlanJobLeased = true
		} else if planJob.IsComplete() {
			jt.completePlanTime = planJob.StatusTime()
		} else {
			pending = append(pending, planJob)
		}
	}

	for _, job := range compactionJobs {
		if job.IsLeased() {
			leased = append(leased, job)
		} else if job.IsComplete() {
			jt.completeCompactionJobs = append(jt.completeCompactionJobs, job)
		} else {
			pending = append(pending, job)
		}
	}

	slices.SortFunc(pending, func(a TrackedJob, b TrackedJob) int {
		return cmp.Compare(a.Order(), b.Order())
	})
	for _, job := range pending {
		jt.toPendingBack(job)
	}

	slices.SortFunc(leased, func(a TrackedJob, b TrackedJob) int {
		return a.StatusTime().Compare(b.StatusTime())
	})
	for _, job := range leased {
		jt.incompleteJobs[job.ID()] = jobLocation{element: jt.active.PushBack(job), leased: true}
	}

	jt.metrics.queue.Recover(pending, leased)
}

// Lease tries to find a pending job in the given lane, returning a non-nil response if one was found.
// becameEmpty reports whether the lane's pending queue is now empty.
func (jt *JobTracker) Lease(l lane) (response *compactorschedulerpb.LeaseJobResponse, becameEmpty bool, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	lst := jt.pending[l]
	if lst.Front() == nil {
		return nil, false, nil
	}
	e := lst.Front()

	j := e.Value.(TrackedJob)
	// Copy the value, don't want to leave a modification if the write fails
	jj := j.CopyBase()

	jj.MarkLeased(jt.clock.Now())
	if err := jt.persister.WriteJob(jj); err != nil {
		return nil, false, err
	}

	lst.Remove(e)
	jt.pendingCount--

	id := jj.ID()
	if id == planJobId {
		jt.isPlanJobLeased = true
	}
	jt.incompleteJobs[id] = jobLocation{element: jt.active.PushBack(jj), leased: true}
	jt.metrics.queue.Leased(jj)

	return jj.ToLeaseResponse(jt.tenant), jt.isLanePendingEmpty(l), nil
}

// isLanePendingEmpty reports whether a lane has no pending jobs. Callers must have exclusive access.
func (jt *JobTracker) isLanePendingEmpty(l lane) bool {
	return jt.pending[l].Len() == 0
}

// nonEmptyLanes returns the lanes with pending jobs. Callers must have exclusive access.
func (jt *JobTracker) nonEmptyLanes() []lane {
	lanes := make([]lane, 0, len(jt.pending))
	for l, lst := range jt.pending {
		if lst.Len() > 0 {
			lanes = append(lanes, l)
		}
	}
	return lanes
}

func (jt *JobTracker) Remove(id string, epoch int64, complete bool) (removed bool, emptied *lane, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	loc, ok := jt.incompleteJobs[id]
	if !ok {
		return false, nil, nil
	}

	j := loc.element.Value.(TrackedJob)
	if j.Epoch() != epoch {
		return false, nil, nil
	}

	if jt.isPlanJobLeased && id == planJobId {
		// A plan job that was leased is being abandoned. Complete is not checked here because plan jobs are completed through OfferCompactionJobs.
		if err := jt.persister.WriteAndDeleteJobs(nil, jt.completedJobsWith(j)); err != nil {
			return false, nil, fmt.Errorf("failed deleting jobs: %w", err)
		}
		jt.stopTrackingCompleteCompactionJobs()
	} else if jt.isPlanJobLeased && complete {
		// A compaction job completed and we should remember it for future conflict resolution since a plan job is currently leased
		copied := j.CopyBase()
		jj, ok := copied.(*TrackedCompactionJob)
		if !ok {
			// This should never happen
			return false, nil, fmt.Errorf("unexpected job type encountered")
		}
		jj.MarkComplete(jt.clock.Now())
		if err := jt.persister.WriteAndDeleteJobs([]TrackedJob{jj}, nil); err != nil {
			return false, nil, fmt.Errorf("failed writing complete job: %w", err)
		}
		jt.completeCompactionJobs = append(jt.completeCompactionJobs, jj)
	} else {
		// This job is either completed or abandoned and we don't need to track it anymore
		if err := jt.persister.DeleteJob(j); err != nil {
			return false, nil, fmt.Errorf("failed deleting job: %w", err)
		}
	}

	delete(jt.incompleteJobs, id)
	if loc.leased {
		jt.active.Remove(loc.element)
		jt.metrics.queue.Complete(j)
		return true, nil, nil
	}

	jt.pending[loc.lane].Remove(loc.element)
	jt.pendingCount--
	jt.metrics.queue.DropPending(j)
	if jt.isLanePendingEmpty(loc.lane) {
		l := loc.lane
		return true, &l, nil
	}
	return true, nil, nil
}

// Maintenance performs two periodic tasks: expiring leases and scheduling plan jobs.
//
// If enforceLeaseExpiration is true, active jobs whose leases have exceeded leaseDuration are
// returned to pending (if within the attempt limit) or dropped.
//
// A new plan job is added to pending when the current planning window (determined by planningInterval
// and compactionWaitPeriod) has not yet been planned.
//
// Maintenance returns the lanes whose pending queues transitioned from empty to non-empty.
func (jt *JobTracker) Maintenance(leaseDuration time.Duration, enforceLeaseExpiration, plan bool, planningInterval, compactionWaitPeriod time.Duration) ([]lane, error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	now := jt.clock.Now()

	var reviveJobs, deleteJobs []TrackedJob
	if enforceLeaseExpiration {
		reviveJobs, deleteJobs = jt.computeLeaseExpiration(leaseDuration, now)
	}

	// Note: a plan job will never be created if there is already an active plan job (even if we're about to expire a lease).
	// Therefore lease expiration and planning are mutually exclusive.
	var planJob *TrackedPlanJob
	if plan {
		planJob = jt.computePlan(planningInterval, compactionWaitPeriod, now)
	}

	if len(reviveJobs) == 0 && len(deleteJobs) == 0 && planJob == nil {
		return nil, nil
	}

	writeJobs := reviveJobs
	if planJob != nil {
		writeJobs = append(writeJobs, planJob)
	}
	if err := jt.persister.WriteAndDeleteJobs(writeJobs, deleteJobs); err != nil {
		return nil, fmt.Errorf("failed persisting during job tracker maintenance: %w", err)
	}

	var becameNonEmpty []lane

	for _, j := range reviveJobs {
		jt.trackFailure(j)
		id := j.ID()
		// This only needs to be checked in the revive case due to plan jobs never respecting a maximum number of leases
		if id == planJobId {
			jt.stopTrackingCompleteCompactionJobs()
		}

		jt.active.Remove(jt.incompleteJobs[id].element)
		if l, wasEmpty := jt.toPendingFront(j); wasEmpty {
			becameNonEmpty = append(becameNonEmpty, l)
		}
		jt.metrics.queue.Revive(j)
	}

	for _, j := range deleteJobs {
		if j.IsLeased() {
			jt.trackFailure(j)
			jt.metrics.queue.Complete(j)
			jt.active.Remove(jt.incompleteJobs[j.ID()].element)
			delete(jt.incompleteJobs, j.ID())
		}
	}

	if planJob != nil {
		// Prefer the plan job at the front of the queue to refresh the view of pending jobs
		if l, wasEmpty := jt.toPendingFront(planJob); wasEmpty {
			becameNonEmpty = append(becameNonEmpty, l)
		}
		jt.metrics.queue.Pending(planJob)
		// Drop the previous completion time since there is now a pending job that overwrote it
		jt.completePlanTime = time.Time{}
	}

	return becameNonEmpty, nil
}

// computeLeaseExpiration iterates through all the active jobs known by the JobTracker to find ones that have expired leases.
// If a job has an expired lease and has been active under the maximum number of times, it will be returned to the front of its pending queue.
// Otherwise a job with an expired lease will be removed from the tracker.
// This function only computes what needs to change without persisting or modifying in-memory state.
// A write lock must be held in order to call this function.
func (jt *JobTracker) computeLeaseExpiration(leaseDuration time.Duration, now time.Time) (reviveJobs, deleteJobs []TrackedJob) {
	var e, next *list.Element

	for e = jt.active.Front(); e != nil; e = next {
		next = e.Next() // get the next element now since it can't be done after removal

		j := e.Value.(TrackedJob)
		if now.Sub(j.StatusTime()) > leaseDuration {
			// Can the job be returned to its pending queue?
			if jt.canRetry(j) {
				// Copy before modifying
				jj := j.CopyBase()
				jj.ClearLease()
				reviveJobs = append(reviveJobs, jj)

				if jj.ID() == planJobId {
					for _, completeJob := range jt.completeCompactionJobs {
						deleteJobs = append(deleteJobs, completeJob)
					}
				}
			} else {
				deleteJobs = append(deleteJobs, j)
			}
		} else {
			// No more expirable jobs. Active is ordered oldest lease first, so no later entries can be expired.
			break
		}
	}

	return reviveJobs, deleteJobs
}

// computePlan determines if a new plan job should be created for the time window determined by planningInterval.
// compactionWaitPeriod is the period of time compactors wait before compacting L1 blocks.
// This function only computes what needs to change without persisting or modifying in-memory state.
// A write lock must be held in order to call this function.
func (jt *JobTracker) computePlan(planningInterval, compactionWaitPeriod time.Duration, now time.Time) *TrackedPlanJob {
	if _, ok := jt.incompleteJobs[planJobId]; ok {
		// There is already a plan job
		return nil
	}

	// L1 blocks are expected on even UTC hours and the planning for them is affected by the compaction wait period.
	// Instead of only accounting for that wait period on even hours, account for it all the time for a better spread.
	nextPlanningWindow := jt.completePlanTime.UTC().Add(-compactionWaitPeriod).Truncate(planningInterval).Add(planningInterval).Add(compactionWaitPeriod)

	if now.Before(nextPlanningWindow) {
		// This window has already been planned
		return nil
	}

	return NewTrackedPlanJob(now)
}

// RenewLease renews a lease to prevent it from being expired. This is intentionally not persisted. A buffer time is used across restarts instead.
func (jt *JobTracker) RenewLease(id string, epoch int64) bool {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	loc, ok := jt.incompleteJobs[id]
	if !ok {
		return false
	}

	j := loc.element.Value.(TrackedJob)
	if j.IsLeased() && j.Epoch() == epoch {
		j.RenewLease(jt.clock.Now())
		jt.active.MoveToBack(loc.element)
		return true
	}
	return false
}

func (jt *JobTracker) CancelLease(id string, epoch int64) (canceled bool, becamePending *lane, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	loc, ok := jt.incompleteJobs[id]
	if !ok {
		return false, nil, nil
	}

	j := loc.element.Value.(TrackedJob)
	if !j.IsLeased() || j.Epoch() != epoch {
		return false, nil, nil
	}

	revive := jt.canRetry(j)
	if revive {
		// Copy the value, don't want to leave a modification if the write fails
		jj := j.CopyBase()
		jj.ClearLease()

		if jj.ID() == planJobId {
			err = jt.persister.WriteAndDeleteJobs([]TrackedJob{jj}, jt.completedJobsWith())
			if err != nil {
				return false, nil, err
			}
		} else {
			err = jt.persister.WriteJob(jj)
			if err != nil {
				return false, nil, err
			}
		}
		jt.active.Remove(loc.element)
		l, wasEmpty := jt.toPendingFront(jj)
		jt.metrics.queue.Revive(jj)
		if wasEmpty {
			becamePending = &l
		}
	} else {
		err := jt.persister.DeleteJob(j)
		if err != nil {
			return false, nil, err
		}
		jt.metrics.queue.Complete(j)
		jt.active.Remove(loc.element)
		delete(jt.incompleteJobs, id)
	}
	jt.trackFailure(j)

	if id == planJobId {
		jt.stopTrackingCompleteCompactionJobs()
	}

	return true, becamePending, nil
}

// trackFailure records a repeated failure metric if the job exceeded the failure threshold.
func (jt *JobTracker) trackFailure(j TrackedJob) {
	if jt.repeatedFailureReportThreshold != infiniteLeases && j.NumLeases() > jt.repeatedFailureReportThreshold {
		jt.metrics.repeatedJobFailures.Inc()
		level.Error(jt.logger).Log("msg", "job is repeatedly failing", "job_id", j.ID(), "num_leases", j.NumLeases())
	}
}

// canRetry returns whether a failed leased job can be retried.
func (jt *JobTracker) canRetry(j TrackedJob) bool {
	return j.ID() == planJobId || jt.maxLeases == infiniteLeases || j.NumLeases() < jt.maxLeases
}

// OfferCompactionJobs processes the results from a plan job. Since planning offers a fresh view of pending work all remaining pending work
// is replaced. Only a subset of the offered jobs may be accepted. The plan job itself will be considered completed if the epoch
// provided was a match.
func (jt *JobTracker) OfferCompactionJobs(jobs []*TrackedCompactionJob, planJobEpoch int64) (accepted int, found bool, transitions []laneTransition, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	planJob, match := jt.checkPlanJobEpoch(planJobEpoch)
	if !match {
		return 0, false, nil, nil
	}

	conflictMap := make(map[string]struct{})
	if len(jobs) > 0 { // if there are no jobs being offered then there is nothing to check conflicts against
		// We don't want to add a job that compacts a block if it has already been compacted (completed compactions) or is actively being compacted (active).
		for _, j := range jt.completeCompactionJobs {
			addBlocksToConflictMap(conflictMap, j)
		}
		for e := jt.active.Front(); e != nil; e = e.Next() {
			// Have to skip over the plan job
			j, ok := e.Value.(*TrackedCompactionJob)
			if ok {
				addBlocksToConflictMap(conflictMap, j)
			}
		}
	}

	acceptedJobs := make([]TrackedJob, 0, len(jobs)+1)
	for _, j := range jobs {
		loc, ok := jt.incompleteJobs[j.ID()]
		if ok {
			prevJ := loc.element.Value.(TrackedJob)
			if prevJ.IsLeased() {
				// We never replace jobs that are in progress
				continue
			}
		}

		if jobConflicts(conflictMap, j) {
			// This job shared a block with either a completed or leased job. We don't want to duplicate work.
			continue
		}

		acceptedJobs = append(acceptedJobs, j)
	}

	// We will be writing these jobs. They should never also be deleted during the upcoming WriteAndDeleteJobs call.
	preventDeleteIds := make(map[string]struct{}, len(acceptedJobs))
	for _, j := range acceptedJobs {
		preventDeleteIds[j.ID()] = struct{}{}
	}

	deleteJobs := make([]TrackedJob, 0, len(jt.completeCompactionJobs)+jt.pendingCount)
	// Delete completed jobs, unless they will be overwritten anyway by an accepted job.
	for _, j := range jt.completeCompactionJobs {
		id := j.ID()
		if _, ok := preventDeleteIds[id]; !ok {
			deleteJobs = append(deleteJobs, j)
		}
	}
	// Previous pending jobs need to be deleted, unless they are already being overwritten. They are getting completely replaced by acceptedJobs.
	for _, lst := range jt.pending {
		for e := lst.Front(); e != nil; e = e.Next() {
			j := e.Value.(TrackedJob)
			id := j.ID()
			if _, ok := preventDeleteIds[id]; !ok {
				deleteJobs = append(deleteJobs, j)
			}
		}
	}

	// Mark the plan job as complete to preserve information on when planning was done.
	pjj := planJob.CopyBase()
	pjj.MarkComplete(jt.clock.Now())
	writeJobs := append(acceptedJobs, pjj)

	err = jt.persister.WriteAndDeleteJobs(writeJobs, deleteJobs)
	if err != nil {
		return 0, true, nil, fmt.Errorf("failed writing offered jobs: %w", err)
	}

	// Remember the time the plan completed to help determine when to submit the next plan job
	jt.completePlanTime = pjj.StatusTime()

	prevNonEmpty := jt.nonEmptyLaneSet()

	// Clear out previously pending jobs and rebuild the per-lane queues in order.
	for _, lst := range jt.pending {
		for e := lst.Front(); e != nil; e = e.Next() {
			j := e.Value.(TrackedJob)
			delete(jt.incompleteJobs, j.ID())
			jt.metrics.queue.DropPending(j)
		}
	}
	jt.pending = make(map[lane]*list.List)
	for _, l := range jt.lanePolicy.AllLanes() {
		jt.pending[l] = list.New()
	}
	jt.pendingCount = 0
	for _, j := range acceptedJobs {
		jt.toPendingBack(j)
		jt.metrics.queue.Pending(j)
	}

	jt.stopTrackingCompleteCompactionJobs()

	// Remove the plan job
	planLoc := jt.incompleteJobs[planJobId]
	jt.active.Remove(planLoc.element)
	delete(jt.incompleteJobs, planJobId)
	jt.metrics.queue.Complete(planJob)
	accepted = len(acceptedJobs)

	transitions = laneTransitionsBetween(prevNonEmpty, jt.nonEmptyLaneSet())
	return accepted, true, transitions, nil
}

// nonEmptyLaneSet returns the set of lanes with pending jobs. Callers must have exclusive access.
func (jt *JobTracker) nonEmptyLaneSet() map[lane]struct{} {
	set := make(map[lane]struct{}, len(jt.pending))
	for l, lst := range jt.pending {
		if lst.Len() > 0 {
			set[l] = struct{}{}
		}
	}
	return set
}

// laneTransitionsBetween reports add transitions for lanes newly non-empty and remove transitions for
// lanes newly empty.
func laneTransitionsBetween(before, after map[lane]struct{}) []laneTransition {
	var transitions []laneTransition
	for l := range after {
		if _, ok := before[l]; !ok {
			transitions = append(transitions, laneTransition{lane: l, kind: rotationAddTracker})
		}
	}
	for l := range before {
		if _, ok := after[l]; !ok {
			transitions = append(transitions, laneTransition{lane: l, kind: rotationRemoveTracker})
		}
	}
	return transitions
}

func addBlocksToConflictMap(conflict map[string]struct{}, job *TrackedCompactionJob) {
	for _, blockID := range job.value.blocks {
		conflict[unsafe.String(unsafe.SliceData(blockID), len(blockID))] = struct{}{}
	}
}

func jobConflicts(conflict map[string]struct{}, job *TrackedCompactionJob) bool {
	for _, blockID := range job.value.blocks {
		if _, ok := conflict[unsafe.String(unsafe.SliceData(blockID), len(blockID))]; ok {
			// One of the blocks this job contains has already been compacted or is currently being compacted.
			return true
		}
	}
	return false
}

func (jt *JobTracker) checkPlanJobEpoch(epoch int64) (*TrackedPlanJob, bool) {
	loc, ok := jt.incompleteJobs[planJobId]
	if !ok {
		return nil, false
	}
	planJob, ok := loc.element.Value.(*TrackedPlanJob)
	if !ok {
		// This should never happen
		return nil, false

	}
	if !planJob.IsLeased() || planJob.Epoch() != epoch {
		return nil, false
	}

	return planJob, true
}

func (jt *JobTracker) stopTrackingCompleteCompactionJobs() {
	jt.isPlanJobLeased = false
	jt.completeCompactionJobs = make([]*TrackedCompactionJob, 0)
}

// CleanupMetrics clears metrics associated with this tenant. Must be called when a tenant is removed.
func (jt *JobTracker) CleanupMetrics() {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()
	jt.metrics.Clear()
}

// completedJobsWith transforms []*TrackedCompactionJob to []TrackedJob while appending any additional provided values
func (jt *JobTracker) completedJobsWith(additional ...TrackedJob) []TrackedJob {
	jobs := make([]TrackedJob, 0, len(jt.completeCompactionJobs)+len(additional))
	for _, completeJob := range jt.completeCompactionJobs {
		jobs = append(jobs, completeJob)
	}
	jobs = append(jobs, additional...)
	return jobs
}
