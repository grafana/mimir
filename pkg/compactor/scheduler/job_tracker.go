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
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

// JobTracker tracks pending, active, and (temporarily) complete jobs for tenants.
//
// Pending jobs are either plan jobs scheduled during Maintenance, or compaction jobs offered
// via OfferCompactionJobs. At most one plan job and one backfill phase plan job exist at a time.
// The plan job plans compaction jobs without a backfill ID, and the backfill phase plan job plans
// jobs with one. Lease moves a selected job from pending to active.
//
// Pending jobs are partitioned into per-lane queues according to the lanePolicy.
//
// Active jobs share a single list ordered by oldest. An active job returns to a pending lane if its lease expires
// (via Maintenance) or is canceled within the attempt limit.
//
// Plan jobs complete via OfferCompactionJobs. Compaction jobs complete via Remove. Compaction jobs that complete
// while the plan job that planned them is active are temporarily retained for conflict detection.
type JobTracker struct {
	persister  JobPersister
	tenant     string
	clock      clock.Clock
	logger     log.Logger
	lanePolicy lanePolicy // determines how to organize pending jobs into lanes

	maxLeases                      int // maximum lease attempts per job where 0 (infiniteLeases) means unlimited. Plan jobs ignore this.
	repeatedFailureReportThreshold int // number of failures before a repeated failure is recorded. 0 (infiniteLeases) means unlimited.
	metrics                        *trackerMetrics

	discoveredBy atomic.Uint32 // the discovery polls that last found this tenant

	mtx               sync.Mutex
	pending           map[lane]*list.List
	active            *list.List               // ordered by oldest lease first
	incompleteJobs    map[string]*list.Element // all incomplete jobs; element is in active or in exactly one lane's pending list
	plan              plannerState
	backfillPhasePlan plannerState
}

// plannerState is the bookkeeping for one plan job
type plannerState struct {
	id           string                  // ID of the plan job
	lanes        []lane                  // lanes holding only jobs this plan job plans, since the lane policies assign each planned job type its own lane
	completeTime time.Time               // time of the last completed plan job. Zero time if planning has never completed or the plan job is currently incomplete (pending or active).
	completeJobs []*TrackedCompactionJob // tracked in order to reject jobs that may be from a stale planning view.
}

func (ps *plannerState) stopTrackingCompleteJobs() {
	ps.completeJobs = nil
}

// isPlanJobLeased reports whether the plan job with the given state is leased. Callers must have exclusive access.
func (jt *JobTracker) isPlanJobLeased(ps *plannerState) bool {
	e, ok := jt.incompleteJobs[ps.id]
	return ok && e.Value.(TrackedJob).IsLeased()
}

// completeJobsWith transforms []*TrackedCompactionJob to []TrackedJob while appending any additional provided values
func (ps *plannerState) completeJobsWith(additional ...TrackedJob) []TrackedJob {
	jobs := make([]TrackedJob, 0, len(ps.completeJobs)+len(additional))
	for _, completeJob := range ps.completeJobs {
		jobs = append(jobs, completeJob)
	}
	jobs = append(jobs, additional...)
	return jobs
}

// plannerState returns the state of the plan job with the given ID, or nil if the ID is not a plan job
func (jt *JobTracker) plannerState(id string) *plannerState {
	switch id {
	case planJobId:
		return &jt.plan
	case backfillPhasePlanJobId:
		return &jt.backfillPhasePlan
	}
	return nil
}

// compactionJobToPlannerState returns the state of the plan job that planned a compaction job
func (jt *JobTracker) compactionJobToPlannerState(j *TrackedCompactionJob) *plannerState {
	if len(j.value.backfillID) > 0 {
		return &jt.backfillPhasePlan
	}
	return &jt.plan
}

// plannedLanes returns the lanes of the given planned job types that the lane policy supports
func plannedLanes(lanePolicy lanePolicy, jobTypes ...compactorschedulerpb.JobType) []lane {
	lanes := make([]lane, 0, len(jobTypes))
	for _, t := range jobTypes {
		if l, ok := lanePolicy.LaneForJobType(t); ok {
			lanes = append(lanes, l)
		}
	}
	return lanes
}

func NewJobTracker(jobPersister JobPersister, tenant string, clock clock.Clock, lanePolicy lanePolicy, maxLeases int, repeatedFailureReportThreshold int, metrics *trackerMetrics, logger log.Logger) *JobTracker {
	pending := make(map[lane]*list.List)
	for _, l := range lanePolicy.AllLanes() {
		pending[l] = list.New()
	}

	return &JobTracker{
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
		incompleteJobs:                 make(map[string]*list.Element),
		plan: plannerState{
			id:    planJobId,
			lanes: plannedLanes(lanePolicy, compactorschedulerpb.JOB_TYPE_COMPACTION),
		},
		backfillPhasePlan: plannerState{
			id:    backfillPhasePlanJobId,
			lanes: plannedLanes(lanePolicy, compactorschedulerpb.JOB_TYPE_BACKFILL_COMPACTION),
		},
	}
}

// toPendingBack adds a job to the back of its lane's queue. Callers must have exclusive access.
func (jt *JobTracker) toPendingBack(j TrackedJob) {
	l := jt.lanePolicy.LaneForJob(j)
	jt.incompleteJobs[j.ID()] = jt.pending[l].PushBack(j)
}

// toPendingFront adds a job to the front of its lane's queue, reporting whether that
// lane was empty beforehand. Callers must have exclusive access.
func (jt *JobTracker) toPendingFront(j TrackedJob) (lane, bool) {
	l := jt.lanePolicy.LaneForJob(j)
	p := jt.pending[l]
	wasEmpty := p.Len() == 0
	jt.incompleteJobs[j.ID()] = p.PushFront(j)
	return l, wasEmpty
}

func (jt *JobTracker) recoverFrom(compactionJobs []*TrackedCompactionJob, planJob *TrackedPlanJob, backfillPhasePlanJob *TrackedBackfillPhasePlanJob) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	leased := make([]TrackedJob, 0, len(compactionJobs)+2)
	pending := make([]TrackedJob, 0, len(compactionJobs)+2)

	if planJob != nil {
		if planJob.IsLeased() {
			leased = append(leased, planJob)
		} else if planJob.IsComplete() {
			jt.plan.completeTime = planJob.StatusTime()
		} else {
			pending = append(pending, planJob)
		}
	}

	if backfillPhasePlanJob != nil {
		if backfillPhasePlanJob.IsLeased() {
			leased = append(leased, backfillPhasePlanJob)
		} else if backfillPhasePlanJob.IsComplete() {
			jt.backfillPhasePlan.completeTime = backfillPhasePlanJob.StatusTime()
		} else {
			pending = append(pending, backfillPhasePlanJob)
		}
	}

	for _, job := range compactionJobs {
		if job.IsLeased() {
			leased = append(leased, job)
		} else if job.IsComplete() {
			ps := jt.compactionJobToPlannerState(job)
			ps.completeJobs = append(ps.completeJobs, job)
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
		jt.incompleteJobs[job.ID()] = jt.active.PushBack(job)
	}

	jt.metrics.queue.Recover(pending, leased)
}

// Lease tries to find a pending job in the given lane, returning a non-nil response if one was found.
// becameEmpty reports whether the lane's pending queue is now empty.
func (jt *JobTracker) Lease(l lane) (response *compactorschedulerpb.LeaseJobResponse, becameEmpty bool, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	p := jt.pending[l]
	e := p.Front()
	if e == nil {
		return nil, false, nil
	}

	j := e.Value.(TrackedJob)
	// Copy the value, don't want to leave a modification if the write fails
	jj := j.CopyBase()

	jj.MarkLeased(jt.clock.Now())
	if err := jt.persister.WriteJob(jj); err != nil {
		return nil, false, err
	}

	p.Remove(e)

	jt.incompleteJobs[jj.ID()] = jt.active.PushBack(jj)
	jt.metrics.queue.Leased(jj)

	response = jj.ToLeaseResponse(jt.tenant)

	// It's a bit gross to handle one specific type here, but it feels cleaner than passing a func to ToLeaseResponse that is ignored by every other type.
	if bp := response.Spec.BackfillPhasePlanning; bp != nil {
		// Note: this is filling in a field in the response. It is therefore not persisted and doesn't have to be.
		bp.HasOutstandingJobs = jt.hasOutstandingBackfillJobs()
	}

	return response, p.Len() == 0, nil
}

// hasOutstandingBackfillJobs reports whether any job planned by the backfill phase plan job is pending or active. Callers must have exclusive access.
func (jt *JobTracker) hasOutstandingBackfillJobs() bool {
	lanes := jt.backfillPhasePlan.lanes
	for _, l := range lanes {
		if jt.pending[l].Len() > 0 {
			return true
		}
	}
	// Active jobs are not kept in lanes, but the lane a job is assigned to still identifies the plan job that planned it
	for e := jt.active.Front(); e != nil; e = e.Next() {
		if slices.Contains(lanes, jt.lanePolicy.LaneForJob(e.Value.(TrackedJob))) {
			return true
		}
	}
	return false
}

// isPendingEmpty reports whether the provided lane has no pending jobs. Callers must have exclusive access.
func (jt *JobTracker) isPendingEmpty(l lane) bool {
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

	e, ok := jt.incompleteJobs[id]
	if !ok {
		return false, nil, nil
	}

	j := e.Value.(TrackedJob)
	if j.Epoch() != epoch {
		return false, nil, nil
	}

	cj, isCompaction := j.(*TrackedCompactionJob)
	if ps := jt.plannerState(id); ps != nil && j.IsLeased() {
		// A plan job that was leased is being abandoned. Complete is not checked here because plan jobs are completed through OfferCompactionJobs.
		if err := jt.persister.WriteAndDeleteJobs(nil, ps.completeJobsWith(j)); err != nil {
			return false, nil, fmt.Errorf("failed deleting jobs: %w", err)
		}
		ps.stopTrackingCompleteJobs()
	} else if isCompaction && complete && jt.isPlanJobLeased(jt.compactionJobToPlannerState(cj)) {
		// A compaction job completed and we should remember it for future conflict resolution since the plan job that planned it is currently leased
		jj := cj.CopyBase().(*TrackedCompactionJob)
		jj.MarkComplete(jt.clock.Now())
		if err := jt.persister.WriteAndDeleteJobs([]TrackedJob{jj}, nil); err != nil {
			return false, nil, fmt.Errorf("failed writing complete job: %w", err)
		}
		ps := jt.compactionJobToPlannerState(jj)
		ps.completeJobs = append(ps.completeJobs, jj)
	} else {
		// This job is either completed or abandoned and we don't need to track it anymore
		if err := jt.persister.DeleteJob(j); err != nil {
			return false, nil, fmt.Errorf("failed deleting job: %w", err)
		}
	}

	delete(jt.incompleteJobs, id)
	if j.IsLeased() {
		jt.active.Remove(e)
		jt.metrics.queue.Complete(j)
		return true, nil, nil
	}

	l := jt.lanePolicy.LaneForJob(j)
	p := jt.pending[l]
	p.Remove(e)
	jt.metrics.queue.DropPending(j)
	if p.Len() == 0 {
		return true, &l, nil
	}
	return true, nil, nil
}

// Maintenance performs two periodic tasks: expiring leases and scheduling plan jobs.
//
// If enforceLeaseExpiration is true, active jobs whose leases have exceeded leaseDuration are
// returned to pending (if within the attempt limit) or dropped.
//
// A new plan job or backfill phase plan job is added to pending when its current planning window
// (determined by planningInterval, and compactionWaitPeriod for plan jobs) has not yet been planned.
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
	var newPlanJobs []TrackedJob
	if plan {
		discoveredBy := discoverySources(jt.discoveredBy.Load())
		// L1 blocks are expected on even UTC hours and the planning for them is affected by the compaction wait period.
		// Instead of only accounting for that wait period on even hours, account for it all the time for a better spread.
		if discoveredBy&discoveredByBlocks != 0 && jt.shouldPlan(&jt.plan, planningInterval, compactionWaitPeriod, now) {
			newPlanJobs = append(newPlanJobs, NewTrackedPlanJob(now))
		}
		// The compaction wait period isn't needed since backfill blocks are not tied to L1 timing
		if discoveredBy&discoveredByBackfills != 0 && jt.shouldPlan(&jt.backfillPhasePlan, planningInterval, 0, now) {
			newPlanJobs = append(newPlanJobs, NewTrackedBackfillPhasePlanJob(now))
		}
	}

	if len(reviveJobs) == 0 && len(deleteJobs) == 0 && len(newPlanJobs) == 0 {
		return nil, nil
	}

	writeJobs := append(reviveJobs, newPlanJobs...)
	if err := jt.persister.WriteAndDeleteJobs(writeJobs, deleteJobs); err != nil {
		return nil, fmt.Errorf("failed persisting during job tracker maintenance: %w", err)
	}

	var becameNonEmpty []lane

	for _, j := range reviveJobs {
		jt.trackFailure(j)
		id := j.ID()
		// This only needs to be checked in the revive case due to plan jobs never respecting a maximum number of leases
		if ps := jt.plannerState(id); ps != nil {
			ps.stopTrackingCompleteJobs()
		}

		jt.active.Remove(jt.incompleteJobs[id])
		if l, wasEmpty := jt.toPendingFront(j); wasEmpty {
			becameNonEmpty = append(becameNonEmpty, l)
		}
		jt.metrics.queue.Revive(j)
	}

	for _, j := range deleteJobs {
		if j.IsLeased() {
			jt.trackFailure(j)
			jt.metrics.queue.Complete(j)
			jt.active.Remove(jt.incompleteJobs[j.ID()])
			delete(jt.incompleteJobs, j.ID())
		}
	}

	for _, j := range newPlanJobs {
		// Prefer the plan job at the front of the queue to refresh the view of pending jobs
		if l, wasEmpty := jt.toPendingFront(j); wasEmpty {
			becameNonEmpty = append(becameNonEmpty, l)
		}
		jt.metrics.queue.Pending(j)
		// Drop the previous completion time since there is now a pending job that overwrote it
		jt.plannerState(j.ID()).completeTime = time.Time{}
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

				if ps := jt.plannerState(jj.ID()); ps != nil {
					for _, completeJob := range ps.completeJobs {
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

// shouldPlan determines if a new plan job with the given state should be created for the time window determined by planningInterval.
// waitPeriod offsets the planning windows.
// This function only computes what needs to change without persisting or modifying in-memory state.
// A write lock must be held in order to call this function.
func (jt *JobTracker) shouldPlan(ps *plannerState, planningInterval, waitPeriod time.Duration, now time.Time) bool {
	if _, ok := jt.incompleteJobs[ps.id]; ok {
		// There is already a plan job
		return false
	}

	nextPlanningWindow := ps.completeTime.UTC().Add(-waitPeriod).Truncate(planningInterval).Add(planningInterval).Add(waitPeriod)

	// Plan unless this window has already been planned
	return !now.Before(nextPlanningWindow)
}

// RenewLease renews a lease to prevent it from being expired. This is intentionally not persisted. A buffer time is used across restarts instead.
func (jt *JobTracker) RenewLease(id string, epoch int64) bool {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	e, ok := jt.incompleteJobs[id]
	if !ok {
		return false
	}

	j := e.Value.(TrackedJob)
	if j.IsLeased() && j.Epoch() == epoch {
		j.RenewLease(jt.clock.Now())
		jt.active.MoveToBack(e)
		return true
	}
	return false
}

func (jt *JobTracker) CancelLease(id string, epoch int64, interrupted bool) (canceled bool, becamePending *lane, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	e, ok := jt.incompleteJobs[id]
	if !ok {
		return false, nil, nil
	}

	j := e.Value.(TrackedJob)
	if !j.IsLeased() || j.Epoch() != epoch {
		return false, nil, nil
	}

	ps := jt.plannerState(id)

	revive := interrupted || jt.canRetry(j)
	if revive {
		// Copy the value, don't want to leave a modification if the write fails
		jj := j.CopyBase()
		jj.ClearLease()
		if interrupted {
			// Don't count the previous lease against the job
			jj.DecrementLeaseCount()
		}

		if ps != nil {
			if err := jt.persister.WriteAndDeleteJobs([]TrackedJob{jj}, ps.completeJobsWith()); err != nil {
				return false, nil, err
			}
		} else {
			if err := jt.persister.WriteJob(jj); err != nil {
				return false, nil, err
			}
		}
		jt.active.Remove(e)
		l, wasEmpty := jt.toPendingFront(jj)
		jt.metrics.queue.Revive(jj)
		if wasEmpty {
			becamePending = &l
		}
	} else {
		if err := jt.persister.DeleteJob(j); err != nil {
			return false, nil, err
		}
		jt.metrics.queue.Complete(j)
		jt.active.Remove(e)
		delete(jt.incompleteJobs, id)
	}
	if !interrupted {
		jt.trackFailure(j)
	}

	if ps != nil {
		ps.stopTrackingCompleteJobs()
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
	switch j.Type() {
	case compactorschedulerpb.JOB_TYPE_PLANNING, compactorschedulerpb.JOB_TYPE_BACKFILL_PHASE_PLANNING:
		return true
	}
	return jt.maxLeases == infiniteLeases || j.NumLeases() < jt.maxLeases
}

// OfferCompactionJobs processes the results from the plan job with the given ID. Since planning offers a fresh view of pending work, all
// remaining pending work planned by that plan job is replaced. Only a subset of the offered jobs may be accepted. The plan job itself will
// be considered completed if the epoch provided was a match.
func (jt *JobTracker) OfferCompactionJobs(planID string, jobs []*TrackedCompactionJob, planJobEpoch int64) (accepted int, found bool, transitions []laneTransition, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	planJob, match := jt.checkPlanJobEpoch(planID, planJobEpoch)
	if !match {
		return 0, false, nil, nil
	}
	// Each plan job only replaces and conflicts with the jobs it planned, so planning and backfill phase planning for a tenant don't interfere
	ps := jt.plannerState(planID)

	conflictMap := make(map[string]struct{})
	if len(jobs) > 0 { // if there are no jobs being offered then there is nothing to check conflicts against
		// We don't want to add a job that compacts a block if it has already been compacted (completed compactions) or is actively being compacted (active).
		for _, j := range ps.completeJobs {
			addBlocksToConflictMap(conflictMap, j)
		}
		for e := jt.active.Front(); e != nil; e = e.Next() {
			// Skip over non-compaction jobs or compaction jobs planned by a differing plan job
			j, ok := e.Value.(*TrackedCompactionJob)
			if ok && jt.compactionJobToPlannerState(j) == ps {
				addBlocksToConflictMap(conflictMap, j)
			}
		}
	}

	acceptedJobs := make([]TrackedJob, 0, len(jobs)+1)
	for _, j := range jobs {
		if e, ok := jt.incompleteJobs[j.ID()]; ok && e.Value.(TrackedJob).IsLeased() {
			// We never replace jobs that are in progress
			continue
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

	pendingCount := 0
	for _, l := range ps.lanes {
		pendingCount += jt.pending[l].Len()
	}
	deleteJobs := make([]TrackedJob, 0, len(ps.completeJobs)+pendingCount)
	// Delete completed jobs, unless they will be overwritten anyway by an accepted job.
	for _, j := range ps.completeJobs {
		id := j.ID()
		if _, ok := preventDeleteIds[id]; !ok {
			deleteJobs = append(deleteJobs, j)
		}
	}
	// Previous pending jobs need to be deleted, unless they are already being overwritten. They are getting completely replaced by acceptedJobs.
	for _, l := range ps.lanes {
		for e := jt.pending[l].Front(); e != nil; e = e.Next() {
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
	ps.completeTime = pjj.StatusTime()

	prevNonEmpty := jt.nonEmptyLaneSet()

	// Clear out previously pending jobs and rebuild this plan job's lane queues in order.
	for _, l := range ps.lanes {
		for e := jt.pending[l].Front(); e != nil; e = e.Next() {
			j := e.Value.(TrackedJob)
			delete(jt.incompleteJobs, j.ID())
			jt.metrics.queue.DropPending(j)
		}
		jt.pending[l] = list.New()
	}
	for _, j := range acceptedJobs {
		jt.toPendingBack(j)
		jt.metrics.queue.Pending(j)
	}

	ps.stopTrackingCompleteJobs()

	// Remove the plan job
	jt.active.Remove(jt.incompleteJobs[planID])
	delete(jt.incompleteJobs, planID)
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

func (jt *JobTracker) checkPlanJobEpoch(planID string, epoch int64) (TrackedJob, bool) {
	pje, ok := jt.incompleteJobs[planID]
	if !ok {
		return nil, false
	}
	planJob := pje.Value.(TrackedJob)
	if !planJob.IsLeased() || planJob.Epoch() != epoch {
		return nil, false
	}

	return planJob, true
}

// CleanupMetrics clears metrics associated with this tenant. Must be called when a tenant is removed.
func (jt *JobTracker) CleanupMetrics() {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()
	jt.metrics.Clear()
}
