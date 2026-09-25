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

const cleanupReleaseMargin = time.Minute

// JobTracker tracks pending, active, and (temporarily) complete jobs for tenants.
//
// Pending jobs are either plan jobs scheduled during Maintenance, or compaction jobs offered
// via OfferJobs. At most one plan job exists at a time. Lease moves a selected
// job from pending to active.
//
// Pending jobs are partitioned into per-lane queues according to the lanePolicy.
//
// Active jobs share a single list ordered by oldest. An active job returns to a pending lane if its lease expires
// (via Maintenance) or is canceled within the attempt limit.
//
// Plan jobs complete via OfferJobs or CompletePlanJob. Other jobs complete via Remove. Jobs that complete
// while a plan job is active are temporarily retained for conflict detection.
type JobTracker struct {
	persister  JobPersister
	tenant     string
	clock      clock.Clock
	logger     log.Logger
	lanePolicy lanePolicy // determines how to organize pending jobs into lanes

	maxLeases                      int  // maximum lease attempts per job where 0 (infiniteLeases) means unlimited. Plan jobs ignore this.
	repeatedFailureReportThreshold int  // number of failures before a repeated failure is recorded. 0 (infiniteLeases) means unlimited.
	backfillMode                   bool // plan jobs are leased as backfill phase planning jobs
	metrics                        *trackerMetrics

	mtx              sync.Mutex
	pending          map[lane]*list.List
	active           *list.List                 // ordered by oldest lease first
	isPlanJobLeased  bool                       // used to decide whether to retain completed jobs
	incompleteJobs   map[string]*list.Element   // all incomplete jobs; element is in active or in exactly one lane's pending list
	completePlanTime time.Time                  // time of the last completed plan job. Zero time if planning has never completed or a plan job is currently incomplete (pending or active).
	completeJobs     []TrackedJob               // tracked in order to reject jobs that may be from a stale planning view.
	parkedCleanup    *TrackedBackfillCleanupJob // a cleanup job that can not be leased until lastContactTimeout after its creation. Not in incompleteJobs.
}

func NewJobTracker(jobPersister JobPersister, tenant string, clock clock.Clock, lanePolicy lanePolicy, maxLeases int, repeatedFailureReportThreshold int, backfillMode bool, metrics *trackerMetrics, logger log.Logger) *JobTracker {
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
		backfillMode:                   backfillMode,
		metrics:                        metrics,
		mtx:                            sync.Mutex{},
		pending:                        pending,
		active:                         list.New(),
		isPlanJobLeased:                false,
		incompleteJobs:                 make(map[string]*list.Element),
		completeJobs:                   make([]TrackedJob, 0),
	}
	return jt
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

// recoveredJobs are the jobs of a tenant read back from persistence
type recoveredJobs struct {
	compaction []*TrackedCompactionJob
	block      []*TrackedBackfillBlockJob
	plan       *TrackedPlanJob            // may be nil
	cleanup    *TrackedBackfillCleanupJob // may be nil
}

func (jt *JobTracker) recoverFrom(jobs recoveredJobs) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	planJob, cleanupJob := jobs.plan, jobs.cleanup
	multiJobs := make([]TrackedJob, 0, len(jobs.compaction)+len(jobs.block))
	for _, j := range jobs.compaction {
		multiJobs = append(multiJobs, j)
	}
	for _, j := range jobs.block {
		multiJobs = append(multiJobs, j)
	}

	leased := make([]TrackedJob, 0, len(multiJobs)+2)
	pending := make([]TrackedJob, 0, len(multiJobs)+2)

	if cleanupJob != nil {
		if cleanupJob.IsLeased() {
			leased = append(leased, cleanupJob)
		} else {
			// Maintenance releases it to pending once enough time has passed since its creation
			jt.parkedCleanup = cleanupJob
		}
	}

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

	for _, job := range multiJobs {
		if job.IsLeased() {
			leased = append(leased, job)
		} else if job.IsComplete() {
			jt.completeJobs = append(jt.completeJobs, job)
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

	response = jj.ToLeaseResponse(jt.tenant)

	id := jj.ID()
	if isPlanJob(jj) {
		jt.isPlanJobLeased = true
		if jt.backfillMode {
			response.Spec.JobType = compactorschedulerpb.JOB_TYPE_BACKFILL_PHASE_PLANNING
			response.Spec.BackfillPhasePlanning = &compactorschedulerpb.BackfillPhasePlanningJob{
				HasOutstandingJobs: len(jt.incompleteJobs) > 1, // the plan job itself is included
			}
		}
	}
	jt.incompleteJobs[id] = jt.active.PushBack(jj)
	jt.metrics.queue.Leased(jj)

	return response, p.Len() == 0, nil
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

	if jt.isPlanJobLeased && isPlanJob(j) {
		// A plan job that was leased is being abandoned. Complete is not checked here because plan jobs are completed through OfferJobs or CompletePlanJob.
		if err := jt.persister.WriteAndDeleteJobs(nil, jt.completedJobsWith(j)); err != nil {
			return false, nil, fmt.Errorf("failed deleting jobs: %w", err)
		}
		jt.stopTrackingCompleteJobs()
	} else if jt.isPlanJobLeased && complete {
		// A job completed and we should remember it for future conflict resolution since a plan job is currently leased
		jj := j.CopyBase()
		jj.MarkComplete(jt.clock.Now())
		if err := jt.persister.WriteAndDeleteJobs([]TrackedJob{jj}, nil); err != nil {
			return false, nil, fmt.Errorf("failed writing complete job: %w", err)
		}
		jt.completeJobs = append(jt.completeJobs, jj)
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
// A new plan job is added to pending when the current planning window (determined by planningInterval
// and compactionWaitPeriod) has not yet been planned.
//
// If enforceLeaseExpiration is true, a parked cleanup job is added to pending once lastContactTimeout (plus a margin) has passed since its creation.
// Workers stop their jobs after that long without contact, so workers of the jobs it replaced have stopped by then.
//
// Maintenance returns the lanes whose pending queues transitioned from empty to non-empty.
func (jt *JobTracker) Maintenance(leaseDuration time.Duration, enforceLeaseExpiration, plan bool, planningInterval, compactionWaitPeriod, lastContactTimeout time.Duration) ([]lane, error) {
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

	// The parked job is already persisted as available, so releasing it only changes in-memory state
	releaseCleanup := enforceLeaseExpiration && jt.parkedCleanup != nil && !now.Before(jt.parkedCleanup.CreationTime().Add(lastContactTimeout+cleanupReleaseMargin))

	if len(reviveJobs) == 0 && len(deleteJobs) == 0 && planJob == nil && !releaseCleanup {
		return nil, nil
	}

	writeJobs := reviveJobs
	if planJob != nil {
		writeJobs = append(writeJobs, planJob)
	}
	if len(writeJobs) > 0 || len(deleteJobs) > 0 {
		if err := jt.persister.WriteAndDeleteJobs(writeJobs, deleteJobs); err != nil {
			return nil, fmt.Errorf("failed persisting during job tracker maintenance: %w", err)
		}
	}

	var becameNonEmpty []lane

	if releaseCleanup {
		if l, wasEmpty := jt.toPendingFront(jt.parkedCleanup); wasEmpty {
			becameNonEmpty = append(becameNonEmpty, l)
		}
		jt.metrics.queue.Pending(jt.parkedCleanup)
		jt.parkedCleanup = nil
	}

	for _, j := range reviveJobs {
		jt.trackFailure(j)
		id := j.ID()
		// This only needs to be checked in the revive case due to plan jobs never respecting a maximum number of leases
		if isPlanJob(j) {
			jt.stopTrackingCompleteJobs()
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

				if isPlanJob(jj) {
					for _, completeJob := range jt.completeJobs {
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
	if _, ok := jt.incompleteJobs[backfillCleanupJobId]; ok || jt.parkedCleanup != nil {
		// A cleanup job replaced all other work and planning resumes once it is done
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

	revive := interrupted || jt.canRetry(j)
	if revive {
		// Copy the value, don't want to leave a modification if the write fails
		jj := j.CopyBase()
		jj.ClearLease()
		if interrupted {
			// Don't count the previous lease against the job
			jj.DecrementLeaseCount()
		}

		if isPlanJob(jj) {
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
		jt.active.Remove(e)
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
		jt.active.Remove(e)
		delete(jt.incompleteJobs, id)
	}
	if !interrupted {
		jt.trackFailure(j)
	}

	if isPlanJob(j) {
		jt.stopTrackingCompleteJobs()
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
	return isPlanJob(j) || jt.maxLeases == infiniteLeases || j.NumLeases() < jt.maxLeases
}

// OfferJobs processes the results from a plan job. Since planning offers a fresh view of pending work all remaining pending work
// is replaced. Only a subset of the offered jobs may be accepted. The plan job itself will be considered completed if the epoch
// provided was a match.
//
// A lone backfill cleanup job also replaces active jobs, and is parked until their workers have had time to stop.
func (jt *JobTracker) OfferJobs(jobs []TrackedJob, planJobEpoch int64) (accepted int, found bool, transitions []laneTransition, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	planJob, match := jt.checkPlanJobEpoch(planJobEpoch)
	if !match {
		return 0, false, nil, nil
	}

	var cleanupJob *TrackedBackfillCleanupJob
	if len(jobs) == 1 {
		cleanupJob, _ = jobs[0].(*TrackedBackfillCleanupJob)
	}

	// Blocks conflict regardless of job type
	conflictMap := make(map[string]struct{})
	if len(jobs) > 0 { // if there are no jobs being offered then there is nothing to check conflicts against
		// We don't want to add a job that works on a block if that work was already completed or is currently active.
		for _, j := range jt.completeJobs {
			addBlocksToConflictMap(conflictMap, j)
		}
		for e := jt.active.Front(); e != nil; e = e.Next() {
			addBlocksToConflictMap(conflictMap, e.Value.(TrackedJob))
		}
	}

	acceptedJobs := make([]TrackedJob, 0, len(jobs)+1)
	for _, j := range jobs {
		e, ok := jt.incompleteJobs[j.ID()]
		if ok {
			prevJ := e.Value.(TrackedJob)
			if prevJ.IsLeased() {
				// We never replace jobs that are in progress
				continue
			}
		}

		if jobConflicts(conflictMap, j) {
			// This job shares a block with either a completed or leased job. We don't want to duplicate work.
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
	for _, lst := range jt.pending {
		pendingCount += lst.Len()
	}
	deleteJobs := make([]TrackedJob, 0, len(jt.completeJobs)+pendingCount)
	// Delete completed jobs, unless they will be overwritten anyway by an accepted job.
	for _, j := range jt.completeJobs {
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
	// Active jobs are replaced by a cleanup job too. Their workers find out on their next lease update.
	var preemptedJobs []TrackedJob
	if cleanupJob != nil {
		for e := jt.active.Front(); e != nil; e = e.Next() {
			j := e.Value.(TrackedJob)
			if !isPlanJob(j) {
				preemptedJobs = append(preemptedJobs, j)
			}
		}
		deleteJobs = append(deleteJobs, preemptedJobs...)
	}

	prevNonEmpty := jt.nonEmptyLaneSet()

	if err := jt.finishPlanJob(planJob, acceptedJobs, deleteJobs); err != nil {
		return 0, true, nil, fmt.Errorf("failed writing offered jobs: %w", err)
	}

	for _, j := range preemptedJobs {
		jt.active.Remove(jt.incompleteJobs[j.ID()])
		delete(jt.incompleteJobs, j.ID())
		jt.metrics.queue.Complete(j)
	}

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
	if cleanupJob != nil {
		// Maintenance releases it to pending once enough time has passed since its creation
		jt.parkedCleanup = cleanupJob
	} else {
		for _, j := range acceptedJobs {
			jt.toPendingBack(j)
			jt.metrics.queue.Pending(j)
		}
	}
	accepted = len(acceptedJobs)

	transitions = laneTransitionsBetween(prevNonEmpty, jt.nonEmptyLaneSet())
	return accepted, true, transitions, nil
}

// CompletePlanJob completes the plan job without changing any other jobs, for when planning found nothing to change.
// The plan job will only be completed if the epoch provided was a match.
func (jt *JobTracker) CompletePlanJob(planJobEpoch int64) (found bool, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	planJob, match := jt.checkPlanJobEpoch(planJobEpoch)
	if !match {
		return false, nil
	}

	if err := jt.finishPlanJob(planJob, nil, jt.completeJobs); err != nil {
		return true, fmt.Errorf("failed completing plan job: %w", err)
	}
	return true, nil
}

// finishPlanJob persists the plan job as complete along with the provided writes and deletes, then stops tracking the
// plan job and any complete jobs. Callers must have exclusive access.
func (jt *JobTracker) finishPlanJob(planJob *TrackedPlanJob, writeJobs, deleteJobs []TrackedJob) error {
	// Mark the plan job as complete to preserve information on when planning was done.
	pjj := planJob.CopyBase()
	pjj.MarkComplete(jt.clock.Now())
	if err := jt.persister.WriteAndDeleteJobs(append(writeJobs, pjj), deleteJobs); err != nil {
		return err
	}

	// Remember the time the plan completed to help determine when to submit the next plan job
	jt.completePlanTime = pjj.StatusTime()

	jt.stopTrackingCompleteJobs()

	// Remove the plan job
	jt.active.Remove(jt.incompleteJobs[planJobId])
	delete(jt.incompleteJobs, planJobId)
	jt.metrics.queue.Complete(planJob)
	return nil
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

func addBlocksToConflictMap(conflict map[string]struct{}, job TrackedJob) {
	for _, blockID := range job.Blocks() {
		conflict[unsafe.String(unsafe.SliceData(blockID), len(blockID))] = struct{}{}
	}
}

func jobConflicts(conflict map[string]struct{}, job TrackedJob) bool {
	for _, blockID := range job.Blocks() {
		if _, ok := conflict[unsafe.String(unsafe.SliceData(blockID), len(blockID))]; ok {
			// Work on one of the blocks this job contains has already been completed or is currently active.
			return true
		}
	}
	return false
}

func (jt *JobTracker) checkPlanJobEpoch(epoch int64) (*TrackedPlanJob, bool) {
	pje, ok := jt.incompleteJobs[planJobId]
	if !ok {
		return nil, false
	}
	planJob, ok := pje.Value.(*TrackedPlanJob)
	if !ok {
		// This should never happen
		return nil, false

	}
	if !planJob.IsLeased() || planJob.Epoch() != epoch {
		return nil, false
	}

	return planJob, true
}

func (jt *JobTracker) stopTrackingCompleteJobs() {
	jt.isPlanJobLeased = false
	jt.completeJobs = make([]TrackedJob, 0)
}

// CleanupMetrics clears metrics associated with this tenant. Must be called when a tenant is removed.
func (jt *JobTracker) CleanupMetrics() {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()
	jt.metrics.Clear()
}

// completedJobsWith copies the complete jobs while appending any additional provided values
func (jt *JobTracker) completedJobsWith(additional ...TrackedJob) []TrackedJob {
	jobs := make([]TrackedJob, 0, len(jt.completeJobs)+len(additional))
	jobs = append(jobs, jt.completeJobs...)
	jobs = append(jobs, additional...)
	return jobs
}
