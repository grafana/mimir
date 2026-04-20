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
// An active job returns to pending if its lease expires (via Maintenance) or is canceled within
// the attempt limit.
//
// Plan jobs complete via OfferCompactionJobs. Compaction jobs complete via Remove. Compaction jobs that complete
// while a plan job is active are temporarily retained for conflict detection.
type JobTracker struct {
	persister JobPersister
	tenant    string
	clock     clock.Clock
	logger    log.Logger

	maxLeases                      int // maximum lease attempts per job where 0 (infiniteLeases) means unlimited. Plan jobs ignore this.
	repeatedFailureReportThreshold int // number of failures before a repeated failure is recorded. 0 (infiniteLeases) means unlimited.
	metrics                        *trackerMetrics

	mtx                    sync.Mutex
	pending                *list.List
	active                 *list.List               // ordered by oldest lease first
	isPlanJobLeased        bool                     // used to decide whether to retain completed compaction jobs
	incompleteJobs         map[string]*list.Element // all incomplete jobs will be in this map, element is in one and only one of pending or active
	completePlanTime       time.Time                // time of the last completed plan job. Zero time if planning has never completed or a plan job is currently incomplete (pending or active).
	completeCompactionJobs []*TrackedCompactionJob  // tracked in order to reject jobs that may be from a stale planning view.
}

func NewJobTracker(jobPersister JobPersister, tenant string, clock clock.Clock, maxLeases int, repeatedFailureReportThreshold int, metrics *trackerMetrics, logger log.Logger) *JobTracker {
	jt := &JobTracker{
		persister:                      jobPersister,
		tenant:                         tenant,
		clock:                          clock,
		logger:                         log.With(logger, "user", tenant),
		maxLeases:                      maxLeases,
		repeatedFailureReportThreshold: repeatedFailureReportThreshold,
		metrics:                        metrics,
		mtx:                            sync.Mutex{},
		pending:                        list.New(),
		active:                         list.New(),
		isPlanJobLeased:                false,
		incompleteJobs:                 make(map[string]*list.Element),
		completeCompactionJobs:         make([]*TrackedCompactionJob, 0),
	}
	return jt
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
		jt.incompleteJobs[job.ID()] = jt.pending.PushBack(job)
	}

	slices.SortFunc(leased, func(a TrackedJob, b TrackedJob) int {
		return a.StatusTime().Compare(b.StatusTime())
	})
	for _, job := range leased {
		jt.incompleteJobs[job.ID()] = jt.active.PushBack(job)
	}

	jt.metrics.queue.Recover(pending, leased)
}

// Lease tries to find a pending job, returning a non-nil response if one was found.
func (jt *JobTracker) Lease() (response *compactorschedulerpb.LeaseJobResponse, becameEmpty bool, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	e := jt.pending.Front()
	if e == nil {
		// There were no jobs
		return nil, false, nil
	}

	j := e.Value.(TrackedJob)
	// Copy the value, don't want to leave a modification if the write fails
	jj := j.CopyBase()

	jj.MarkLeased(jt.clock.Now())
	if err := jt.persister.WriteJob(jj); err != nil {
		return nil, false, err
	}

	jt.pending.Remove(e)

	id := jj.ID()
	if id == planJobId {
		jt.isPlanJobLeased = true
	}
	jt.incompleteJobs[id] = jt.active.PushBack(jj)
	jt.metrics.queue.Leased(jj)

	return jj.ToLeaseResponse(jt.tenant), jt.isPendingEmpty(), nil
}

// Does not acquire any lock. It is required that any callers hold an appropriate lock.
func (jt *JobTracker) isPendingEmpty() bool {
	return jt.pending.Front() == nil
}

func (jt *JobTracker) Remove(id string, epoch int64, complete bool) (removed bool, becameEmpty bool, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	e, ok := jt.incompleteJobs[id]
	if !ok {
		return false, false, nil
	}

	j := e.Value.(TrackedJob)
	if j.Epoch() != epoch {
		return false, false, nil
	}

	if jt.isPlanJobLeased && id == planJobId {
		// A plan job that was leased is being abandoned. Complete is not checked here because plan jobs are completed through OfferCompactionJobs.
		if err := jt.persister.WriteAndDeleteJobs(nil, jt.completedJobsWith(j)); err != nil {
			return false, false, fmt.Errorf("failed deleting jobs: %w", err)
		}
		jt.stopTrackingCompleteCompactionJobs()
	} else if jt.isPlanJobLeased && complete {
		// A compaction job completed and we should remember it for future conflict resolution since a plan job is currently leased
		copied := j.CopyBase()
		jj, ok := copied.(*TrackedCompactionJob)
		if !ok {
			// This should never happen
			return false, false, fmt.Errorf("unexpected job type encountered")
		}
		jj.MarkComplete(jt.clock.Now())
		if err := jt.persister.WriteAndDeleteJobs([]TrackedJob{jj}, nil); err != nil {
			return false, false, fmt.Errorf("failed writing complete job: %w", err)
		}
		jt.completeCompactionJobs = append(jt.completeCompactionJobs, jj)
	} else {
		// This job is either completed or abandoned and we don't need to track it anymore
		if err := jt.persister.DeleteJob(j); err != nil {
			return false, false, fmt.Errorf("failed deleting job: %w", err)
		}
	}

	delete(jt.incompleteJobs, id)
	if j.IsLeased() {
		jt.active.Remove(e)
		jt.metrics.queue.Complete(j)
		return true, false, nil
	}

	jt.pending.Remove(e)
	jt.metrics.queue.DropPending(j)
	return true, jt.isPendingEmpty(), nil
}

// Maintenance performs two periodic tasks: expiring leases and scheduling plan jobs.
//
// If enforceLeaseExpiration is true, active jobs whose leases have exceeded leaseDuration are
// returned to pending (if within the attempt limit) or dropped.
//
// A new plan job is added to pending when the current planning window (determined by planningInterval
// and compactionWaitPeriod) has not yet been planned.
//
// Maintenance returns true when err is nil and the pending queue transitioned from empty to non-empty.
func (jt *JobTracker) Maintenance(leaseDuration time.Duration, enforceLeaseExpiration, plan bool, planningInterval, compactionWaitPeriod time.Duration) (bool, error) {
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
		return false, nil
	}

	writeJobs := reviveJobs
	if planJob != nil {
		writeJobs = append(writeJobs, planJob)
	}
	if err := jt.persister.WriteAndDeleteJobs(writeJobs, deleteJobs); err != nil {
		return false, fmt.Errorf("failed persisting during job tracker maintenance: %w", err)
	}

	wasEmpty := jt.isPendingEmpty()

	for _, j := range reviveJobs {
		jt.trackFailure(j)
		id := j.ID()
		// This only needs to be checked in the revive case due to plan jobs never respecting a maximum number of leases
		if id == planJobId {
			jt.stopTrackingCompleteCompactionJobs()
		}

		jt.active.Remove(jt.incompleteJobs[id])
		jt.incompleteJobs[id] = jt.pending.PushFront(j)
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
		jt.incompleteJobs[planJobId] = jt.pending.PushBack(planJob)
		jt.metrics.queue.Pending(planJob)
		// Drop the previous completion time since there is now a pending job that overwrote it
		jt.completePlanTime = time.Time{}
	}

	return wasEmpty && !jt.isPendingEmpty(), nil
}

// computeLeaseExpiration iterates through all the active jobs known by the JobTracker to find ones that have expired leases.
// If a job has an expired lease and has been active under the maximum number of times, it will be returned to the front of the queue.
// Otherwise a job with an expired lease will be removed from the tracker.
// This function only computes what needs to change without persisting or modifying in-memory state.
// A write lock must be held in order to call this function.
func (jt *JobTracker) computeLeaseExpiration(leaseDuration time.Duration, now time.Time) (reviveJobs, deleteJobs []TrackedJob) {
	var e, next *list.Element

	for e = jt.active.Front(); e != nil; e = next {
		next = e.Next() // get the next element now since it can't be done after removal

		j := e.Value.(TrackedJob)
		if now.Sub(j.StatusTime()) > leaseDuration {
			// Can the job be returned to the queue?
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
	nextPlanningWindow := jt.completePlanTime.Add(planningInterval).UTC().Truncate(planningInterval).Add(compactionWaitPeriod)

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

func (jt *JobTracker) CancelLease(id string, epoch int64) (canceled bool, becamePending bool, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	e, ok := jt.incompleteJobs[id]
	if !ok {
		return false, false, nil
	}

	j := e.Value.(TrackedJob)
	if !j.IsLeased() || j.Epoch() != epoch {
		return false, false, nil
	}

	wasEmpty := jt.isPendingEmpty()
	revive := jt.canRetry(j)
	if revive {
		// Copy the value, don't want to leave a modification if the write fails
		jj := j.CopyBase()
		jj.ClearLease()

		if jj.ID() == planJobId {
			err = jt.persister.WriteAndDeleteJobs([]TrackedJob{jj}, jt.completedJobsWith())
			if err != nil {
				return false, false, err
			}
		} else {
			err = jt.persister.WriteJob(jj)
			if err != nil {
				return false, false, err
			}
		}
		jt.active.Remove(jt.incompleteJobs[jj.ID()])
		jt.incompleteJobs[jj.ID()] = jt.pending.PushFront(jj)
		jt.metrics.queue.Revive(jj)

	} else {
		err := jt.persister.DeleteJob(j)
		if err != nil {
			return false, false, err
		}
		jt.metrics.queue.Complete(j)
		jt.active.Remove(jt.incompleteJobs[j.ID()])
		delete(jt.incompleteJobs, j.ID())
	}
	jt.trackFailure(j)

	if id == planJobId {
		jt.stopTrackingCompleteCompactionJobs()
	}

	return true, wasEmpty && revive, nil
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
func (jt *JobTracker) OfferCompactionJobs(jobs []*TrackedCompactionJob, planJobEpoch int64) (accepted int, found bool, transition rotationTransition, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	planJob, match := jt.checkPlanJobEpoch(planJobEpoch)
	if !match {
		return 0, false, rotationNoChange, nil
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
		e, ok := jt.incompleteJobs[j.ID()]
		if ok {
			prevJ := e.Value.(TrackedJob)
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

	deleteJobs := make([]TrackedJob, 0, len(jt.completeCompactionJobs)+jt.pending.Len())
	// Delete completed jobs, unless they will be overwritten anyway by an accepted job.
	for _, j := range jt.completeCompactionJobs {
		id := j.ID()
		if _, ok := preventDeleteIds[id]; !ok {
			deleteJobs = append(deleteJobs, j)
		}
	}
	// Previous pending jobs need to be deleted, unless they are already being overwritten. They are getting completely replaced by acceptedJobs.
	for e := jt.pending.Front(); e != nil; e = e.Next() {
		j := e.Value.(TrackedJob)
		id := j.ID()
		if _, ok := preventDeleteIds[id]; !ok {
			deleteJobs = append(deleteJobs, j)
		}
	}

	// Mark the plan job as complete to preserve information on when planning was done.
	pjj := planJob.CopyBase()
	pjj.MarkComplete(jt.clock.Now())
	writeJobs := append(acceptedJobs, pjj)

	err = jt.persister.WriteAndDeleteJobs(writeJobs, deleteJobs)
	if err != nil {
		return 0, true, rotationNoChange, fmt.Errorf("failed writing offered jobs: %w", err)
	}

	// Remember the time the plan completed to help determine when to submit the next plan job
	jt.completePlanTime = pjj.StatusTime()

	wasEmpty := jt.isPendingEmpty()

	// Clear out previously pending jobs from incompleteJobs
	for e := jt.pending.Front(); e != nil; e = e.Next() {
		j := e.Value.(TrackedJob)
		delete(jt.incompleteJobs, j.ID())
		jt.metrics.queue.DropPending(j)
	}

	// Recreate the pending list in order
	jt.pending = list.New()
	for _, j := range acceptedJobs {
		jt.incompleteJobs[j.ID()] = jt.pending.PushBack(j)
		jt.metrics.queue.Pending(j)
	}

	jt.stopTrackingCompleteCompactionJobs()

	// Remove the plan job
	e := jt.incompleteJobs[planJobId]
	jt.active.Remove(e)
	delete(jt.incompleteJobs, planJobId)
	jt.metrics.queue.Complete(planJob)
	accepted = len(acceptedJobs)

	// Determine rotation transition
	transition = rotationNoChange
	if accepted > 0 && wasEmpty {
		transition = rotationAddTracker
	} else if accepted == 0 && !wasEmpty {
		transition = rotationRemoveTracker
	}
	return accepted, true, transition, nil
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
