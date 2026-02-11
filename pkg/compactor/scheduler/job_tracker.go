// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"cmp"
	"container/list"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
	"unsafe"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

// JobTracker tracks pending, active, and (temporarily) complete jobs for tenants.
// TODO: Some kind of feedback mechanism so the Spawner can know if a submitted plan job failed or completed
type JobTracker struct {
	persister JobPersister
	tenant    string

	maxLeases int
	metrics   *trackerMetrics

	mtx                    sync.Mutex
	pending                *list.List
	active                 *list.List
	isPlanJobLeased        bool                     // we track blocks from completed jobs while a plan job is leased
	incompleteJobs         map[string]*list.Element // all incomplete jobs will be in this map, element is in one and only one of pending or active
	completeCompactionJobs []*TrackedCompactionJob  // tracked in order to reject jobs that may be from a stale planning view.
}

func NewJobTracker(jobPersister JobPersister, tenant string, maxLeases int, metrics *trackerMetrics) *JobTracker {
	jt := &JobTracker{
		persister:              jobPersister,
		tenant:                 tenant,
		maxLeases:              maxLeases,
		metrics:                metrics,
		mtx:                    sync.Mutex{},
		pending:                list.New(),
		active:                 list.New(),
		isPlanJobLeased:        false,
		incompleteJobs:         make(map[string]*list.Element),
		completeCompactionJobs: make([]*TrackedCompactionJob, 0),
	}
	return jt
}

func (jt *JobTracker) recoverFrom(jobs []TrackedJob) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	leased := make([]TrackedJob, 0, len(jobs))
	pending := make([]TrackedJob, 0, len(jobs))
	for _, job := range jobs {
		if job.IsLeased() {
			if job.ID() == planJobId {
				jt.isPlanJobLeased = true
			}
			leased = append(leased, job)
		} else if job.IsComplete() {
			compactionJob, ok := job.(*TrackedCompactionJob)
			if !ok {
				// Only compaction jobs are expected to be persisted as complete
				continue
			}
			jt.completeCompactionJobs = append(jt.completeCompactionJobs, compactionJob)
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

	jt.metrics.pendingJobs.Set(float64(jt.pending.Len()))
	jt.metrics.activeJobs.Set(float64(jt.active.Len()))
}

func (jt *JobTracker) PrepareForShutdown() {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	jt.incompleteJobs = make(map[string]*list.Element)
	jt.pending = list.New()
	jt.active = list.New()
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

	jj.MarkLeased()
	if err := jt.persister.WriteJob(jj); err != nil {
		return nil, false, err
	}

	jt.pending.Remove(e)

	id := jj.ID()
	if id == planJobId {
		jt.isPlanJobLeased = true
	}
	jt.incompleteJobs[id] = jt.active.PushBack(jj)

	jt.metrics.pendingJobs.Set(float64(jt.pending.Len()))
	jt.metrics.activeJobs.Set(float64(jt.active.Len()))

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
		// A plan job that was leased is being abandoned. Complete is not checked here because it is an invalid case.
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
		jj.MarkComplete()
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
		jt.metrics.activeJobs.Set(float64(jt.active.Len()))
		return true, false, nil
	}

	jt.pending.Remove(e)
	jt.metrics.pendingJobs.Set(float64(jt.pending.Len()))
	return true, jt.isPendingEmpty(), nil
}

// ExpireLeases iterates through all the active jobs known by the JobTracker to find ones that have expired leases.
// If a job has an expired lease and has been active under the maximum number of times, it is returned to the front of the queue
// Otherwise a job with an expired lease will be removed from the tracker.
func (jt *JobTracker) ExpireLeases(leaseDuration time.Duration, now time.Time) (bool, error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	wasEmpty := jt.isPendingEmpty()

	var e, next *list.Element

	var deleteJobs []TrackedJob
	var reviveJobs []TrackedJob
	for e = jt.active.Front(); e != nil; e = next {
		next = e.Next() // get the next element now since it can't be done after removal

		j := e.Value.(TrackedJob)
		if now.Sub(j.StatusTime()) > leaseDuration {
			// Can the job be returned to the queue?
			if jt.maxLeases == infiniteLeases || j.NumLeases() < jt.maxLeases {
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
			// No more expirable jobs
			break
		}
	}

	if len(deleteJobs) == 0 && len(reviveJobs) == 0 {
		return false, nil
	}

	err := jt.persister.WriteAndDeleteJobs(reviveJobs, deleteJobs)
	if err != nil {
		return false, fmt.Errorf("failed persisting expiration: %w", err)
	}

	for _, j := range reviveJobs {
		id := j.ID()
		// This only needs to be checked in the revive case due to plan jobs never respecting a maximum number of leases
		if id == planJobId {
			jt.stopTrackingCompleteCompactionJobs()
		}

		jt.active.Remove(jt.incompleteJobs[id])
		jt.incompleteJobs[id] = jt.pending.PushFront(j)
	}

	for _, j := range deleteJobs {
		if j.IsLeased() {
			jt.active.Remove(jt.incompleteJobs[j.ID()])
			delete(jt.incompleteJobs, j.ID())
		}
	}

	jt.metrics.activeJobs.Set(float64(jt.active.Len()))
	jt.metrics.pendingJobs.Set(float64(jt.pending.Len()))

	return wasEmpty && len(reviveJobs) > 0, nil
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
		j.RenewLease()
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
	revive := jt.maxLeases == infiniteLeases || j.NumLeases() < jt.maxLeases
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
		jt.metrics.pendingJobs.Set(float64(jt.pending.Len()))
	} else {
		err := jt.persister.DeleteJob(j)
		if err != nil {
			return false, false, err
		}
		jt.active.Remove(jt.incompleteJobs[j.ID()])
		delete(jt.incompleteJobs, j.ID())
	}

	jt.metrics.activeJobs.Set(float64(jt.active.Len()))

	if id == planJobId {
		jt.stopTrackingCompleteCompactionJobs()
	}

	return true, wasEmpty && revive, nil
}

func (jt *JobTracker) Offer(jobs []TrackedJob, epoch int64) (accepted int, becamePending bool, err error) {
	// Plan jobs are injected in isolation
	if len(jobs) == 1 && jobs[0].ID() == planJobId {
		return jt.offerPlanJob(jobs[0])
	}
	return jt.offerCompactionJobs(jobs, epoch)
}

func (jt *JobTracker) offerPlanJob(job TrackedJob) (accepted int, becamePending bool, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	if _, ok := jt.incompleteJobs[planJobId]; ok {
		// A plan job is already present, do not replace it
		return 0, false, nil
	}

	if err := jt.persister.WriteJob(job); err != nil {
		return 0, false, fmt.Errorf("failed persisting plan job offer: %w", err)
	}

	wasEmpty := jt.isPendingEmpty()
	jt.incompleteJobs[planJobId] = jt.pending.PushBack(job)
	jt.metrics.pendingJobs.Set(float64(jt.pending.Len()))
	return 1, wasEmpty, nil
}

func (jt *JobTracker) offerCompactionJobs(jobs []TrackedJob, epoch int64) (accepted int, becamePending bool, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	planJob, err := jt.checkPlanJobEpoch(epoch)
	if err != nil {
		return 0, false, err
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

	writeJobs := make([]TrackedJob, 0, len(jobs))
	for _, j := range jobs {
		e, ok := jt.incompleteJobs[j.ID()]
		var prevJ *TrackedCompactionJob
		if ok {
			prevJ, ok = e.Value.(*TrackedCompactionJob)
			if ok && prevJ.IsLeased() {
				// Don't add this job
				continue
			}
		}

		cj, ok := j.(*TrackedCompactionJob)
		if !ok || jobConflicts(conflictMap, cj) {
			continue
		}

		writeJobs = append(writeJobs, j)
	}

	preventDeleteIds := make(map[string]struct{}, len(writeJobs)+len(jt.completeCompactionJobs)+jt.pending.Len())
	deleteJobs := make([]TrackedJob, 0, len(jt.completeCompactionJobs)+jt.pending.Len()+1)
	for _, j := range writeJobs {
		preventDeleteIds[j.ID()] = struct{}{}
	}
	for _, j := range jt.completeCompactionJobs {
		id := j.ID()
		if _, ok := preventDeleteIds[id]; !ok {
			deleteJobs = append(deleteJobs, j)
			preventDeleteIds[id] = struct{}{}
		}
	}
	for e := jt.pending.Front(); e != nil; e = e.Next() {
		j := e.Value.(TrackedJob)
		id := j.ID()
		if _, ok := preventDeleteIds[id]; !ok {
			deleteJobs = append(deleteJobs, j)
			preventDeleteIds[id] = struct{}{} // this may not be necessary
		}
	}
	deleteJobs = append(deleteJobs, planJob) // we know the plan job was in the active list due to the epoch check

	err = jt.persister.WriteAndDeleteJobs(writeJobs, deleteJobs)
	if err != nil {
		return 0, false, fmt.Errorf("failed writing offered jobs: %w", err)
	}

	wasEmpty := jt.isPendingEmpty()
	for e := jt.pending.Front(); e != nil; e = e.Next() {
		// Note: This assumes no other job types besides compaction/planning.
		j := e.Value.(TrackedJob)
		delete(jt.incompleteJobs, j.ID())
	}
	// Recreate the pending list in order
	jt.pending = list.New()
	for _, j := range writeJobs {
		jt.incompleteJobs[j.ID()] = jt.pending.PushBack(j)
	}

	jt.stopTrackingCompleteCompactionJobs()

	// Remove the plan job
	e := jt.incompleteJobs[planJobId]
	jt.active.Remove(e)
	delete(jt.incompleteJobs, planJobId)
	jt.metrics.activeJobs.Set(float64(jt.active.Len()))

	jt.metrics.pendingJobs.Set(float64(jt.pending.Len()))
	accepted = len(writeJobs)
	return accepted, wasEmpty && accepted > 0, nil
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

func (jt *JobTracker) checkPlanJobEpoch(epoch int64) (*TrackedPlanJob, error) {
	pje, ok := jt.incompleteJobs[planJobId]
	if !ok {
		return nil, errors.New("received plan job results, but there was no known plan job")
	}
	planJob, ok := pje.Value.(*TrackedPlanJob)
	if !ok {
		// This should never happen
		return nil, errors.New("invalid plan job type")

	}
	if !planJob.IsLeased() || planJob.Epoch() != epoch {
		return nil, errors.New("given results, but plan job lease not owned")
	}

	return planJob, nil
}

func (jt *JobTracker) stopTrackingCompleteCompactionJobs() {
	jt.isPlanJobLeased = false
	jt.completeCompactionJobs = make([]*TrackedCompactionJob, 0)
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
