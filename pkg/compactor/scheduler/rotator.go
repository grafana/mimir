package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const outsideRotation int = -1

type Rotator struct {
	tenantStateMap       map[string]*TenantRotationState
	rotation             []string
	rotationIndexCounter *atomic.Int32 // only increments, overflow is okay
	mtx                  *sync.RWMutex

	jobTrackerFactory func() *JobTracker[string, *CompactionJob]
}

type TenantRotationState struct {
	tracker       *JobTracker[string, *CompactionJob]
	rotationIndex int
}

func NewRotator(jobTrackerFactory func() *JobTracker[string, *CompactionJob]) *Rotator {
	return &Rotator{
		tenantStateMap:       make(map[string]*TenantRotationState),
		rotation:             make([]string, 0, 10), // initial size doesn't really matter
		rotationIndexCounter: &atomic.Int32{},
		mtx:                  &sync.RWMutex{},
		jobTrackerFactory:    jobTrackerFactory,
	}
}

func (r *Rotator) LeaseJob(ctx context.Context, canAccept func(*CompactionJob) bool) (*CompactionJob, bool) {
	r.mtx.RLock()

	length := len(r.rotation)
	if length == 0 {
		// avoid divide by zero
		r.mtx.RUnlock()
		return nil, false
	}

	i := ((int(r.rotationIndexCounter.Add(1)) % length) + length) % length

	// Check possibly all tenants. Tenants get removed from the rotation once they are out of work,
	// but this may still encounter some due to canAccept or due to holding the read lock
	for range length {
		tenant := r.rotation[i]
		job, remove := r.tenantStateMap[tenant].tracker.Lease(canAccept)
		if job != nil {
			// must drop the read lock before reacquiring the write lock in removeFromRotation or returning
			r.mtx.RUnlock()
			if remove {
				r.possiblyRemoveFromRotation(tenant)
			}
			return job, true
		}
		i += 1
		if i == length {
			i = 0
		}
	}

	r.mtx.RUnlock()
	return nil, false
}

func (r *Rotator) RenewJobLease(tenant string, key string, leaseTime time.Time) bool {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		return false
	}

	return tenantState.tracker.RenewLease(key, leaseTime)
}

func (r *Rotator) CancelJobLease(tenant string, key string, leaseTime time.Time) bool {
	r.mtx.RLock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		r.mtx.RUnlock()
		return false
	}

	canceled, wasEmpty := tenantState.tracker.CancelLease(key, leaseTime)
	if !wasEmpty {
		r.mtx.RUnlock()
		return canceled
	}

	// Drop the read lock to acquire a write lock
	r.mtx.RUnlock()
	r.mtx.Lock()
	defer r.mtx.Unlock()

	tenantState, ok = r.tenantStateMap[tenant]
	if !ok {
		return false
	}

	if tenantState.rotationIndex == outsideRotation {
		tenantState.rotationIndex = len(r.rotation)
		r.rotation = append(r.rotation, tenant)
	}

	return canceled
}

func (r *Rotator) OfferJobs(tenant string, jobs []*Job[string, *CompactionJob], shouldReplace func(*CompactionJob, *CompactionJob) bool) int {
	if len(jobs) == 0 {
		return 0
	}

	r.mtx.RLock()

	var added int
	var wasEmpty bool
	tenantState, hadState := r.tenantStateMap[tenant]
	if hadState {
		added, wasEmpty = tenantState.tracker.Offer(jobs, shouldReplace)
		if !wasEmpty {
			r.mtx.RUnlock()
			return added
		}
		jobs = nil // we want to add the tenant to the rotation, but not offer the same jobs
	}

	// Drop the read lock to acquire a write lock in possiblyAddToRotation
	r.mtx.RUnlock()
	addedAfter := r.possiblyAddToRotation(tenant, jobs, shouldReplace)
	if hadState {
		return added
	}
	return addedAfter

}

func (r *Rotator) RemoveJobIf(tenant string, key string, predicate func(existing *CompactionJob) bool) bool {
	r.mtx.RLock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		r.mtx.RUnlock()
		return false
	}
	removed, emptyAvailable := tenantState.tracker.RemoveIf(key, predicate)

	r.mtx.RUnlock()

	if emptyAvailable {
		// This branch is unlikely. The lease would have had to have expired but we're honoring the completion anyway (because it's too late)
		r.possiblyRemoveFromRotation(tenant)
	}
	return removed
}

func (r *Rotator) RemoveTenant(tenant string) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		return
	}

	// Note: don't care if there are leased/available jobs in this tenant.
	// A caller would only call this function if it sees the tenant as entirely empty. We could still be
	// generating plan jobs that achieve nothing in that case.
	if tenantState.rotationIndex != outsideRotation {
		r.removeFromRotation(tenantState)
		delete(r.tenantStateMap, tenant)
	}
}

func (r *Rotator) possiblyAddToRotation(tenant string, jobs []*Job[string, *CompactionJob], shouldReplace func(*CompactionJob, *CompactionJob) bool) int {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		if jobs == nil {
			return 0
		}
		tenantState = &TenantRotationState{
			r.jobTrackerFactory(),
			outsideRotation,
		}
		r.tenantStateMap[tenant] = tenantState
	}

	if tenantState.rotationIndex == outsideRotation {
		tenantState.rotationIndex = len(r.rotation)
		r.rotation = append(r.rotation, tenant)
	}

	if jobs != nil {
		added, _ := tenantState.tracker.Offer(jobs, shouldReplace)
		return added
	}
	return 0
}

func (r *Rotator) possiblyRemoveFromRotation(tenant string) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	// State may have changed by the time this lock was acquired, double check.
	// Since we hold the write lock, we have exclusive access to all trackers.
	// Therefore, the tracker lock isn't necessary for isAvailableEmpty(). Be quick.
	tenantState, ok := r.tenantStateMap[tenant]
	if ok && tenantState.rotationIndex != outsideRotation && tenantState.tracker.isAvailableEmpty() {
		r.removeFromRotation(tenantState)
	}
}

// Removes the specified tenant from the rotation. A write lock must be held in order to call this function.
func (r *Rotator) removeFromRotation(tenantState *TenantRotationState) {
	length := len(r.rotation)
	if tenantState.rotationIndex != length-1 {
		// Swap with the last tenant. This causes an imperfect rotation, but such is life
		r.rotation[tenantState.rotationIndex] = r.rotation[length-1]
		r.tenantStateMap[r.rotation[length-1]].rotationIndex = tenantState.rotationIndex
	}
	// Remove the tenant from the rotation
	r.rotation = r.rotation[:length-1]
	tenantState.rotationIndex = -1
}
