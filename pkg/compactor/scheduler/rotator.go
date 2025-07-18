package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

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

func (r *Rotator) Lease(ctx context.Context, canAccept func(*CompactionJob) bool) (*CompactionJob, bool) {
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

func (r *Rotator) RenewLease(tenant string, key string, leaseTime time.Time) bool {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		return false
	}

	return tenantState.tracker.RenewLease(key, leaseTime)
}

func (r *Rotator) CancelLease(tenant string, key string, leaseTime time.Time) bool {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	var canceled bool
	var wasEmpty bool
	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		canceled, wasEmpty := tenantState.tracker.CancelLease(key, leaseTime)
		if !wasEmpty {
			r.mtx.RUnlock()
			return canceled
		}
	}

	// Drop the read lock to acquire a write lock
	r.mtx.RUnlock()
	r.possiblyAddToRotation(tenant, nil, nil)
	return
	// TODO: Possibly add back to rotation

	return tenantState.tracker.CancelLease(key, leaseTime)
}

func (r *Rotator) Offer(tenant string, jobs []*Job[string, *CompactionJob], shouldReplace func(*CompactionJob, *CompactionJob) bool) int {
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

	// Drop the read lock to acquire a write lock
	r.mtx.RUnlock()
	addedAfter := r.possiblyAddToRotation(tenant, jobs, shouldReplace)
	if hadState {
		return added
	}
	return addedAfter

}

func (r *Rotator) RemoveIf(tenant string, key string, predicate func(existing *CompactionJob) bool) bool {
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
			-1,
		}
		r.tenantStateMap[tenant] = tenantState
	}

	if tenantState.rotationIndex == -1 {
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
	if ok && tenantState.rotationIndex != -1 && tenantState.tracker.isAvailableEmpty() {
		length := len(r.rotation)
		if tenantState.rotationIndex != length-1 {
			// Swap with the last tenant. This causes an imperfect rotation, but such is life
			r.rotation[tenantState.rotationIndex] = r.rotation[length-1]
		}
		// Remove the tenant from the rotation
		r.rotation = r.rotation[:length-1]
		tenantState.rotationIndex = -1
	}
}
