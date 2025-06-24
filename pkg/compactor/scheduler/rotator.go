package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/dskit/services"
)

const outsideRotation int = -1

// Rotator rotates initial calls to LeaseJob between per-tenant job trackers for fairness and to reduce contention.
// Hints are given by a tenant's job tracker when an operation causes it to transition states:
//
//	available jobs -> no available jobs: The tenant's job tracker is taken out of rotation
//	no available jobs -> available jobs: The tenant's job tracker is placed into rotation
//
// A mapping is maintained for each tenant even if they are not currently in the rotation for lease interaction.
// Note: The rotation mechansim could be extended for weighted fairness if needed.
type Rotator struct {
	services.Service

	tenantStateMap map[string]*TenantRotationState
	rotation       []string
	mtx            *sync.RWMutex

	rotationIndexCounter *atomic.Int32 // only increments, overflow is okay

	leaseDuration      time.Duration
	leaseCheckInterval time.Duration
	maxLeases          int
}

type TenantRotationState struct {
	tracker       *JobTracker[string, *CompactionJob]
	rotationIndex int
}

func NewRotator(leaseDuration time.Duration, leaseCheckInterval time.Duration, maxLeases int) *Rotator {
	r := &Rotator{
		tenantStateMap:       make(map[string]*TenantRotationState),
		rotation:             make([]string, 0, 10), // initial size doesn't really matter
		mtx:                  &sync.RWMutex{},
		rotationIndexCounter: &atomic.Int32{},
		leaseDuration:        leaseDuration,
		leaseCheckInterval:   leaseCheckInterval,
		maxLeases:            maxLeases,
	}
	r.Service = services.NewTimerService(leaseCheckInterval, nil, r.iter, nil)
	return r
}

func (r *Rotator) iter(_ context.Context) error {
	r.LeaseMaintenance(r.leaseDuration)
	return nil
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
		job, transition := r.tenantStateMap[tenant].tracker.Lease(canAccept)
		if job != nil {
			// must drop the read lock if acquiring the write lock in possiblyRemoveFromRotation 
			r.mtx.RUnlock()
			if transition {
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

	canceled, transition := tenantState.tracker.CancelLease(key, leaseTime)
	if !transition {
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

	if tenantState.rotationIndex == outsideRotation && !tenantState.tracker.isAvailableEmpty() {
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
	tenantState, ok := r.tenantStateMap[tenant]
	if ok {
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
	if ok {
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

func (r *Rotator) LeaseMaintenance(leaseDuration time.Duration) {
	r.mtx.RLock()
	addRotationFor := make([]string, 10) // size is arbitrary
	for tenant, tenantState := range r.tenantStateMap {
		if tenantState.tracker.ExpireLeases(leaseDuration) {
			addRotationFor = append(addRotationFor, tenant)
		}
	}
	r.mtx.RUnlock()

	if len(addRotationFor) == 0 {
		// No tenant needs to be moved into the rotation
		return
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()
	for _, tenant := range addRotationFor {
		tenantState, ok := r.tenantStateMap[tenant]
		if ok && tenantState.rotationIndex == outsideRotation && !tenantState.tracker.isAvailableEmpty() {
			tenantState.rotationIndex = len(r.rotation)
			r.rotation = append(r.rotation, tenant)
		}
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
			NewJobTracker[string, *CompactionJob](r.maxLeases),
			outsideRotation,
		}
		r.tenantStateMap[tenant] = tenantState
	}
	// TODO: Correctness seems off here

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
