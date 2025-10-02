// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/grafana/dskit/services"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

const outsideRotation int = -1

// Rotator rotates initial calls to LeaseJob between per-tenant job trackers for fairness and to reduce contention.
// Hints are given by a tenant's job tracker when an operation causes it to transition states:
//
//	pending jobs -> no pending jobs: The tenant's job tracker is taken out of rotation
//	no pending jobs -> pending jobs: The tenant's job tracker is placed into rotation
//
// A mapping is maintained for each tenant even if they are not currently in the rotation for lease interaction.
// Note: The rotation mechanism could be extended for weighted fairness if needed.
type Rotator struct {
	services.Service

	planTracker          *JobTracker[struct{}]
	leaseDuration        time.Duration
	leaseCheckInterval   time.Duration
	maxLeases            int
	rotationIndexCounter *atomic.Int32 // only increments, overflow is okay

	mtx            *sync.RWMutex
	tenantStateMap map[string]*TenantRotationState
	rotation       []string
}

type TenantRotationState struct {
	tracker       *JobTracker[*CompactionJob]
	rotationIndex int
}

func NewRotator(planTracker *JobTracker[struct{}], leaseDuration time.Duration, leaseCheckInterval time.Duration, maxLeases int) *Rotator {
	r := &Rotator{
		planTracker:          planTracker,
		leaseDuration:        leaseDuration,
		leaseCheckInterval:   leaseCheckInterval,
		maxLeases:            maxLeases,
		rotationIndexCounter: atomic.NewInt32(0),
		mtx:                  &sync.RWMutex{},
		tenantStateMap:       make(map[string]*TenantRotationState),
		rotation:             make([]string, 0, 10), // initial size doesn't really matter
	}
	r.Service = services.NewTimerService(leaseCheckInterval, nil, r.iter, nil)
	return r
}

func (r *Rotator) iter(_ context.Context) error {
	r.LeaseMaintenance(r.leaseDuration)
	return nil
}

func (r *Rotator) LeaseJob(ctx context.Context, canAccept func(string, *CompactionJob) bool) (*compactorschedulerpb.LeaseJobResponse, bool) {
	r.mtx.RLock()

	length := len(r.rotation)
	if length == 0 {
		// avoid divide by zero
		r.mtx.RUnlock()
		return nil, false
	}

	// Handle potential overflow of rotationIndexCounter
	i := ((int(r.rotationIndexCounter.Add(1)) % length) + length) % length

	// Check possibly all tenants. Tenants get removed from the rotation once they are out of work,
	// but this may still encounter some due to canAccept or due to holding the read lock
	for range length {
		tenant := r.rotation[i]
		k, job, epoch, transition := r.tenantStateMap[tenant].tracker.Lease(canAccept)
		if job != nil {
			// must drop the read lock if acquiring the write lock in possiblyRemoveFromRotation
			r.mtx.RUnlock()
			if transition {
				r.possiblyRemoveFromRotation(tenant)
			}
			return &compactorschedulerpb.LeaseJobResponse{
				Key: &compactorschedulerpb.JobKey{
					Id:    k,
					Epoch: epoch,
				},
				Spec: &compactorschedulerpb.JobSpec{
					JobType: compactorschedulerpb.COMPACTION,
					Tenant:  tenant,
					Job: &compactorschedulerpb.CompactionJob{
						BlockIds: job.blocks,
						Split:    job.isSplit,
					},
				},
			}, true
		}
		i += 1
		if i == length {
			i = 0
		}
	}

	r.mtx.RUnlock()
	return nil, false
}

func (r *Rotator) RenewJobLease(tenant string, key string, epoch int64) bool {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		return false
	}

	return tenantState.tracker.RenewLease(key, epoch)
}

func (r *Rotator) CancelJobLease(tenant string, key string, epoch int64) bool {
	r.mtx.RLock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		r.mtx.RUnlock()
		return false
	}

	canceled, transition := tenantState.tracker.CancelLease(key, epoch)
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

	if tenantState.rotationIndex == outsideRotation && !tenantState.tracker.isPendingEmpty() {
		tenantState.rotationIndex = len(r.rotation)
		r.rotation = append(r.rotation, tenant)
	}

	return canceled
}

func (r *Rotator) OfferJobs(tenant string, jobs []*Job[*CompactionJob], shouldReplace func(*CompactionJob, *CompactionJob) bool) int {
	if len(jobs) == 0 {
		return 0
	}

	r.mtx.RLock()

	var added int
	var transition bool
	tenantState, ok := r.tenantStateMap[tenant]
	if ok {
		added, transition = tenantState.tracker.Offer(jobs, shouldReplace)
		if !transition {
			r.mtx.RUnlock()
			return added
		}
	}

	// Drop the read lock to prepare for acquiring a write lock
	r.mtx.RUnlock()

	r.mtx.Lock()
	defer r.mtx.Unlock()

	tenantState, ok = r.tenantStateMap[tenant]
	if !ok {
		tenantState = &TenantRotationState{
			NewJobTracker[*CompactionJob](r.maxLeases),
			outsideRotation,
		}
	}

	if transition {
		// If we thought we should transition the tracker before make sure it still contains jobs
		transition = !tenantState.tracker.isPendingEmpty()
	} else {
		// This branch is taken when the tenantState wasn't found when under the read lock
		added, transition = tenantState.tracker.Offer(jobs, shouldReplace)
	}

	if transition && tenantState.rotationIndex == outsideRotation {
		r.tenantStateMap[tenant] = tenantState
		r.addToRotation(tenant, tenantState)
	}
	return added
}

func (r *Rotator) RemoveJob(tenant string, key string, epoch int64) bool {
	r.mtx.RLock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		r.mtx.RUnlock()
		return false
	}
	removed, emptyAvailable := tenantState.tracker.Remove(key, epoch)

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

	// Note: don't care if there are active/pending jobs in this tenant.
	// A caller would only call this function if it sees the tenant as entirely empty. We could still be
	// generating plan jobs that achieve nothing in that case.
	if tenantState.rotationIndex != outsideRotation {
		r.removeFromRotation(tenantState)
		delete(r.tenantStateMap, tenant)
	}
}

func (r *Rotator) LeaseMaintenance(leaseDuration time.Duration) {
	r.planTracker.ExpireLeases(leaseDuration)

	r.mtx.RLock()
	addRotationFor := make([]string, 0, 10) // size is arbitrary
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
		if ok && tenantState.rotationIndex == outsideRotation && !tenantState.tracker.isPendingEmpty() {
			tenantState.rotationIndex = len(r.rotation)
			r.rotation = append(r.rotation, tenant)
		}
	}
}

func (r *Rotator) possiblyRemoveFromRotation(tenant string) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	// State may have changed by the time this lock was acquired, double check.
	// Since we hold the write lock, we have exclusive access to all trackers.
	// Therefore, the tracker lock isn't necessary for isPendingEmpty(). Be quick.
	tenantState, ok := r.tenantStateMap[tenant]
	if ok && tenantState.rotationIndex != outsideRotation && tenantState.tracker.isPendingEmpty() {
		r.removeFromRotation(tenantState)
	}
}

// Adds the specified tenant to the rotation. A write lock must be held in order to call this function.
func (r *Rotator) addToRotation(tenant string, tenantState *TenantRotationState) {
	r.rotation = append(r.rotation, tenant)
	tenantState.rotationIndex = len(r.rotation) - 1
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
