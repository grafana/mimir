// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"iter"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
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

	planTracker                   *JobTracker[struct{}]
	leaseDuration                 time.Duration
	leaseCheckInterval            time.Duration
	initialLeaseMaintenanceBuffer time.Duration
	metrics                       *schedulerMetrics
	rotationIndexCounter          *atomic.Int32 // only increments, overflow is okay
	logger                        log.Logger

	mtx            *sync.RWMutex
	tenantStateMap map[string]*TenantRotationState
	rotation       []string
}

type TenantRotationState struct {
	tracker       *JobTracker[*CompactionJob]
	rotationIndex int
}

func NewRotator(planTracker *JobTracker[struct{}], leaseDuration time.Duration, leaseCheckInterval time.Duration, metrics *schedulerMetrics, logger log.Logger) *Rotator {
	r := &Rotator{
		planTracker:                   planTracker,
		leaseDuration:                 leaseDuration,
		leaseCheckInterval:            leaseCheckInterval,
		initialLeaseMaintenanceBuffer: leaseDuration * 2,
		metrics:                       metrics,
		rotationIndexCounter:          atomic.NewInt32(0),
		mtx:                           &sync.RWMutex{},
		tenantStateMap:                make(map[string]*TenantRotationState),
		rotation:                      make([]string, 0, 10), // initial size doesn't really matter
		logger:                        logger,
	}

	r.Service = services.NewTimerService(leaseCheckInterval, nil, r.iter, nil)
	return r
}

func (r *Rotator) iter(ctx context.Context) error {
	r.LeaseMaintenance(ctx, r.leaseDuration)
	return nil
}

func (r *Rotator) Tenants() iter.Seq[string] {
	return func(yield func(string) bool) {
		r.mtx.RLock()
		defer r.mtx.RUnlock()
		for tenant := range r.tenantStateMap {
			if !yield(tenant) {
				return
			}
		}
	}
}

// PrepareForShutdown empties out tenants and the rotation. This prevents further persist calls to the underlying state.
func (r *Rotator) PrepareForShutdown() {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.tenantStateMap = make(map[string]*TenantRotationState)
	r.rotation = []string{}
}

func (r *Rotator) RecoverFrom(m map[string]*JobTracker[*CompactionJob]) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	// Place recovered tenants that have pending work in the rotation
	for tenant, tracker := range m {
		rotationState := &TenantRotationState{
			tracker:       tracker,
			rotationIndex: outsideRotation,
		}
		r.tenantStateMap[tenant] = rotationState
		if !tracker.isPendingEmpty() {
			r.addToRotation(tenant, rotationState)
		}
	}
}

func (r *Rotator) LeaseJob(ctx context.Context, canAccept func(string, *CompactionJob) bool) (*compactorschedulerpb.LeaseJobResponse, bool, error) {
	r.mtx.RLock()

	length := len(r.rotation)
	if length == 0 {
		// avoid divide by zero
		r.mtx.RUnlock()
		return nil, false, nil
	}

	// Handle potential overflow of rotationIndexCounter
	i := ((int(r.rotationIndexCounter.Add(1)) % length) + length) % length

	// Check possibly all tenants. Tenants get removed from the rotation once they are out of work,
	// but this may still encounter some due to canAccept or due to holding the read lock
	for range length {
		tenant := r.rotation[i]
		k, job, epoch, transition, err := r.tenantStateMap[tenant].tracker.Lease(canAccept)
		if err != nil {
			r.mtx.RUnlock()
			return nil, false, err
		}
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
			}, true, nil
		}
		i += 1
		if i == length {
			i = 0
		}
	}

	r.mtx.RUnlock()
	return nil, false, nil
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

func (r *Rotator) CancelJobLease(tenant string, key string, epoch int64) (bool, error) {
	r.mtx.RLock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		r.mtx.RUnlock()
		return false, nil
	}

	canceled, transition, err := tenantState.tracker.CancelLease(key, epoch)
	if err != nil {
		r.mtx.RUnlock()
		return false, err
	}
	if !transition {
		r.mtx.RUnlock()
		return canceled, nil
	}

	// Drop the read lock to acquire a write lock
	r.mtx.RUnlock()
	r.mtx.Lock()
	defer r.mtx.Unlock()

	tenantState, ok = r.tenantStateMap[tenant]
	if !ok {
		return canceled, nil
	}

	if tenantState.rotationIndex == outsideRotation && !tenantState.tracker.isPendingEmpty() {
		tenantState.rotationIndex = len(r.rotation)
		r.rotation = append(r.rotation, tenant)
	}

	return canceled, nil
}

func (r *Rotator) OfferJobs(tenant string, jobs []*Job[*CompactionJob], shouldReplace func(*CompactionJob, *CompactionJob) bool) (int, error) {
	if len(jobs) == 0 {
		return 0, nil
	}

	r.mtx.RLock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		r.mtx.RUnlock()
		return 0, nil
	}
	added, transition, err := tenantState.tracker.Offer(jobs, shouldReplace)
	if err != nil {
		r.mtx.RUnlock()
		return 0, err
	}

	// Must still be holding the read lock to read rotation index
	if transition && tenantState.rotationIndex == outsideRotation {
		r.mtx.RUnlock() // drop read lock to acquire write lock
		r.mtx.Lock()
		// Double check still present, not in rotation, and there are pending jobs
		if tenantState, ok := r.tenantStateMap[tenant]; ok && tenantState.rotationIndex == outsideRotation && !tenantState.tracker.isPendingEmpty() {
			r.addToRotation(tenant, tenantState)
		}
		r.mtx.Unlock()
		return added, nil
	}

	r.mtx.RUnlock()
	return added, nil
}

func (r *Rotator) RemoveJob(tenant string, key string, epoch int64) (bool, error) {
	r.mtx.RLock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		r.mtx.RUnlock()
		return false, nil
	}

	removed, emptyAvailable, err := tenantState.tracker.Remove(key, epoch)
	r.mtx.RUnlock()
	if err != nil {
		return false, err
	}

	if emptyAvailable {
		// This branch is unlikely. The lease would have had to have expired but we're honoring the completion anyway (because it's too late)
		r.possiblyRemoveFromRotation(tenant)
	}
	return removed, nil
}

// AddTenant adds the tenant to the rotator if the tenant did not already exist
func (r *Rotator) AddTenant(tenant string, jobTracker *JobTracker[*CompactionJob]) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if _, ok := r.tenantStateMap[tenant]; ok {
		return
	}

	rotationState := &TenantRotationState{
		tracker:       jobTracker,
		rotationIndex: outsideRotation,
	}

	r.tenantStateMap[tenant] = rotationState
	if !jobTracker.isPendingEmpty() {
		r.addToRotation(tenant, rotationState)
	}
}

func (r *Rotator) RemoveTenant(tenant string) (*JobTracker[*CompactionJob], bool) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		return nil, false
	}

	// Note: don't care if there are active/pending jobs in this tenant.
	// A caller would only call this function if it sees the tenant as entirely empty. We could still be
	// generating plan jobs that achieve nothing in that case.
	if tenantState.rotationIndex != outsideRotation {
		r.removeFromRotation(tenantState)
	}
	delete(r.tenantStateMap, tenant)
	return tenantState.tracker, true
}

func (r *Rotator) LeaseMaintenance(ctx context.Context, leaseDuration time.Duration) {
	// We avoid having to serialize lease renewals by providing an initial buffer time before enforcing lease expiration
	if r.initialLeaseMaintenanceBuffer != 0 {
		select {
		case <-time.After(r.initialLeaseMaintenanceBuffer):
			r.initialLeaseMaintenanceBuffer = 0
			break
		case <-ctx.Done():
			return
		}
	}

	_, err := r.planTracker.ExpireLeases(leaseDuration)
	if err != nil {
		level.Warn(r.logger).Log("msg", "background lease expiration failed for planning job tracker", "err", err)
	}

	r.mtx.RLock()
	addRotationFor := make([]string, 0, 10) // size is arbitrary
	for tenant, tenantState := range r.tenantStateMap {
		if ctx.Err() != nil {
			r.mtx.RUnlock()
			return
		}
		transition, err := tenantState.tracker.ExpireLeases(leaseDuration)
		if err != nil {
			level.Warn(r.logger).Log("msg", "background lease expiration failed for tenant compaction job tracker", "tenant", tenant, "err", err)
		} else if transition {
			addRotationFor = append(addRotationFor, tenant)
		}
	}
	r.mtx.RUnlock()

	if len(addRotationFor) == 0 || ctx.Err() != nil {
		// No tenant needs to be moved into the rotation or we're shutting down and don't care
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
