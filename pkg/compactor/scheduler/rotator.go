// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

type rotationTransition int

const (
	rotationNoChange rotationTransition = iota
	rotationAddTracker
	rotationRemoveTracker
)

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

	leaseDuration                    time.Duration
	planningInterval                 time.Duration
	compactionWaitPeriod             time.Duration
	maintenanceInterval              time.Duration
	intervalsBeforeLeaseExpiration   int
	intervalsBeforeColdStartPlanning int
	clock                            clock.Clock
	pendingJobsLastEmpty             prometheus.Gauge
	logger                           log.Logger

	mtx            sync.RWMutex
	tenantStateMap map[string]*TenantRotationState
	rotation       *list.List
	// cursor points to the next element in 'rotation' to be leased.
	// it is nil iff 'rotation' is empty.
	cursor atomic.Pointer[list.Element]
}

type TenantRotationState struct {
	tracker *JobTracker
	// element is the tenant's slot in 'rotation', or nil outside the rotation.
	element *list.Element
}

func NewRotator(leaseDuration, planningInterval, compactionWaitPeriod, maintenanceInterval time.Duration, intervalsBeforeLeaseExpiration, intervalsBeforeColdStartPlanning int, pendingJobsLastEmpty prometheus.Gauge, logger log.Logger) *Rotator {
	r := &Rotator{
		leaseDuration:                    leaseDuration,
		planningInterval:                 planningInterval,
		compactionWaitPeriod:             compactionWaitPeriod,
		maintenanceInterval:              maintenanceInterval,
		intervalsBeforeLeaseExpiration:   intervalsBeforeLeaseExpiration,
		intervalsBeforeColdStartPlanning: intervalsBeforeColdStartPlanning,
		clock:                            clock.New(),
		pendingJobsLastEmpty:             pendingJobsLastEmpty,
		mtx:                              sync.RWMutex{},
		tenantStateMap:                   make(map[string]*TenantRotationState),
		rotation:                         list.New(),
		logger:                           logger,
	}

	r.Service = services.NewTimerService(maintenanceInterval, r.start, r.iter, nil)
	return r
}

// start starts the service that performs regular maintenance. It is expected that RecoverFrom is called before starting the service.
func (r *Rotator) start(ctx context.Context) error {
	return r.iter(ctx)
}

func (r *Rotator) iter(ctx context.Context) error {
	// TimerService is single threaded and this is the only usage of the intervalsBeforeLeaseExpiration variable so accessing and mutating it is safe
	expireLeases := true
	plan := true
	if r.intervalsBeforeLeaseExpiration > 0 {
		r.intervalsBeforeLeaseExpiration--
		level.Info(r.logger).Log("msg", "lease expiration will not be enforced this maintenance interval to provide cushion between restarts", "intervals_remaining", r.intervalsBeforeLeaseExpiration, "interval_duration", r.maintenanceInterval)
		expireLeases = false
	}
	// RecoverFrom sets intervalsBeforeColdStartPlanning before the service is started and is the only other use of this variable so accessing and mutating it is safe
	if r.intervalsBeforeColdStartPlanning > 0 {
		r.intervalsBeforeColdStartPlanning--
		level.Info(r.logger).Log("msg", "planning will not be performed this maintenance interval to prevent possible job duplication", "intervals_remaining", r.intervalsBeforeColdStartPlanning, "interval_duration", r.maintenanceInterval)
		plan = false
	}

	if plan || expireLeases {
		r.Maintenance(ctx, expireLeases, plan)
	}

	return nil
}

// PrepareForShutdown empties out tenants and the rotation. This prevents further persist calls to the underlying state.
func (r *Rotator) PrepareForShutdown() {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.tenantStateMap = make(map[string]*TenantRotationState)
	r.rotation = list.New()
	r.cursor.Store(nil)
}

func (r *Rotator) RecoverFrom(jobTrackers map[string]*JobTracker, creationTime time.Time) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if r.intervalsBeforeColdStartPlanning > 0 {
		elapsed := r.clock.Since(creationTime)
		fullDelay := time.Duration(r.intervalsBeforeColdStartPlanning) * r.maintenanceInterval
		if elapsed >= fullDelay {
			// Enough time has passed
			r.intervalsBeforeColdStartPlanning = 0
		} else {
			remaining := fullDelay - elapsed
			// Ceiling integer division. The -1 prevents rounding up exact multiples.
			intervals := int((remaining + r.maintenanceInterval - 1) / r.maintenanceInterval)
			// Defensively ensure we at most set the configured value
			r.intervalsBeforeColdStartPlanning = min(r.intervalsBeforeColdStartPlanning, intervals)
		}
	}

	// Place recovered tenants that have pending work in the rotation
	for tenant, jobTracker := range jobTrackers {
		rotationState := &TenantRotationState{
			tracker: jobTracker,
		}
		r.tenantStateMap[tenant] = rotationState
		if !jobTracker.isPendingEmpty() {
			r.addToRotation(tenant, rotationState)
		}
	}

	if r.rotation.Len() == 0 {
		r.pendingJobsLastEmpty.Set(float64(r.clock.Now().Unix()))
	}
}

func (r *Rotator) LeaseJob(ctx context.Context) (*compactorschedulerpb.LeaseJobResponse, bool, error) {
	r.mtx.RLock()

	length := r.rotation.Len()
	if length == 0 {
		r.mtx.RUnlock()
		return nil, false, nil
	}

	// Take a starting tenant from the shared cursor for fairness across concurrent callers, then walk
	// the rotation locally: walking the shared cursor instead would let a concurrent caller advance it
	// past tenants we never tried, including one with pending work.
	elem := r.advanceCursor()
	for range length {
		tenant := elem.Value.(string)
		response, transition, err := r.tenantStateMap[tenant].tracker.Lease()
		if err != nil {
			r.mtx.RUnlock()
			return nil, false, err
		}
		if response != nil {
			// must drop the read lock if acquiring the write lock in possiblyRemoveFromRotation
			r.mtx.RUnlock()
			if transition {
				r.possiblyRemoveFromRotation(tenant)
			}
			return response, true, nil
		}
		elem = elem.Next()
		if elem == nil {
			elem = r.rotation.Front()
		}
	}

	r.mtx.RUnlock()
	return nil, false, nil
}

// advanceCursor atomically advances the cursor by one and returns its previous element.
// Must be called while holding the lock, with a non-empty rotation.
func (r *Rotator) advanceCursor() *list.Element {
	for {
		elem := r.cursor.Load()
		next := elem.Next()
		if next == nil {
			next = r.rotation.Front()
		}
		if r.cursor.CompareAndSwap(elem, next) {
			return elem
		}
	}
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

	if tenantState.element == nil && !tenantState.tracker.isPendingEmpty() {
		r.addToRotation(tenant, tenantState)
	}

	return canceled, nil
}

func (r *Rotator) OfferCompactionJobs(tenant string, jobs []*TrackedCompactionJob, planJobEpoch int64) (int, bool, error) {
	r.mtx.RLock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		r.mtx.RUnlock()
		return 0, false, nil
	}
	added, found, transition, err := tenantState.tracker.OfferCompactionJobs(jobs, planJobEpoch)
	if err != nil {
		r.mtx.RUnlock()
		return 0, found, err
	}

	// Must still be holding the read lock to read rotation membership
	switch transition {
	case rotationAddTracker:
		if tenantState.element == nil {
			r.mtx.RUnlock() // drop read lock to acquire write lock
			r.mtx.Lock()
			// Double check still present, not in rotation, and there are pending jobs
			if tenantState, ok := r.tenantStateMap[tenant]; ok && tenantState.element == nil && !tenantState.tracker.isPendingEmpty() {
				r.addToRotation(tenant, tenantState)
			}
			r.mtx.Unlock()
			return added, found, nil
		}
	case rotationRemoveTracker:
		if tenantState.element != nil {
			r.mtx.RUnlock() // drop read lock to acquire write lock
			r.possiblyRemoveFromRotation(tenant)
			return added, found, nil
		}
	}

	r.mtx.RUnlock()
	return added, found, nil
}

func (r *Rotator) RemoveJob(tenant string, key string, epoch int64, complete bool) (bool, error) {
	r.mtx.RLock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		r.mtx.RUnlock()
		return false, nil
	}

	removed, emptyAvailable, err := tenantState.tracker.Remove(key, epoch, complete)
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
func (r *Rotator) AddTenant(tenant string, jobTracker *JobTracker) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if _, ok := r.tenantStateMap[tenant]; ok {
		return
	}

	rotationState := &TenantRotationState{
		tracker: jobTracker,
	}

	r.tenantStateMap[tenant] = rotationState
	if !jobTracker.isPendingEmpty() {
		r.addToRotation(tenant, rotationState)
	}
}

func (r *Rotator) RemoveTenant(tenant string) (*JobTracker, bool) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		return nil, false
	}

	// Note: don't care if there are active/pending jobs in this tenant.
	// A caller would only call this function if it sees the tenant as entirely empty. We could still be
	// generating plan jobs that achieve nothing in that case.
	if tenantState.element != nil {
		r.removeFromRotation(tenantState)
	}
	delete(r.tenantStateMap, tenant)
	return tenantState.tracker, true
}

func (r *Rotator) Maintenance(ctx context.Context, enforceLeaseExpiration, plan bool) {
	r.mtx.RLock()
	addRotationFor := make([]string, 0, 10) // size is arbitrary
	for tenant, tenantState := range r.tenantStateMap {
		if ctx.Err() != nil {
			r.mtx.RUnlock()
			return
		}
		transition, err := tenantState.tracker.Maintenance(r.leaseDuration, enforceLeaseExpiration, plan, r.planningInterval, r.compactionWaitPeriod)
		if err != nil {
			level.Warn(r.logger).Log("msg", "background maintenance failed for job tracker", "user", tenant, "err", err)
		} else if transition {
			addRotationFor = append(addRotationFor, tenant)
		}
	}
	stayingEmpty := len(addRotationFor) == 0 && r.rotation.Len() == 0
	r.mtx.RUnlock()

	if len(addRotationFor) == 0 || ctx.Err() != nil {
		// No tenant needs to be moved into the rotation or we're shutting down and don't care
		if stayingEmpty && ctx.Err() == nil {
			// Ensures periodic updates for the time we last saw an empty queue rather than only on transition.
			// The context check is to ensure we don't update this when mid-shutdown.
			r.pendingJobsLastEmpty.Set(float64(r.clock.Now().Unix()))
		}
		return
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()
	for _, tenant := range addRotationFor {
		tenantState, ok := r.tenantStateMap[tenant]
		if ok && tenantState.element == nil && !tenantState.tracker.isPendingEmpty() {
			r.addToRotation(tenant, tenantState)
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
	if ok && tenantState.element != nil && tenantState.tracker.isPendingEmpty() {
		r.removeFromRotation(tenantState)
	}
}

// addToRotation appends the specified tenant to the rotation. A write lock must be held.
func (r *Rotator) addToRotation(tenant string, tenantState *TenantRotationState) {
	tenantState.element = r.rotation.PushBack(tenant)
	if r.cursor.Load() == nil {
		r.cursor.Store(tenantState.element)
	}
}

// removeFromRotation removes the specified tenant from the rotation. A write lock must be held in order to call this function.
func (r *Rotator) removeFromRotation(tenantState *TenantRotationState) {
	elem := tenantState.element
	if r.cursor.Load() == elem {
		r.advanceCursor()
		if r.cursor.Load() == elem {
			r.cursor.Store(nil)
		}
	}
	r.rotation.Remove(elem)
	tenantState.element = nil

	if r.rotation.Len() == 0 {
		r.pendingJobsLastEmpty.Set(float64(r.clock.Now().Unix()))
	}
}
