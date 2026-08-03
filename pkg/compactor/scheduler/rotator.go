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
	rotationAddTracker rotationTransition = iota
	rotationRemoveTracker
)

// Rotator rotates calls to LeaseJob between per-tenant job trackers for fairness and to reduce contention.
// Each lane has its own rotation: a tenant is in a lane's rotation while it has pending work in that lane. Hints
// are given by a tenant's job tracker when an operation changes a lane's pending state:
//
//	pending jobs -> no pending jobs: The tenant is taken out of that lane's rotation
//	no pending jobs -> pending jobs: The tenant is placed into that lane's rotation
//
// A mapping is maintained for each tenant even if they are not currently in any rotation for lease updates.
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
	tenantStateMap map[string]*tenantRotationState
	laneRotations  map[lane]*laneRotation
}

// rotation of tenants with pending work in a lane
type laneRotation struct {
	rotation *list.List
	cursor   atomic.Pointer[list.Element] // points to next element (tenant) in the rotation; nil iff rotation is empty
}

type tenantRotationState struct {
	tracker  *JobTracker
	elements map[lane]*list.Element // tenant's slot in each lane's rotation (if they are present in that lane)
}

func NewRotator(leaseDuration, planningInterval, compactionWaitPeriod, maintenanceInterval time.Duration, intervalsBeforeLeaseExpiration, intervalsBeforeColdStartPlanning int, lanePolicy lanePolicy, pendingJobsLastEmpty prometheus.Gauge, logger log.Logger) *Rotator {
	laneRotations := make(map[lane]*laneRotation)
	for _, lane := range lanePolicy.AllLanes() {
		laneRotations[lane] = &laneRotation{rotation: list.New()}
	}

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
		tenantStateMap:                   make(map[string]*tenantRotationState),
		laneRotations:                    laneRotations,
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

// PrepareForShutdown empties out tenants and all lane rotations. This prevents further persist calls to the underlying state.
func (r *Rotator) PrepareForShutdown() {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.tenantStateMap = make(map[string]*tenantRotationState)
	for _, lr := range r.laneRotations {
		lr.rotation = list.New()
		lr.cursor.Store(nil)
	}
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

	// Place recovered tenants into the rotation of every lane they have pending work in
	for tenant, jobTracker := range jobTrackers {
		rotationState := &tenantRotationState{
			tracker:  jobTracker,
			elements: make(map[lane]*list.Element),
		}
		r.tenantStateMap[tenant] = rotationState
		for _, l := range jobTracker.nonEmptyLanes() {
			r.addToRotation(l, tenant, rotationState)
		}
	}

	if r.allRotationsEmpty() {
		r.pendingJobsLastEmpty.Set(float64(r.clock.Now().Unix()))
	}
}

func (r *Rotator) LeaseJob(ctx context.Context, lanes []lane) (*compactorschedulerpb.LeaseJobResponse, bool, error) {
	r.mtx.RLock()

	for _, l := range lanes {

		// The lane must be valid according to the policy.
		lr := r.laneRotations[l]

		length := lr.rotation.Len()
		if length == 0 {
			continue
		}

		// Take a starting tenant from the lane's cursor for fairness across concurrent callers, then walk
		// the rotation locally: walking the shared cursor instead would let a concurrent caller advance it
		// past tenants we never tried, including one with pending work.
		elem := r.advanceCursor(lr)
		for range length {
			tenant := elem.Value.(string)
			response, becameEmpty, err := r.tenantStateMap[tenant].tracker.Lease(l)
			if err != nil {
				r.mtx.RUnlock()
				return nil, false, err
			}
			if response != nil {
				// must drop the read lock if acquiring the write lock in possiblyRemoveFromRotation
				r.mtx.RUnlock()
				if becameEmpty {
					r.possiblyRemoveFromRotation(l, tenant)
				}
				return response, true, nil
			}
			elem = elem.Next()
			if elem == nil {
				elem = lr.rotation.Front()
			}
		}
	}

	r.mtx.RUnlock()
	return nil, false, nil
}

// advanceCursor atomically advances a lane's cursor by one and returns its previous element.
// Must be called while holding the lock, with a non-empty rotation.
func (r *Rotator) advanceCursor(lr *laneRotation) *list.Element {
	for {
		elem := lr.cursor.Load()
		next := elem.Next()
		if next == nil {
			next = lr.rotation.Front()
		}
		if lr.cursor.CompareAndSwap(elem, next) {
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

func (r *Rotator) CancelJobLease(tenant string, key string, epoch int64, interrupted bool) (bool, error) {
	r.mtx.RLock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		r.mtx.RUnlock()
		return false, nil
	}

	canceled, becamePending, err := tenantState.tracker.CancelLease(key, epoch, interrupted)
	if err != nil {
		r.mtx.RUnlock()
		return false, err
	}
	if becamePending == nil {
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

	l := *becamePending
	if tenantState.elements[l] == nil && !tenantState.tracker.isPendingEmpty(l) {
		r.addToRotation(l, tenant, tenantState)
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
	added, found, transitions, err := tenantState.tracker.OfferCompactionJobs(jobs, planJobEpoch)
	if err != nil {
		r.mtx.RUnlock()
		return 0, found, err
	}

	if len(transitions) == 0 {
		r.mtx.RUnlock()
		return added, found, nil
	}

	// Drop the read lock to acquire a write lock and apply the membership changes.
	r.mtx.RUnlock()
	r.mtx.Lock()
	defer r.mtx.Unlock()

	tenantState, ok = r.tenantStateMap[tenant]
	if !ok {
		return added, found, nil
	}

	for _, t := range transitions {
		switch t.kind {
		case rotationAddTracker:
			if tenantState.elements[t.lane] == nil && !tenantState.tracker.isPendingEmpty(t.lane) {
				r.addToRotation(t.lane, tenant, tenantState)
			}
		case rotationRemoveTracker:
			if tenantState.elements[t.lane] != nil && tenantState.tracker.isPendingEmpty(t.lane) {
				r.removeFromRotation(t.lane, tenantState)
			}
		}
	}

	return added, found, nil
}

func (r *Rotator) RemoveJob(tenant string, key string, epoch int64, complete bool) (bool, error) {
	r.mtx.RLock()

	tenantState, ok := r.tenantStateMap[tenant]
	if !ok {
		r.mtx.RUnlock()
		return false, nil
	}

	removed, emptied, err := tenantState.tracker.Remove(key, epoch, complete)
	r.mtx.RUnlock()
	if err != nil {
		return false, err
	}

	if emptied != nil {
		// This branch is unlikely. The lease would have had to have expired but we're honoring the completion anyway (because it's too late)
		r.possiblyRemoveFromRotation(*emptied, tenant)
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

	rotationState := &tenantRotationState{
		tracker:  jobTracker,
		elements: make(map[lane]*list.Element),
	}

	r.tenantStateMap[tenant] = rotationState
	for _, l := range jobTracker.nonEmptyLanes() {
		r.addToRotation(l, tenant, rotationState)
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
	for l := range tenantState.elements {
		r.removeFromRotation(l, tenantState)
	}
	delete(r.tenantStateMap, tenant)
	return tenantState.tracker, true
}

func (r *Rotator) Maintenance(ctx context.Context, enforceLeaseExpiration, plan bool) {
	type tenantLane struct {
		tenant string
		lane   lane
	}

	r.mtx.RLock()
	addRotationFor := make([]tenantLane, 0, 10) // size is arbitrary
	for tenant, tenantState := range r.tenantStateMap {
		if ctx.Err() != nil {
			r.mtx.RUnlock()
			return
		}
		becameNonEmpty, err := tenantState.tracker.Maintenance(r.leaseDuration, enforceLeaseExpiration, plan, r.planningInterval, r.compactionWaitPeriod)
		if err != nil {
			level.Warn(r.logger).Log("msg", "background maintenance failed for job tracker", "user", tenant, "err", err)
			continue
		}
		for _, l := range becameNonEmpty {
			addRotationFor = append(addRotationFor, tenantLane{tenant: tenant, lane: l})
		}
	}
	stayingEmpty := len(addRotationFor) == 0 && r.allRotationsEmpty()
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
	for _, tl := range addRotationFor {
		tenantState, ok := r.tenantStateMap[tl.tenant]
		if ok && tenantState.elements[tl.lane] == nil && !tenantState.tracker.isPendingEmpty(tl.lane) {
			r.addToRotation(tl.lane, tl.tenant, tenantState)
		}
	}
}

func (r *Rotator) possiblyRemoveFromRotation(l lane, tenant string) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	// State may have changed by the time this lock was acquired, double check.
	// Since we hold the write lock, we have exclusive access to all trackers.
	// Therefore, the tracker lock isn't necessary for isPendingEmpty(). Be quick.
	tenantState, ok := r.tenantStateMap[tenant]
	if ok && tenantState.elements[l] != nil && tenantState.tracker.isPendingEmpty(l) {
		r.removeFromRotation(l, tenantState)
	}
}

// addToRotation appends the tenant to a lane's rotation. A write lock must be held in order to call this function.
func (r *Rotator) addToRotation(l lane, tenant string, tenantState *tenantRotationState) {
	lr := r.laneRotations[l]
	tenantState.elements[l] = lr.rotation.PushBack(tenant)
	if lr.cursor.Load() == nil {
		lr.cursor.Store(tenantState.elements[l])
	}
}

// removeFromRotation removes the tenant from a lane's rotation. A write lock must be held in order to call this function.
func (r *Rotator) removeFromRotation(l lane, tenantState *tenantRotationState) {
	lr := r.laneRotations[l]
	elem := tenantState.elements[l]
	if lr.cursor.Load() == elem {
		r.advanceCursor(lr)
		if lr.cursor.Load() == elem {
			lr.cursor.Store(nil)
		}
	}
	lr.rotation.Remove(elem)
	delete(tenantState.elements, l)

	if r.allRotationsEmpty() {
		r.pendingJobsLastEmpty.Set(float64(r.clock.Now().Unix()))
	}
}

// allRotationsEmpty reports whether no tenant has pending work in any lane. A write lock must be held in order to call this function.
func (r *Rotator) allRotationsEmpty() bool {
	for _, lr := range r.laneRotations {
		if lr.rotation.Len() > 0 {
			return false
		}
	}
	return true
}
