// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/util/zeropool"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
	"github.com/grafana/mimir/pkg/usagetracker/tenantshard"
)

var refsPool zeropool.Pool[[]uint64]

const shards = 16
const noLimit = math.MaxUint64

// trackerStore holds the core business logic of the usage-tracker abstracted in a testable way.
// trackerStore should not depend on wall clock: time.Now() should be always injected as a parameter,
// and timer calls should be made from the outside.
type trackerStore struct {
	mtx           sync.RWMutex
	sortedTenants []string
	tenants       map[string]*trackedTenant

	// sortedUsersCloseToLimit is an immutable list of user IDs that are close to their limits.
	// This field is replaced atomically in updateLimits() and read without the lock.
	// This list is sorted.
	sortedUsersCloseToLimit []string

	// dependencies
	limiter limiter
	events  events

	// config
	idleTimeout                         time.Duration
	userCloseToLimitPercentageThreshold int

	// misc
	logger log.Logger
}

// limiter provides the local series limit for a tenant.
type limiter interface {
	localSeriesLimit(userID string) uint64
	zonesCount() uint64
}

// events provides an abstraction to publish usage-tracker events.
type events interface {
	publishCreatedSeries(ctx context.Context, tenantID string, series []uint64, timestamp time.Time) error
}

func newTrackerStore(idleTimeout time.Duration, userCloseToLimitPercentageThreshold int, logger log.Logger, l limiter, ev events) *trackerStore {
	t := &trackerStore{
		tenants:                             make(map[string]*trackedTenant),
		limiter:                             l,
		events:                              ev,
		logger:                              logger,
		idleTimeout:                         idleTimeout,
		userCloseToLimitPercentageThreshold: userCloseToLimitPercentageThreshold,
		sortedUsersCloseToLimit:             nil, // will be populated by updateLimits
	}
	return t
}

// trackSeries is used in tests so we can provide custom time.Now() value.
// trackSeries will modify and reuse the input series slice.
func (t *trackerStore) trackSeries(ctx context.Context, tenantID string, series []uint64, timeNow time.Time) (rejectedRefs []uint64, err error) {
	tenant := t.getOrCreateTenant(tenantID)
	defer tenant.RUnlock()

	// Sort series by shard to minimize lock contention by taking mutex once for each shard.
	slices.SortFunc(series, func(a, b uint64) int { return int(a%shards) - int(b%shards) })

	now := clock.ToMinutes(timeNow)

	// We don't pool rejectedRefs because we don't have full control of its lifecycle.
	createdRefs := refsPool.Get()[:0]
	i0 := 0
	for i := 1; i <= len(series); i++ {
		// Track series if shard changes on the next element or if we're at the end of series.
		if shard := uint8(series[i0] % shards); i == len(series) || shard != uint8(series[i]%shards) {
			m := tenant.shards[shard]
			m.Lock()
			for _, ref := range series[i0:i] {
				if created, rejected := m.Put(ref, now, tenant.series, tenant.currentLimit, true); created {
					createdRefs = append(createdRefs, ref)
				} else if rejected {
					rejectedRefs = append(rejectedRefs, ref)
				}
			}
			m.Unlock()
			i0 = i
		}
	}

	level.Debug(t.logger).Log("msg", "tracked series", "tenant", tenantID, "received_len", len(series), "created_len", len(createdRefs), "rejected_len", len(rejectedRefs), "now", timeNow.Unix(), "now_minutes", now)
	if len(createdRefs) == 0 {
		return rejectedRefs, nil
	}

	if err := t.events.publishCreatedSeries(ctx, tenantID, createdRefs, timeNow); err != nil {
		level.Error(t.logger).Log("msg", "failed to publish created series", "tenant", tenantID, "err", err, "created_len", len(createdRefs), "now", timeNow.Unix(), "now_minutes", now)
		return nil, err
	}

	return rejectedRefs, nil
}

func (t *trackerStore) processCreatedSeriesEvent(tenantID string, series []uint64, eventTimestamp, timeNow time.Time) {
	if timeNow.Sub(eventTimestamp) >= t.idleTimeout {
		// It doesn't make sense to process this event, we're not going to have lower timestamp for any series.
		// This potentially creates a case where:
		// - we're at the limit,
		// - a different instance created series
		// - it is accepting updates for it because it's already created
		// - we're processing it too late, so we're not creating it
		// - so we're rejecting samples for it.
		// However, this scenario will be fixed by the next snapshot reload.
		return
	}

	tenant := t.getOrCreateTenant(tenantID)
	defer tenant.RUnlock()

	// Sort series by shard. We're going to accept all of them, so we can start on shard 0 here.
	slices.SortFunc(series, func(a, b uint64) int { return int(a%shards) - int(b%shards) })

	timestamp := clock.ToMinutes(eventTimestamp)
	i0 := 0
	for i := 1; i <= len(series); i++ {
		// Track series if shard changes on the next element or if we're at the end of series.
		if shard := uint8(series[i0] % shards); i == len(series) || shard != uint8(series[i]%shards) {
			m := tenant.shards[shard]
			m.Lock()
			for _, ref := range series[i0:i] {
				_, _ = m.Put(ref, timestamp, tenant.series, nil, false)
			}
			m.Unlock()
			i0 = i
		}
	}
}

func currentSeriesLimit(series uint64, limit uint64, zonesCount uint64) uint64 {
	// If we're at or over the limit (can happen if limit was decreased or series exceeded limit),
	// return the limit itself to avoid underflow in the subtraction below.
	if series >= limit {
		return limit
	}
	room := limit - series
	allowance := room / zonesCount
	if zonesCount > 1 {
		allowance += room % zonesCount
	}
	return series + allowance
}

// getOrCreateTenant returns the trackedTenant for the given userID, with shards for the limit provided.
// The tenant returned is RLock'ed() and needs to be RUnlocked() after use.
func (t *trackerStore) getOrCreateTenant(tenantID string) *trackedTenant {
	limit := zeroAsNoLimit(t.limiter.localSeriesLimit(tenantID))
	zonesCount := t.limiter.zonesCount()

	t.mtx.RLock()
	if tenant, ok := t.tenants[tenantID]; ok {
		tenant.RLock()
		t.mtx.RUnlock()
		return tenant
	}
	t.mtx.RUnlock()

	t.mtx.Lock()
	if tenant, ok := t.tenants[tenantID]; ok {
		tenant.RLock()
		t.mtx.Unlock()
		return tenant
	}

	// Let's prepare a tenant with all shards instead of doing it while locked.
	tenant := &trackedTenant{
		series:       atomic.NewUint64(0),
		currentLimit: atomic.NewUint64(currentSeriesLimit(0, limit, zonesCount)),
	}
	capacity := int(limit / shards)
	if limit == noLimit || limit == 0 {
		capacity = 512 // let's be modest.
	} else if capacity > math.MaxUint32 {
		capacity = math.MaxUint32
	}
	for i := range tenant.shards {
		tenant.shards[i] = tenantshard.New(uint32(capacity))
	}

	t.tenants[tenantID] = tenant
	i, found := slices.BinarySearch(t.sortedTenants, tenantID)
	if found {
		// This should never happen, let's panic instead of having an inconsistent list.
		panic(fmt.Errorf("tenant %s already exists in the sorted list: %v", tenantID, t.sortedTenants))
	}
	t.sortedTenants = slices.Insert(t.sortedTenants, i, tenantID)

	tenant.RLock()
	t.mtx.Unlock()
	return tenant
}

func (t *trackerStore) cleanup(now time.Time) {
	watermark := clock.ToMinutes(now.Add(-t.idleTimeout))

	// We will work on a copy of tenants.
	t.mtx.RLock()
	tenantsClone := maps.Clone(t.tenants)
	t.mtx.RUnlock()

	var deletionCandidates []string
	for tenantID, tenant := range tenantsClone {
		for _, shard := range tenant.shards {
			shard.Lock()
			removed := shard.Cleanup(watermark)
			shard.Unlock()

			// Update the tenant's counter when not holding the mutex anymore.
			if removed > 0 {
				tenant.series.Add(-uint64(removed))
			}
		}

		if tenant.series.Load() == 0 {
			deletionCandidates = append(deletionCandidates, tenantID)
		}
	}

	if len(deletionCandidates) == 0 {
		return
	}

	t.mtx.Lock()
	for _, tenantID := range deletionCandidates {
		tenant, ok := t.tenants[tenantID]
		if !ok {
			continue // weird, two concurrent cleanups maybe?
		}
		// Make sure nobody is appending.
		// Since we have the t.mtx, we know that nobody can also get this tenant.
		tenant.Lock()
		if tenant.series.Load() == 0 {
			delete(t.tenants, tenantID)
			index, found := slices.BinarySearch(t.sortedTenants, tenantID)
			if !found {
				panic(fmt.Errorf("tenant %s not found in the sorted list: %v", tenantID, t.sortedTenants))
			}
			t.sortedTenants = slices.Delete(t.sortedTenants, index, index+1)
		}
		tenant.Unlock()
	}
	t.mtx.Unlock()
}

func (t *trackerStore) updateLimits() {
	t.mtx.RLock()
	tenantsClone := maps.Clone(t.tenants)
	sortedTenants := slices.Clone(t.sortedTenants)
	t.mtx.RUnlock()

	zonesCount := t.limiter.zonesCount()
	var sortedCloseToLimit []string

	for _, tenantID := range sortedTenants {
		tenant := tenantsClone[tenantID]
		limit := zeroAsNoLimit(t.limiter.localSeriesLimit(tenantID))
		series := tenant.series.Load()
		tenant.currentLimit.Store(currentSeriesLimit(series, limit, zonesCount))

		// Determine if this user is close to their limit.
		// A user is close if: series >= (limit * percentageThreshold / 100)
		if limit != noLimit {
			percentageThreshold := limit * uint64(t.userCloseToLimitPercentageThreshold) / 100

			if series >= percentageThreshold {
				sortedCloseToLimit = append(sortedCloseToLimit, tenantID)
			}
		}
	}

	t.mtx.Lock()
	t.sortedUsersCloseToLimit = sortedCloseToLimit
	t.mtx.Unlock()
}

// getSortedUsersCloseToLimit returns the list of user IDs that are close to their series limit.
// The returned slice is safe to read concurrently as it's immutable and replaced atomically in updateLimits().
// The returned slice is sorted.
func (t *trackerStore) getSortedUsersCloseToLimit() []string {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.sortedUsersCloseToLimit
}

// seriesCountsForTests should only be used in tests because it holds the mutex while loading all atomic values.
func (t *trackerStore) seriesCountsForTests() map[string]uint64 {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	counts := make(map[string]uint64, len(t.tenants))
	for tenantID, tenant := range t.tenants {
		counts[tenantID] = tenant.series.Load()
	}
	return counts
}

type trackedTenant struct {
	sync.RWMutex
	series       *atomic.Uint64
	currentLimit *atomic.Uint64
	shards       [shards]*tenantshard.Map
}

func zeroAsNoLimit(v uint64) uint64 {
	if v == 0 {
		return noLimit
	}
	return v
}
