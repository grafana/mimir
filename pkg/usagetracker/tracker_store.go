// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"cmp"
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

const noLimit = math.MaxUint64

// trackerStore holds the core business logic of the usage-tracker abstracted in a testable way.
// trackerStore should not depend on wall clock: time.Now() should be always injected as a parameter,
// and timer calls should be made from the outside.
type trackerStore struct {
	// numShards is the number of shards each tenant is split into. It is fixed for the
	// lifetime of the store and encoded into every snapshot so that snapshots written
	// with a different shard count can be detected and discarded on load.
	numShards int

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
	enableVerboseSeriesMetrics          bool
	minTimeBetweenShardsCleanup         time.Duration

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

func newTrackerStore(idleTimeout time.Duration, userCloseToLimitPercentageThreshold int, logger log.Logger, l limiter, ev events, enableVerboseSeriesMetrics bool, minTimeBetweenShardsCleanup time.Duration, numShards int) *trackerStore {
	t := &trackerStore{
		numShards:                           numShards,
		tenants:                             make(map[string]*trackedTenant),
		limiter:                             l,
		events:                              ev,
		logger:                              logger,
		idleTimeout:                         idleTimeout,
		userCloseToLimitPercentageThreshold: userCloseToLimitPercentageThreshold,
		enableVerboseSeriesMetrics:          enableVerboseSeriesMetrics,
		minTimeBetweenShardsCleanup:         minTimeBetweenShardsCleanup,
		sortedUsersCloseToLimit:             nil, // will be populated by updateLimits
	}
	return t
}

// trackSeries is used in tests so we can provide custom time.Now() value.
// trackSeries will modify and reuse the input series slice.
func (t *trackerStore) trackSeries(ctx context.Context, tenantID string, series []uint64, timeNow time.Time) (rejectedRefs []uint64, err error) {
	tenant := t.getOrCreateTenant(tenantID)
	defer tenant.RUnlock()

	groupByModuloShards(series, t.numShards)

	now := clock.ToMinutes(timeNow)
	numShards := uint64(t.numShards)

	// We don't pool rejectedRefs because we don't have full control of its lifecycle.
	createdRefs := refsPool.Get()[:0]
	i0 := 0
	for i := 1; i <= len(series); i++ {
		// Track series if shard changes on the next element or if we're at the end of series.
		if shard := uint8(series[i0] % numShards); i == len(series) || shard != uint8(series[i]%numShards) {
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

	if t.enableVerboseSeriesMetrics && len(createdRefs) > 0 {
		tenant.seriesCreated.Add(uint64(len(createdRefs)))
	}

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

	// Group series by shard. We're going to accept all of them, so we can start on shard 0 here.
	groupByModuloShards(series, t.numShards)

	timestamp := clock.ToMinutes(eventTimestamp)
	numShards := uint64(t.numShards)
	i0 := 0
	for i := 1; i <= len(series); i++ {
		// Track series if shard changes on the next element or if we're at the end of series.
		if shard := uint8(series[i0] % numShards); i == len(series) || shard != uint8(series[i]%numShards) {
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
		series:        atomic.NewUint64(0),
		currentLimit:  atomic.NewUint64(currentSeriesLimit(0, limit, zonesCount)),
		seriesCreated: atomic.NewUint64(0),
		seriesRemoved: atomic.NewUint64(0),
	}
	capacity := int(limit / uint64(t.numShards))
	if limit == noLimit || limit == 0 {
		capacity = 512 // let's be modest.
	} else if capacity > math.MaxUint32 {
		capacity = math.MaxUint32
	}
	tenant.shards = make([]*tenantshard.Map, t.numShards)
	for i := range tenant.shards {
		tenant.shards[i] = tenantshard.New(uint32(capacity), uint32(t.numShards))
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
	var totalTenants, tenantsDeleted, totalSeries, seriesRemoved int
	var deletionCandidates []string
	var totalIntroducedDelay time.Duration
	t0 := time.Now()
	defer func() {
		level.Info(t.logger).Log("msg", "cleanup finished", "duration", time.Since(t0), "total_tenants", totalTenants, "tenants_deleted", tenantsDeleted, "total_series", totalSeries, "series_removed", seriesRemoved, "total_introduced_delay", totalIntroducedDelay)
	}()
	watermark := clock.ToMinutes(now.Add(-t.idleTimeout))

	// We will work on a copy of tenants.
	t.mtx.RLock()
	tenantsClone := maps.Clone(t.tenants)
	t.mtx.RUnlock()
	totalTenants = len(tenantsClone)
	if totalTenants == 0 {
		return
	}

	// Cleanup by shards instead of by tenants, to avoid holding the mutex for a single tenant for too long.
	// See comment below.
	for s := 0; s < t.numShards; s++ {
		var timeAfterFirstTenantCleanup time.Time
		for _, tenant := range tenantsClone {
			shard := tenant.shards[s]

			shard.Lock()
			totalSeries += shard.Count()
			removed := shard.Cleanup(watermark, tenant.currentLimit)
			shard.Unlock()
			if removed > 0 {
				tenant.series.Add(-uint64(removed))
				seriesRemoved += removed
				if t.enableVerboseSeriesMetrics {
					tenant.seriesRemoved.Add(uint64(removed))
				}
			}

			if timeAfterFirstTenantCleanup.IsZero() {
				timeAfterFirstTenantCleanup = time.Now()
			}
		}

		// We're introducing an artificial delay between shards to avoid mutex contention on the tracking path.
		// If we cleanup all shards of same tenant in a row, we may be holding the mutexes for too long,
		// blocking the latency-sensitive trackSeries calls.
		//
		// This is usually not needed in multi-tenant instances, where separate tenants are going to introduce delays
		// between trackSeries calls of different tenants, but in large single-tenant instances we might just block for a while,
		// so we make sure that there's enough delay between different shards.
		if shouldWait := t.minTimeBetweenShardsCleanup - time.Since(timeAfterFirstTenantCleanup); shouldWait > 0 {
			totalIntroducedDelay += shouldWait
			time.Sleep(shouldWait)
		}
	}

	// Check all tenants and see if any of them are now empty.
	// We don't need to take the mutex for this check, and in most situations we won't find any candidates.
	for tenantID, tenant := range tenantsClone {
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
			tenantsDeleted++
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

// ShardStats holds the debug stats of a single shard of a single tenant.
type ShardStats struct {
	Tenant string `json:"tenant"`
	Shard  int    `json:"shard"`
	tenantshard.Stats
}

// shardStats returns the debug stats of every shard of every tenant, sorted by tenant and then by shard.
// It snapshots the tenants under the store's lock and then reads each shard under its own lock,
// so it never holds the store lock while locking shards.
func (t *trackerStore) shardStats() []ShardStats {
	// Work on a copy of tenants, like cleanup() does.
	t.mtx.RLock()
	tenantsClone := maps.Clone(t.tenants)
	t.mtx.RUnlock()

	rows := make([]ShardStats, 0, len(tenantsClone)*t.numShards)
	for tenantID, tenant := range tenantsClone {
		for s := range t.numShards {
			rows = append(rows, ShardStats{
				Tenant: tenantID,
				Shard:  s,
				Stats:  tenant.shards[s].Stats(),
			})
		}
	}

	// maps.Clone iteration order is random, so sort for stable output.
	slices.SortFunc(rows, func(a, b ShardStats) int {
		return cmp.Or(cmp.Compare(a.Tenant, b.Tenant), cmp.Compare(a.Shard, b.Shard))
	})
	return rows
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
	shards       []*tenantshard.Map

	seriesCreated *atomic.Uint64
	seriesRemoved *atomic.Uint64
}

func zeroAsNoLimit(v uint64) uint64 {
	if v == 0 {
		return noLimit
	}
	return v
}

// groupByModuloShards sorts series by shard to minimize lock contention by taking mutex once for each shard.
// It arranges the series hashes into contiguous groups of hashes of same modulo numShards.
// This is O(N), specifically it iterates all series twice, and makes the re-arrangement in place.
// The scratch arrays are sized to tenantshard.MaxNumShards and used up to numShards, so this
// is allocation-free regardless of the configured shard count.
func groupByModuloShards(series []uint64, numShards int) {
	var counts, pos [tenantshard.MaxNumShards]int
	mod64 := uint64(numShards)
	// count how many series belong to each shard.
	// This will be later "the number of series from each shard correctly placed"
	// This is the first O(series)
	for _, ref := range series {
		counts[ref%mod64]++
	}
	// pos is where each shard's next element should be
	// We'll update this as we check the elements.
	for i := 1; i < numShards; i++ {
		pos[i] = pos[i-1] + counts[i-1]
	}

	for i := 0; i < len(series); i++ {
		for mod := series[i] % mod64; counts[mod] > 0; mod = series[i] % mod64 {
			// put this element where it should be, swap them
			series[pos[mod]], series[i] = series[i], series[pos[mod]]
			// if there's next element for this mod, it's on the next position
			pos[mod]++
			// count this element as moved
			counts[mod]--
		}
	}
}
