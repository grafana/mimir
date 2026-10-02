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

const shards = tenantshard.NumShards
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
	// newShard creates the per-tenant shard maps of the configured implementation.
	newShard tenantshard.Factory

	// config
	idleTimeout                         time.Duration
	userCloseToLimitPercentageThreshold int
	enableVerboseSeriesMetrics          bool
	minTimeBetweenShardsCleanup         time.Duration

	// misc
	logger log.Logger

	// lastSnapshotBytes is the size in bytes of the most recent snapshot this store wrote.
	lastSnapshotBytes atomic.Int64
}

// limiter provides the local series limit for a tenant.
type limiter interface {
	localSeriesLimit(userID string) uint64
	zonesCount() uint64
}

// events provides an abstraction to publish usage-tracker events.
type events interface {
	publishCreatedSeries(ctx context.Context, tenantID string, series []uint64, locality []uint32, timestamp time.Time) error
}

func newTrackerStore(idleTimeout time.Duration, userCloseToLimitPercentageThreshold int, logger log.Logger, l limiter, ev events, enableVerboseSeriesMetrics bool, minTimeBetweenShardsCleanup time.Duration, newShard tenantshard.Factory) *trackerStore {
	t := &trackerStore{
		tenants:                             make(map[string]*trackedTenant),
		limiter:                             l,
		events:                              ev,
		newShard:                            newShard,
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
func (t *trackerStore) trackSeries(ctx context.Context, tenantID string, series []uint64, locality []uint32, timeNow time.Time) (rejectedRefs []uint64, err error) {
	if locality != nil && len(locality) != len(series) {
		return nil, fmt.Errorf("locality hashes length %d does not match series length %d", len(locality), len(series))
	}
	tenant := t.getOrCreateTenant(tenantID)
	defer tenant.RUnlock()

	groupByModuloShards(series, locality)

	now := clock.ToMinutes(timeNow)

	// We don't pool rejectedRefs because we don't have full control of its lifecycle.
	createdRefs := refsPool.Get()[:0]
	var createdLocality []uint32
	i0 := 0
	for i := 1; i <= len(series); i++ {
		// Track series if shard changes on the next element or if we're at the end of series.
		if shard := uint8(series[i0] % shards); i == len(series) || shard != uint8(series[i]%shards) {
			m := tenant.shards[shard]
			m.Lock()
			for j, ref := range series[i0:i] {
				created, rejected := m.Put(ref, now, tenant.series, tenant.currentLimit, true)
				var loc uint32
				if locality != nil {
					loc = locality[i0+j]
				}
				if created {
					createdRefs = append(createdRefs, ref)
					if locality != nil {
						createdLocality = append(createdLocality, loc)
					}
				} else if rejected {
					rejectedRefs = append(rejectedRefs, ref)
				}
				if !rejected {
					t.noteLocality(tenant, m, ref, loc)
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

	if err := t.events.publishCreatedSeries(ctx, tenantID, createdRefs, createdLocality, timeNow); err != nil {
		level.Error(t.logger).Log("msg", "failed to publish created series", "tenant", tenantID, "err", err, "created_len", len(createdRefs), "now", timeNow.Unix(), "now_minutes", now)
		return nil, err
	}

	return rejectedRefs, nil
}

func (t *trackerStore) processCreatedSeriesEvent(tenantID string, series []uint64, locality []uint32, eventTimestamp, timeNow time.Time) {
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

	if locality != nil && len(locality) != len(series) {
		locality = nil
	}
	// Group series by shard. We're going to accept all of them, so we can start on shard 0 here.
	groupByModuloShards(series, locality)

	timestamp := clock.ToMinutes(eventTimestamp)
	i0 := 0
	for i := 1; i <= len(series); i++ {
		// Track series if shard changes on the next element or if we're at the end of series.
		if shard := uint8(series[i0] % shards); i == len(series) || shard != uint8(series[i]%shards) {
			m := tenant.shards[shard]
			m.Lock()
			for j, ref := range series[i0:i] {
				_, _ = m.Put(ref, timestamp, tenant.series, nil, false)
				var loc uint32
				if locality != nil {
					loc = locality[i0+j]
				}
				t.noteLocality(tenant, m, ref, loc)
			}
			m.Unlock()
			i0 = i
		}
	}
}

// noteLocality stores a non-zero locality hash on a series the caller just
// Put, and counts its band the first time one is recorded. The caller holds
// the shard lock. A series that already has a locality keeps it.
func (t *trackerStore) noteLocality(tenant *trackedTenant, m tenantshard.Map, ref uint64, loc uint32) {
	if loc == 0 {
		return
	}
	prev, found := m.SetLocality(ref, loc)
	if !found || prev != 0 {
		if found && prev != loc {
			m.SetLocality(ref, prev)
		}
		return
	}
	tenant.bands.admit(uint16(loc >> 16))
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
		bands:         &bandTable{},
	}
	capacity := int(limit / shards)
	if limit == noLimit || limit == 0 {
		capacity = 512 // let's be modest.
	} else if capacity > math.MaxUint32 {
		capacity = math.MaxUint32
	}
	for i := range tenant.shards {
		tenant.shards[i] = t.newShard(uint32(capacity))
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
	for s := 0; s < shards; s++ {
		var timeAfterFirstTenantCleanup time.Time
		for _, tenant := range tenantsClone {
			shard := tenant.shards[s]

			shard.Lock()
			totalSeries += shard.Count()
			shard.SetRemoveHook(func(loc uint32) {
				if loc != 0 {
					tenant.bands.expire(uint16(loc >> 16))
				}
			})
			removed := shard.Cleanup(watermark, tenant.currentLimit)
			shard.SetRemoveHook(nil)
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

	rows := make([]ShardStats, 0, len(tenantsClone)*shards)
	for tenantID, tenant := range tenantsClone {
		for s := range shards {
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

// tenantBandView is one tenant's band table on this tracker partition.
type tenantBandView struct {
	userID         string
	total          uint64
	localitySeries uint64
	counts         []bandCount
}

// tenantBands returns the retained locality bands. An empty userID returns every tenant.
func (t *trackerStore) tenantBands(userID string) []tenantBandView {
	t.mtx.RLock()
	tenantsClone := maps.Clone(t.tenants)
	sorted := slices.Clone(t.sortedTenants)
	t.mtx.RUnlock()

	out := make([]tenantBandView, 0, len(sorted))
	for _, id := range sorted {
		if userID != "" && id != userID {
			continue
		}
		tenant := tenantsClone[id]
		localitySeries, counts := tenant.bands.snapshot()
		out = append(out, tenantBandView{
			userID:         id,
			total:          tenant.series.Load(),
			localitySeries: localitySeries,
			counts:         counts,
		})
	}
	return out
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
	shards       [shards]tenantshard.Map

	seriesCreated *atomic.Uint64
	seriesRemoved *atomic.Uint64

	// bands is the hottest locality bands of this tenant on this tracker partition.
	bands *bandTable
}

// lastSnapshotBytes is the size of the most recent snapshot this store wrote.
// It lives on the store so the collector can export it; the partition handler sets it.
func (t *trackerStore) setLastSnapshotBytes(n int64) { t.lastSnapshotBytes.Store(n) }

func zeroAsNoLimit(v uint64) uint64 {
	if v == 0 {
		return noLimit
	}
	return v
}

// groupByModuloShards sorts series by shard to minimize lock contention by taking mutex once for each shard.
// It arranges the series hashes into contiguous groups of hashes of same modulo shards.
// This is O(N), specifically it iterates all series twice, and makes the re-arrangement in place.
func groupByModuloShards(series []uint64, locality []uint32) {
	var counts, pos [shards]int
	// count how many series belong to each shard.
	// This will be later "the number of series from each shard correctly placed"
	// This is the first O(series)
	for _, ref := range series {
		counts[ref%shards]++
	}
	// pos is where each shard's next element should be
	// We'll update this as we check the elements.
	for i := 1; i < shards; i++ {
		pos[i] = pos[i-1] + counts[i-1]
	}

	for i := 0; i < len(series); i++ {
		for mod := series[i] % shards; counts[mod] > 0; mod = series[i] % shards {
			// put this element where it should be, swap them
			series[pos[mod]], series[i] = series[i], series[pos[mod]]
			if locality != nil {
				locality[pos[mod]], locality[i] = locality[i], locality[pos[mod]]
			}
			// if there's next element for this mod, it's on the next position
			pos[mod]++
			// count this element as moved
			counts[mod]--
		}
	}
}
