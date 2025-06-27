// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
	"github.com/grafana/mimir/pkg/usagetracker/tenantshard"
)

const shards = tenantshard.Shards
const noLimit = math.MaxUint64

// trackerStore holds the core business logic of the usage-tracker abstracted in a testable way.
// trackerStore should not depend on wall clock: time.Now() should be always injected as a parameter,
// and timer calls should be made from the outside.
type trackerStore struct {
	mtx     sync.RWMutex
	tenants map[string]*trackedTenant

	// dependencies
	limiter limiter
	events  events

	// config
	idleTimeout time.Duration

	// misc
	logger log.Logger
}

// limiter provides the local series limit for a tenant.
type limiter interface {
	localSeriesLimit(userID string) uint64
}

// events provides an abstraction to publish usage-tracker events.
type events interface {
	publishCreatedSeries(ctx context.Context, tenantID string, series []uint64, timestamp time.Time) error
}

func newTrackerStore(idleTimeout time.Duration, logger log.Logger, l limiter, ev events) *trackerStore {
	t := &trackerStore{
		tenants:     make(map[string]*trackedTenant),
		limiter:     l,
		events:      ev,
		logger:      logger,
		idleTimeout: idleTimeout,
	}
	return t
}

func (t *trackerStore) seriesCounts() map[string]uint64 {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	counts := make(map[string]uint64, len(t.tenants))
	for tenantID, tenant := range t.tenants {
		counts[tenantID] = tenant.series.Load()
	}
	return counts
}

// trackSeries is used in tests so we can provide custom time.Now() value.
// trackSeries will modify and reuse the input series slice.
func (t *trackerStore) trackSeries(ctx context.Context, tenantID string, series []uint64, timeNow time.Time) (rejectedRefs []uint64, err error) {
	limit := zeroAsNoLimit(t.limiter.localSeriesLimit(tenantID))
	tenant := t.getOrCreateTenant(tenantID, limit)
	defer tenant.RUnlock()

	// Sort series by shard.
	// Start each tenant on its own shard to avoid hotspots.
	tenantStartingShard := xxhash.Sum64String(tenantID) % shards
	slices.SortFunc(series, func(a, b uint64) int {
		return int((a%shards+tenantStartingShard)%shards) - int((b%shards+tenantStartingShard)%shards)
	})

	now := clock.ToMinutes(timeNow)

	// TODO: Pool this slice.
	var createdRefs []uint64
	i0 := 0
	for i := 1; i <= len(series); i++ {
		// Track series if shard changes on the next element or if we're at the end of series.
		if shard := uint8(series[i0] % shards); i == len(series) || shard != uint8(series[i]%shards) {
			m := tenant.shards[shard]
			m.Lock()
			for _, ref := range series[i0:i] {
				if created, rejected := m.Put(ref, now, tenant.series, limit, true); created {
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

	limit := zeroAsNoLimit(t.limiter.localSeriesLimit(tenantID))
	tenant := t.getOrCreateTenant(tenantID, limit)
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
				_, _ = m.Put(ref, timestamp, tenant.series, limit, false)
			}
			m.Unlock()
			i0 = i
		}
	}
}

// getOrCreateTenant returns the trackedTenant for the given tenantID, with shards for the limit provided.
// The tenant returned is RLock'ed() and needs to be RUnlocked() after use.
func (t *trackerStore) getOrCreateTenant(tenantID string, limit uint64) *trackedTenant {
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
		series: atomic.NewUint64(0),
	}
	capacity := int(limit / shards)
	if limit == noLimit || limit == 0 {
		capacity = 512 // let's be modest.
	} else if capacity > math.MaxUint32 {
		capacity = math.MaxUint32
	}
	for i := range tenant.shards {
		tenant.shards[i] = tenantshard.New(uint32(capacity), uint8(i), shards)
	}
	t.tenants[tenantID] = tenant
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
			shard.Cleanup(watermark, tenant.series)
			shard.Unlock()
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
		}
		tenant.Unlock()
	}
	t.mtx.Unlock()
}

type trackedTenant struct {
	sync.RWMutex
	series *atomic.Uint64
	shards [shards]*tenantshard.Map
}

func zeroAsNoLimit(v uint64) uint64 {
	if v == 0 {
		return noLimit
	}
	return v
}
