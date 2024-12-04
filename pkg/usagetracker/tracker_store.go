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

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.uber.org/atomic"
)

const snapshotEncodingVersion = 1

const shards = 128

// trackerStore holds the core business logic of the usage-tracker abstracted in a testable way.
// trackerStore should not depend on wall clock: time.Now() should be always injected as a parameter,
// and timer calls should be made from the outside.
type trackerStore struct {
	// lock[i] locks data[i] map.
	lock [shards]shardLock
	// data[i] map is protected by lock[i].
	data [shards]map[string]*tenantShard

	tenantsMtx sync.RWMutex
	tenants    map[string]*tenantInfo

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
	publishCreatedSeries(ctx context.Context, tenantID string, series []uint64) error
}

func newTrackerStore(idleTimeout time.Duration, logger log.Logger, l limiter, ev events) *trackerStore {
	t := &trackerStore{
		tenants:     make(map[string]*tenantInfo),
		limiter:     l,
		events:      ev,
		logger:      logger,
		idleTimeout: idleTimeout,
	}
	for i := range t.data {
		t.data[i] = make(map[string]*tenantShard)
	}
	return t
}

// shardLock is borrowed from https://github.com/prometheus/prometheus/blob/cd1f8ac129a289be8e7d98b6de57a9ba5814c406/tsdb/agent/series.go#L132-L136
type shardLock struct {
	sync.RWMutex
	// Padding to avoid multiple locks being on the same cache line.
	_ [40]byte
}

// tenantInfo holds the global information about the tenant (specifically its total series amount).
type tenantInfo struct {
	series            atomic.Uint64
	markedForDeletion atomic.Bool
}

// trackSeries is used in tests so we can provide custom time.Now() value.
// trackSeries will modify and reuse the input series slice.
func (t *trackerStore) trackSeries(ctx context.Context, tenantID string, series []uint64, timeNow time.Time) (rejected []uint64, err error) {
	info := t.getOrCreateTenantInfo(tenantID)
	limit := t.limiter.localSeriesLimit(tenantID)
	if limit == 0 {
		limit = math.MaxUint64
	}

	// Sort series by shard.
	// Start each tenant on its own shard to avoid hotspots.
	tenantStartingShard := xxhash.Sum64String(tenantID) % shards
	slices.SortFunc(series, func(a, b uint64) int {
		return int((a%shards+tenantStartingShard)%shards) - int((b%shards+tenantStartingShard)%shards)
	})

	now := toMinutes(timeNow)
	created := make([]uint64, 0, len(series)) // TODO: This should probably be pooled.
	rejected = series[:0]                     // Rejected will never have more series than created, so we can reuse the same slice.
	i0 := 0
	for i := 1; i <= len(series); i++ {
		// Track series if shard changes on the next element or if we're at the end of series.
		if currentShard := uint8(series[i0] % shards); i == len(series) || currentShard != uint8(series[i]%shards) {
			shard := t.getOrCreateTenantShard(tenantID, currentShard, limit)
			created, rejected = shard.trackSeries(series[i0:i], now, &info.series, limit, created, rejected)
			i0 = i
		}
	}

	level.Debug(t.logger).Log("msg", "tracked series", "tenant", tenantID, "received_len", len(series), "created_len", len(created), "rejected_len", len(rejected), "now", timeNow.Unix(), "now_minutes", now)
	if err := t.events.publishCreatedSeries(ctx, tenantID, created); err != nil {
		level.Error(t.logger).Log("msg", "failed to publish created series", "tenant", tenantID, "err", err, "created_len", len(created), "now", timeNow.Unix(), "now_minutes", now)
		return nil, err
	}

	return rejected, nil
}

func (t *trackerStore) getOrCreateTenantShard(userID string, shard uint8, limit uint64) *tenantShard {
	t.lock[shard].RLock()
	m := t.data[shard][userID]
	if m != nil {
		// Signal that we're still using this shard.
		// It's probably cheaper to update this every time rather than checking the value.
		m.markedForDeletion.Store(false)
		t.lock[shard].RUnlock()
		return m
	}
	t.lock[shard].RUnlock()

	t.lock[shard].Lock()
	defer t.lock[shard].Unlock()
	if m = t.data[shard][userID]; m != nil {
		return m
	}

	m = &tenantShard{series: make(map[uint64]*atomic.Int32, int(limit/shards))}
	t.data[shard][userID] = m
	return m
}

func (t *trackerStore) getOrCreateTenantInfo(tenantID string) *tenantInfo {
	t.tenantsMtx.RLock()
	if info, ok := t.tenants[tenantID]; ok {
		// It is important to mark it as not marked for deletion, before we release the read lock.
		info.markedForDeletion.Store(false)
		t.tenantsMtx.RUnlock()
		return info
	}
	t.tenantsMtx.RUnlock()

	t.tenantsMtx.Lock()
	defer t.tenantsMtx.Unlock()
	if info, ok := t.tenants[tenantID]; ok {
		return info
	}

	info := &tenantInfo{}
	t.tenants[tenantID] = info
	return info
}

// casIfGreater will CAS ts to now if now is greater than the value stored in ts.
// It will retry until it's possible to CAS or the condition is not et.
// It returns the last seen value that was stored in ts before CAS-ing.
func casIfGreater(now minutes, ts *atomic.Int32) (lastSeen minutes) {
	lastSeen = minutes(ts.Load())
	for now.greaterThan(lastSeen) && !ts.CompareAndSwap(int32(lastSeen), int32(now)) {
		lastSeen = minutes(ts.Load())
	}
	return lastSeen
}

func (t *trackerStore) cleanup(now time.Time) {
	watermark := toMinutes(now.Add(-t.idleTimeout))

	// We will work on a copy of tenants.
	t.tenantsMtx.RLock()
	tenants := maps.Clone(t.tenants)
	t.tenantsMtx.RUnlock()

	for i := range t.data {
		t.lock[i].Lock()
		// First, remove the tenants that are marked for deletion already.
		// We need to do this while holding the mutex,
		// this way we're sure that nobody updated the markedForDeletion value between the check and deletion.
		for tenantID, m := range t.data[i] {
			if m.markedForDeletion.Load() {
				delete(t.data[i], tenantID)
			}
		}

		// Now take a clone of data and work with that.
		// This is orders of tens of thousands elements map, so it's not a big deal.
		// It's better than holding the mutex while we're inspecting each tenant.
		// We don't care about the tenants added *after* we took the snapshot, they're too new to cleanup.
		shard := maps.Clone(t.data[i])
		t.lock[i].Unlock()

		for tenantID, info := range tenants {
			// Check if this tenant has data in this shard.
			if m, ok := shard[tenantID]; ok {
				m.cleanup(&info.series, watermark)
			}
		}
	}

	tenantsToDelete := make([]string, 0, len(tenants))
	// Check tenants to delete on our copy of t.tenants.
	// The ones added after we took the copy are likely to have some series.
	for tenantID, info := range tenants {
		if info.markedForDeletion.Load() {
			tenantsToDelete = append(tenantsToDelete, tenantID)
			continue
		}
		if info.series.Load() == 0 {
			// No series, mark for deletion on the next cleanup.
			info.markedForDeletion.Store(true)
		}
	}
	if len(tenantsToDelete) == 0 {
		return
	}

	t.tenantsMtx.Lock()
	defer t.tenantsMtx.Unlock()
	for _, tenantID := range tenantsToDelete {
		info, ok := t.tenants[tenantID]
		if !ok {
			// This shouldn't happen unless we have two concurrent cleanup() calls.
			continue
		}
		if info.series.Load() > 0 {
			// Someone added series in the meantime.
			info.markedForDeletion.Store(false)
			continue
		}
		if info.markedForDeletion.Load() {
			// Was previously marked for deletion.
			// Nobody retrieved it since the last cleanup, so we can safely delete it as we know nobody is adding series for it.
			delete(t.tenants, tenantID)
		}
	}
}

type tenantShard struct {
	shardLock
	series            map[uint64]*atomic.Int32
	markedForDeletion atomic.Bool
}

// trackSeries will track the provided series trying to keep totalTenantSeries below the limit.
// Each time a series is created, it will be appended to the created slice and totalTenantSeries will be increased.
//
// The input slices created and rejected should be returned with the series that were created and rejected appended to them.
func (shard *tenantShard) trackSeries(series []uint64, now minutes, totalTenantSeries *atomic.Uint64, limit uint64, created, rejected []uint64) (_created, _rejected []uint64) {
	// pending contains the series that have to be created.
	// As we advance through series, we reuse the same slice to avoid allocations.
	pending := series[:0]

	// First try to update the series that already exist, the ones that don't exist are moved to pending.
	shard.RLock()
	for len(series) > 0 {
		s := series[0]
		series = series[1:]

		ts, ok := shard.series[s]
		if !ok {
			pending = append(pending, s)
			continue
		}

		// CAS it if it's higher.
		casIfGreater(now, ts)
	}
	shard.RUnlock()

	// Only take the Lock() if there are pending series to create, and we're below the limit.
	if len(pending) > 0 && totalTenantSeries.Load() < limit {
		shard.Lock()
		for len(pending) > 0 {
			s := pending[0]
			ts, ok := shard.series[s]
			if ok {
				// Was created in the meantime.
				casIfGreater(now, ts)
			} else if totalTenantSeries.Load() >= limit {
				// We've reached the limit.
				// Note that while we're holding the mutex on this shard,
				// other request may have increased the series count in a different shard.
				break
			} else {
				shard.series[s] = atomic.NewInt32(int32(now))
				created = append(created, s)
				totalTenantSeries.Inc()
			}
			pending = pending[1:]
		}
		shard.Unlock()
	}

	// Whatever is pending is rejected (if any)
	rejected = append(rejected, pending...)
	return created, rejected
}

func (shard *tenantShard) cleanup(totalTenantSeries *atomic.Uint64, watermark minutes) {
	// Work on the stack.
	// If we have 1e9 series, 33% of that churning every 2 hours, that's ~21K per shard (1e9/3/120/128).
	// This is ~168K on the stack, which is fine, but it saves lots of allocations.
	// This should only make the stack grow once.
	var stackSeries [1 << 16]uint64
	candidates := stackSeries[:0]

	shard.RLock()
	if len(shard.series) == 0 {
		// If it has no series, mark for deletion on the next cleanup, and return.
		shard.markedForDeletion.Store(true)
		shard.RUnlock()
		return
	}

	for s, ts := range shard.series {
		if lastSeen := minutes(ts.Load()); watermark.greaterThan(lastSeen) {
			candidates = append(candidates, s)
		}
	}
	shard.RUnlock()

	if len(candidates) == 0 {
		return
	}

	shard.Lock()
	defer shard.Unlock()
	for s, ts := range shard.series {
		if lastSeen := minutes(ts.Load()); watermark.greaterThan(lastSeen) {
			delete(shard.series, s)
			totalTenantSeries.Dec()
		}
	}
}

func toMinutes(t time.Time) minutes {
	return minutes(t.Sub(t.Truncate(4 * time.Hour)).Minutes())
}

// minutes represents the minutes passed since the last 4-hour boundary (00:00, 04:00, 08:00, 12:00, 16:00, 20:00).
// This value only makes sense within the last hour (it could make sense in the last 2 hours, but we don't want to get close to the ambiguous values).
type minutes int32

// sub subtracts other from m, taking into account the 4-hour clock face, assuming both values aren't more than 1h apart.
// It does *not* return *minutes* because it returns a duration, while minutes is a timestamp.
func (m minutes) sub(other minutes) int {
	sign := 1
	if m < other {
		m, other, sign = other, m, -1
	}

	if m-other < 60 {
		return sign * int(m-other)
	}
	return sign * int(m-other-4*60)
}

// minutesGreater returns true if this value is greater than otheron a four-hour clock face assuming that none of the values is ever older than 1h.
func (m minutes) greaterThan(other minutes) bool {
	if m > other {
		return m-other < 60
	}
	return m+(4*60)-other < 60
}

func (m minutes) String() string { return fmt.Sprintf("%dm", m) }
