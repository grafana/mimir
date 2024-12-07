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

const shards = 128
const noLimit = math.MaxUint64

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
	publishCreatedSeries(ctx context.Context, tenantID string, series []uint64, timestamp time.Time) error
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
	series atomic.Uint64
	refs   atomic.Uint64
}

func (t *tenantInfo) release() { t.refs.Dec() }

func (t *trackerStore) seriesCounts() map[string]uint64 {
	t.tenantsMtx.RLock()
	defer t.tenantsMtx.RUnlock()

	counts := make(map[string]uint64, len(t.tenants))
	for tenantID, info := range t.tenants {
		counts[tenantID] = info.series.Load()
	}
	return counts
}

// trackSeries is used in tests so we can provide custom time.Now() value.
// trackSeries will modify and reuse the input series slice.
func (t *trackerStore) trackSeries(ctx context.Context, tenantID string, series []uint64, timeNow time.Time) (rejected []uint64, err error) {
	info := t.getOrCreateTenantInfo(tenantID)
	defer info.release()

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
	if err := t.events.publishCreatedSeries(ctx, tenantID, created, timeNow); err != nil {
		level.Error(t.logger).Log("msg", "failed to publish created series", "tenant", tenantID, "err", err, "created_len", len(created), "now", timeNow.Unix(), "now_minutes", now)
		return nil, err
	}

	return rejected, nil
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

	info := t.getOrCreateTenantInfo(tenantID)
	defer info.release()

	// We need the limit to create the tenant shard of an appropriate size.
	// We are not going to limit series creation based on the shard.
	limit := t.limiter.localSeriesLimit(tenantID)
	if limit == 0 {
		limit = noLimit
	}

	// Sort series by shard. We're going to accept all of them, so we can start on shard 0 here.
	slices.SortFunc(series, func(a, b uint64) int { return int(a%shards) - int(b%shards) })

	timestamp := toMinutes(eventTimestamp)
	i0 := 0
	for i := 1; i <= len(series); i++ {
		// Track series if shard changes on the next element or if we're at the end of series.
		if currentShard := uint8(series[i0] % shards); i == len(series) || currentShard != uint8(series[i]%shards) {
			shard := t.getOrCreateTenantShard(tenantID, currentShard, limit)
			shard.processCreatedSeriesEvent(series[i0:i], timestamp, &info.series)
			i0 = i
		}
	}
}

func (t *trackerStore) getOrCreateTenantShard(userID string, shard uint8, limit uint64) *tenantShard {
	t.lock[shard].RLock()
	m := t.data[shard][userID]
	if m != nil {
		t.lock[shard].RUnlock()
		return m
	}
	t.lock[shard].RUnlock()

	t.lock[shard].Lock()
	defer t.lock[shard].Unlock()
	if m = t.data[shard][userID]; m != nil {
		return m
	}

	var series map[uint64]*atomic.Int32
	if limit == noLimit {
		series = make(map[uint64]*atomic.Int32)
	} else {
		series = make(map[uint64]*atomic.Int32, int(limit/shards))
	}
	m = &tenantShard{series: series}
	t.data[shard][userID] = m
	return m
}

// getOrCreateTenantInfo returns the tenantInfo for the tenantID.
// Caller should call tenantInfo.release() after using it.
func (t *trackerStore) getOrCreateTenantInfo(tenantID string) *tenantInfo {
	t.tenantsMtx.RLock()
	if info, ok := t.tenants[tenantID]; ok {
		// It is important to increase references count before we release the lock.
		info.refs.Inc()
		t.tenantsMtx.RUnlock()
		return info
	}
	t.tenantsMtx.RUnlock()

	t.tenantsMtx.Lock()
	defer t.tenantsMtx.Unlock()
	if info, ok := t.tenants[tenantID]; ok {
		info.refs.Inc()
		return info
	}

	info := &tenantInfo{}
	info.refs.Inc()
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
	tenantsClone := maps.Clone(t.tenants)
	t.tenantsMtx.RUnlock()

	for i := range t.data {
		t.lock[i].Lock()
		// Take a clone of data and work with that.
		// This is orders of tens of thousands elements map, so it's not a big deal.
		// It's better than holding the mutex while we're inspecting each tenant.
		// We don't care about the tenants added *after* we took the snapshot, they're too new to cleanup.
		shardClone := maps.Clone(t.data[i])
		t.lock[i].Unlock()

		for tenantID, shard := range shardClone {
			if empty := t.cleanupTenantShard(tenantID, shard, watermark, tenantsClone); empty {
				t.maybeRemoveTenantShard(tenantID, uint8(i))
			}
		}
	}

	tenantsToDelete := make([]string, 0, len(tenantsClone))
	// Check tenants to delete on our copy of t.tenants.
	// The ones added after we took the copy are likely to have some series.
	for tenantID, info := range tenantsClone {
		if info.series.Load() == 0 {
			tenantsToDelete = append(tenantsToDelete, tenantID)
			continue
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
			continue
		}
		if info.refs.Load() == 0 {
			// No series and nobody is adding them, so we can delete it.
			delete(t.tenants, tenantID)
		}
	}
}

func (t *trackerStore) cleanupTenantShard(tenantID string, shard *tenantShard, watermark minutes, tenantsClone map[string]*tenantInfo) (empty bool) {
	info, ok := tenantsClone[tenantID]
	if !ok {
		// This shard might have been added after we made our clone.
		info = t.getOrCreateTenantInfo(tenantID)
		// We need to release this one correctly.
		defer info.release()
	}
	return shard.cleanup(&info.series, watermark)
}

func (t *trackerStore) maybeRemoveTenantShard(tenantID string, s uint8) {
	info := t.getOrCreateTenantInfo(tenantID)
	defer info.release()

	t.lock[s].Lock()
	defer t.lock[s].Unlock()
	if info.refs.Load() > 1 { // 1 for us.
		// Someone else is interacting with this tenant.
		return
	}

	shard, ok := t.data[s][tenantID]
	if !ok {
		return // how did this happen?
	}
	if !shard.empty() {
		return // someone added series while we were thinking.
	}

	// Okay, nobody is interacting with the tenant, and shard is empty, now it's safe to delete it.
	delete(t.data[s], tenantID)
}

type tenantShard struct {
	shardLock
	series map[uint64]*atomic.Int32
}

func (shard *tenantShard) empty() bool {
	shard.RLock()
	defer shard.RUnlock()
	return len(shard.series) == 0
}

// trackSeries will track the provided series trying to keep totalTenantSeries below the limit.
// Each time a series is created, it will be appended to the created slice and totalTenantSeries will be increased.
//
// The input slices created and rejected should be returned with the series that were created and rejected appended to them.
func (shard *tenantShard) trackSeries(series []uint64, now minutes, totalTenantSeries *atomic.Uint64, limit uint64, created, rejected []uint64) (_created, _rejected []uint64) {
	// missing contains the series that have to be created.
	// As we advance through series, we reuse the same slice to avoid allocations.
	missing := series[:0]

	// First try to update the series that already exist, the ones that don't exist are moved to missing.
	shard.RLock()
	for len(series) > 0 {
		s := series[0]
		series = series[1:]

		ts, ok := shard.series[s]
		if !ok {
			missing = append(missing, s)
			continue
		}

		ts.Store(int32(now))
	}
	shard.RUnlock()

	// If no series missing, return.
	if len(missing) == 0 {
		return created, rejected
	}

	// Only take the Lock() if we're below the limit.
	if totalTenantSeries.Load() < limit {
		shard.Lock()
		for len(missing) > 0 {
			s := missing[0]
			ts, ok := shard.series[s]
			if ok {
				ts.Store(int32(now))
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
			missing = missing[1:]
		}
		shard.Unlock()
	}

	// Whatever is still missing is rejected (if any)
	rejected = append(rejected, missing...)
	return created, rejected
}

// processCreatedSeriesEvent creates the series coming from an event from Kafka,
// i.e. series that were created by a different instance.
// This does not check the limits, as we prioritize staying consistent across replicas over enforcing limits.
// Timestamp is not updated if series exists already.
func (shard *tenantShard) processCreatedSeriesEvent(series []uint64, timestamp minutes, totalTenantSeries *atomic.Uint64) {
	// missing contains the series that have to be created.
	// As we advance through series, we reuse the same slice to avoid allocations.
	missing := series[:0]

	// Find the missing ones.
	shard.RLock()
	for len(series) > 0 {
		s := series[0]
		series = series[1:]

		_, ok := shard.series[s]
		if !ok {
			missing = append(missing, s)
		}
	}
	shard.RUnlock()

	if len(missing) == 0 {
		return
	}

	shard.Lock()
	for _, s := range missing {
		_, ok := shard.series[s]
		if !ok {
			// Still doesn't exist, so create with the timestamp from the event.
			shard.series[s] = atomic.NewInt32(int32(timestamp))
			totalTenantSeries.Inc()
		}
	}
	shard.Unlock()
}

func (shard *tenantShard) cleanup(totalTenantSeries *atomic.Uint64, watermark minutes) (empty bool) {
	// Work on the stack.
	// If we have 1e9 series, 33% of that churning every 2 hours, that's ~21K per shard (1e9/3/120/128).
	// This is ~168K on the stack, which is fine, but it saves lots of allocations.
	// This should only make the stack grow once.
	var stackSeries [1 << 16]uint64
	candidates := stackSeries[:0]

	shard.RLock()
	empty = len(shard.series) == 0
	for s, ts := range shard.series {
		if lastSeen := minutes(ts.Load()); watermark.greaterOrEqualThan(lastSeen) {
			candidates = append(candidates, s)
		}
	}
	shard.RUnlock()

	if len(candidates) == 0 {
		return empty
	}

	shard.Lock()
	defer shard.Unlock()
	for s, ts := range shard.series {
		if lastSeen := minutes(ts.Load()); watermark.greaterOrEqualThan(lastSeen) {
			delete(shard.series, s)
			totalTenantSeries.Dec()
		}
	}
	return len(shard.series) == 0
}

func areInValidSpanToCompareMinutes(a, b time.Time) bool {
	if a.After(b) {
		a, b = b, a
	}
	return b.Sub(a) < time.Hour
}

func toMinutes(t time.Time) minutes {
	return minutes(t.Sub(t.Truncate(2 * time.Hour)).Minutes())
}

// minutes represents the minutes passed since the last 2-hour boundary (00:00, 02:00, 04:00, etc.).
// This value only makes sense within the last hour.
type minutes int32

// sub subtracts other from m, taking into account the 2-hour clock face, assuming both values aren't more than 1h apart.
// It does *not* return *minutes* because it returns a duration, while minutes is a timestamp.
func (m minutes) sub(other minutes) int {
	sign := 1
	if m < other {
		m, other, sign = other, m, -1
	}

	if m-other < 60 {
		return sign * int(m-other)
	}
	return sign * int(m-other-2*60)
}

// minutesGreater returns true if this value is greater than other on a four-hour clock face assuming that none of the values is ever older than 1h.
func (m minutes) greaterThan(other minutes) bool {
	if m > other {
		return m-other < 60
	}
	return m+(2*60)-other < 60
}

// greaterOrEqualThan returns true if this value is greater or equal than other on a four-hour clock face assuming that none of the values is ever older than 1h.
func (m minutes) greaterOrEqualThan(other minutes) bool {
	return m == other || m.greaterThan(other)
}

func (m minutes) String() string { return fmt.Sprintf("%dm", m) }
