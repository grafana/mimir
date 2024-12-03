package usagetracker

import (
	"context"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
)

const shards = 128

// trackerStore holds the core business logic of the usage-tracker abstraccted in a testable way.
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

	// misc
	logger log.Logger
}

type limiter interface {
	localSeriesLimit(userID string) uint64
}

func newTrackerStore(logger log.Logger, l limiter) *trackerStore {
	t := &trackerStore{
		tenants: make(map[string]*tenantInfo),
		limiter: l,
		logger:  logger,
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

type tenantShard struct {
	shardLock
	series map[uint64]*atomic.Int32
}

type tenantInfo struct {
	series     atomic.Uint64
	lastUpdate atomic.Int64
}

// trackSeries is used in tests so we can provide custom time.Now() value.
func (t *trackerStore) trackSeries(_ context.Context, req *usagetrackerpb.TrackSeriesRequest, now time.Time) (*usagetrackerpb.TrackSeriesResponse, error) {
	info := t.getOrCreateUpdatedTenantInfo(req.UserID, now)
	limit := t.limiter.localSeriesLimit(req.UserID)
	if limit == 0 {
		limit = math.MaxUint64
	}

	// Sort series by shard.
	// Start each tenant on its own shard to avoid hotspots.
	tenantStartingShard := xxhash.Sum64String(req.UserID) % shards
	series := req.SeriesHashes
	slices.SortFunc(series, func(a, b uint64) int {
		return int((a%shards+tenantStartingShard)%shards) - int((b%shards+tenantStartingShard)%shards)
	})

	nowMinutes := int32(now.Sub(now.Truncate(4 * time.Hour)).Minutes())

	created := make([]uint64, 0, len(series))
	rejected := series[:0]
	i0 := 0
	for i := 1; i <= len(series); i++ {
		if i == len(series) || (series[i]%shards) != (series[i0]%shards) {
			created, rejected = t.trackShardSeries(req.UserID, info, series[i0:i], limit, created, rejected, nowMinutes)
			i0 = i
		}
	}

	// TODO: send the created series to Kafka events topic.
	level.Info(t.logger).Log("msg", "tracked series", "userID", req.UserID, "received", len(series), "created", len(created), "rejected", len(rejected))

	return &usagetrackerpb.TrackSeriesResponse{RejectedSeriesHashes: rejected}, nil
}

func (t *trackerStore) trackShardSeries(userID string, info *tenantInfo, series []uint64, limit uint64, created, rejected []uint64, now int32) (_created, _rejected []uint64) {
	shard := series[0] % shards
	m := t.getOrCreateTenantShard(userID, shard, limit)

	// pending contains the series that have to be created.
	// As we advance through series, we reuse the same slice to avoid allocations.
	pending := series[:0]

	// First try to update the series that already exist, the ones that don't exist are moved to pending.
	m.RLock()
	for len(series) > 0 {
		s := series[0]
		series = series[1:]

		ts, ok := m.series[s]
		if !ok {
			pending = append(pending, s)
			continue
		}

		// CAS it if it's higher.
		casIfGreater(now, ts)
	}
	m.RLock()

	// Only take the Lock() if there are pending series to create, and we're below the limit.
	if len(pending) > 0 && info.series.Load() < limit {
		m.Lock()
		for len(pending) > 0 {
			s := pending[0]
			ts, ok := m.series[s]
			if ok {
				// Was created in the meantime.
				casIfGreater(now, ts)
			} else if info.series.Load() >= limit {
				// We've reached the limit.
				// Note that while we're holding the mutex on this shard,
				// other request may have increased the series count in a different shard.
				break
			} else {
				m.series[s] = atomic.NewInt32(now)
				created = append(created, s)
				info.series.Inc()
			}
			pending = pending[1:]
		}
		m.Unlock()
	}

	// Whatever is pending is rejected (if any)
	rejected = append(rejected, pending...)
	return created, rejected
}

func (t *trackerStore) getOrCreateTenantShard(userID string, shard uint64, limit uint64) *tenantShard {
	t.lock[shard].RLock()
	m := t.data[shard][userID]
	t.lock[shard].RUnlock()
	if m != nil {
		return m
	}

	t.lock[shard].Lock()
	defer t.lock[shard].Unlock()
	if m = t.data[shard][userID]; m != nil {
		return m
	}

	m = &tenantShard{series: make(map[uint64]*atomic.Int32, int(limit/shards))}
	t.data[shard][userID] = m
	return m
}

func (t *trackerStore) getOrCreateUpdatedTenantInfo(tenantID string, now time.Time) *tenantInfo {
	t.tenantsMtx.RLock()
	if info, ok := t.tenants[tenantID]; ok {
		// blind update here, we don't care if we overwrite the lastUpdate value with one second earlier.
		info.lastUpdate.Store(now.Unix())
		// don't unlock until we've updated the lastUpdate value.
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
	info.lastUpdate.Store(now.Unix())

	t.tenants[tenantID] = info
	return info
}

func casIfGreater(now int32, ts *atomic.Int32) {
	for val := ts.Load(); minutesGreater(now, val) && !ts.CompareAndSwap(val, now); val = ts.Load() {
	}
}

// minutesGreater returns true if a is greater than b as minutes values on a four-hour clock face assuming that none of the values is ever older than 1h.
func minutesGreater(a, b int32) bool {
	if a > b {
		// true: 40-20 = 20
		// true: 130-100 = 30
		// false: 190-10 = 180 (that 190 is from the previous hour)
		return a-b < 60
	}
	// true: 10, 190: 250-195 = 55
	// false: 10, 120: 250-120 = 130
	return a+(4*60)-b < 60
}
