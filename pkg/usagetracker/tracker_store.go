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
	events  events

	// misc
	logger log.Logger
}

type limiter interface {
	localSeriesLimit(userID string) uint64
}

type events interface {
	publishCreatedSeries(ctx context.Context, userID string, series []uint64) error
}

func newTrackerStore(logger log.Logger, l limiter, ev events) *trackerStore {
	t := &trackerStore{
		tenants: make(map[string]*tenantInfo),
		limiter: l,
		events:  ev,
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
func (t *trackerStore) trackSeries(ctx context.Context, req *usagetrackerpb.TrackSeriesRequest, now time.Time) (*usagetrackerpb.TrackSeriesResponse, error) {
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

	nowMinutes := toMinutes(now)

	created := make([]uint64, 0, len(series))
	rejected := series[:0]
	i0 := 0
	for i := 1; i <= len(series); i++ {
		if i == len(series) || (series[i]%shards) != (series[i0]%shards) {
			created, rejected = t.trackShardSeries(req.UserID, info, series[i0:i], limit, created, rejected, nowMinutes)
			i0 = i
		}
	}

	level.Debug(t.logger).Log("msg", "tracked series", "userID", req.UserID, "received_len", len(series), "created_len", len(created), "rejected_len", len(rejected), "now", now.Unix(), "now_minutes", nowMinutes)
	if err := t.events.publishCreatedSeries(ctx, req.UserID, created); err != nil {
		level.Error(t.logger).Log("msg", "failed to publish created series", "userID", req.UserID, "err", err, "created_len", len(created), "now", now.Unix(), "now_minutes", nowMinutes)
		return nil, err
	}

	return &usagetrackerpb.TrackSeriesResponse{RejectedSeriesHashes: rejected}, nil
}

func (t *trackerStore) trackShardSeries(userID string, info *tenantInfo, series []uint64, limit uint64, created, rejected []uint64, now minutes) (_created, _rejected []uint64) {
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
				m.series[s] = atomic.NewInt32(int32(now))
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

func casIfGreater(now minutes, ts *atomic.Int32) {
	for val := ts.Load(); now.greaterThan(minutes(val)) && !ts.CompareAndSwap(val, int32(now)); val = ts.Load() {
	}
}

func toMinutes(t time.Time) minutes {
	return minutes(t.Sub(t.Truncate(4 * time.Hour)).Minutes())
}

// minutes represents the minutes passed since the last 4-hour boundary (00:00, 04:00, 08:00, 12:00, 16:00, 20:00).
// This value only makes sense within the last hour (it could make sense in the last 2 hours, but we don't want to get close to the ambiguous values).
type minutes int32

// minutesGreater returns true if this value is greater than otheron a four-hour clock face assuming that none of the values is ever older than 1h.
func (m minutes) greaterThan(other minutes) bool {
	if m > other {
		// true: 40-20 = 20
		// true: 130-100 = 30
		// false: 190-10 = 180 (that 190 is from the previous hour)
		return m-other < 60
	}
	// true: 10, 190: 250-195 = 55
	// false: 10, 120: 250-120 = 130
	return m+(4*60)-other < 60
}
