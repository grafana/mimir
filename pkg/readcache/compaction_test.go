// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"math"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestTimeToNextReadcacheCompaction(t *testing.T) {
	interval := 15 * time.Minute
	zones := []string{"zone-b", "zone-a"} // unsorted on purpose
	hour := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	// Exactly on zone A's slot: the next tick is one full interval later.
	require.Equal(t, interval, timeToNextReadcacheCompaction(hour, interval, "zone-a", zones, log.NewNopLogger()))
	// Zone B is half an interval (7m30s) after zone A.
	require.Equal(t, interval/2, timeToNextReadcacheCompaction(hour, interval, "zone-b", zones, log.NewNopLogger()))
	// One minute into the hour, zone A waits the rest of its slot.
	require.Equal(t, 14*time.Minute, timeToNextReadcacheCompaction(hour.Add(time.Minute), interval, "zone-a", zones, log.NewNopLogger()))

	require.Equal(t, interval, timeToNextReadcacheCompaction(hour, interval, "", zones, log.NewNopLogger()))
	require.Equal(t, interval, timeToNextReadcacheCompaction(hour, interval, "zone-a", []string{"zone-a"}, log.NewNopLogger()))
	require.Equal(t, interval, timeToNextReadcacheCompaction(hour, interval, "zone-c", zones, log.NewNopLogger()))
}

func TestCompactionScheduleJitter(t *testing.T) {
	r := &Readcache{}
	r.cfg.BlocksStorage.TSDB.HeadCompactionInterval = 15 * time.Minute
	r.cfg.BlocksStorage.TSDB.HeadCompactionIntervalJitterEnabled = true
	r.cfg.InstanceRing.InstanceZone = "zone-a"
	// No ring: nothing to stagger against, and no jitter.
	first, standard := r.compactionSchedule(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	require.Equal(t, 15*time.Minute, standard)
	require.Equal(t, 15*time.Minute, first)
}

func TestNextForcedHeadCompactionRange(t *testing.T) {
	const block = int64(1000)

	_, _, ok, last := nextForcedHeadCompactionRange(block, math.MaxInt64, math.MinInt64, 0)
	require.False(t, ok)
	require.True(t, last)

	minT, maxT, ok, last := nextForcedHeadCompactionRange(block, 100, 500, 500)
	require.True(t, ok)
	require.True(t, last)
	require.Equal(t, int64(100), minT)
	require.Equal(t, int64(500), maxT)

	// Spans two block ranges: the first slice stops at the range boundary.
	minT, maxT, ok, last = nextForcedHeadCompactionRange(block, 100, 2500, 2500)
	require.True(t, ok)
	require.False(t, last)
	require.Equal(t, int64(100), minT)
	require.Equal(t, int64(999), maxT)

	// Cut is behind the head min: nothing to compact.
	_, _, ok, _ = nextForcedHeadCompactionRange(block, 2000, 2500, 1500)
	require.False(t, ok)
}

func TestBeginAppendFence(t *testing.T) {
	p := &partitionTSDB{}

	tracked, err := p.beginAppend(100)
	require.NoError(t, err)
	require.True(t, tracked)
	p.endAppend(tracked)

	p.appendState = tsdbAppendForced
	p.forcedMaxTime = 1000
	_, err = p.beginAppend(1000)
	require.ErrorIs(t, err, errTSDBCompactionOverlap)
	tracked, err = p.beginAppend(1001)
	require.NoError(t, err)
	require.False(t, tracked)

	p.appendState = tsdbAppendClosing
	_, err = p.beginAppend(5000)
	require.ErrorIs(t, err, errTSDBClosed)
}

func TestCloseIdleTSDBRemovesEmptyDirectory(t *testing.T) {
	cfg := newTestConfig(t, false, 0)
	cfg.LocalBlockRetention = time.Hour
	limits := validation.NewOverrides(validation.Limits{}, nil)

	db, err := openPartitionTSDB(
		"tenant-1", 7, 0, cfg.DataDir, cfg.BlocksStorage.TSDB, cfg.LocalBlockRetention,
		limits, 0, nil, nil, nil, newTestLookupPlanMetrics(), prometheus.NewRegistry(), log.NewNopLogger(),
	)
	require.NoError(t, err)
	db.touchLastAppend(time.Now().Add(-2 * time.Hour))

	part := newPartitionState(7)
	part.tenants["tenant-1"] = db
	r := &Readcache{
		cfg:        cfg,
		logger:     log.NewNopLogger(),
		partitions: map[int32]*partitionState{7: part},
	}

	require.True(t, r.shouldCloseIdleTSDB(db, time.Now()))
	r.closeIdleTSDB(db)

	_, statErr := os.Stat(db.Dir())
	require.True(t, os.IsNotExist(statErr))
	part.tenantsMu.RLock()
	_, stillThere := part.tenants["tenant-1"]
	part.tenantsMu.RUnlock()
	require.False(t, stillThere)

	// A head that still has series is not closed.
	live, err := openPartitionTSDB(
		"tenant-2", 7, 0, cfg.DataDir, cfg.BlocksStorage.TSDB, cfg.LocalBlockRetention,
		limits, 0, nil, nil, nil, newTestLookupPlanMetrics(), prometheus.NewRegistry(), log.NewNopLogger(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = live.Close() })
	app := live.Appender(context.Background())
	_, err = app.Append(0, labels.FromStrings("__name__", "up"), time.Now().UnixMilli(), 1)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	live.touchLastAppend(time.Now().Add(-2 * time.Hour))
	require.False(t, r.shouldCloseIdleTSDB(live, time.Now()))
}

func TestCloseIdleTSDBDropsTenantWhenCloseFails(t *testing.T) {
	cfg := newTestConfig(t, false, 0)
	cfg.LocalBlockRetention = time.Hour
	limits := validation.NewOverrides(validation.Limits{}, nil)

	db, err := openPartitionTSDB(
		"tenant-1", 7, 0, cfg.DataDir, cfg.BlocksStorage.TSDB, cfg.LocalBlockRetention,
		limits, 0, nil, nil, nil, newTestLookupPlanMetrics(), prometheus.NewRegistry(), log.NewNopLogger(),
	)
	require.NoError(t, err)
	db.touchLastAppend(time.Now().Add(-2 * time.Hour))
	db.closeErrHook = func() error { return os.ErrClosed }

	part := newPartitionState(7)
	part.tenants["tenant-1"] = db
	r := &Readcache{
		cfg:        cfg,
		logger:     log.NewNopLogger(),
		partitions: map[int32]*partitionState{7: part},
	}

	r.closeIdleTSDB(db)

	_, statErr := os.Stat(db.Dir())
	require.True(t, os.IsNotExist(statErr))
	part.tenantsMu.RLock()
	_, stillThere := part.tenants["tenant-1"]
	part.tenantsMu.RUnlock()
	require.False(t, stillThere, "a failed close must not leave the dead TSDB in the map")
	require.True(t, db.IsClosed())
}

func TestCloseIdleTSDBAbortsWhenAlreadyDetached(t *testing.T) {
	cfg := newTestConfig(t, false, 0)
	cfg.LocalBlockRetention = time.Hour
	limits := validation.NewOverrides(validation.Limits{}, nil)

	db, err := openPartitionTSDB(
		"tenant-1", 7, 0, cfg.DataDir, cfg.BlocksStorage.TSDB, cfg.LocalBlockRetention,
		limits, 0, nil, nil, nil, newTestLookupPlanMetrics(), prometheus.NewRegistry(), log.NewNopLogger(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	db.touchLastAppend(time.Now().Add(-2 * time.Hour))

	part := newPartitionState(7)
	// Freeze already moved this TSDB out of the live map.
	r := &Readcache{
		cfg:        cfg,
		logger:     log.NewNopLogger(),
		partitions: map[int32]*partitionState{7: part},
	}

	r.closeIdleTSDB(db)

	_, statErr := os.Stat(db.Dir())
	require.NoError(t, statErr)
	require.False(t, db.IsClosed())
	tracked, err := db.beginAppend(1)
	require.NoError(t, err)
	db.endAppend(tracked)
}

func TestDetachLiveTenantsLeavesIdleClose(t *testing.T) {
	closing := &partitionTSDB{tenantID: "closing", appendState: tsdbAppendClosing}
	closed := &partitionTSDB{tenantID: "closed", closed: true}
	live := &partitionTSDB{tenantID: "live"}

	part := newPartitionState(7)
	part.tenants["closing"] = closing
	part.tenants["closed"] = closed
	part.tenants["live"] = live

	got := detachLiveTenants(part, []*partitionTSDB{closing, closed, live})
	require.Equal(t, map[string]*partitionTSDB{"live": live}, got)

	part.tenantsMu.RLock()
	defer part.tenantsMu.RUnlock()
	require.Equal(t, closing, part.tenants["closing"])
	require.Equal(t, closed, part.tenants["closed"])
	_, stillLive := part.tenants["live"]
	require.False(t, stillLive)
}

func TestListTSDBsForTenantSkipsClosed(t *testing.T) {
	part := newPartitionState(7)
	part.warm.Store(true)
	closed := &partitionTSDB{tenantID: "tenant", closed: true}
	part.tenants["tenant"] = closed

	r := &Readcache{
		partitions: map[int32]*partitionState{7: part},
		frozen: map[int32][]*frozenEpoch{
			7: {{
				tenants: map[string]*partitionTSDB{
					"tenant": {tenantID: "tenant", closed: true},
				},
			}},
		},
	}

	dbs, err := r.listTSDBsForTenant("tenant", &client.QueryAttributionHint{PartitionId: 7})
	require.NoError(t, err)
	require.Empty(t, dbs)
}
