// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// TestReadcache_AdminPage_RendersOwnedPartitionsAndRanges exercises
// the readcache admin page end-to-end: load some current and
// historical ranges into a running readcache via the same RPC path
// the rebalancer uses, then verify the rendered HTML reflects them.
// The test asserts on the data the page is supposed to convey
// (partition ids, current vs historical range counts, hex range
// formatting) rather than full HTML structure so cosmetic template
// edits don't bounce CI.
func TestReadcache_AdminPage_RendersOwnedPartitionsAndRanges(t *testing.T) {
	cfg := newTestConfig(t, true, 4)
	cfg.OwnedPartitions = "0,1"

	limits := validation.NewOverrides(validation.Limits{}, nil)
	r, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	// Block the background series walker for the duration of the
	// test. running() spawns refreshSeriesStats in a goroutine at
	// startup; if it lands between our setHashRanges and
	// applyWalkResult calls below it would walk an empty head,
	// see count=0 for [0,99] in P0's historical, GC the entry, and
	// invalidate the snapshot we capture. Acquiring the lock
	// before StartAndAwaitRunning is the only way to win that race
	// — refreshSeriesStats TryLocks and returns immediately when
	// it fails.
	r.seriesWalkMu.Lock()
	defer r.seriesWalkMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, r) }()

	// Initial assignment: P0 owns [0, 99], P1 owns [100, 199].
	_, err = r.setHashRanges(ctx, &ingester_client.SetHashRangesRequest{
		Ranges: []ingester_client.HashRangeEntry{
			{Lo: 0, Hi: 99, PartitionId: 0},
			{Lo: 100, Hi: 199, PartitionId: 1},
		},
	})
	require.NoError(t, err)

	// Slicer round moves [0, 99] off P0 onto P1 — this leaves
	// historical residue on P0.
	_, err = r.setHashRanges(ctx, &ingester_client.SetHashRangesRequest{
		Ranges: []ingester_client.HashRangeEntry{
			{Lo: 100, Hi: 199, PartitionId: 1},
			{Lo: 0, Hi: 99, PartitionId: 1},
		},
	})
	require.NoError(t, err)

	// Inject walker results so the page has non-zero per-range counts.
	r.partitionMu.RLock()
	p0 := r.partitions[0]
	p1 := r.partitions[1]
	r.partitionMu.RUnlock()
	require.NotNil(t, p0)
	require.NotNil(t, p1)

	p0Snap := p0.ranges.rangesSnapshot()
	require.Equal(t, []assignment.HashRange{hr(0, 99)}, p0Snap,
		"P0 should have its formerly-current range [0,99] as historical now")
	require.True(t, p0.ranges.applyWalkResult(p0Snap, []int64{1234}, []string{`{__name__="http_requests_total",instance="host-7",job="readcache"}`}))

	p1Snap := p1.ranges.rangesSnapshot()
	require.Equal(t, []assignment.HashRange{hr(0, 99), hr(100, 199)}, p1Snap)
	require.True(t, p1.ranges.applyWalkResult(p1Snap, []int64{56, 78}, []string{`{__name__="cpu_seconds_total",instance="host-3"}`, `{__name__="memory_rss",instance="host-9"}`}))

	// Now hit ServeHTTP.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, AdminPathPrefix, nil)
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "text/html")

	body := rec.Body.String()

	assert.Contains(t, body, cfg.InstanceID, "instance ID should be in the page")
	assert.Contains(t, body, "P0", "owned partition 0 should be listed")
	assert.Contains(t, body, "P1", "owned partition 1 should be listed")

	// Range formatting (8-hex lo-hi). The current and historical
	// ranges should both render. P1's current ranges are [0, 99]
	// and [100, 199]; P0's historical range is [0, 99].
	assert.Contains(t, body, "00000000-00000063", "[0, 99] should render as 8-hex")
	assert.Contains(t, body, "00000064-000000c7", "[100, 199] should render as 8-hex")

	// The per-range counts should appear. fmtSeries renders 1234 as
	// "1.23K", 56 and 78 as plain integers.
	assert.Contains(t, body, "1.23K", "residue count should render")
	assert.True(t, strings.Contains(body, ">56<") || strings.Contains(body, "56\n"), "p1 growth count should render")

	// Example series captured by the (mocked) walker should render
	// next to their ranges. html/template auto-escapes quotes, so
	// look for the metric names directly.
	assert.Contains(t, body, "http_requests_total", "P0 historical range example should render")
	assert.Contains(t, body, "cpu_seconds_total", "P1 current range example #1 should render")
	assert.Contains(t, body, "memory_rss", "P1 current range example #2 should render")

	// The "no partitions" empty-state must NOT appear because we own
	// two partitions.
	assert.NotContains(t, body, "owns no partitions",
		"the empty-state message must not render when we own partitions")
}

// TestReadcache_AdminPage_ListsManagedTSDBs verifies the "Managed
// TSDBs" listing surfaces both the live TSDB this pod is ingesting
// into and the read-only frozen epoch retained for a partition that
// moved away, with their partition/epoch, offset span, active flag
// and (for the frozen epoch) the computed reap-time expiry.
func TestReadcache_AdminPage_ListsManagedTSDBs(t *testing.T) {
	const (
		activeTenant = "tenant-active"
		frozenTenant = "tenant-frozen"
		activePID    = int32(7)
		frozenPID    = int32(9)
	)

	cfg := newTestConfig(t, false, 0)
	cfg.LocalBlockRetention = time.Hour
	limits := validation.NewOverrides(validation.Limits{}, nil)

	r := &Readcache{
		logger:     log.NewNopLogger(),
		cfg:        cfg,
		limits:     limits,
		partitions: map[int32]*partitionState{},
		frozen:     map[int32][]*frozenEpoch{},
		epochSeq:   map[int32]int{},
	}

	appendSample := func(db *partitionTSDB, metric string, ts int64) {
		app := db.Appender(context.Background())
		_, err := app.Append(0, labels.FromStrings(model.MetricNameLabel, metric), ts, 1)
		require.NoError(t, err)
		require.NoError(t, app.Commit())
	}

	// Live partition: a TSDB this pod is actively ingesting into.
	activeDB, err := openPartitionTSDB(activeTenant, activePID, 0, cfg.DataDir, cfg.BlocksStorage.TSDB,
		cfg.LocalBlockRetention, limits, 0, nil, nil, nil, prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, err)
	appendSample(activeDB, "active_metric", time.Now().Add(-1*time.Minute).UnixMilli())

	activeStartedAt := time.Now().Add(-30 * time.Minute)
	activeP := newPartitionState(activePID)
	activeP.tenants[activeTenant] = activeDB
	activeP.startOffset.Store(1000)
	activeP.startedConsumingAt.Store(activeStartedAt.UnixMilli())
	activeP.warm.Store(true)
	r.partitions[activePID] = activeP

	// A second partition that we freeze, leaving a read-only epoch.
	frozenDB, err := openPartitionTSDB(frozenTenant, frozenPID, 0, cfg.DataDir, cfg.BlocksStorage.TSDB,
		cfg.LocalBlockRetention, limits, 0, nil, nil, nil, prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, err)
	frozenSampleTS := time.Now().Add(-2 * time.Minute).UnixMilli()
	appendSample(frozenDB, "frozen_metric", frozenSampleTS)

	frozenStartedAt := time.Now().Add(-45 * time.Minute)
	frozenP := newPartitionState(frozenPID)
	frozenP.tenants[frozenTenant] = frozenDB
	frozenP.startOffset.Store(500)
	frozenP.startedConsumingAt.Store(frozenStartedAt.UnixMilli())
	freezeAt := time.Now()
	require.NoError(t, r.freezePartition(frozenPID, frozenP))

	data := r.buildAdminPageData()
	require.Equal(t, 1, data.NumActiveTSDBs)
	require.Equal(t, 1, data.NumFrozenTSDBs)
	require.Len(t, data.TSDBs, 2)

	// Active TSDB sorts first.
	active := data.TSDBs[0]
	assert.Equal(t, activeTenant, active.Tenant)
	assert.Equal(t, activePID, active.PartitionID)
	assert.True(t, active.Active)
	assert.True(t, active.Warm)
	assert.Equal(t, int64(1000), active.StartOffset)
	assert.NotEmpty(t, active.MaxT, "active TSDB with a sample should report a data max")
	assert.Empty(t, active.Expires, "an active TSDB has no fixed expiry")
	assert.Equal(t, activeStartedAt.UTC().Format(time.RFC3339), active.StartedConsuming)
	assert.Empty(t, active.StoppedConsuming, "an active TSDB is still being consumed")

	frozen := data.TSDBs[1]
	assert.Equal(t, frozenTenant, frozen.Tenant)
	assert.Equal(t, frozenPID, frozen.PartitionID)
	assert.False(t, frozen.Active)
	assert.Equal(t, int64(500), frozen.StartOffset)
	wantExpiry := time.UnixMilli(frozenSampleTS).Add(cfg.LocalBlockRetention + frozenEpochReapGrace).UTC().Format(time.RFC3339)
	assert.Equal(t, wantExpiry, frozen.Expires, "frozen epoch expiry should be maxT + retention + grace")
	assert.False(t, frozen.Expired, "a freshly-frozen epoch within retention is not yet reapable")
	assert.Equal(t, frozenStartedAt.UTC().Format(time.RFC3339), frozen.StartedConsuming)
	assert.NotEmpty(t, frozen.StoppedConsuming, "a frozen epoch records when consumption stopped")
	stoppedAt, perr := time.Parse(time.RFC3339, frozen.StoppedConsuming)
	require.NoError(t, perr)
	assert.WithinDuration(t, freezeAt, stoppedAt, time.Minute, "stop time should be the freeze time")

	// Render and sanity-check the visible section.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, AdminPathPrefix, nil)
	r.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "Managed TSDBs")
	assert.Contains(t, body, activeTenant)
	assert.Contains(t, body, frozenTenant)
	assert.Contains(t, body, "[active")
	assert.Contains(t, body, "[frozen]")
	assert.Contains(t, body, "1000", "active start offset should render")
	assert.Contains(t, body, "500", "frozen start offset should render")
	assert.NotContains(t, body, "holding no TSDBs open")
}

// TestReadcache_AdminPage_ShowsBlockDetails verifies the per-TSDB
// detail panel surfaces the persisted-block breakdown (ULID, time
// bounds, series/sample counts, size) once the head has been compacted
// into a block.
func TestReadcache_AdminPage_ShowsBlockDetails(t *testing.T) {
	const (
		tenantID  = "tenant-1"
		pid       = int32(2)
		blockSpan = 100 * time.Millisecond
	)

	cfg := newTestConfig(t, false, 0)
	// Keep blocks (no time-retention) and shrink the block range so a
	// couple of samples compact into a real block without spanning 2h.
	cfg.LocalBlockRetention = 0
	cfg.BlocksStorage.TSDB.BlockRanges = mimir_tsdb.DurationList{blockSpan}
	limits := validation.NewOverrides(validation.Limits{}, nil)

	r := &Readcache{
		logger:     log.NewNopLogger(),
		cfg:        cfg,
		limits:     limits,
		partitions: map[int32]*partitionState{},
		frozen:     map[int32][]*frozenEpoch{},
		epochSeq:   map[int32]int{},
	}

	db, err := openPartitionTSDB(tenantID, pid, 0, cfg.DataDir, cfg.BlocksStorage.TSDB,
		cfg.LocalBlockRetention, limits, 0, nil, nil, nil, prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, err)

	app := db.Appender(context.Background())
	for i := 0; i < 3; i++ {
		_, err := app.Append(0, labels.FromStrings(model.MetricNameLabel, "m", "series", string(rune('a'+i))), int64(i), float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
	require.NoError(t, db.CompactHead())
	require.Len(t, db.Blocks(), 1, "head should have compacted into exactly one block")
	wantULID := db.Blocks()[0].Meta().ULID.String()

	p := newPartitionState(pid)
	p.tenants[tenantID] = db
	p.warm.Store(true)
	r.partitions[pid] = p

	data := r.buildAdminPageData()
	require.Len(t, data.TSDBs, 1)
	tsdbView := data.TSDBs[0]
	require.Equal(t, 1, tsdbView.NumBlocks)
	require.Len(t, tsdbView.Blocks, 1)

	block := tsdbView.Blocks[0]
	assert.Equal(t, wantULID, block.ULID)
	assert.Equal(t, uint64(3), block.NumSeries)
	assert.Equal(t, uint64(3), block.NumSamples)
	assert.Positive(t, block.SizeBytes, "block size should be reported")
	assert.NotEmpty(t, block.MinT)
	assert.NotEmpty(t, block.MaxT)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, AdminPathPrefix, nil)
	r.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "Blocks (1)")
	assert.Contains(t, body, wantULID, "block ULID should render in the detail panel")
	assert.NotContains(t, body, "all data is still in the in-memory head")
}

// TestReadcache_AdminPage_EmptyState exercises the path where no
// partitions are owned: the page should still render successfully and
// surface the empty-state message rather than blowing up on an
// empty partitions map.
func TestReadcache_AdminPage_EmptyState(t *testing.T) {
	cfg := newTestConfig(t, true, 4)
	cfg.OwnedPartitions = ""

	limits := validation.NewOverrides(validation.Limits{}, nil)
	r, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, r) }()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, AdminPathPrefix, nil)
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, cfg.InstanceID)
	assert.Contains(t, body, "owns no partitions",
		"empty state copy should appear when no partitions are owned")
}
