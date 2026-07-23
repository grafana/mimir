// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestPartitionEpochDir(t *testing.T) {
	// Epoch 0 keeps the legacy layout so the common single-epoch case
	// is unchanged; later epochs get a suffix to avoid colliding with
	// a still-open frozen epoch of the same partition.
	assert.Equal(t, filepath.Join("/data", "tenant-1", "partition-3"),
		partitionEpochDir("/data", "tenant-1", 3, 0))
	assert.Equal(t, filepath.Join("/data", "tenant-1", "partition-3-epoch-1"),
		partitionEpochDir("/data", "tenant-1", 3, 1))
	assert.Equal(t, filepath.Join("/data", "tenant-1", "partition-3-epoch-2"),
		partitionEpochDir("/data", "tenant-1", 3, 2))

	partitionID, epoch, ok := parsePartitionEpochDirName("partition-3")
	require.True(t, ok)
	assert.Equal(t, int32(3), partitionID)
	assert.Zero(t, epoch)

	partitionID, epoch, ok = parsePartitionEpochDirName("partition-3-epoch-2")
	require.True(t, ok)
	assert.Equal(t, int32(3), partitionID)
	assert.Equal(t, 2, epoch)

	for _, invalid := range []string{"partition", "partition-x", "partition-3-epoch-0", "partition-3-epoch-x", "other-3"} {
		_, _, ok := parsePartitionEpochDirName(invalid)
		assert.False(t, ok, invalid)
	}
}

// TestReadcache_FreezeKeepsSliceQueryableThenReaps is the storage-side
// counterpart of the distributor's interval fan-out: after a partition
// moves away, the previous owner must keep serving the slice it
// ingested until the slice ages out of the readcache horizon.
func TestReadcache_FreezeKeepsSliceQueryableThenReaps(t *testing.T) {
	const tenantID = "tenant-1"
	const pid = int32(3)

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

	// Stand up a live partition with one committed sample two minutes
	// in the past, then publish it into r.partitions as epoch 0.
	db, err := openPartitionTSDB(tenantID, pid, 0, cfg.DataDir, cfg.BlocksStorage.TSDB,
		cfg.LocalBlockRetention, limits, 0, nil, nil, nil, prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, err)

	sampleTS := time.Now().Add(-2 * time.Minute).UnixMilli()
	app := db.Appender(context.Background())
	_, err = app.Append(0, labels.FromStrings(model.MetricNameLabel, "up"), sampleTS, 1)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	p := newPartitionState(pid)
	p.tenants[tenantID] = db
	r.partitions[pid] = p

	hint := &client.QueryAttributionHint{PartitionId: pid}

	// Freeze (the partition moved to another readcache). The caller
	// (removePartition) removes it from the live map first.
	r.partitionMu.Lock()
	delete(r.partitions, pid)
	r.partitionMu.Unlock()
	require.NoError(t, r.freezePartition(pid, p))

	// The slice is no longer a live partition but is still queryable
	// from the frozen epoch.
	dbs, err := r.listTSDBsForTenant(tenantID, hint)
	require.NoError(t, err)
	require.Len(t, dbs, 1, "frozen epoch must stay queryable after the partition moved away")
	require.Same(t, db, dbs[0])

	// A reap at "now" keeps it: its newest sample is well within
	// LocalBlockRetention.
	r.reapFrozenEpochs(time.Now())
	dbs, err = r.listTSDBsForTenant(tenantID, hint)
	require.NoError(t, err)
	require.Len(t, dbs, 1, "epoch within retention must be kept")

	// Advancing the clock past maxT + retention + grace reaps it.
	r.reapFrozenEpochs(time.Now().Add(cfg.LocalBlockRetention + frozenEpochReapGrace + 10*time.Minute))
	dbs, err = r.listTSDBsForTenant(tenantID, hint)
	// Assignment log may still name this pod as a historical owner; an
	// empty success would silently under-count. Fail fast instead.
	require.Error(t, err)
	require.Contains(t, err.Error(), partitionEpochUnavailableDetail)
	require.Nil(t, dbs)
}

// newFrozenTestReadcache builds a bare Readcache suitable for
// freeze/reap tests (no Kafka, no rings) rooted at cfg.DataDir.
func newFrozenTestReadcache(cfg Config, limits *validation.Overrides) *Readcache {
	return &Readcache{
		logger:     log.NewNopLogger(),
		cfg:        cfg,
		limits:     limits,
		partitions: map[int32]*partitionState{},
		frozen:     map[int32][]*frozenEpoch{},
		epochSeq:   map[int32]int{},
	}
}

// freezeTestPartition opens a partition TSDB for (tenantID, pid,
// epoch), commits one sample at sampleTS (skipped when sampleTS == 0),
// and freezes it through the production freezePartition path. It
// returns the TSDB's on-disk directory.
func freezeTestPartition(t *testing.T, r *Readcache, limits *validation.Overrides, tenantID string, pid int32, epoch int, sampleTS int64) string {
	t.Helper()

	db, err := openPartitionTSDB(tenantID, pid, epoch, r.cfg.DataDir, r.cfg.BlocksStorage.TSDB,
		r.cfg.LocalBlockRetention, limits, 0, nil, nil, nil, prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, err)

	if sampleTS != 0 {
		app := db.Appender(context.Background())
		_, err = app.Append(0, labels.FromStrings(model.MetricNameLabel, "up"), sampleTS, 1)
		require.NoError(t, err)
		require.NoError(t, app.Commit())
	}

	p := newPartitionState(pid)
	p.epoch = epoch
	p.tenants[tenantID] = db
	require.NoError(t, r.freezePartition(pid, p))
	return db.dir
}

// TestReadcache_FreezeWritesMarker verifies freezePartition persists
// the epoch's reap state into each tenant dir, so a restarted process
// can still delete the directory once it ages out.
func TestReadcache_FreezeWritesMarker(t *testing.T) {
	const tenantID = "tenant-1"
	const pid = int32(3)

	cfg := newTestConfig(t, false, 0)
	cfg.LocalBlockRetention = time.Hour
	limits := validation.NewOverrides(validation.Limits{}, nil)
	r := newFrozenTestReadcache(cfg, limits)

	sampleTS := time.Now().Add(-2 * time.Minute).UnixMilli()
	dir := freezeTestPartition(t, r, limits, tenantID, pid, 1, sampleTS)

	data, err := os.ReadFile(filepath.Join(dir, frozenMarkerFilename))
	require.NoError(t, err, "freeze must write a marker into the tenant dir")

	var marker frozenMarker
	require.NoError(t, json.Unmarshal(data, &marker))
	assert.Equal(t, pid, marker.PartitionID)
	assert.Equal(t, 1, marker.Epoch)
	assert.Equal(t, tenantID, marker.Tenant)
	assert.Equal(t, sampleTS, marker.MaxT, "marker must carry the epoch's maxT (the reap key)")
	assert.Equal(t, sampleTS, marker.MinT)
	assert.NotZero(t, marker.StoppedConsumingAt)
}

// TestReadcache_RestoreFrozenEpochsOnStartup covers the restart story:
// frozen epoch directories left on disk by a previous process must be
// reopened (the distributor still routes to this pod for the slices
// they hold) when still within the serving horizon, and deleted when
// already past it — the pre-restore behavior was to silently orphan
// them, leaking disk and dropping queryability.
func TestReadcache_RestoreFrozenEpochsOnStartup(t *testing.T) {
	const tenantID = "tenant-1"
	const pid = int32(3)

	setup := func(t *testing.T, sampleTS int64) (Config, *validation.Overrides, string) {
		cfg := newTestConfig(t, false, 0)
		cfg.LocalBlockRetention = time.Hour
		limits := validation.NewOverrides(validation.Limits{}, nil)

		// "Previous process": freeze one epoch, then drop it from
		// memory (as a restart would) — only the dir + marker remain.
		prev := newFrozenTestReadcache(cfg, limits)
		dir := freezeTestPartition(t, prev, limits, tenantID, pid, 0, sampleTS)
		for _, eps := range prev.frozen {
			for _, ep := range eps {
				for _, db := range ep.tenants {
					require.NoError(t, db.Close())
				}
			}
		}
		return cfg, limits, dir
	}

	t.Run("unexpired epoch is reopened, queryable, and reaped when it ages out", func(t *testing.T) {
		sampleTS := time.Now().Add(-2 * time.Minute).UnixMilli()
		cfg, limits, dir := setup(t, sampleTS)

		r := newFrozenTestReadcache(cfg, limits)
		r.restoreFrozenEpochsOnStartup(time.Now())

		require.DirExists(t, dir, "an unexpired frozen dir must survive the restore")
		require.Len(t, r.frozen[pid], 1, "the epoch must be reconstructed into the frozen map")
		ep := r.frozen[pid][0]
		assert.Equal(t, 0, ep.epoch)
		assert.Equal(t, sampleTS, ep.maxT, "epoch bounds must be restored from the marker")
		assert.Equal(t, sampleTS, ep.minT)
		assert.NotZero(t, ep.stoppedConsumingAt, "consume-window state must be restored from the marker")
		assert.Equal(t, 1, r.epochSeq[pid],
			"epochSeq must be seeded past the restored epoch so a re-acquisition can't collide with its dir")

		// The restored slice is queryable again, with the pre-restart
		// sample intact (it lived in the WAL: freezing does not
		// compact the head).
		hint := &client.QueryAttributionHint{PartitionId: pid}
		dbs, err := r.listTSDBsForTenant(tenantID, hint)
		require.NoError(t, err)
		require.Len(t, dbs, 1, "restored frozen epoch must be queryable")
		mn, mx := dbs[0].sampleBounds()
		assert.Equal(t, sampleTS, mn, "the pre-restart sample must survive the WAL replay")
		assert.Equal(t, sampleTS, mx)

		// Still kept at "now".
		r.reapFrozenEpochs(time.Now())
		require.DirExists(t, dir)

		// Reaped by the normal path once past maxT + retention + grace.
		r.reapFrozenEpochs(time.Now().Add(cfg.LocalBlockRetention + frozenEpochReapGrace + 10*time.Minute))
		require.NoDirExists(t, dir, "aged-out restored epoch must be closed and deleted by the reaper")
		assert.Empty(t, r.frozen[pid])
	})

	t.Run("already-expired dir is deleted immediately without reopening", func(t *testing.T) {
		sampleTS := time.Now().Add(-2 * time.Minute).UnixMilli()
		cfg, limits, dir := setup(t, sampleTS)

		r := newFrozenTestReadcache(cfg, limits)
		r.restoreFrozenEpochsOnStartup(time.Now().Add(cfg.LocalBlockRetention + frozenEpochReapGrace + 10*time.Minute))

		require.NoDirExists(t, dir, "an expired frozen dir must be deleted at startup")
		assert.Empty(t, r.frozen)
		assert.Zero(t, r.epochSeq[pid], "a deleted epoch's number is safe to reuse")
	})

	t.Run("empty frozen epoch is deleted immediately", func(t *testing.T) {
		// sampleTS == 0 skips the append: the epoch's bounds stay at
		// the sentinel and the marker's MaxT is math.MinInt64, which
		// is below any cutoff.
		cfg, limits, dir := setup(t, 0)

		r := newFrozenTestReadcache(cfg, limits)
		r.restoreFrozenEpochsOnStartup(time.Now())

		require.NoDirExists(t, dir, "a frozen dir that never held data must be deleted at startup")
		assert.Empty(t, r.frozen)
	})

	t.Run("unmarked live dir is restored as frozen", func(t *testing.T) {
		cfg := newTestConfig(t, false, 0)
		cfg.LocalBlockRetention = time.Hour
		limits := validation.NewOverrides(validation.Limits{}, nil)

		// A live TSDB dir without a marker can be left by an older
		// binary or an abrupt crash. It must remain queryable even if
		// the first post-restart assignment moves the partition away.
		db, err := openPartitionTSDB(tenantID, pid, 0, cfg.DataDir, cfg.BlocksStorage.TSDB,
			cfg.LocalBlockRetention, limits, 0, nil, nil, nil, prometheus.NewRegistry(), log.NewNopLogger())
		require.NoError(t, err)
		sampleTS := time.Now().Add(-2 * time.Minute).UnixMilli()
		app := db.Appender(context.Background())
		_, err = app.Append(0, labels.FromStrings(model.MetricNameLabel, "up"), sampleTS, 1)
		require.NoError(t, err)
		require.NoError(t, app.Commit())
		require.NoError(t, db.Close())

		r := newFrozenTestReadcache(cfg, limits)
		r.restoreFrozenEpochsOnStartup(time.Now())

		require.Len(t, r.frozen[pid], 1)
		assert.Equal(t, 1, r.epochSeq[pid])
		require.FileExists(t, filepath.Join(db.dir, frozenMarkerFilename))

		dbs, err := r.listTSDBsForTenant(tenantID, &client.QueryAttributionHint{PartitionId: pid})
		require.NoError(t, err)
		require.Len(t, dbs, 1)
		minT, maxT := dbs[0].sampleBounds()
		assert.Equal(t, sampleTS, minT)
		assert.Equal(t, sampleTS, maxT)
	})

	t.Run("multiple tenants of one epoch are grouped back together", func(t *testing.T) {
		cfg := newTestConfig(t, false, 0)
		cfg.LocalBlockRetention = time.Hour
		limits := validation.NewOverrides(validation.Limits{}, nil)

		// Freeze one epoch holding two tenants (freezePartition
		// freezes all tenants of a partition as one epoch).
		prev := newFrozenTestReadcache(cfg, limits)
		sampleTS := time.Now().Add(-2 * time.Minute).UnixMilli()
		p := newPartitionState(pid)
		for _, tenant := range []string{"tenant-a", "tenant-b"} {
			db, err := openPartitionTSDB(tenant, pid, 0, cfg.DataDir, cfg.BlocksStorage.TSDB,
				cfg.LocalBlockRetention, limits, 0, nil, nil, nil, prometheus.NewRegistry(), log.NewNopLogger())
			require.NoError(t, err)
			app := db.Appender(context.Background())
			_, err = app.Append(0, labels.FromStrings(model.MetricNameLabel, "up"), sampleTS, 1)
			require.NoError(t, err)
			require.NoError(t, app.Commit())
			p.tenants[tenant] = db
		}
		require.NoError(t, prev.freezePartition(pid, p))
		for _, eps := range prev.frozen {
			for _, ep := range eps {
				for _, db := range ep.tenants {
					require.NoError(t, db.Close())
				}
			}
		}

		r := newFrozenTestReadcache(cfg, limits)
		r.restoreFrozenEpochsOnStartup(time.Now())

		require.Len(t, r.frozen[pid], 1, "both tenant dirs must restore into ONE epoch, not one epoch each")
		ep := r.frozen[pid][0]
		assert.Len(t, ep.tenants, 2)
		assert.Contains(t, ep.tenants, "tenant-a")
		assert.Contains(t, ep.tenants, "tenant-b")

		// Cleanup: close the restored DBs.
		r.reapFrozenEpochs(time.Now().Add(cfg.LocalBlockRetention + frozenEpochReapGrace + 10*time.Minute))
	})
}

func TestReadcache_RemoveUnownedFrozenPartitionOffsets(t *testing.T) {
	cfg := newTestConfig(t, false, 0)
	require.NoError(t, os.MkdirAll(cfg.DataDir, 0o755))

	r := newFrozenTestReadcache(cfg, validation.NewOverrides(validation.Limits{}, nil))
	r.frozen[3] = []*frozenEpoch{{partitionID: 3}}
	r.frozen[4] = []*frozenEpoch{{partitionID: 4}}

	for _, partitionID := range []int32{3, 4} {
		require.NoError(t, os.WriteFile(r.partitionOffsetFilePath(partitionID), []byte("{}"), 0o644))
	}

	r.removeUnownedFrozenPartitionOffsets(map[int32]struct{}{3: {}})

	require.FileExists(t, r.partitionOffsetFilePath(3))
	require.NoFileExists(t, r.partitionOffsetFilePath(4))
}
