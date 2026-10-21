// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/ingest"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func newSeriesstoreTestConfig(t *testing.T) Config {
	cfg := newTestConfig(t, false, 0)
	cfg.Engine = mimir_tsdb.EngineSeriesstore
	cfg.BlocksStorage.TSDB.Engine = mimir_tsdb.EngineSeriesstore
	cfg.LocalBlockRetention = time.Hour
	return cfg
}

func openSeriesstoreTestTSDB(t *testing.T, cfg Config, limits *validation.Overrides, tenantID string, pid int32) *partitionTSDB {
	t.Helper()
	db, err := openPartitionTSDB(tenantID, pid, 0, cfg.DataDir, cfg.BlocksStorage.TSDB,
		cfg.LocalBlockRetention, limits, 0, nil, nil, nil, newTestLookupPlanMetrics(), prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, err)
	_, ok := db.db.(seriesstoreStore)
	require.True(t, ok, "the partition must be stored in a seriesstore engine")
	return db
}

func appendTestSample(t *testing.T, db *partitionTSDB, metric string, ts int64) {
	t.Helper()
	app := db.Appender(context.Background())
	_, err := app.Append(0, labels.FromStrings(model.MetricNameLabel, metric), ts, 1)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
}

func queryTestSamples(t *testing.T, db *partitionTSDB, metric string) []int64 {
	t.Helper()
	q, err := db.ChunkQuerier(0, time.Now().Add(time.Hour).UnixMilli())
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()
	set := q.Select(context.Background(), true, nil, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, metric))
	var out []int64
	for set.Next() {
		chunks := set.At().Iterator(nil)
		for chunks.Next() {
			samples := chunks.At().Chunk.Iterator(nil)
			for samples.Next() != chunkenc.ValNone {
				ts, _ := samples.At()
				out = append(out, ts)
			}
			require.NoError(t, samples.Err())
		}
		require.NoError(t, chunks.Err())
	}
	require.NoError(t, set.Err())
	return out
}

// copyDataDir copies a readcache's data directory as a crash would leave it: what the engines wrote, without what
// their close would.
func copyDataDir(t *testing.T, from string) string {
	t.Helper()
	to := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.CopyFS(to, os.DirFS(from)))
	return to
}

// A frozen seriesstore engine writes its head to disk and snapshots: it frees the memory of a partition nothing
// appends to anymore, and a crash restores all of it, since no Kafka offset is kept to replay it from.
func TestSeriesstore_FreezeMovesTheHeadToDiskAndSnapshotsIt(t *testing.T) {
	const (
		tenantID = "tenant-1"
		pid      = int32(3)
	)
	cfg := newSeriesstoreTestConfig(t)
	limits := validation.NewOverrides(validation.Limits{}, nil)
	r := newFrozenTestReadcache(cfg, limits)

	db := openSeriesstoreTestTSDB(t, cfg, limits, tenantID, pid)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	sampleTS := time.Now().Add(-2 * time.Minute).UnixMilli()
	appendTestSample(t, db, "up", sampleTS)
	p := newPartitionState(pid)
	p.tenants[tenantID] = db
	require.NoError(t, r.freezePartition(pid, p))

	require.Zero(t, db.Head().NumSeries(), "the frozen engine's series must be on disk")
	dbs, err := r.listTSDBsForTenant(tenantID, &client.QueryAttributionHint{PartitionId: pid})
	require.NoError(t, err)
	require.Len(t, dbs, 1)
	require.Equal(t, []int64{sampleTS}, queryTestSamples(t, dbs[0], "up"))

	crashed := newFrozenTestReadcache(cfg, limits)
	crashed.cfg.DataDir = copyDataDir(t, cfg.DataDir)
	crashed.restoreFrozenEpochsOnStartup(time.Now())
	t.Cleanup(func() {
		for _, ep := range crashed.frozen[pid] {
			for _, db := range ep.tenants {
				require.NoError(t, db.Close())
			}
		}
	})
	dbs, err = crashed.listTSDBsForTenant(tenantID, &client.QueryAttributionHint{PartitionId: pid})
	require.NoError(t, err)
	require.Len(t, dbs, 1, "the frozen epoch must be restored from its snapshot")
	require.Equal(t, []int64{sampleTS}, queryTestSamples(t, dbs[0], "up"))
}

// A partition resumed after a crash replays Kafka from the oldest offset its engines restored a checkpoint of, or from
// a timestamp when one restored no snapshot; after a clean shutdown, from the committed offset.
func TestSeriesstore_ResumeReplaysFromTheOldestCheckpoint(t *testing.T) {
	const pid = int32(3)
	limits := validation.NewOverrides(validation.Limits{}, nil)
	ctx := context.Background()

	open := func(t *testing.T, cfg Config, tenants ...string) (*Readcache, *partitionState) {
		r := newFrozenTestReadcache(cfg, limits)
		p := newPartitionState(pid)
		for _, tenant := range tenants {
			db := openSeriesstoreTestTSDB(t, cfg, limits, tenant, pid)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			p.tenants[tenant] = db
		}
		r.partitions[pid] = p
		return r, p
	}
	offset := func(t *testing.T, r *Readcache) int64 {
		offset, ok := ingest.ReadOffsetFile(r.partitionOffsetFilePath(pid), pid, log.NewNopLogger())
		require.True(t, ok)
		return offset
	}

	t.Run("checkpoints", func(t *testing.T) {
		cfg := newSeriesstoreTestConfig(t)
		r, p := open(t, cfg, "tenant-a", "tenant-b")
		for _, db := range p.tenants {
			appendTestSample(t, db, "up", time.Now().UnixMilli())
		}
		require.NoError(t, ingest.WriteOffsetFile(r.partitionOffsetFilePath(pid), pid, 5, log.NewNopLogger()))
		r.checkpointSeriesstoreHeads(ctx)
		require.NoError(t, ingest.WriteOffsetFile(r.partitionOffsetFilePath(pid), pid, 9, log.NewNopLogger()))

		crashedCfg := cfg
		crashedCfg.DataDir = copyDataDir(t, cfg.DataDir)
		crashed, crashedPartition := open(t, crashedCfg, "tenant-a", "tenant-b")
		require.False(t, crashed.rewindSeriesstoreResume(crashedPartition, crashed.partitionOffsetFilePath(pid)))
		require.Equal(t, int64(5), offset(t, crashed))
		for _, db := range crashedPartition.tenants {
			require.Len(t, queryTestSamples(t, db, "up"), 1, "the head must be restored from the checkpoint")
		}
	})

	t.Run("no snapshot", func(t *testing.T) {
		cfg := newSeriesstoreTestConfig(t)
		r, p := open(t, cfg, "tenant-a")
		appendTestSample(t, p.tenants["tenant-a"], "up", time.Now().UnixMilli())
		require.NoError(t, ingest.WriteOffsetFile(r.partitionOffsetFilePath(pid), pid, 9, log.NewNopLogger()))

		crashedCfg := cfg
		crashedCfg.DataDir = copyDataDir(t, cfg.DataDir)
		crashed, crashedPartition := open(t, crashedCfg, "tenant-a")
		require.True(t, crashed.rewindSeriesstoreResume(crashedPartition, crashed.partitionOffsetFilePath(pid)))
	})

	t.Run("clean shutdown", func(t *testing.T) {
		cfg := newSeriesstoreTestConfig(t)
		r := newFrozenTestReadcache(cfg, limits)
		db := openSeriesstoreTestTSDB(t, cfg, limits, "tenant-a", pid)
		appendTestSample(t, db, "up", time.Now().UnixMilli())
		require.NoError(t, ingest.WriteOffsetFile(r.partitionOffsetFilePath(pid), pid, 5, log.NewNopLogger()))
		p := newPartitionState(pid)
		p.tenants["tenant-a"] = db
		r.partitions[pid] = p
		r.checkpointSeriesstoreHeads(ctx)
		require.NoError(t, ingest.WriteOffsetFile(r.partitionOffsetFilePath(pid), pid, 9, log.NewNopLogger()))
		require.NoError(t, db.Close())

		restarted, restartedPartition := open(t, cfg, "tenant-a")
		require.False(t, restarted.rewindSeriesstoreResume(restartedPartition, restarted.partitionOffsetFilePath(pid)))
		require.Equal(t, int64(9), offset(t, restarted), "a clean shutdown's snapshot has every record")
		require.Len(t, queryTestSamples(t, restartedPartition.tenants["tenant-a"], "up"), 1)
	})
}
