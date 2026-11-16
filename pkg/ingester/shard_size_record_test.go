// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestUserTSDB_ObserveShardSize(t *testing.T) {
	dir := t.TempDir()
	open := func() *userTSDB {
		db, err := tsdb.Open(dir, promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return &userTSDB{userID: "test", db: db}
	}
	t0 := time.UnixMilli(1_000_000)

	u := open()
	require.Zero(t, u.shardSizeUnchangedSince(), "not checked yet")
	u.observeShardSize(3, t0, log.NewNopLogger())
	require.Equal(t, t0.UnixMilli(), u.shardSizeUnchangedSince(), "nothing kept: the shard size counts as changed now")
	u.observeShardSize(3, t0.Add(time.Hour), log.NewNopLogger())
	require.Equal(t, t0.UnixMilli(), u.shardSizeUnchangedSince(), "unchanged")

	t1 := t0.Add(2 * time.Hour)
	u.observeShardSize(4, t1, log.NewNopLogger())
	require.Equal(t, t1.UnixMilli(), u.shardSizeUnchangedSince(), "the shard grew")
	require.NoError(t, u.db.Close())

	// A restart keeps the time when the shard size is unchanged.
	u = open()
	u.observeShardSize(4, t1.Add(time.Hour), log.NewNopLogger())
	require.Equal(t, t1.UnixMilli(), u.shardSizeUnchangedSince(), "kept across the restart")
	require.NoError(t, u.db.Close())

	// The shard grew while the ingester was down.
	u = open()
	t2 := t1.Add(3 * time.Hour)
	u.observeShardSize(5, t2, log.NewNopLogger())
	require.Equal(t, t2.UnixMilli(), u.shardSizeUnchangedSince(), "changed while down: counts as changed at the restart")
	rec, err := readShardSizeRecord(dir)
	require.NoError(t, err)
	require.Equal(t, shardSizeRecord{Version: shardSizeRecordVersion1, ShardSize: 5, SinceMs: t2.UnixMilli()}, rec)
	require.NoError(t, u.db.Close())

	// A file that can't be read counts as nothing kept.
	require.NoError(t, os.WriteFile(filepath.Join(dir, shardSizeRecordFilename), []byte("{"), 0o644))
	u = open()
	t3 := t2.Add(time.Hour)
	u.observeShardSize(5, t3, log.NewNopLogger())
	require.Equal(t, t3.UnixMilli(), u.shardSizeUnchangedSince())
}

// The record is kept only while the tenant has the cardinality estimate enabled.
func TestUserTSDB_TrackShardSize(t *testing.T) {
	dir := t.TempDir()
	db, err := tsdb.Open(dir, promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	u := &userTSDB{userID: "test", db: db}
	file := filepath.Join(dir, shardSizeRecordFilename)
	t0 := time.UnixMilli(1_000_000)

	u.trackShardSize(false, 3, t0, log.NewNopLogger())
	require.Zero(t, u.shardSizeUnchangedSince())
	require.NoFileExists(t, file)

	u.trackShardSize(true, 3, t0, log.NewNopLogger())
	require.Equal(t, t0.UnixMilli(), u.shardSizeUnchangedSince())
	require.FileExists(t, file)

	u.trackShardSize(false, 3, t0.Add(time.Hour), log.NewNopLogger())
	require.Zero(t, u.shardSizeUnchangedSince(), "unknown while the estimate is disabled")
	require.NoFileExists(t, file)

	// The shard size was not watched in between, so it counts as changed now.
	t1 := t0.Add(2 * time.Hour)
	u.trackShardSize(true, 3, t1, log.NewNopLogger())
	require.Equal(t, t1.UnixMilli(), u.shardSizeUnchangedSince())
}

// With ingest storage the shard size record is kept whether or not the owned
// series service runs, so SeriesCounts can tell the shard size is unchanged.
func TestIngester_ShardSizeRecord_WithoutOwnedSeriesService(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	require.False(t, cfg.UseIngesterOwnedSeriesForLimits || cfg.UpdateIngesterOwnedSeries, "the defaults")
	cfg.OwnedSeriesUpdateInterval = 50 * time.Millisecond
	enabled := defaultLimitsTestConfig()
	enabled.CardinalityEstimateEnabled = true
	overrides := validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(map[string]*validation.Limits{"test": &enabled}))
	i, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, prometheus.NewRegistry(), log.NewNopLogger())
	require.Nil(t, i.ownedSeriesService)
	require.NotNil(t, i.shardSizeService)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), i) })
	test.Poll(t, time.Second, 1, func() interface{} { return healthyInstancesCount(i.instanceRing) })

	ctx := user.InjectOrgID(context.Background(), "test")
	req := mimirpb.ToWriteRequest(
		[][]mimirpb.LabelAdapter{mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "a"))},
		[]mimirpb.Sample{{Value: 1, TimestampMs: 1000}}, nil, nil, mimirpb.API)
	require.NoError(t, i.PushToStorageAndReleaseRequest(ctx, req))
	// A tenant without the estimate.
	require.NoError(t, i.PushToStorageAndReleaseRequest(user.InjectOrgID(context.Background(), "other"), req))
	other := i.getTSDB("other")
	require.NotNil(t, other)

	db := i.getTSDB("test")
	require.NotNil(t, db)
	require.Eventually(t, func() bool { return db.shardSizeUnchangedSince() != 0 }, 5*time.Second, 10*time.Millisecond)
	since := db.shardSizeUnchangedSince()
	require.Eventually(t, func() bool {
		rec, err := readShardSizeRecord(db.db.Dir())
		return err == nil && rec.SinceMs == since
	}, 5*time.Second, 10*time.Millisecond, "kept in the TSDB directory")
	// Both tenants were there for the updates that follow.
	time.Sleep(4 * cfg.OwnedSeriesUpdateInterval)
	require.NoFileExists(t, filepath.Join(other.db.Dir(), shardSizeRecordFilename), "no record for a tenant without the estimate")
	require.Zero(t, other.shardSizeUnchangedSince())

	resp, _, err := i.seriesCounts(ctx, &client.SeriesCountsRequest{StartTimestampMs: 0, EndTimestampMs: 2000, ShardSizeUnchangedBeforeMs: since + 1})
	require.NoError(t, err)
	require.False(t, resp.ShardSizeChanged)
	require.Equal(t, since, resp.ShardSizeUnchangedSinceMs)
	require.Len(t, resp.Groups, 1)

	// The owned series service keeps the record when it runs, and nothing asks for it without ingest storage.
	cfg = defaultIngesterTestConfig(t)
	cfg.UpdateIngesterOwnedSeries = true
	tracking, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), tracking))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), tracking) })
	require.NotNil(t, tracking.ownedSeriesService)
	require.Nil(t, tracking.shardSizeService)
	classic := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.Nil(t, classic.shardSizeService)
}
