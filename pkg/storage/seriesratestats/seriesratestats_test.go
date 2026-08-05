// SPDX-License-Identifier: AGPL-3.0-only

package seriesratestats

import (
	crand "crypto/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// makeChunk returns a single chunk meta holding numSamples float samples starting
// at startMs, spaced stepMs apart.
func makeChunk(t *testing.T, startMs int64, numSamples int, stepMs int64) []chunks.Meta {
	t.Helper()

	chk := chunkenc.NewXORChunk()
	app, err := chk.Appender()
	require.NoError(t, err)
	ts := startMs
	for i := 0; i < numSamples; i++ {
		app.Append(0, ts, float64(i))
		ts += stepMs
	}
	return []chunks.Meta{{Chunk: chk, MinTime: startMs, MaxTime: startMs + int64(numSamples-1)*stepMs}}
}

func newTestCollector(t *testing.T, cfg Config) (*collector, string) {
	t.Helper()

	dir := t.TempDir()
	meta := &tsdb.BlockMeta{ULID: ulid.MustNew(ulid.Now(), crand.Reader), MinTime: 0, MaxTime: 2 * time.Hour.Milliseconds()}
	return newCollector(cfg, log.NewNopLogger(), meta, dir), dir
}

func TestCollectorAdaptiveSelection(t *testing.T) {
	c, dir := newTestCollector(t, DefaultConfig())

	// 100 background series at 1 sample/min over the full block: rate ~0.0167 samples/s.
	for i := 0; i < 100; i++ {
		c.Add(labels.FromStrings("__name__", "background", "idx", string(rune('a'+i%26))), makeChunk(t, 0, 120, 60_000))
	}
	// 3 outliers at ~1, ~3 and ~2 samples/s over 10 minutes.
	c.Add(labels.FromStrings("__name__", "hot", "idx", "1"), makeChunk(t, 0, 600, 1_000))
	c.Add(labels.FromStrings("__name__", "hot", "idx", "2"), makeChunk(t, 0, 1800, 333))
	c.Add(labels.FromStrings("__name__", "hot", "idx", "3"), makeChunk(t, 0, 1200, 500))
	// A sparse series with a tiny span: without the span floor its rate would be
	// 2000 samples/s and it would top the leaderboard as junk.
	c.Add(labels.FromStrings("__name__", "sparse"), makeChunk(t, 0, 2, 1))

	c.Done()

	stats, err := ReadFromDir(dir)
	require.NoError(t, err)

	require.Equal(t, Version, stats.Version)
	require.Equal(t, uint64(104), stats.Summary.NumSeries)
	// The block's median is the background rate, so the absolute floor is the bar.
	require.Equal(t, c.cfg.FloorRate, stats.Summary.SignificanceBar)
	require.False(t, stats.Summary.Truncated)

	// Only the outliers are stored, sorted by decreasing rate.
	require.Len(t, stats.Series, 3)
	require.Equal(t, "2", stats.Series[0].Labels["idx"])
	require.Equal(t, uint64(1800), stats.Series[0].Samples)
	require.Equal(t, "3", stats.Series[1].Labels["idx"])
	require.Equal(t, "1", stats.Series[2].Labels["idx"])
	for _, s := range stats.Series {
		require.Equal(t, "hot", s.Labels["__name__"])
		require.Greater(t, s.Rate, stats.Summary.SignificanceBar)
	}
}

func TestCollectorRelativeBar(t *testing.T) {
	// In a block where every series is fast, the bar scales with the block's own
	// median instead of the absolute floor.
	c, dir := newTestCollector(t, DefaultConfig())

	// 10 series at ~1 sample/s: median 1.0, so the bar is 2.0.
	for i := 0; i < 10; i++ {
		c.Add(labels.FromStrings("__name__", "base", "idx", string(rune('a'+i))), makeChunk(t, 0, 600, 1_000))
	}
	// 2 series at ~3 samples/s clear the bar.
	c.Add(labels.FromStrings("__name__", "hot", "idx", "1"), makeChunk(t, 0, 1800, 333))
	c.Add(labels.FromStrings("__name__", "hot", "idx", "2"), makeChunk(t, 0, 1800, 333))

	c.Done()

	stats, err := ReadFromDir(dir)
	require.NoError(t, err)

	require.InDelta(t, 2.0, stats.Summary.SignificanceBar, 0.1)
	require.Len(t, stats.Series, 2)
	for _, s := range stats.Series {
		require.Equal(t, "hot", s.Labels["__name__"])
	}
}

func TestCollectorUniformBlockStoresOnlySummary(t *testing.T) {
	c, dir := newTestCollector(t, DefaultConfig())

	// All series share the same rate, well above the absolute floor: none of them
	// stands out, so the sidecar collapses to just the summary.
	for i := 0; i < 20; i++ {
		c.Add(labels.FromStrings("__name__", "uniform", "idx", string(rune('a'+i))), makeChunk(t, 0, 600, 1_000))
	}

	c.Done()

	stats, err := ReadFromDir(dir)
	require.NoError(t, err)

	require.Empty(t, stats.Series)
	require.Equal(t, uint64(20), stats.Summary.NumSeries)
	require.Equal(t, uint64(20*600), stats.Summary.NumSamples)
	require.False(t, stats.Summary.Truncated)
}

func TestCollectorTruncation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxSeries = 2
	c, dir := newTestCollector(t, cfg)

	// 20 slow background series and 4 outliers, with a cap of 2.
	for i := 0; i < 20; i++ {
		c.Add(labels.FromStrings("__name__", "background", "idx", string(rune('a'+i))), makeChunk(t, 0, 120, 60_000))
	}
	c.Add(labels.FromStrings("__name__", "hot", "idx", "1"), makeChunk(t, 0, 600, 1_000))
	c.Add(labels.FromStrings("__name__", "hot", "idx", "2"), makeChunk(t, 0, 1200, 500))
	c.Add(labels.FromStrings("__name__", "hot", "idx", "3"), makeChunk(t, 0, 1800, 333))
	c.Add(labels.FromStrings("__name__", "hot", "idx", "4"), makeChunk(t, 0, 2400, 250))

	c.Done()

	stats, err := ReadFromDir(dir)
	require.NoError(t, err)

	require.Len(t, stats.Series, 2)
	require.Equal(t, "4", stats.Series[0].Labels["idx"])
	require.Equal(t, "3", stats.Series[1].Labels["idx"])
	require.True(t, stats.Summary.Truncated)
}

func TestCollectorEmptyBlock(t *testing.T) {
	c, dir := newTestCollector(t, DefaultConfig())
	c.Done()

	stats, err := ReadFromDir(dir)
	require.NoError(t, err)

	require.Empty(t, stats.Series)
	require.Equal(t, uint64(0), stats.Summary.NumSeries)
	require.Equal(t, c.cfg.FloorRate, stats.Summary.SignificanceBar)
	require.False(t, stats.Summary.Truncated)
}

// TestEndToEndCompactionAndUpload exercises the full generation path: the observer
// factory is installed through tsdb.Options, head compaction writes the sidecar into
// the block directory, and block.Upload includes it in the uploaded block files.
func TestEndToEndCompactionAndUpload(t *testing.T) {
	opts := tsdb.DefaultOptions()
	opts.NoLockfile = true
	opts.SeriesStatsObserverFactory = NewObserverFactory(DefaultConfig(), log.NewNopLogger())

	db, err := tsdb.Open(t.TempDir(), promslog.NewNopLogger(), nil, opts, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	app := db.Appender(t.Context())
	// 5 background series at 1 sample/15s for 10 minutes.
	for i := 0; i < 5; i++ {
		lbls := labels.FromStrings("__name__", "background", "idx", string(rune('a'+i)))
		for ts := int64(0); ts < 600_000; ts += 15_000 {
			_, err := app.Append(0, lbls, ts, 1.0)
			require.NoError(t, err)
		}
	}
	// One hot series at 10 samples/s for 10 minutes.
	hot := labels.FromStrings("__name__", "hot")
	for ts := int64(0); ts < 600_000; ts += 100 {
		_, err := app.Append(0, hot, ts, 1.0)
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())

	require.NoError(t, db.CompactHead(tsdb.NewRangeHead(db.Head(), 0, 2*time.Hour.Milliseconds()-1)))
	require.Len(t, db.Blocks(), 1)
	blockDir := db.Blocks()[0].Dir()

	// The sidecar was written into the block directory during compaction.
	stats, err := ReadFromDir(blockDir)
	require.NoError(t, err)
	require.Equal(t, uint64(6), stats.Summary.NumSeries)
	require.Len(t, stats.Series, 1)
	require.Equal(t, "hot", stats.Series[0].Labels["__name__"])
	require.Equal(t, uint64(6000), stats.Series[0].Samples)
	require.InDelta(t, 10.0, stats.Series[0].Rate, 0.1)

	// Upload includes the sidecar and registers it in the block's file list.
	bkt := objstore.NewInMemBucket()
	meta, err := block.Upload(t.Context(), log.NewNopLogger(), bkt, blockDir, nil)
	require.NoError(t, err)

	blockID := db.Blocks()[0].Meta().ULID.String()
	exists, err := bkt.Exists(t.Context(), filepath.Join(blockID, block.SeriesRateStatsFilename))
	require.NoError(t, err)
	require.True(t, exists)

	found := false
	for _, f := range meta.Thanos.Files {
		if f.RelPath == block.SeriesRateStatsFilename {
			found = true
			require.Greater(t, f.SizeBytes, int64(0))
		}
	}
	require.True(t, found, "series rate stats sidecar not registered in meta.Thanos.Files")
}
