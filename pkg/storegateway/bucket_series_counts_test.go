// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/axiomhq/hyperloglog"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/test"
)

const hourMs = int64(60 * 60 * 1000)

// chunkAt is a two-sample chunk spanning [minT, maxT], both inclusive.
func chunkAt(minT, maxT int64) chunks.Meta {
	return must(chunks.ChunkFromSamples([]chunks.Sample{test.Sample{TS: minT, Val: 1}, test.Sample{TS: maxT, Val: 1}}))
}

// prepareSeriesCountsStore builds a store over two four-hour blocks whose
// chunk times are set by hand, and returns it with the blocks' IDs.
func prepareSeriesCountsStore(t *testing.T) (*BucketStore, ulid.ULID, ulid.ULID) {
	return prepareSeriesCountsStoreWithStrategy(t, nil)
}

// keepFirstGroupStrategy fetches only the first matcher's postings and
// leaves the other matchers pending, so the series have to be filtered after
// loading.
type keepFirstGroupStrategy struct{}

func (keepFirstGroupStrategy) name() string { return "keepFirstGroup" }
func (keepFirstGroupStrategy) selectPostings(groups []postingGroup) (selected, omitted []postingGroup) {
	if len(groups) == 0 {
		return nil, nil
	}
	return groups[:1], groups[1:]
}

func prepareSeriesCountsStoreWithStrategy(t *testing.T, strategy postingsSelectionStrategy) (*BucketStore, ulid.ULID, ulid.ULID) {
	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	dir := t.TempDir()

	upA := labels.FromStrings("__name__", "up", "pod", "p1")
	upB := labels.FromStrings("__name__", "up", "pod", "p2")
	upC := labels.FromStrings("__name__", "up", "pod", "p3")
	errA := labels.FromStrings("__name__", "errors_total", "pod", "p1")
	first := block.SeriesSpecs{
		{Labels: upA, Chunks: []chunks.Meta{chunkAt(0, hourMs-1)}},                          // hour 0
		{Labels: upB, Chunks: []chunks.Meta{chunkAt(hourMs, 2*hourMs+10)}},                  // hours 1 and 2
		{Labels: upC, Chunks: []chunks.Meta{chunkAt(0, 10), chunkAt(3*hourMs, 4*hourMs-1)}}, // hours 0 and 3
		{Labels: errA, Chunks: []chunks.Meta{chunkAt(2*hourMs, 2*hourMs+5)}},                // hour 2
	}
	// The next block range repeats upA, so summing the two blocks would count it twice.
	second := block.SeriesSpecs{
		{Labels: upA, Chunks: []chunks.Meta{chunkAt(4*hourMs, 8*hourMs-1)}},
		{Labels: labels.FromStrings("__name__", "up", "pod", "p4"), Chunks: []chunks.Meta{chunkAt(4*hourMs, 5*hourMs)}},
	}
	var ids []ulid.ULID
	for _, specs := range []block.SeriesSpecs{first, second} {
		meta, err := block.GenerateBlockFromSpec(dir, specs)
		require.NoError(t, err)
		_, err = block.Upload(context.Background(), log.NewNopLogger(), bkt, filepath.Join(dir, meta.ULID.String()), nil)
		require.NoError(t, err)
		ids = append(ids, meta.ULID)
	}

	cfg := defaultPrepareStoreConfig(t)
	cfg.numBlocks = 0 // only the blocks uploaded above
	if strategy != nil {
		cfg.postingsStrategy = strategy
	}
	s := prepareStoreWithTestBlocks(t, bkt, cfg)
	return s.store, ids[0], ids[1]
}

func countsByGroup(resp *storegatewaypb.SeriesCountsResponse) map[string][]int64 {
	out := map[string][]int64{}
	for _, g := range resp.Groups {
		out[g.Value] = g.Counts
	}
	return out
}

func TestBucketStore_SeriesCounts(t *testing.T) {
	store, b1, b2 := prepareSeriesCountsStore(t)
	ctx := t.Context()
	up := []*storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"}}

	t.Run("a block inside the window is counted from postings", func(t *testing.T) {
		resp, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {3}, "errors_total": {1}}, countsByGroup(resp))
		assert.Equal(t, []string{b1.String()}, resp.BlockIds)
		assert.Equal(t, int64(1), resp.BlocksFromPostings)
		assert.Zero(t, resp.SeriesFetchedBytes, "no series entry is read")

		byPod, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"p1": {1}, "p2": {1}, "p3": {1}}, countsByGroup(byPod), "each pod's postings intersected with up")
		assert.Equal(t, int64(1), byPod.BlocksFromPostings)
		assert.Zero(t, byPod.SeriesFetchedBytes)

		noLabel, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "job"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"": {3}}, countsByGroup(noLabel), "series without the label count under the empty value")

		allPods, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"p1": {2}, "p2": {1}, "p3": {1}}, countsByGroup(allPods), "without matchers the offsets alone give the counts")
		assert.Zero(t, allPods.SeriesFetchedBytes)
	})

	t.Run("a window that cuts through the block reads series entries", func(t *testing.T) {
		resp, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4*hourMs - 1})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {3}, "errors_total": {1}}, countsByGroup(resp))
		assert.Zero(t, resp.BlocksFromPostings)
		assert.Positive(t, resp.SeriesFetchedBytes, "the first read of a block comes from the bucket")
		// The bucket read is a whole partition, so on a block this small it's
		// far larger than the series entries actually decoded.
		assert.Positive(t, resp.IndexBytes)
		assert.Less(t, resp.IndexBytes, resp.PostingsFetchedBytes+resp.SeriesFetchedBytes)
	})

	t.Run("no matchers reads the all-postings list and counts every series", func(t *testing.T) {
		all, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs})
		require.NoError(t, err)
		anyName := []*storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: "__name__", Value: ".+"}}
		viaNames, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: anyName, MinTime: 0, MaxTime: 4 * hourMs})
		require.NoError(t, err)
		assert.Equal(t, countsByGroup(viaNames), countsByGroup(all))
		assert.Equal(t, int64(4), all.SeriesCounted)
		assert.LessOrEqual(t, all.PostingsFetchedBytes, viaNames.PostingsFetchedBytes, "one all-postings list costs no more than one list per name")
	})

	t.Run("a matcher left pending by the postings strategy is applied to the series", func(t *testing.T) {
		// A store whose strategy fetches only the first matcher's postings
		// leaves the second matcher pending, so the filtering iterator has to
		// apply it to the loaded series.
		lazyStore, lb1, _ := prepareSeriesCountsStoreWithStrategy(t, keepFirstGroupStrategy{})
		upOnP1P2 := []*storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
			{Type: storepb.LabelMatcher_RE, Name: "pod", Value: "p[12]"},
		}
		resp, err := lazyStore.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{lb1.String()}, Matchers: upOnP1P2, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"p1": {1}, "p2": {1}}, countsByGroup(resp), "p3 is filtered out by the pending matcher")
		assert.Equal(t, int64(2), resp.SeriesCounted)
	})

	t.Run("window inside the block", func(t *testing.T) {
		// Only upB has a chunk in [1h, 3h); upA ends just before it and upC
		// has a gap over it. errA is in hour 2.
		resp, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: hourMs, MaxTime: 3 * hourMs})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {1}, "errors_total": {1}}, countsByGroup(resp))
	})

	t.Run("hourly buckets", func(t *testing.T) {
		resp, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, MinTime: 0, MaxTime: 4 * hourMs, StepMs: hourMs})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {2, 1, 1, 1}}, countsByGroup(resp))
	})

	t.Run("group by another label", func(t *testing.T) {
		resp, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"p1": {1}, "p2": {1}, "p3": {1}}, countsByGroup(resp))
	})

	t.Run("hashes dedup a series repeated across blocks", func(t *testing.T) {
		resp, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, Matchers: up, MinTime: 0, MaxTime: 8 * hourMs, Hashes: true})
		require.NoError(t, err)
		require.Len(t, resp.Groups, 1)
		assert.Equal(t, "up", resp.Groups[0].Value)
		assert.Len(t, resp.Groups[0].Hashes, 4, "upA is in both blocks and counts once")
		assert.Contains(t, resp.Groups[0].Hashes, labels.StableHash(labels.FromStrings("__name__", "up", "pod", "p1")))
		assert.Empty(t, resp.Groups[0].Counts)
	})

	t.Run("a hash set above sketch_above becomes a sketch", func(t *testing.T) {
		resp, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, Matchers: up, MinTime: 0, MaxTime: 8 * hourMs, Hashes: true, SketchAbove: 3})
		require.NoError(t, err)
		require.Len(t, resp.Groups, 1)
		g := resp.Groups[0]
		assert.Empty(t, g.Hashes)
		require.NotEmpty(t, g.Sketch)
		sk, err := hyperloglog.NewSketch(SeriesCountsSketchPrecision, false)
		require.NoError(t, err)
		require.NoError(t, sk.UnmarshalBinary(g.Sketch))
		assert.Equal(t, uint64(4), sk.Estimate(), "four distinct series, small enough for HLL to count exactly")

		resp, err = store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, Matchers: up, MinTime: 0, MaxTime: 8 * hourMs, Hashes: true, SketchAbove: 4})
		require.NoError(t, err)
		assert.Len(t, resp.Groups[0].Hashes, 4, "at the threshold the group keeps its hashes")
		assert.Empty(t, resp.Groups[0].Sketch)
	})

	t.Run("max_index_bytes rejects before reading series", func(t *testing.T) {
		// Four series at no less than the 32-byte floor are more than 100 bytes.
		// The window ends before the block does, so the series entries are needed.
		_, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4*hourMs - 1, MaxIndexBytes: 100})
		require.Error(t, err)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
		assert.Contains(t, err.Error(), "then 4 series at an estimated")
	})

	t.Run("max_index_bytes above the plan is exact", func(t *testing.T) {
		resp, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs, MaxIndexBytes: 1 << 20})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {3}, "errors_total": {1}}, countsByGroup(resp))
		assert.LessOrEqual(t, resp.IndexBytes, int64(1<<20))
	})

	t.Run("unknown blocks are skipped", func(t *testing.T) {
		resp, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{ulid.MustNew(1, nil).String()}, MinTime: 0, MaxTime: 4 * hourMs})
		require.NoError(t, err)
		assert.Empty(t, resp.BlockIds)
		assert.Empty(t, resp.Groups)
	})

	for name, req := range map[string]*storegatewaypb.SeriesCountsRequest{
		"empty window":             {MinTime: 10, MaxTime: 10},
		"step doesn't divide":      {MinTime: 0, MaxTime: 4 * hourMs, StepMs: 3 * hourMs},
		"hashes with step":         {MinTime: 0, MaxTime: 4 * hourMs, StepMs: hourMs, Hashes: true},
		"too many buckets":         {MinTime: 0, MaxTime: 4 * hourMs, StepMs: 1},
		"bad block ID":             {MinTime: 0, MaxTime: 4 * hourMs, BlockIds: []string{"x"}},
		"negative step":            {MinTime: 0, MaxTime: 4 * hourMs, StepMs: -1},
		"negative max_index_bytes": {MinTime: 0, MaxTime: 4 * hourMs, MaxIndexBytes: -1},
		"negative sketch_above":    {MinTime: 0, MaxTime: 4 * hourMs, SketchAbove: -1},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := store.SeriesCounts(ctx, req)
			require.Error(t, err)
		})
	}
}

func TestSeriesCounts_IndexBytesChecks(t *testing.T) {
	c := &seriesCounts{maxIndexBytes: 2000}
	require.NoError(t, c.checkPlan(900, 2, 2*512, 0), "900 + 2 x 512 is under the limit")
	err := c.checkPlan(1000, 2, 2*512, 0)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "1000 + 2 x 512 is over the limit")
	require.NoError(t, c.checkRead(2000))
	assert.Equal(t, codes.ResourceExhausted, status.Code(c.checkRead(2001)), "series larger than the plan assumed")

	unlimited := &seriesCounts{}
	require.NoError(t, unlimited.checkPlan(1<<40, 1<<20, 1<<29, 0))
	require.NoError(t, unlimited.checkRead(1<<40))
}

func TestSeriesEntryEstimate(t *testing.T) {
	meta := func(series uint64, indexBytes int64) *block.Meta {
		m := &block.Meta{}
		m.Stats.NumSeries = series
		if indexBytes > 0 {
			m.Thanos.Files = []block.File{{RelPath: "chunks/000001", SizeBytes: 1 << 30}, {RelPath: block.IndexFilename, SizeBytes: indexBytes}}
		}
		return m
	}
	assert.Equal(t, int64(150), seriesEntryEstimate(meta(1000, 150_000)), "the index size over the series")
	assert.Equal(t, int64(512), seriesEntryEstimate(meta(1000, 0)), "no index size: the p99")
	assert.Equal(t, int64(512), seriesEntryEstimate(meta(0, 150_000)), "no series count: the p99")
	assert.Equal(t, int64(512), seriesEntryEstimate(meta(10, 1<<20)), "never above the p99")
	assert.Equal(t, int64(32), seriesEntryEstimate(meta(1000, 1000)), "never below the floor")
}
