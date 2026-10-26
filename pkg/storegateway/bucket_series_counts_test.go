// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/axiomhq/hyperloglog"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
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

// countIndexHeaderVersions runs the count tests on both index-header
// versions: version 2 reads the postings offsets table from the bucket.
var countIndexHeaderVersions = map[string][]prepareStoreConfigOption{
	"index-header v1": nil,
	"index-header v2": {withIndexHeaderBucketReaderEnabled()},
}

// prepareSeriesCountsStore builds a store over two four-hour blocks whose
// chunk times are set by hand, and returns it with the blocks' IDs.
func prepareSeriesCountsStore(t *testing.T, opts ...prepareStoreConfigOption) (*BucketStore, ulid.ULID, ulid.ULID) {
	return prepareSeriesCountsStoreWithStrategy(t, nil, opts...)
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

func prepareSeriesCountsStoreWithStrategy(t *testing.T, strategy postingsSelectionStrategy, opts ...prepareStoreConfigOption) (*BucketStore, ulid.ULID, ulid.ULID) {
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
	s, ids := prepareSeriesCountsStoreFromSpecs(t, bkt, dir, []block.SeriesSpecs{first, second}, strategy, opts...)
	return s, ids[0], ids[1]
}

// prepareSeriesCountsStoreFromSpecs uploads one block per spec, in the
// directory dir, to bkt and returns a store over them with the blocks' IDs.
func prepareSeriesCountsStoreFromSpecs(t *testing.T, bkt objstore.Bucket, dir string, blocks []block.SeriesSpecs, strategy postingsSelectionStrategy, opts ...prepareStoreConfigOption) (*BucketStore, []ulid.ULID) {
	var ids []ulid.ULID
	for _, specs := range blocks {
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
	for _, o := range opts {
		o(cfg)
	}
	s := prepareStoreWithTestBlocks(t, bkt, cfg)
	return s.store, ids
}

func countsByGroup(resp *storegatewaypb.SeriesCountsResponse) map[string][]int64 {
	out := map[string][]int64{}
	for _, g := range resp.Groups {
		out[g.Value] = g.Counts
	}
	return out
}

func TestBucketStore_SeriesCounts(t *testing.T) {
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			testBucketStoreSeriesCounts(t, opts...)
		})
	}
}

func testBucketStoreSeriesCounts(t *testing.T, opts ...prepareStoreConfigOption) {
	store, b1, b2 := prepareSeriesCountsStore(t, opts...)
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

	t.Run("with no matchers a block is counted from the offsets table without fetching its postings", func(t *testing.T) {
		for name, req := range map[string]*storegatewaypb.SeriesCountsRequest{
			"by metric name": {BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs},
			"by pod":         {BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod"},
		} {
			resp, err := store.SeriesCounts(ctx, req)
			require.NoError(t, err, name)
			assert.Equal(t, int64(1), resp.BlocksFromPostings, name)
			assert.Zero(t, resp.PostingsFetchedBytes, name)
			assert.Zero(t, resp.IndexBytes, "%s: no postings list and no series entry is read", name)
		}
		all, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "job"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"": {4}}, countsByGroup(all), "series without the label are counted from the series count alone")
	})

	t.Run("a request refused on the offsets table fetches no postings", func(t *testing.T) {
		before := postingsBytesObserved(t, store)
		_, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, MinTime: 0, MaxTime: 8 * hourMs, GroupBy: "pod", MaxIndexBytes: 1})
		require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
		assert.Contains(t, err.Error(), "pod postings offsets table")
		assert.Equal(t, before, postingsBytesObserved(t, store), "no postings list was touched or fetched")
	})

	t.Run("the series count from the offsets table is the length of the all-postings list", func(t *testing.T) {
		counted := 0
		store.blockSet.forEach(func(b *bucketBlock) {
			stats := newSafeQueryStats()
			b.ensureIndexHeaderLoaded(ctx, stats)
			fromTable, err := allPostingsCount(ctx, b.indexHeaderReader)
			require.NoError(t, err)
			indexr := b.indexReader(store.postingsStrategy)
			defer func() { require.NoError(t, indexr.Close()) }()
			list, err := allPostingsForBlock(ctx, indexr, stats)
			require.NoError(t, err)
			assert.Equal(t, int64(len(list)), fromTable, "block %s", b.meta.ULID)
			assert.Equal(t, int64(b.meta.Stats.NumSeries), fromTable, "block %s", b.meta.ULID)
			counted++
		})
		assert.Equal(t, 2, counted)
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

	t.Run("no matchers counts every series, as one postings list per name does", func(t *testing.T) {
		all, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs})
		require.NoError(t, err)
		anyName := []*storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: "__name__", Value: ".+"}}
		viaNames, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: anyName, MinTime: 0, MaxTime: 4 * hourMs})
		require.NoError(t, err)
		assert.Equal(t, countsByGroup(viaNames), countsByGroup(all))
		assert.Equal(t, int64(4), all.SeriesCounted)
		assert.Zero(t, all.PostingsFetchedBytes, "the block is counted from the offsets table")
		assert.Positive(t, viaNames.PostingsFetchedBytes, "matching every name reads a postings list per name")
	})

	t.Run("a matcher left pending by the postings strategy is applied to the series", func(t *testing.T) {
		// A store whose strategy fetches only the first matcher's postings
		// leaves the second matcher pending, so the filtering iterator has to
		// apply it to the loaded series.
		lazyStore, lb1, _ := prepareSeriesCountsStoreWithStrategy(t, keepFirstGroupStrategy{}, opts...)
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
		assert.Zero(t, resp.WindowMs)
	})

	t.Run("hourly buckets two hours wide", func(t *testing.T) {
		resp, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, MinTime: 0, MaxTime: 4 * hourMs, StepMs: hourMs, WindowMs: 2 * hourMs})
		require.NoError(t, err)
		// Buckets [0, 2h), [1h, 3h) and [2h, 4h): p1 is in hour 0, p2 in hours 1 and 2, p3 in hours 0 and 3.
		assert.Equal(t, map[string][]int64{"up": {3, 1, 2}}, countsByGroup(resp))
		assert.Equal(t, 2*hourMs, resp.WindowMs, "the window is echoed")
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

	t.Run("the tenant's limit bounds a request without max_index_bytes or with a larger one", func(t *testing.T) {
		store.cardinalityEstimateMaxIndexBytes = func() int64 { return 100 }
		defer func() { store.cardinalityEstimateMaxIndexBytes = nil }()
		for _, asked := range []int64{0, 1 << 20} {
			_, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4*hourMs - 1, MaxIndexBytes: asked})
			require.Error(t, err, "max_index_bytes %d", asked)
			assert.Equal(t, codes.ResourceExhausted, status.Code(err))
			assert.Contains(t, err.Error(), "more than max_index_bytes 100")
		}
	})

	t.Run("max_index_bytes above the plan is exact", func(t *testing.T) {
		resp, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs, MaxIndexBytes: 1 << 20})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {3}, "errors_total": {1}}, countsByGroup(resp))
		assert.LessOrEqual(t, resp.IndexBytes, int64(1<<20))
	})

	t.Run("max_index_bytes covers the group-by label's postings offsets table", func(t *testing.T) {
		req := func(limit int64) *storegatewaypb.SeriesCountsRequest {
			return &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod", MaxIndexBytes: limit}
		}
		resp, err := store.SeriesCounts(ctx, req(0))
		require.NoError(t, err)
		require.Equal(t, int64(1), resp.BlocksFromPostings)
		require.Positive(t, resp.PostingsOffsetsTableBytes, "the pod offsets were walked")
		total := resp.IndexBytes + resp.PostingsOffsetsTableBytes

		resp, err = store.SeriesCounts(ctx, req(total))
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"p1": {2}, "p2": {1}, "p3": {1}}, countsByGroup(resp))

		_, err = store.SeriesCounts(ctx, req(total-1))
		require.Equal(t, codes.ResourceExhausted, status.Code(err), "nothing else is read, so only the table doesn't fit")
		assert.Contains(t, err.Error(), "pod postings offsets table")
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
		"window without step":      {MinTime: 0, MaxTime: 4 * hourMs, WindowMs: hourMs},
		"window shorter than step": {MinTime: 0, MaxTime: 4 * hourMs, StepMs: hourMs, WindowMs: hourMs / 2},
		"window doesn't fit":       {MinTime: 0, MaxTime: 4 * hourMs, StepMs: hourMs, WindowMs: 5 * hourMs},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := store.SeriesCounts(ctx, req)
			require.Error(t, err)
		})
	}
}

// A block's native histogram series, integer and float, are counted like
// float series: from postings, and from their chunk metas when the window
// cuts the block.
func TestBucketStore_SeriesCountsNativeHistograms(t *testing.T) {
	histChunk := func(minT, maxT int64) chunks.Meta {
		return must(chunks.ChunkFromSamples([]chunks.Sample{
			test.Sample{TS: minT, Hist: test.GenerateTestHistogram(1)},
			test.Sample{TS: maxT, Hist: test.GenerateTestHistogram(2)},
		}))
	}
	floatHistChunk := func(minT, maxT int64) chunks.Meta {
		return must(chunks.ChunkFromSamples([]chunks.Sample{
			test.Sample{TS: minT, FloatHist: test.GenerateTestFloatHistogram(1)},
			test.Sample{TS: maxT, FloatHist: test.GenerateTestFloatHistogram(2)},
		}))
	}
	specs := block.SeriesSpecs{
		{Labels: labels.FromStrings("__name__", "h", "kind", "int"), Chunks: []chunks.Meta{histChunk(0, hourMs-1)}},                 // hour 0
		{Labels: labels.FromStrings("__name__", "h", "kind", "float"), Chunks: []chunks.Meta{floatHistChunk(2*hourMs, 3*hourMs-1)}}, // hour 2
		{Labels: labels.FromStrings("__name__", "f"), Chunks: []chunks.Meta{chunkAt(hourMs, 2*hourMs-1)}},                           // hour 1
	}
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			bkt, err := filesystem.NewBucket(t.TempDir())
			require.NoError(t, err)
			store, ids := prepareSeriesCountsStoreFromSpecs(t, bkt, t.TempDir(), []block.SeriesSpecs{specs}, nil, opts...)
			id := ids[0].String()
			ctx := t.Context()
			h := []*storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "h"}}

			resp, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{id}, MinTime: 0, MaxTime: 3 * hourMs})
			require.NoError(t, err)
			assert.Equal(t, map[string][]int64{"h": {2}, "f": {1}}, countsByGroup(resp), "the whole block, from postings")

			resp, err = store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{id}, Matchers: h, MinTime: 0, MaxTime: 3 * hourMs, StepMs: hourMs, GroupBy: "kind"})
			require.NoError(t, err)
			assert.Equal(t, map[string][]int64{"int": {1, 0, 0}, "float": {0, 0, 1}}, countsByGroup(resp), "per hour, from the chunk metas")

			resp, err = store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{id}, MinTime: hourMs, MaxTime: 3 * hourMs, GroupBy: "kind"})
			require.NoError(t, err)
			assert.Equal(t, map[string][]int64{"float": {1}, "": {1}}, countsByGroup(resp), "a window that cuts the block leaves out the histogram outside it")
		})
	}
}

func TestSeriesCounts_IndexBytesChecks(t *testing.T) {
	c := &seriesCounts{maxIndexBytes: 2000}
	require.NoError(t, c.checkPlan(900, 2, 2*512, 0), "900 + 2 x 512 is under the limit")
	err := c.checkPlan(1000, 2, 2*512, 0)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "1000 + 2 x 512 is over the limit")
	require.NoError(t, c.checkTable(1000, 1000))
	assert.Equal(t, codes.ResourceExhausted, status.Code(c.checkTable(1000, 1001)), "postings plus the offsets table are over the limit")
	require.NoError(t, c.checkRead(2000))
	assert.Equal(t, codes.ResourceExhausted, status.Code(c.checkRead(2001)), "series larger than the plan assumed")

	unlimited := &seriesCounts{}
	require.NoError(t, unlimited.checkPlan(1<<40, 1<<20, 1<<29, 0))
	require.NoError(t, unlimited.checkRead(1<<40))
	require.NoError(t, unlimited.checkTable(1<<40, 1<<40))
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

// postingsBytesObserved is the postings bytes every request so far touched
// and fetched, as the store recorded them.
func postingsBytesObserved(t *testing.T, store *BucketStore) [2]float64 {
	sum := func(vec *prometheus.SummaryVec) float64 {
		m := &dto.Metric{}
		require.NoError(t, vec.WithLabelValues("postings", "").(prometheus.Metric).Write(m))
		return m.GetSummary().GetSampleSum()
	}
	return [2]float64{sum(store.metrics.seriesDataSizeTouched), sum(store.metrics.seriesDataSizeFetched)}
}

func TestAllPostingsCount_SmallBlocks(t *testing.T) {
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			bkt, err := filesystem.NewBucket(t.TempDir())
			require.NoError(t, err)
			one := block.SeriesSpecs{{Labels: labels.FromStrings("__name__", "up"), Chunks: []chunks.Meta{chunkAt(0, 10)}}}
			store, ids := prepareSeriesCountsStoreFromSpecs(t, bkt, t.TempDir(), []block.SeriesSpecs{one}, nil, opts...)
			require.Len(t, ids, 1)

			ctx := t.Context()
			stats := newSafeQueryStats()
			store.blockSet.forEach(func(b *bucketBlock) {
				b.ensureIndexHeaderLoaded(ctx, stats)
				fromTable, err := allPostingsCount(ctx, b.indexHeaderReader)
				require.NoError(t, err)
				assert.Equal(t, int64(1), fromTable)
			})

			// A one-series block inside the window is counted without its postings.
			resp, err := store.SeriesCounts(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{ids[0].String()}, MinTime: 0, MaxTime: 4 * hourMs})
			require.NoError(t, err)
			assert.Equal(t, map[string][]int64{"up": {1}}, countsByGroup(resp))
			assert.Zero(t, resp.PostingsFetchedBytes)
		})

		t.Run(name+", a block whose offsets table costs more than its series is walked", func(t *testing.T) {
			bkt, err := filesystem.NewBucket(t.TempDir())
			require.NoError(t, err)
			// One series with a long label value: walking the label's offsets table
			// is dearer than reading the series entry, so the block is not counted
			// from the table, and the postings list it skipped must be fetched.
			long := strings.Repeat("v", 300)
			specs := block.SeriesSpecs{{Labels: labels.FromStrings("__name__", "up", "pod", long), Chunks: []chunks.Meta{chunkAt(0, 10)}}}
			store, ids := prepareSeriesCountsStoreFromSpecs(t, bkt, t.TempDir(), []block.SeriesSpecs{specs}, nil, opts...)

			resp, err := store.SeriesCounts(t.Context(), &storegatewaypb.SeriesCountsRequest{BlockIds: []string{ids[0].String()}, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod"})
			require.NoError(t, err)
			assert.Equal(t, map[string][]int64{long: {1}}, countsByGroup(resp))
			assert.Zero(t, resp.BlocksFromPostings, "the block was walked")
			assert.Positive(t, resp.PostingsFetchedBytes, "the all-postings list was fetched after all")
			assert.Positive(t, resp.SeriesFetchedBytes)
		})
	}
}
