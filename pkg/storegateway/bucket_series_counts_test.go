// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/axiomhq/hyperloglog"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/storage/indexheader"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb"
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
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {3}, "errors_total": {1}}, countsByGroup(resp))
		assert.Equal(t, []string{b1.String()}, resp.BlockIds)
		assert.Equal(t, int64(1), resp.BlocksFromPostings)
		assert.Zero(t, resp.SeriesFetchedBytes, "no series entry is read")

		byPod, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"p1": {1}, "p2": {1}, "p3": {1}}, countsByGroup(byPod), "each pod's postings intersected with up")
		assert.Equal(t, int64(1), byPod.BlocksFromPostings)
		assert.Zero(t, byPod.SeriesFetchedBytes)

		noLabel, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "job"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"": {3}}, countsByGroup(noLabel), "series without the label count under the empty value")

		allPods, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"p1": {2}, "p2": {1}, "p3": {1}}, countsByGroup(allPods), "without matchers the offsets alone give the counts")
		assert.Zero(t, allPods.SeriesFetchedBytes)
	})

	t.Run("with no matchers a block is counted from the offsets table without fetching its postings", func(t *testing.T) {
		for name, req := range map[string]*storegatewaypb.SeriesCountsRequest{
			"by metric name": {BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs},
			"by pod":         {BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod"},
		} {
			resp, err := store.countSeries(ctx, req)
			require.NoError(t, err, name)
			assert.Equal(t, int64(1), resp.BlocksFromPostings, name)
			assert.Zero(t, resp.PostingsFetchedBytes, name)
			assert.Zero(t, resp.IndexBytes, "%s: no postings list and no series entry is read", name)
		}
		all, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "job"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"": {4}}, countsByGroup(all), "series without the label are counted from the series count alone")
	})

	t.Run("a request refused on the offsets table fetches no postings", func(t *testing.T) {
		before := postingsBytesObserved(t, store)
		_, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, MinTime: 0, MaxTime: 8 * hourMs, GroupBy: "pod", MaxIndexBytes: 1})
		require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
		assert.Contains(t, err.Error(), "pod postings offsets table")
		assert.Equal(t, before, postingsBytesObserved(t, store), "no postings list was fetched")
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
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4*hourMs - 1})
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
		all, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs})
		require.NoError(t, err)
		anyName := []*storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: "__name__", Value: ".+"}}
		viaNames, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: anyName, MinTime: 0, MaxTime: 4 * hourMs})
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
		resp, err := lazyStore.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{lb1.String()}, Matchers: upOnP1P2, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"p1": {1}, "p2": {1}}, countsByGroup(resp), "p3 is filtered out by the pending matcher")
		assert.Equal(t, int64(2), resp.SeriesCounted)
	})

	t.Run("window inside the block", func(t *testing.T) {
		// Only upB has a chunk in [1h, 3h); upA ends just before it and upC
		// has a gap over it. errA is in hour 2.
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: hourMs, MaxTime: 3 * hourMs})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {1}, "errors_total": {1}}, countsByGroup(resp))
	})

	t.Run("hourly buckets", func(t *testing.T) {
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, MinTime: 0, MaxTime: 4 * hourMs, StepMs: hourMs})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {2, 1, 1, 1}}, countsByGroup(resp))
		assert.Zero(t, resp.WindowMs)
	})

	t.Run("hourly buckets two hours wide", func(t *testing.T) {
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, MinTime: 0, MaxTime: 4 * hourMs, StepMs: hourMs, WindowMs: 2 * hourMs})
		require.NoError(t, err)
		// Buckets [0, 2h), [1h, 3h) and [2h, 4h): p1 is in hour 0, p2 in hours 1 and 2, p3 in hours 0 and 3.
		assert.Equal(t, map[string][]int64{"up": {3, 1, 2}}, countsByGroup(resp))
		assert.Equal(t, 2*hourMs, resp.WindowMs, "the window is echoed")
	})

	t.Run("group by another label", func(t *testing.T) {
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"p1": {1}, "p2": {1}, "p3": {1}}, countsByGroup(resp))
	})

	t.Run("hashes dedup a series repeated across blocks", func(t *testing.T) {
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, Matchers: up, MinTime: 0, MaxTime: 8 * hourMs, Hashes: true})
		require.NoError(t, err)
		require.Len(t, resp.Groups, 1)
		assert.Equal(t, "up", resp.Groups[0].Value)
		assert.Len(t, resp.Groups[0].Hashes, 4, "upA is in both blocks and counts once")
		assert.Contains(t, resp.Groups[0].Hashes, labels.StableHash(labels.FromStrings("__name__", "up", "pod", "p1")))
		assert.Empty(t, resp.Groups[0].Counts)
	})

	t.Run("a hash set above sketch_above becomes a sketch", func(t *testing.T) {
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, Matchers: up, MinTime: 0, MaxTime: 8 * hourMs, Hashes: true, SketchAbove: 3})
		require.NoError(t, err)
		require.Len(t, resp.Groups, 1)
		g := resp.Groups[0]
		assert.Empty(t, g.Hashes)
		require.NotEmpty(t, g.Sketch)
		sk, err := hyperloglog.NewSketch(SeriesCountsSketchPrecision, false)
		require.NoError(t, err)
		require.NoError(t, sk.UnmarshalBinary(g.Sketch))
		assert.Equal(t, uint64(4), sk.Estimate(), "four distinct series, small enough for HLL to count exactly")

		resp, err = store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, Matchers: up, MinTime: 0, MaxTime: 8 * hourMs, Hashes: true, SketchAbove: 4})
		require.NoError(t, err)
		assert.Len(t, resp.Groups[0].Hashes, 4, "at the threshold the group keeps its hashes")
		assert.Empty(t, resp.Groups[0].Sketch)
	})

	t.Run("max_index_bytes rejects before reading series", func(t *testing.T) {
		// Four series at no less than the 32-byte floor are more than 100 bytes.
		// The window ends before the block does, so the series entries are needed.
		_, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4*hourMs - 1, MaxIndexBytes: 100})
		require.Error(t, err)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
		assert.Contains(t, err.Error(), "then 4 series at an estimated")
	})

	t.Run("the tenant's limit bounds a request without max_index_bytes or with a larger one", func(t *testing.T) {
		store.cardinalityEstimateMaxIndexBytes = func() int64 { return 100 }
		defer func() { store.cardinalityEstimateMaxIndexBytes = nil }()
		for _, asked := range []int64{0, 1 << 20} {
			_, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4*hourMs - 1, MaxIndexBytes: asked})
			require.Error(t, err, "max_index_bytes %d", asked)
			assert.Equal(t, codes.ResourceExhausted, status.Code(err))
			assert.Contains(t, err.Error(), "more than max_index_bytes 100")
		}
	})

	t.Run("max_index_bytes above the plan is exact", func(t *testing.T) {
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs, MaxIndexBytes: 1 << 20})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {3}, "errors_total": {1}}, countsByGroup(resp))
		assert.LessOrEqual(t, resp.IndexBytes, int64(1<<20))
	})

	t.Run("max_index_bytes covers the group-by label's postings offsets table", func(t *testing.T) {
		req := func(limit int64) *storegatewaypb.SeriesCountsRequest {
			return &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod", MaxIndexBytes: limit}
		}
		resp, err := store.countSeries(ctx, req(0))
		require.NoError(t, err)
		require.Equal(t, int64(1), resp.BlocksFromPostings)
		require.Positive(t, resp.PostingsOffsetsTableBytes, "the pod offsets were walked")
		total := resp.IndexBytes + resp.PostingsOffsetsTableBytes

		resp, err = store.countSeries(ctx, req(total))
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"p1": {2}, "p2": {1}, "p3": {1}}, countsByGroup(resp))

		_, err = store.countSeries(ctx, req(total-1))
		require.Equal(t, codes.ResourceExhausted, status.Code(err), "nothing else is read, so only the table doesn't fit")
		assert.Contains(t, err.Error(), "pod postings offsets table")
	})

	t.Run("unknown blocks are skipped", func(t *testing.T) {
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{ulid.MustNew(1, nil).String()}, MinTime: 0, MaxTime: 4 * hourMs})
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
			_, err := store.countSeries(ctx, req)
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

			resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{id}, MinTime: 0, MaxTime: 3 * hourMs})
			require.NoError(t, err)
			assert.Equal(t, map[string][]int64{"h": {2}, "f": {1}}, countsByGroup(resp), "the whole block, from postings")

			resp, err = store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{id}, Matchers: h, MinTime: 0, MaxTime: 3 * hourMs, StepMs: hourMs, GroupBy: "kind"})
			require.NoError(t, err)
			assert.Equal(t, map[string][]int64{"int": {1, 0, 0}, "float": {0, 0, 1}}, countsByGroup(resp), "per hour, from the chunk metas")

			resp, err = store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{id}, MinTime: hourMs, MaxTime: 3 * hourMs, GroupBy: "kind"})
			require.NoError(t, err)
			assert.Equal(t, map[string][]int64{"float": {1}, "": {1}}, countsByGroup(resp), "a window that cuts the block leaves out the histogram outside it")
		})
	}
}

func TestSeriesCounts_IndexBytesChecks(t *testing.T) {
	c := &seriesCounts{maxIndexBytes: 2000}
	require.NoError(t, c.checkPlan(900, 2, 2*512, 0, 0), "900 + 2 x 512 is under the limit")
	err := c.checkPlan(1000, 2, 2*512, 0, 0)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "1000 + 2 x 512 is over the limit")
	require.NoError(t, c.checkPlan(900, 2, 2*512, 0, 76), "the counts fit")
	err = c.checkPlan(900, 2, 2*512, 0, 77)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "the counts the groups will hold are over the limit")
	assert.Contains(t, err.Error(), "holding 77 bytes of counts")
	require.NoError(t, c.checkTable(1000, 1000))
	assert.Equal(t, codes.ResourceExhausted, status.Code(c.checkTable(1000, 1001)), "postings plus the offsets table are over the limit")
	require.NoError(t, c.checkRead(2000))
	assert.Equal(t, codes.ResourceExhausted, status.Code(c.checkRead(2001)), "series larger than the plan assumed")

	unlimited := &seriesCounts{}
	require.NoError(t, unlimited.checkPlan(1<<40, 1<<20, 1<<29, 0, 1<<40))
	require.NoError(t, unlimited.checkRead(1<<40))
	require.NoError(t, unlimited.checkTable(1<<40, 1<<40))
}

// The counts a request holds, 8 bytes per group per bucket, count against
// max_index_bytes with the index read: during the walk as groups are added,
// and before it from the groups a block is known to hold.
func TestBucketStore_SeriesCountsBoundsGroupMemory(t *testing.T) {
	t.Run("the walk charges each new group's counts", func(t *testing.T) {
		stats := newSafeQueryStats()
		c, err := newSeriesCounts(&storegatewaypb.SeriesCountsRequest{MinTime: 0, MaxTime: 4 * hourMs, StepMs: hourMs, GroupBy: "pod", MaxIndexBytes: 1000 + 3*4*8 - 1})
		require.NoError(t, err)
		require.Equal(t, 4, c.buckets)
		for _, pod := range []string{"p1", "p2", "p3"} {
			c.add(seriesChunkRefs{lset: labels.FromStrings("__name__", "up", "pod", pod), refs: []seriesChunkRef{{minTime: 0, maxTime: 10}}})
		}
		c.add(seriesChunkRefs{lset: labels.FromStrings("__name__", "up", "pod", "p1"), refs: []seriesChunkRef{{minTime: 0, maxTime: 10}}})
		require.Len(t, c.counts, 3)
		err = c.checkRead(c.limitedBytes(stats) + 1000)
		require.Equal(t, codes.ResourceExhausted, status.Code(err), "three groups of four buckets are 96 bytes, one over what is left")
		assert.Contains(t, err.Error(), "96 of them counts held")

		c.maxIndexBytes++
		require.NoError(t, c.checkRead(c.limitedBytes(stats)+1000), "an existing group adds nothing")
	})

	for name, opts := range countIndexHeaderVersions {
		t.Run(name+", the groups a block is known to hold are priced before the walk", func(t *testing.T) {
			store, b1, _ := prepareSeriesCountsStore(t, opts...)
			ctx := t.Context()
			var est, bound int64
			store.blockSet.forEach(func(b *bucketBlock) {
				if b.meta.ULID == b1 {
					est = seriesEntryEstimate(b.meta)
					b.ensureIndexHeaderLoaded(ctx, newSafeQueryStats())
					n, err := b.indexHeaderReader.LabelValuesCountLowerBound(ctx, "pod")
					require.NoError(t, err)
					bound = int64(n)
				}
			})
			require.Positive(t, est)
			require.Positive(t, bound, "the index-header's samples bound the pods from below")
			// Hourly buckets over the block: four series are walked at est
			// bytes each after their 4-byte postings, and the pods the block is
			// known to hold make that many groups of four counts.
			counts := bound * 4 * 8
			req := func(limit int64) *storegatewaypb.SeriesCountsRequest {
				return &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs, StepMs: hourMs, GroupBy: "pod", MaxIndexBytes: limit}
			}
			seriesRead := func() float64 {
				return testutil.ToFloat64(store.metrics.cardinalityEstimateFetchedBytes.WithLabelValues("series"))
			}
			before := seriesRead()
			_, err := store.countSeries(ctx, req(4*est+4*4+counts-1))
			require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
			assert.Contains(t, err.Error(), fmt.Sprintf("holding %d bytes of counts", counts))
			assert.Equal(t, before, seriesRead(), "refused before any series entry was read")

			resp, err := store.countSeries(ctx, req(4*est+4*4+counts))
			require.NoError(t, err)
			assert.Equal(t, map[string][]int64{"p1": {1, 0, 1, 0}, "p2": {0, 1, 1, 0}, "p3": {1, 0, 0, 1}}, countsByGroup(resp), "up and errors_total on p1 in hours 0 and 2")
		})
	}
}

// With many buckets, the store-gateway counter agrees with the definition
// of a bucket count for every bucket: a series counts in a bucket when one of
// its chunks overlaps it.
func TestSeriesCounts_ManyBucketsAgainstBruteForce(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	const minT, step = int64(1000), int64(10)
	for _, window := range []int64{0, 3 * step} {
		buckets := cardinality.MaxCounterBuckets
		w := max(window, step)
		maxT := minT + w + int64(buckets-1)*step
		c, err := newSeriesCounts(&storegatewaypb.SeriesCountsRequest{MinTime: minT, MaxTime: maxT, StepMs: step, WindowMs: window})
		require.NoError(t, err)
		require.Equal(t, buckets, c.buckets)

		lset := labels.FromStrings("__name__", "a")
		want := make([]int64, buckets)
		span := maxT - minT
		for range 200 {
			var refs []seriesChunkRef
			for range 1 + rnd.Intn(3) {
				from := minT - span/10 + rnd.Int63n(span+span/5)
				refs = append(refs, seriesChunkRef{minTime: from, maxTime: from + rnd.Int63n(span/4)})
			}
			c.add(seriesChunkRefs{lset: lset, refs: refs})
			for i := range buckets {
				start := minT + int64(i)*step
				for _, r := range refs {
					if r.maxTime >= start && r.minTime < start+w {
						want[i]++
						break
					}
				}
			}
		}
		require.Len(t, c.counts, 1, "window %d", window)
		assert.Equal(t, want, c.counts["a"], "window %d", window)
	}
}

// A group's key is a copy of the group-by value: the value is a slice of the
// series' label data, and the key must not keep the whole label set alive.
func TestSeriesCounts_GroupKeysDoNotPinLabelSets(t *testing.T) {
	lset := labels.FromStrings("__name__", "a", "pod", "a-long-pod-name")
	for name, c := range map[string]*seriesCounts{
		"counts": {groupBy: "pod", buckets: 1, counts: map[string][]int64{}, sets: map[string]map[uint64]struct{}{}},
		"hashes": {groupBy: "pod", buckets: 1, hashes: true, counts: map[string][]int64{}, sets: map[string]map[uint64]struct{}{}},
	} {
		c.add(seriesChunkRefs{lset: lset})
		keys := slices.Collect(maps.Keys(c.counts))
		if c.hashes {
			keys = slices.Collect(maps.Keys(c.sets))
		}
		require.Equal(t, []string{"a-long-pod-name"}, keys, name)
		assert.NotSame(t, unsafe.StringData(lset.Get("pod")), unsafe.StringData(keys[0]), "%s: the key is a copy of the value", name)
	}

	c := &seriesCounts{groupBy: "pod", buckets: 1, counts: map[string][]int64{}}
	v := lset.Get("pod")
	c.addCounts(map[string]int64{v: 2}, 0)
	keys := slices.Collect(maps.Keys(c.counts))
	require.Equal(t, []string{"a-long-pod-name"}, keys)
	assert.NotSame(t, unsafe.StringData(v), unsafe.StringData(keys[0]), "addCounts copies the value too")
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

// postingsBytesObserved is the postings bytes the store's count requests
// fetched from object storage, by the cardinality estimate count metric. The
// test stores have no index cache, so that is every postings byte read.
func postingsBytesObserved(t *testing.T, store *BucketStore) float64 {
	return testutil.ToFloat64(store.metrics.cardinalityEstimateFetchedBytes.WithLabelValues("postings"))
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
			resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{ids[0].String()}, MinTime: 0, MaxTime: 4 * hourMs})
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

			resp, err := store.countSeries(t.Context(), &storegatewaypb.SeriesCountsRequest{BlockIds: []string{ids[0].String()}, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod"})
			require.NoError(t, err)
			assert.Equal(t, map[string][]int64{long: {1}}, countsByGroup(resp))
			assert.Zero(t, resp.BlocksFromPostings, "the block was walked")
			assert.Positive(t, resp.PostingsFetchedBytes, "the all-postings list was fetched after all")
			assert.Positive(t, resp.SeriesFetchedBytes)
		})
	}
}

// A block whose index-header can't load is left out of a count answer's
// block IDs, so the querier asks another store-gateway for it, and the other
// blocks are still counted. A cancelled request fails instead.
func TestBucketStore_CountsLeaveOutABlockWhoseIndexHeaderDoesNotLoad(t *testing.T) {
	store, b1, b2 := prepareSeriesCountsStore(t)
	ctx := context.Background()
	alone, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 8 * hourMs})
	require.NoError(t, err)

	loadErr := errors.New("index-header is corrupt")
	store.blockSet.forEach(func(b *bucketBlock) {
		if b.meta.ULID == b2 {
			b.indexHeaderReader = &failingIndexHeaderReader{Reader: b.indexHeaderReader, err: loadErr}
		}
	})

	resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, MinTime: 0, MaxTime: 8 * hourMs})
	require.NoError(t, err)
	assert.Equal(t, []string{b1.String()}, resp.BlockIds)
	assert.Equal(t, countsByGroup(alone), countsByGroup(resp))

	// A block counted from its offsets table alone is left out the same way.
	srv := &seriesCountsServerMock{ctx: ctx}
	require.NoError(t, store.SeriesCounts(&storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, MinTime: 0, MaxTime: 8 * hourMs}, srv))
	require.NotEmpty(t, srv.sent)
	assert.Equal(t, []string{b1.String()}, srv.sent[0].BlockIds)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	store.blockSet.forEach(func(b *bucketBlock) {
		if b.meta.ULID == b2 {
			b.indexHeaderReader.(*failingIndexHeaderReader).err = context.Canceled
		}
	})
	_, err = store.countSeries(cancelled, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, MinTime: 0, MaxTime: 8 * hourMs})
	assert.Equal(t, codes.Canceled, status.Code(err), err)
}

// An index-header with no all-postings entry is corrupt: the index writer
// always writes one. Its block is left out of a count answer's block IDs,
// like a block whose index-header doesn't load, rather than counted as a
// block of no series.
func TestBucketStore_CountsLeaveOutABlockWithoutAllPostings(t *testing.T) {
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			store, b1, b2 := prepareSeriesCountsStore(t, opts...)
			ctx := t.Context()
			alone, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 8 * hourMs})
			require.NoError(t, err)

			var corrupt *bucketBlock
			store.blockSet.forEach(func(b *bucketBlock) {
				if b.meta.ULID == b2 {
					b.indexHeaderReader = &noAllPostingsIndexHeaderReader{Reader: b.indexHeaderReader}
					corrupt = b
				}
			})
			require.NotNil(t, corrupt)

			_, err = allPostingsCount(ctx, corrupt.indexHeaderReader)
			require.ErrorIs(t, err, indexheader.NotFoundRangeErr, "the count is not 0")
			indexr := corrupt.indexReader(store.postingsStrategy)
			_, err = allPostingsForBlock(ctx, indexr, newSafeQueryStats())
			require.ErrorIs(t, err, indexheader.NotFoundRangeErr, "the list is not empty")
			require.NoError(t, indexr.Close())

			for _, req := range []*storegatewaypb.SeriesCountsRequest{
				{BlockIds: []string{b1.String(), b2.String()}, MinTime: 0, MaxTime: 8 * hourMs},               // counted from the offsets tables
				{BlockIds: []string{b1.String(), b2.String()}, MinTime: 0, MaxTime: 8 * hourMs, Hashes: true}, // walked
			} {
				resp, err := store.countSeries(ctx, req)
				require.NoError(t, err)
				assert.Equal(t, []string{b1.String()}, resp.BlockIds, "hashes %v", req.Hashes)
				if !req.Hashes {
					assert.Equal(t, countsByGroup(alone), countsByGroup(resp))
				}
			}
		})
	}
}

// noAllPostingsIndexHeaderReader is an index-header whose postings offsets
// table has no all-postings entry.
type noAllPostingsIndexHeaderReader struct {
	indexheader.Reader
}

func (r *noAllPostingsIndexHeaderReader) PostingsOffset(ctx context.Context, name, value string) (index.Range, error) {
	if name == allPostingsKey.Name && value == allPostingsKey.Value {
		return index.Range{}, indexheader.NotFoundRangeErr
	}
	return r.Reader.PostingsOffset(ctx, name, value)
}

// failingIndexHeaderReader is an index-header that doesn't load: every read
// fails with err, as a lazy reader's does while its load keeps failing.
type failingIndexHeaderReader struct {
	indexheader.Reader
	err error
}

func (r *failingIndexHeaderReader) IndexVersion(context.Context) (int, error) { return 0, r.err }
func (r *failingIndexHeaderReader) PostingsOffset(context.Context, string, string) (index.Range, error) {
	return index.Range{}, r.err
}
func (r *failingIndexHeaderReader) LookupSymbol(context.Context, uint32) (string, error) {
	return "", r.err
}
func (r *failingIndexHeaderReader) SymbolsReader(context.Context) (streamindex.SymbolsReader, error) {
	return nil, r.err
}
func (r *failingIndexHeaderReader) LabelValuesOffsets(context.Context, string, string, func(string) bool) ([]streamindex.PostingListOffset, error) {
	return nil, r.err
}
func (r *failingIndexHeaderReader) LabelNames(context.Context) ([]string, error) { return nil, r.err }
func (r *failingIndexHeaderReader) LabelValuesCountLowerBound(context.Context, string) (int, error) {
	return 0, r.err
}
func (r *failingIndexHeaderReader) LabelValuesOffsetsTableBytes(context.Context, string) (int64, error) {
	return 0, r.err
}

// Count requests write their own metrics, not the Series ones, each
// observation of which means one Series request.
func TestBucketStore_CountsHaveTheirOwnMetrics(t *testing.T) {
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			testBucketStoreCountsHaveTheirOwnMetrics(t, opts...)
		})
	}
}

func testBucketStoreCountsHaveTheirOwnMetrics(t *testing.T, opts ...prepareStoreConfigOption) {
	store, b1, b2 := prepareSeriesCountsStore(t, opts...)
	ctx := t.Context()
	seriesSamples := func() (n uint64) {
		for _, vec := range []*prometheus.SummaryVec{store.metrics.seriesDataTouched, store.metrics.seriesDataFetched, store.metrics.seriesDataSizeTouched, store.metrics.seriesDataSizeFetched} {
			for _, dataType := range []string{"postings", "series"} {
				m := &dto.Metric{}
				require.NoError(t, vec.WithLabelValues(dataType, "").(prometheus.Metric).Write(m))
				n += m.GetSummary().GetSampleCount()
			}
		}
		return n
	}
	loads := func() float64 {
		return testutil.ToFloat64(store.metrics.cardinalityEstimateIndexHeaderLoads)
	}
	fetched := func(dataType string) float64 {
		return testutil.ToFloat64(store.metrics.cardinalityEstimateFetchedBytes.WithLabelValues(dataType))
	}
	before := seriesSamples()

	up := []*storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"}}
	req := &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, Matchers: up, MinTime: 0, MaxTime: 8*hourMs - 1}
	_, err := store.countSeries(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, before, seriesSamples(), "nothing goes into the Series metrics")
	assert.Equal(t, float64(2), loads(), "both index-headers were loaded for the request")
	loadTimes := &dto.Metric{}
	require.NoError(t, store.metrics.cardinalityEstimateIndexHeaderLoadDuration.Write(loadTimes))
	assert.Equal(t, uint64(2), loadTimes.GetHistogram().GetSampleCount())
	assert.Positive(t, fetched("postings"))
	assert.Positive(t, fetched("series"))

	_, err = store.countSeries(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, float64(2), loads(), "loaded index-headers aren't loaded again")

	// A block inside the window is counted from the group-by label's
	// postings, which walks its offsets table.
	tableBefore := fetched("offsets_table")
	resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod"})
	require.NoError(t, err)
	require.Positive(t, resp.PostingsOffsetsTableBytes)
	assert.Equal(t, float64(resp.PostingsOffsetsTableBytes), fetched("offsets_table")-tableBefore)
	// A request refused before the walk is charged the table but didn't read it.
	tableBefore = fetched("offsets_table")
	_, err = store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod", MaxIndexBytes: 1})
	require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
	assert.Equal(t, tableBefore, fetched("offsets_table"))

	assert.Equal(t, before, seriesSamples())
}

// With matchers, a SeriesCounts request prices every block's postings from
// the postings offsets table before it fetches any, at what the fetch reads.
func TestBucketStore_SeriesCountsPricesPostingsFirst(t *testing.T) {
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			testBucketStoreSeriesCountsPricesPostingsFirst(t, opts...)
		})
	}
}

func testBucketStoreSeriesCountsPricesPostingsFirst(t *testing.T, opts ...prepareStoreConfigOption) {
	store, b1, b2 := prepareSeriesCountsStore(t, opts...)
	ctx := t.Context()
	selectors := map[string][]*labels.Matcher{
		"equal":                 {labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")},
		"regex over most names": {labels.MustNewMatcher(labels.MatchRegexp, "__name__", "up|errors_total")},
		"not equal":             {labels.MustNewMatcher(labels.MatchNotEqual, "pod", "p1")},
		"not regex":             {labels.MustNewMatcher(labels.MatchNotRegexp, "pod", "p1|p2")},
		"matches empty":         {labels.MustNewMatcher(labels.MatchRegexp, "pod", "p1|")},
		"two matchers":          {labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"), labels.MustNewMatcher(labels.MatchNotEqual, "pod", "p3")},
	}

	t.Run("the price is what the fetch reads", func(t *testing.T) {
		store.blockSet.forEach(func(b *bucketBlock) {
			require.NoError(t, b.loadIndexHeader(ctx, newSafeQueryStats()))
			for name, ms := range selectors {
				indexr := b.indexReader(store.postingsStrategy)
				plan, err := indexr.planPostings(ctx, ms)
				require.NoError(t, err, name)
				stats := newSafeQueryStats()
				planned, _, err := indexr.expandPlannedPostings(ctx, plan, stats)
				require.NoError(t, err, name)
				assert.Equal(t, int64(stats.export().postingsFetchedSizeSum), indexr.plannedFetchBytes(plan), "%s, block %s", name, b.meta.ULID)
				direct, _, err := indexr.expandedPostings(ctx, ms, newSafeQueryStats())
				require.NoError(t, err, name)
				assert.Equal(t, direct, planned, name)
				require.NoError(t, indexr.Close())
			}
		})
	})

	t.Run("a selector over most names is refused before any postings list is read", func(t *testing.T) {
		before := postingsBytesObserved(t, store)
		pb, err := storepb.PromMatchersToMatchers(selectors["regex over most names"]...)
		require.NoError(t, err)
		req := &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, MinTime: 0, MaxTime: 8*hourMs - 1, MaxIndexBytes: 10}
		for i := range pb {
			req.Matchers = append(req.Matchers, &pb[i])
		}
		_, err = store.countSeries(ctx, req)
		require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
		assert.Contains(t, err.Error(), "of postings lists its matchers select over 2 blocks")
		assert.Equal(t, before, postingsBytesObserved(t, store), "no postings list was fetched")
	})

	t.Run("blocks past the plans' memory cap are planned again when fetched, with the same answer", func(t *testing.T) {
		prev := countPostingsPlansMaxBytes
		t.Cleanup(func() { countPostingsPlansMaxBytes = prev })
		for name, ms := range selectors {
			pb, err := storepb.PromMatchersToMatchers(ms...)
			require.NoError(t, err)
			req := &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, MinTime: 0, MaxTime: 8*hourMs - 1, MaxIndexBytes: 1 << 30}
			for i := range pb {
				req.Matchers = append(req.Matchers, &pb[i])
			}
			countPostingsPlansMaxBytes = prev
			kept, err := store.countSeries(ctx, req)
			require.NoError(t, err, name)
			countPostingsPlansMaxBytes = 0
			replanned, err := store.countSeries(ctx, req)
			require.NoError(t, err, name)
			req.MaxIndexBytes = 0
			unpriced, err := store.countSeries(ctx, req)
			require.NoError(t, err, name)
			assert.Equal(t, countsByGroup(kept), countsByGroup(replanned), name)
			assert.Equal(t, countsByGroup(kept), countsByGroup(unpriced), name)
		}
	})
}

// With no matchers, a block that is walked has its all-postings list priced
// from the postings offsets table, and the list is fetched only once
// max_index_bytes and max_hashes have passed.
func TestBucketStore_SeriesCountsPricesAllPostingsFirst(t *testing.T) {
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			store, b1, b2 := prepareSeriesCountsStore(t, opts...)
			ctx := t.Context()
			// Hashes over two block ranges: both blocks are walked.
			req := func() *storegatewaypb.SeriesCountsRequest {
				return &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, MinTime: 0, MaxTime: 8 * hourMs, Hashes: true}
			}

			resp, err := store.countSeries(ctx, req())
			require.NoError(t, err)
			require.Len(t, resp.Groups, 2)
			allPostings := resp.PostingsFetchedBytes
			require.GreaterOrEqual(t, allPostings, int64(4*6), "six series over the two blocks, 4 bytes each")

			t.Run("max_index_bytes below the all-postings lists refuses before any is fetched", func(t *testing.T) {
				before := postingsBytesObserved(t, store)
				limited := req()
				limited.MaxIndexBytes = 4*6 - 1
				_, err := store.countSeries(ctx, limited)
				require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
				assert.Contains(t, err.Error(), "of all-postings lists over 2 blocks")
				assert.Equal(t, before, postingsBytesObserved(t, store), "no postings list was fetched")

				limited.MaxIndexBytes = allPostings + 6*tsdb.EstimatedSeriesP99Size
				again, err := store.countSeries(ctx, limited)
				require.NoError(t, err, "the lists and the series entries fit")
				assert.Equal(t, resp.SeriesCounted, again.SeriesCounted)
			})

			t.Run("max_hashes below the first block's series refuses before any is fetched", func(t *testing.T) {
				before := postingsBytesObserved(t, store)
				limited := req()
				limited.MaxHashes = 3
				_, err := store.countSeries(ctx, limited)
				require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
				n, ok := cardinality.HashesNeeded(status.Convert(err).Message())
				require.True(t, ok, err)
				assert.Equal(t, int64(4), n, "the first block's four series, from the offsets table")
				assert.Equal(t, before, postingsBytesObserved(t, store), "no postings list was fetched")
			})
		})
	}
}

// Asked for hashes, a request is refused before any series entry is read
// when the blocks of one time range already match more series than
// max_hashes, and during the walk once the distinct hashes held pass it.
func TestBucketStore_SeriesCountsMaxHashes(t *testing.T) {
	store, b1, b2 := prepareSeriesCountsStore(t)
	ctx := t.Context()
	up := []*storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"}}
	req := func(maxHashes int64, hashes bool) *storegatewaypb.SeriesCountsRequest {
		return &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, Matchers: up, MinTime: 0, MaxTime: 8 * hourMs, Hashes: hashes, MaxHashes: maxHashes}
	}
	seriesRead := func() float64 {
		return testutil.ToFloat64(store.metrics.cardinalityEstimateFetchedBytes.WithLabelValues("series"))
	}
	hashesNeeded := func(err error) int64 {
		require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
		n, ok := cardinality.HashesNeeded(status.Convert(err).Message())
		require.True(t, ok, err)
		return n
	}

	// Three up series in the first block, two in the second, and up{pod=p1}
	// in both: four distinct.
	before := seriesRead()
	_, err := store.countSeries(ctx, req(2, true))
	assert.Equal(t, int64(3), hashesNeeded(err), "the first block's range alone")
	assert.Equal(t, before, seriesRead(), "no series entry was read")

	_, err = store.countSeries(ctx, req(3, true))
	assert.Equal(t, int64(4), hashesNeeded(err), "refused during the walk")
	assert.Greater(t, seriesRead(), before)

	resp, err := store.countSeries(ctx, req(4, true))
	require.NoError(t, err, "a series in two block ranges is held once")
	assert.NotEmpty(t, resp.Groups)
	_, err = store.countSeries(ctx, req(1, false))
	require.NoError(t, err, "counts hold no hashes")
	_, err = store.countSeries(ctx, req(-1, true))
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	// A window that cuts through the blocks isn't priced before the walk:
	// their series may have no chunk in it.
	cut := req(2, true)
	cut.MaxTime = 4*hourMs - 1
	before = seriesRead()
	_, err = store.countSeries(ctx, cut)
	assert.Equal(t, int64(3), hashesNeeded(err), "refused during the walk at the third series in the window")
	assert.Greater(t, seriesRead(), before)

	// With a query shard of two, a shard is priced at half the series, and
	// refused during the walk past what it really hashes.
	sharded := req(0, true)
	sharded.Matchers = append(sharded.Matchers, &storepb.LabelMatcher{Type: storepb.LabelMatcher_EQ, Name: sharding.ShardLabel, Value: sharding.FormatShardIDLabelValue(0, 2)})
	resp, err = store.countSeries(ctx, sharded)
	require.NoError(t, err)
	var inShard int64
	for _, g := range resp.Groups {
		inShard += int64(len(g.Hashes))
	}
	require.Positive(t, inShard)
	sharded.MaxHashes = inShard
	_, err = store.countSeries(ctx, sharded)
	require.NoError(t, err)
	sharded.MaxHashes = inShard - 1
	_, err = store.countSeries(ctx, sharded)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), err)
}

// Each phase of a count request has its own span, with what it covered.
func TestBucketStore_CountsHaveSpansPerPhase(t *testing.T) {
	spans := testSpanExporter(t)

	store, b1, b2 := prepareSeriesCountsStore(t)
	ctx := t.Context()
	up := []*storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"}}
	_, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, Matchers: up, MinTime: 0, MaxTime: 8*hourMs - 1})
	require.NoError(t, err)

	attrs := map[string]map[string]int64{}
	for _, s := range spans.GetSpans() {
		m := map[string]int64{}
		for _, a := range s.Attributes {
			m[string(a.Key)] = a.Value.AsInt64()
		}
		attrs[s.Name] = m
	}
	require.Contains(t, attrs, "BucketStore.SeriesCounts plan")
	assert.Equal(t, int64(2), attrs["BucketStore.SeriesCounts plan"]["blocks_loaded"])
	require.Contains(t, attrs, "BucketStore.SeriesCounts postings")
	assert.Equal(t, int64(5), attrs["BucketStore.SeriesCounts postings"]["series_matched_summed_over_blocks"], "up{pod=p1} is in both blocks")
	require.Contains(t, attrs, "BucketStore.SeriesCounts walk")
	assert.Equal(t, int64(2), attrs["BucketStore.SeriesCounts walk"]["blocks"])
	assert.Equal(t, int64(5), attrs["BucketStore.SeriesCounts walk"]["series_walked"])
}

// SeriesCounts streams its answer, and says how many blocks it counted
// from the postings offsets table alone.
func TestBucketStore_SeriesCountsStreams(t *testing.T) {
	store, b1, b2 := prepareSeriesCountsStore(t)
	srv := &seriesCountsServerMock{ctx: t.Context()}
	require.NoError(t, store.SeriesCounts(&storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, MinTime: 0, MaxTime: 8 * hourMs, GroupBy: "pod"}, srv))
	require.Len(t, srv.sent, 1)
	assert.Equal(t, int64(2), srv.sent[0].BlocksFromOffsetsTable, "both blocks inside the window, no matchers")
	assert.ElementsMatch(t, []string{b1.String(), b2.String()}, srv.sent[0].BlockIds)

	srv = &seriesCountsServerMock{ctx: t.Context()}
	up := []*storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"}}
	require.NoError(t, store.SeriesCounts(&storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: "pod"}, srv))
	assert.Zero(t, srv.sent[0].BlocksFromOffsetsTable, "a matcher reads postings")

	// A refusal is sent as the call's error, with nothing streamed before it.
	srv = &seriesCountsServerMock{ctx: t.Context()}
	err := store.SeriesCounts(&storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, MinTime: 0, MaxTime: 4 * hourMs, MaxIndexBytes: 1}, srv)
	require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
	assert.Empty(t, srv.sent)
}

type seriesCountsServerMock struct {
	storegatewaypb.StoreGateway_SeriesCountsServer
	ctx  context.Context
	sent []*storegatewaypb.SeriesCountsResponse
}

func (m *seriesCountsServerMock) Send(r *storegatewaypb.SeriesCountsResponse) error {
	m.sent = append(m.sent, r)
	return nil
}
func (m *seriesCountsServerMock) Context() context.Context { return m.ctx }

// max_groups caps the groups of an answer for any group_by: before any
// postings offsets table is walked when, with no matchers, one block inside
// the window already holds more values than the cap, and after counting.
func TestBucketStore_SeriesCountsMaxGroups(t *testing.T) {
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			store, b1, _ := prepareSeriesCountsStore(t, opts...)
			ctx := t.Context()
			walked := 0
			store.blockSet.forEach(func(b *bucketBlock) {
				b.indexHeaderReader = &interceptedIndexReader{Reader: b.indexHeaderReader, onLabelValuesOffsetsCalled: func(string) error {
					walked++
					return nil
				}}
			})
			// The first block holds up{pod=p1,p2,p3} and errors_total{pod=p1}.
			req := func(groupBy string, maxGroups int64, matchers ...*storepb.LabelMatcher) *storegatewaypb.SeriesCountsRequest {
				return &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: matchers, MinTime: 0, MaxTime: 4 * hourMs, GroupBy: groupBy, MaxGroups: maxGroups}
			}

			_, err := store.countSeries(ctx, req("__name__", 1))
			require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
			assert.ErrorContains(t, err, "holds at least 2 distinct values of __name__, more than max_groups 1")
			assert.Zero(t, walked, "no postings offsets table is walked")
			// The samples give a lower bound only, here 2 of pod's 3 values, so
			// the answer's groups refuse it.
			_, err = store.countSeries(ctx, req("pod", 2))
			require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
			assert.ErrorContains(t, err, "3 distinct values of pod, more than max_groups 2", "any group_by")

			walked = 0
			resp, err := store.countSeries(ctx, req("__name__", 2))
			require.NoError(t, err)
			assert.Len(t, resp.Groups, 2)
			assert.Equal(t, 1, walked, "under the cap the table is walked")

			// With a matcher the block's values say nothing about the groups,
			// so the cap is checked on the answer.
			up := &storepb.LabelMatcher{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"}
			_, err = store.countSeries(ctx, req("__name__", 1, up))
			require.NoError(t, err, "one group")
			_, err = store.countSeries(ctx, req("pod", 2, up))
			require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
			assert.ErrorContains(t, err, "the blocks hold 3 distinct values of pod, more than max_groups 2")

			// A window that cuts through the block isn't checked before
			// counting: its values may have no sample in the window.
			cut := req("__name__", 1)
			cut.MaxTime = 2 * hourMs
			resp, err = store.countSeries(ctx, cut)
			require.NoError(t, err, "errors_total has no sample in the first two hours")
			assert.Len(t, resp.Groups, 1)

			_, err = store.countSeries(ctx, req("pod", -1))
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

// The metric-name table of a block range is a SeriesCounts request with no
// matchers grouped by __name__, counted from the postings offsets tables.
func TestBucketStore_SeriesCountsNameTable(t *testing.T) {
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			testBucketStoreSeriesCountsNameTable(t, opts...)
		})
	}
}

func testBucketStoreSeriesCountsNameTable(t *testing.T, opts ...prepareStoreConfigOption) {
	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)

	cfg := defaultPrepareStoreConfig(t)
	// Non-overlapping blocks: the first block range holds only the first
	// block, with series[:4].
	cfg.nonOverlappingBlocks = true
	cfg.numBlocks = 2
	cfg.series = []labels.Labels{
		labels.FromStrings("__name__", "up", "job", "a"),
		labels.FromStrings("__name__", "up", "job", "b"),
		labels.FromStrings("__name__", "up", "job", "c"),
		labels.FromStrings("__name__", "requests_total", "job", "a"),
		labels.FromStrings("__name__", "up", "job", "d"),
		labels.FromStrings("__name__", "errors_total", "job", "a"),
		labels.FromStrings("__name__", "errors_total", "job", "b"),
		labels.FromStrings("__name__", "errors_total", "job", "c"),
	}
	for _, o := range opts {
		o(cfg)
	}
	s := prepareStoreWithTestBlocks(t, bkt, cfg)

	var (
		first      string
		minT, maxT int64
	)
	s.store.blockSet.forEach(func(b *bucketBlock) {
		if b.meta.MinTime == s.minTime {
			first, minT, maxT = b.meta.ULID.String(), b.meta.MinTime, b.meta.MaxTime
		}
	})
	require.NotEmpty(t, first)
	walked := 0
	s.store.blockSet.forEach(func(b *bucketBlock) {
		b.indexHeaderReader = &interceptedIndexReader{Reader: b.indexHeaderReader, onLabelValuesOffsetsCalled: func(string) error {
			walked++
			return nil
		}}
	})
	table := func(ids []string, maxIndexBytes int64) (*seriesCountsServerMock, error) {
		srv := &seriesCountsServerMock{ctx: t.Context()}
		err := s.store.SeriesCounts(&storegatewaypb.SeriesCountsRequest{BlockIds: ids, MinTime: minT, MaxTime: maxT, GroupBy: "__name__", MaxIndexBytes: maxIndexBytes}, srv)
		return srv, err
	}

	// It counts exactly the requested blocks it holds, says which, and that
	// it read only their offsets tables.
	unknown := ulid.MustNew(1, nil)
	srv, err := table([]string{first, unknown.String()}, 0)
	require.NoError(t, err)
	require.NotEmpty(t, srv.sent)
	assert.Equal(t, []string{first}, srv.sent[0].BlockIds)
	assert.Equal(t, int64(1), srv.sent[0].BlocksFromOffsetsTable)
	counts := map[string]int64{}
	for _, msg := range srv.sent {
		for _, g := range msg.Groups {
			counts[g.Value] = g.Counts[0]
		}
	}
	assert.Equal(t, map[string]int64{"up": 3, "requests_total": 1}, counts)
	assert.Zero(t, srv.sent[0].IndexBytes, "no postings and no series entries are read")
	tableBytes := srv.sent[0].PostingsOffsetsTableBytes
	require.Positive(t, tableBytes, "the first message reports the table walked")

	// The tenant's max_index_bytes, and the request's, bound the table
	// before it is walked.
	s.store.cardinalityEstimateMaxIndexBytes = func() int64 { return 1 }
	walked = 0
	srv, err = table([]string{first}, 0)
	require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
	require.ErrorContains(t, err, "__name__ postings offsets table")
	assert.Zero(t, walked)
	assert.Empty(t, srv.sent)
	s.store.cardinalityEstimateMaxIndexBytes = nil
	_, err = table([]string{first}, tableBytes)
	require.NoError(t, err)
	walked = 0
	_, err = table([]string{first}, tableBytes-1)
	require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
	assert.Zero(t, walked)

	// An unknown block is a one-message stream that says nothing was counted.
	srv, err = table([]string{unknown.String()}, 0)
	require.NoError(t, err)
	require.Len(t, srv.sent, 1)
	assert.Empty(t, srv.sent[0].BlockIds)
	assert.Empty(t, srv.sent[0].Groups)

	_, err = table([]string{"not-a-ulid"}, 0)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
