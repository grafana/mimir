// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
	"github.com/grafana/mimir/pkg/storage/indexheader"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/util/test"
)

const hourMs = int64(60 * 60 * 1000)

// groupHashes is the hashes whose memory one group is charged as.
const groupHashes = int64(cardinality.GroupBytes / cardinality.HashBytes)

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
	up := []cardinalitypb.LabelMatcher{{Type: labels.MatchEqual, Name: "__name__", Value: "up"}}

	t.Run("a block inside the window is counted from postings", func(t *testing.T) {
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 4 * hourMs})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {3}, "errors_total": {1}}, countsByGroup(resp))
		assert.Equal(t, []string{b1.String()}, resp.BlockIds)
		assert.Equal(t, int64(1), resp.BlocksFromPostings)
		assert.Zero(t, resp.SeriesFetchedBytes, "no series entry is read")

		byPod, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, StartMs: 0, EndMs: 4 * hourMs, GroupBy: "pod"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"p1": {1}, "p2": {1}, "p3": {1}}, countsByGroup(byPod), "each pod's postings intersected with up")
		assert.Equal(t, int64(1), byPod.BlocksFromPostings)
		assert.Zero(t, byPod.SeriesFetchedBytes)

		noLabel, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, StartMs: 0, EndMs: 4 * hourMs, GroupBy: "job"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"": {3}}, countsByGroup(noLabel), "series without the label count under the empty value")

		allPods, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 4 * hourMs, GroupBy: "pod"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"p1": {2}, "p2": {1}, "p3": {1}}, countsByGroup(allPods), "without matchers the offsets alone give the counts")
		assert.Zero(t, allPods.SeriesFetchedBytes)
	})

	t.Run("with no matchers a block is counted from the offsets table without fetching its postings", func(t *testing.T) {
		for name, req := range map[string]*storegatewaypb.SeriesCountsRequest{
			"by metric name": {BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 4 * hourMs},
			"by pod":         {BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 4 * hourMs, GroupBy: "pod"},
		} {
			resp, err := store.countSeries(ctx, req)
			require.NoError(t, err, name)
			assert.Equal(t, int64(1), resp.BlocksFromPostings, name)
			assert.Zero(t, resp.PostingsFetchedBytes, name)
			assert.Zero(t, resp.IndexBytes, "%s: no postings list and no series entry is read", name)
		}
		all, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 4 * hourMs, GroupBy: "job"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"": {4}}, countsByGroup(all), "series without the label are counted from the series count alone")
	})

	t.Run("a request refused on the offsets table fetches no postings", func(t *testing.T) {
		before := postingsBytesObserved(t, store)
		_, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, StartMs: 0, EndMs: 8 * hourMs, GroupBy: "pod", MaxIndexBytes: 1})
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
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 4*hourMs - 1})
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
		all, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 4 * hourMs})
		require.NoError(t, err)
		anyName := []cardinalitypb.LabelMatcher{{Type: labels.MatchRegexp, Name: "__name__", Value: ".+"}}
		viaNames, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: anyName, StartMs: 0, EndMs: 4 * hourMs})
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
		upOnP1P2 := []cardinalitypb.LabelMatcher{
			{Type: labels.MatchEqual, Name: "__name__", Value: "up"},
			{Type: labels.MatchRegexp, Name: "pod", Value: "p[12]"},
		}
		resp, err := lazyStore.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{lb1.String()}, Matchers: upOnP1P2, StartMs: 0, EndMs: 4 * hourMs, GroupBy: "pod"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"p1": {1}, "p2": {1}}, countsByGroup(resp), "p3 is filtered out by the pending matcher")
		assert.Equal(t, int64(2), resp.SeriesCounted)
	})

	t.Run("window inside the block", func(t *testing.T) {
		// Only upB has a chunk in [1h, 3h); upA ends just before it and upC
		// has a gap over it. errA is in hour 2.
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: hourMs, EndMs: 3 * hourMs})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {1}, "errors_total": {1}}, countsByGroup(resp))
	})

	t.Run("group by another label", func(t *testing.T) {
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, StartMs: 0, EndMs: 4 * hourMs, GroupBy: "pod"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"p1": {1}, "p2": {1}, "p3": {1}}, countsByGroup(resp))
	})

	t.Run("hashes dedup a series repeated across blocks", func(t *testing.T) {
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, Matchers: up, StartMs: 0, EndMs: 8 * hourMs, Hashes: true})
		require.NoError(t, err)
		require.Len(t, resp.Groups, 1)
		assert.Equal(t, "up", resp.Groups[0].Value)
		assert.Len(t, resp.Groups[0].Hashes, 4, "upA is in both blocks and counts once")
		assert.Contains(t, resp.Groups[0].Hashes, labels.StableHash(labels.FromStrings("__name__", "up", "pod", "p1")))
		assert.Empty(t, resp.Groups[0].Counts)
	})

	t.Run("max_index_bytes rejects before reading series", func(t *testing.T) {
		// Four series at no less than the 32-byte floor are more than 100 bytes.
		// The window ends before the block does, so the series entries are needed.
		_, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 4*hourMs - 1, MaxIndexBytes: 100})
		require.Error(t, err)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
		assert.Contains(t, err.Error(), "then 4 series at an estimated")
	})

	t.Run("max_hashes rejects a request for the hashes of more series", func(t *testing.T) {
		// b1 holds four series.
		_, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 4*hourMs - 1, Hashes: true, MaxHashes: 2})
		require.Error(t, err)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
		assert.Contains(t, err.Error(), "more than max_hashes 2")
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 4*hourMs - 1, MaxHashes: 2})
		require.NoError(t, err, "counts hold no hashes")
		assert.NotEmpty(t, resp.Groups)
	})

	t.Run("max_index_bytes above the plan is exact", func(t *testing.T) {
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 4 * hourMs, MaxIndexBytes: 1 << 20})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {3}, "errors_total": {1}}, countsByGroup(resp))
		assert.LessOrEqual(t, resp.IndexBytes, int64(1<<20))
	})

	t.Run("max_index_bytes covers the group-by label's postings offsets table", func(t *testing.T) {
		req := func(limit int64) *storegatewaypb.SeriesCountsRequest {
			return &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 4 * hourMs, GroupBy: "pod", MaxIndexBytes: limit}
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
		notHeld := store.metrics.cardinalityEstimateBlocksUncounted.WithLabelValues("not_held")
		before := testutil.ToFloat64(notHeld)
		resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{ulid.MustNew(1, nil).String()}, StartMs: 0, EndMs: 4 * hourMs})
		require.NoError(t, err)
		assert.Empty(t, resp.BlockIds)
		assert.Empty(t, resp.Groups)
		assert.Equal(t, before+1, testutil.ToFloat64(notHeld))
	})

	for name, req := range map[string]*storegatewaypb.SeriesCountsRequest{
		"empty window":             {StartMs: 10, EndMs: 10},
		"bad block ID":             {StartMs: 0, EndMs: 4 * hourMs, BlockIds: []string{"x"}},
		"negative max_index_bytes": {StartMs: 0, EndMs: 4 * hourMs, MaxIndexBytes: -1},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := store.countSeries(ctx, req)
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err), err)
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
			h := []cardinalitypb.LabelMatcher{{Type: labels.MatchEqual, Name: "__name__", Value: "h"}}

			resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{id}, StartMs: 0, EndMs: 3 * hourMs})
			require.NoError(t, err)
			assert.Equal(t, map[string][]int64{"h": {2}, "f": {1}}, countsByGroup(resp), "the whole block, from postings")

			resp, err = store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{id}, Matchers: h, StartMs: 0, EndMs: hourMs, GroupBy: "kind"})
			require.NoError(t, err)
			assert.Equal(t, map[string][]int64{"int": {1}}, countsByGroup(resp), "hour 0, from the chunk metas")

			resp, err = store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{id}, StartMs: hourMs, EndMs: 3 * hourMs, GroupBy: "kind"})
			require.NoError(t, err)
			assert.Equal(t, map[string][]int64{"float": {1}, "": {1}}, countsByGroup(resp), "a window that cuts the block leaves out the histogram outside it")
		})
	}
}

func TestSeriesCounts_IndexBytesChecks(t *testing.T) {
	c, err := newSeriesCountsState(&storegatewaypb.SeriesCountsRequest{StartMs: 0, EndMs: hourMs, MaxIndexBytes: 2000})
	require.NoError(t, err)
	require.NoError(t, c.checkPlan(900, 2, 2*512, 0), "900 + 2 x 512 is under the limit")
	err = c.checkPlan(1000, 2, 2*512, 0)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "1000 + 2 x 512 is over the limit")
	require.NoError(t, c.checkTable("pod", 1000, 1000))
	assert.Equal(t, codes.ResourceExhausted, status.Code(c.checkTable("pod", 1000, 1001)), "postings plus the offsets table are over the limit")
	require.NoError(t, c.checkRead(2000))
	assert.Equal(t, codes.ResourceExhausted, status.Code(c.checkRead(2001)), "series larger than the plan assumed")

	unlimited, err := newSeriesCountsState(&storegatewaypb.SeriesCountsRequest{StartMs: 0, EndMs: hourMs})
	require.NoError(t, err)
	require.NoError(t, unlimited.checkPlan(1<<40, 1<<20, 1<<29, 0))
	require.NoError(t, unlimited.checkRead(1<<40))
	require.NoError(t, unlimited.checkTable("pod", 1<<40, 1<<40))
}

// The response reports index_bytes as max_index_bytes charges them, so a
// caller that adds up answers stays within the limit the store-gateways apply.
func TestSeriesCounts_ResponseIndexBytesIsTheLimitedMeasure(t *testing.T) {
	for name, tc := range map[string]struct {
		touched, fetched int
		want             int64
	}{
		"postings fetched with the gaps between their lists": {touched: 100, fetched: 400, want: 400 + 50},
		"postings served from the cache":                     {touched: 100, fetched: 0, want: 100 + 50},
	} {
		t.Run(name, func(t *testing.T) {
			c, err := newSeriesCountsState(&storegatewaypb.SeriesCountsRequest{StartMs: 0, EndMs: hourMs})
			require.NoError(t, err)
			c.stats = newSafeQueryStats()
			c.stats.update(func(st *queryStats) {
				st.postingsTouchedSizeSum = tc.touched
				st.postingsFetchedSizeSum = tc.fetched
				st.seriesProcessedSizeSum = 50
			})
			resp := &storegatewaypb.SeriesCountsResponse{}
			require.NoError(t, c.finishResponse(resp))
			assert.Equal(t, tc.want, resp.IndexBytes)
			assert.Equal(t, indexBytes(c.stats), resp.IndexBytes)
		})
	}
}

// checkRead reports the series counted so far, and one block's walk calls
// it while another block's walk adds series under the mutex, so the count
// has to be safe to read without it. Fails under the race detector.
func TestSeriesCounts_CheckReadWhileAnotherBlockAdds(t *testing.T) {
	c, err := newSeriesCountsState(&storegatewaypb.SeriesCountsRequest{StartMs: 0, EndMs: hourMs, MaxIndexBytes: 1})
	require.NoError(t, err)
	series := seriesChunkRefs{lset: labels.FromStrings("__name__", "up")}
	var mtx sync.Mutex
	added := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		mtx.Lock()
		require.NoError(t, c.add(series))
		mtx.Unlock()
		close(added)
		// Over the limit, so every call reads the count for its message.
		for range 1000 {
			require.Error(t, c.checkRead(2))
		}
	}()
	go func() {
		defer wg.Done()
		<-added
		for range 1000 {
			mtx.Lock()
			require.NoError(t, c.add(series))
			mtx.Unlock()
		}
	}()
	wg.Wait()
	assert.Equal(t, int64(1001), c.counter.Counted())
}

// A group's key is a copy of the group-by value: the value is a slice of the
// series' label data, and the key must not keep the whole label set alive.
func TestSeriesCounts_GroupKeysDoNotPinLabelSets(t *testing.T) {
	lset := labels.FromStrings("__name__", "a", "pod", "a-long-pod-name")
	values := func(t *testing.T, c *seriesCountsState) []string {
		groups := c.counter.Groups()
		var out []string
		for _, g := range groups {
			out = append(out, g.Value)
		}
		return out
	}
	for name, req := range map[string]*storegatewaypb.SeriesCountsRequest{
		"counts": {StartMs: 0, EndMs: hourMs, GroupBy: "pod"},
		"hashes": {StartMs: 0, EndMs: hourMs, GroupBy: "pod", Hashes: true},
	} {
		c, err := newSeriesCountsState(req)
		require.NoError(t, err)
		require.NoError(t, c.add(seriesChunkRefs{lset: lset}))
		keys := values(t, c)
		require.Equal(t, []string{"a-long-pod-name"}, keys, name)
		assert.NotSame(t, unsafe.StringData(lset.Get("pod")), unsafe.StringData(keys[0]), "%s: the key is a copy of the value", name)
	}

	c, err := newSeriesCountsState(&storegatewaypb.SeriesCountsRequest{StartMs: 0, EndMs: hourMs, GroupBy: "pod"})
	require.NoError(t, err)
	v := lset.Get("pod")
	require.NoError(t, c.addCounts(map[string]int64{v: 2}, 0))
	keys := values(t, c)
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
			resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{ids[0].String()}, StartMs: 0, EndMs: 4 * hourMs})
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

			resp, err := store.countSeries(t.Context(), &storegatewaypb.SeriesCountsRequest{BlockIds: []string{ids[0].String()}, StartMs: 0, EndMs: 4 * hourMs, GroupBy: "pod"})
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
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			store, b1, b2 := prepareSeriesCountsStore(t, opts...)
			ctx := context.Background()
			alone, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 8 * hourMs})
			require.NoError(t, err)

			loadErr := errors.New("index-header is corrupt")
			store.blockSet.forEach(func(b *bucketBlock) {
				if b.meta.ULID == b2 {
					b.indexHeaderReader = &failingIndexHeaderReader{Reader: b.indexHeaderReader, err: loadErr}
				}
			})

			resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, StartMs: 0, EndMs: 8 * hourMs})
			require.NoError(t, err)
			assert.Equal(t, []string{b1.String()}, resp.BlockIds)
			assert.Equal(t, countsByGroup(alone), countsByGroup(resp))
			assert.Equal(t, 1.0, testutil.ToFloat64(store.metrics.cardinalityEstimateBlocksUncounted.WithLabelValues("index_header_load_failed")))

			// A block counted from its offsets table alone is left out the same way.
			srv := &seriesCountsServerMock{ctx: ctx}
			require.NoError(t, store.SeriesCounts(&storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, StartMs: 0, EndMs: 8 * hourMs}, srv))
			require.NotEmpty(t, srv.sent)
			assert.Equal(t, []string{b1.String()}, srv.sent[0].BlockIds)

			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			store.blockSet.forEach(func(b *bucketBlock) {
				if b.meta.ULID == b2 {
					b.indexHeaderReader.(*failingIndexHeaderReader).err = context.Canceled
				}
			})
			_, err = store.countSeries(cancelled, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, StartMs: 0, EndMs: 8 * hourMs})
			assert.Equal(t, codes.Canceled, status.Code(err), err)
		})
	}
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
			alone, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 8 * hourMs})
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
				{BlockIds: []string{b1.String(), b2.String()}, StartMs: 0, EndMs: 8 * hourMs},               // counted from the offsets tables
				{BlockIds: []string{b1.String(), b2.String()}, StartMs: 0, EndMs: 8 * hourMs, Hashes: true}, // walked
			} {
				resp, err := store.countSeries(ctx, req)
				require.NoError(t, err)
				assert.Equal(t, []string{b1.String()}, resp.BlockIds, "hashes %v", req.Hashes)
				assert.Equal(t, 1.0, testutil.ToFloat64(store.metrics.cardinalityEstimateBlocksUncounted.WithLabelValues("no_all_postings")), "hashes %v", req.Hashes)
				store.metrics.cardinalityEstimateBlocksUncounted.Reset()
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
func (r *failingIndexHeaderReader) LabelValuesStats(context.Context, string) (streamindex.LabelValuesStats, error) {
	return streamindex.LabelValuesStats{}, r.err
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
	loads := func() uint64 {
		m := &dto.Metric{}
		require.NoError(t, store.metrics.cardinalityEstimateIndexHeaderLoadDuration.Write(m))
		return m.GetHistogram().GetSampleCount()
	}
	fetched := func(dataType string) float64 {
		return testutil.ToFloat64(store.metrics.cardinalityEstimateFetchedBytes.WithLabelValues(dataType))
	}
	before := seriesSamples()

	up := []cardinalitypb.LabelMatcher{{Type: labels.MatchEqual, Name: "__name__", Value: "up"}}
	req := &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, Matchers: up, StartMs: 0, EndMs: 8*hourMs - 1}
	_, err := store.countSeries(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, before, seriesSamples(), "nothing goes into the Series metrics")
	assert.Equal(t, uint64(2), loads(), "both index-headers were loaded for the request")
	assert.Positive(t, fetched("postings"))
	assert.Positive(t, fetched("series"))

	_, err = store.countSeries(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), loads(), "loaded index-headers aren't loaded again")

	// A block inside the window is counted from the group-by label's
	// postings, which walks its offsets table.
	tableBefore := fetched("offsets_table")
	resp, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 4 * hourMs, GroupBy: "pod"})
	require.NoError(t, err)
	require.Positive(t, resp.PostingsOffsetsTableBytes)
	assert.Equal(t, float64(resp.PostingsOffsetsTableBytes), fetched("offsets_table")-tableBefore)
	// A request refused before the walk is charged the table but didn't read it.
	tableBefore = fetched("offsets_table")
	_, err = store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 4 * hourMs, GroupBy: "pod", MaxIndexBytes: 1})
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
		req := &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, StartMs: 0, EndMs: 8*hourMs - 1, MaxIndexBytes: 10}
		req.Matchers = cardinalitypb.ToLabelMatchers(selectors["regex over most names"])
		_, err := store.countSeries(ctx, req)
		require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
		assert.Contains(t, err.Error(), "of postings lists its matchers select over 2 blocks")
		assert.Equal(t, before, postingsBytesObserved(t, store), "no postings list was fetched")
	})

	t.Run("blocks past the plans' memory cap are planned again when fetched, with the same answer", func(t *testing.T) {
		prev := store.countPostingsPlansMaxBytes
		t.Cleanup(func() { store.countPostingsPlansMaxBytes = prev })
		for name, ms := range selectors {
			req := &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, StartMs: 0, EndMs: 8*hourMs - 1, MaxIndexBytes: 1 << 30}
			req.Matchers = cardinalitypb.ToLabelMatchers(ms)
			store.countPostingsPlansMaxBytes = prev
			kept, err := store.countSeries(ctx, req)
			require.NoError(t, err, name)
			store.countPostingsPlansMaxBytes = 0
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
				return &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, StartMs: 0, EndMs: 8 * hourMs, Hashes: true}
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
				assert.Contains(t, err.Error(), "6 series at an estimated", "priced from meta.json before the index-headers load")
				assert.Contains(t, err.Error(), "24 bytes of postings")
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
				n, ok := cardinality.HashesNeeded(err)
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
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			store, b1, b2 := prepareSeriesCountsStore(t, opts...)
			ctx := t.Context()
			up := []cardinalitypb.LabelMatcher{{Type: labels.MatchEqual, Name: "__name__", Value: "up"}}
			req := func(maxHashes int64, hashes bool) *storegatewaypb.SeriesCountsRequest {
				return &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, Matchers: up, StartMs: 0, EndMs: 8 * hourMs, Hashes: hashes, MaxHashes: maxHashes}
			}
			seriesRead := func() float64 {
				return testutil.ToFloat64(store.metrics.cardinalityEstimateFetchedBytes.WithLabelValues("series"))
			}
			hashesNeeded := func(err error) int64 {
				require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
				n, ok := cardinality.HashesNeeded(err)
				require.True(t, ok, err)
				return n
			}

			// Three up series in the first block, two in the second, and up{pod=p1}
			// in both: four distinct.
			before := seriesRead()
			_, err := store.countSeries(ctx, req(2, true))
			assert.Equal(t, int64(3), hashesNeeded(err), "the first block's range alone")
			assert.Equal(t, before, seriesRead(), "no series entry was read")

			_, err = store.countSeries(ctx, req(3+groupHashes, true))
			assert.Equal(t, 4+groupHashes, hashesNeeded(err), "refused during the walk, with the one group charged")
			assert.Greater(t, seriesRead(), before)

			resp, err := store.countSeries(ctx, req(4+groupHashes, true))
			require.NoError(t, err, "a series in two block ranges is held once")
			assert.NotEmpty(t, resp.Groups)
			_, err = store.countSeries(ctx, req(1, false))
			require.NoError(t, err, "counts hold no hashes")
			_, err = store.countSeries(ctx, req(-1, true))
			assert.Equal(t, codes.InvalidArgument, status.Code(err))

			// A window that cuts through the blocks isn't priced before the walk:
			// their series may have no chunk in it.
			cut := req(2+groupHashes, true)
			cut.EndMs = 4*hourMs - 1
			before = seriesRead()
			_, err = store.countSeries(ctx, cut)
			assert.Equal(t, 3+groupHashes, hashesNeeded(err), "refused during the walk at the third series in the window")
			assert.Greater(t, seriesRead(), before)

			// With a query shard nothing is priced before the walk, which refuses
			// past what the shard really hashes.
			sharded := req(0, true)
			sharded.Matchers = append(sharded.Matchers, cardinalitypb.LabelMatcher{Type: labels.MatchEqual, Name: sharding.ShardLabel, Value: sharding.FormatShardIDLabelValue(0, 2)})
			resp, err = store.countSeries(ctx, sharded)
			require.NoError(t, err)
			var inShard int64
			for _, g := range resp.Groups {
				inShard += int64(len(g.Hashes))
			}
			require.Positive(t, inShard)
			sharded.MaxHashes = inShard + int64(len(resp.Groups))*groupHashes
			_, err = store.countSeries(ctx, sharded)
			require.NoError(t, err)
			sharded.MaxHashes--
			_, err = store.countSeries(ctx, sharded)
			assert.Equal(t, codes.ResourceExhausted, status.Code(err), err)
		})
	}
}

// The plan prices every matching series at the block's average entry size,
// never above the p99; a request whose matching series are far larger than
// that passes the plan and is stopped during the walk, once what was read
// passes max_index_bytes.
func TestBucketStore_SeriesCountsStopsMidRead(t *testing.T) {
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			// Two hundred one-chunk series keep the index's average entry
			// small, while the three big series carry 300 chunks each, well
			// over the p99 the plan prices at most.
			var specs block.SeriesSpecs
			for i := range 200 {
				specs = append(specs, &block.SeriesSpec{Labels: labels.FromStrings("__name__", "small", "i", fmt.Sprintf("%03d", i)), Chunks: []chunks.Meta{chunkAt(0, 4*hourMs-1)}})
			}
			for i := range 3 {
				var cs []chunks.Meta
				for c := range 300 {
					start := int64(c) * 10_000
					cs = append(cs, chunkAt(start, start+9_000))
				}
				specs = append(specs, &block.SeriesSpec{Labels: labels.FromStrings("__name__", "big", "i", fmt.Sprint(i)), Chunks: cs})
			}
			bkt, err := filesystem.NewBucket(t.TempDir())
			require.NoError(t, err)
			store, ids := prepareSeriesCountsStoreFromSpecs(t, bkt, t.TempDir(), []block.SeriesSpecs{specs}, nil, opts...)
			ctx := t.Context()
			big := []cardinalitypb.LabelMatcher{{Type: labels.MatchEqual, Name: "__name__", Value: "big"}}
			// The window ends before the block does, so the series entries
			// are read.
			req := func(limit int64) *storegatewaypb.SeriesCountsRequest {
				return &storegatewaypb.SeriesCountsRequest{BlockIds: []string{ids[0].String()}, Matchers: big, StartMs: 0, EndMs: 4*hourMs - 1, MaxIndexBytes: limit}
			}
			seriesRead := func() float64 {
				return testutil.ToFloat64(store.metrics.cardinalityEstimateFetchedBytes.WithLabelValues("series"))
			}

			unlimited, err := store.countSeries(ctx, req(0))
			require.NoError(t, err)
			assert.Equal(t, map[string][]int64{"big": {3}}, countsByGroup(unlimited))

			// walkStarted reports whether a request allowed limit index
			// bytes passed the checks before the walk: it read series
			// entries, and either was answered or stopped during the walk.
			walkStarted := func(t *testing.T, limit int64) bool {
				before := seriesRead()
				_, err := store.countSeries(ctx, req(limit))
				if err != nil {
					require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
					require.Equal(t, cardinality.LimitMaxIndexBytes, cardinality.RefusedBy(err), err)
				}
				return seriesRead() > before
			}
			// The least limit that passes the checks before the walk, the
			// postings one and then the plan: below it no series entry is
			// read, at and above it the walk starts.
			plan := int64(sort.Search(int(unlimited.IndexBytes), func(n int) bool { return walkStarted(t, int64(n)+1) })) + 1
			require.False(t, walkStarted(t, plan-1), "refused before any series entry was read")
			// That refusal names the limit that would pass it.
			_, err = store.countSeries(ctx, req(plan-1))
			assert.ErrorContains(t, err, fmt.Sprintf("would read about %d index bytes", plan))
			require.Less(t, plan, unlimited.IndexBytes, "the big series entries are larger than the plan assumed")

			// Allowed what the plan priced, the walk starts and stops.
			before := seriesRead()
			_, err = store.countSeries(ctx, req(plan))
			require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
			assert.Equal(t, cardinality.LimitMaxIndexBytes, cardinality.RefusedBy(err))
			assert.Greater(t, seriesRead(), before, "series entries were read before the stop")

			resp, err := store.countSeries(ctx, req(unlimited.IndexBytes))
			require.NoError(t, err, "what the request reads is exactly what it is allowed")
			assert.Equal(t, map[string][]int64{"big": {3}}, countsByGroup(resp))
		})
	}
}

// Before any series entry is read, the hashes a request will hold are
// bounded from below by the blocks inside the window: blocks of one range
// with distinct compactor shard IDs share no series, so their series counts
// add up, while blocks without a shard ID may share all of theirs.
func TestBucketStore_SeriesCountsMaxHashesSumsShardBlocks(t *testing.T) {
	upPods := func(pods ...string) block.SeriesSpecs {
		var specs block.SeriesSpecs
		for _, pod := range pods {
			specs = append(specs, &block.SeriesSpec{Labels: labels.FromStrings("__name__", "up", "pod", pod), Chunks: []chunks.Meta{chunkAt(0, 4*hourMs-1)}})
		}
		return specs
	}
	// Two blocks of the same range, three and two series.
	blocks := []block.SeriesSpecs{upPods("p1", "p2", "p3"), upPods("p4", "p5")}
	up := []cardinalitypb.LabelMatcher{{Type: labels.MatchEqual, Name: "__name__", Value: "up"}}
	hashesNeeded := func(t *testing.T, err error) int64 {
		require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
		n, ok := cardinality.HashesNeeded(err)
		require.True(t, ok, err)
		return n
	}
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			for _, sharded := range []bool{true, false} {
				t.Run(fmt.Sprintf("shard IDs: %t", sharded), func(t *testing.T) {
					var shardIDs []string
					if sharded {
						shardIDs = []string{"1_of_2", "2_of_2"}
					}
					store, ids := prepareSeriesCountsShardStore(t, shardIDs, blocks, opts...)
					ctx := t.Context()
					req := func(maxHashes int64) *storegatewaypb.SeriesCountsRequest {
						return &storegatewaypb.SeriesCountsRequest{BlockIds: []string{ids[0].String(), ids[1].String()}, Matchers: up, StartMs: 0, EndMs: 4 * hourMs, Hashes: true, MaxHashes: maxHashes}
					}
					seriesRead := func() float64 {
						return testutil.ToFloat64(store.metrics.cardinalityEstimateFetchedBytes.WithLabelValues("series"))
					}

					before := seriesRead()
					_, err := store.countSeries(ctx, req(4))
					if sharded {
						assert.Equal(t, int64(5), hashesNeeded(t, err))
						assert.Equal(t, before, seriesRead(), "the shards add up to five before any series entry is read")
					} else {
						assert.Equal(t, codes.ResourceExhausted, status.Code(err), err)
						assert.Greater(t, seriesRead(), before, "blocks without a shard ID may share series, so only the larger one bounds the hashes and the walk refuses")
					}

					resp, err := store.countSeries(ctx, req(5+groupHashes))
					require.NoError(t, err)
					require.Len(t, resp.Groups, 1)
					assert.Len(t, resp.Groups[0].Hashes, 5)
				})
			}
		})
	}
}

// prepareSeriesCountsShardStore is prepareSeriesCountsStoreFromSpecs with a
// compactor shard ID label on each block, when shardIDs is set.
func prepareSeriesCountsShardStore(t *testing.T, shardIDs []string, blocks []block.SeriesSpecs, opts ...prepareStoreConfigOption) (*BucketStore, []ulid.ULID) {
	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	dir := t.TempDir()
	var ids []ulid.ULID
	for i, specs := range blocks {
		meta, err := block.GenerateBlockFromSpec(dir, specs)
		require.NoError(t, err)
		if shardIDs != nil {
			meta.Thanos.Labels = map[string]string{block.CompactorShardIDExternalLabel: shardIDs[i]}
			require.NoError(t, meta.WriteToDir(log.NewNopLogger(), filepath.Join(dir, meta.ULID.String())))
		}
		_, err = block.Upload(context.Background(), log.NewNopLogger(), bkt, filepath.Join(dir, meta.ULID.String()), nil)
		require.NoError(t, err)
		ids = append(ids, meta.ULID)
	}
	cfg := defaultPrepareStoreConfig(t)
	cfg.numBlocks = 0
	for _, o := range opts {
		o(cfg)
	}
	return prepareStoreWithTestBlocks(t, bkt, cfg).store, ids
}

// Each phase of a count request has its own span, with what it covered.
func TestBucketStore_CountsHaveSpansPerPhase(t *testing.T) {
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			spans := testSpanExporter(t)

			store, b1, b2 := prepareSeriesCountsStore(t, opts...)
			ctx := t.Context()
			up := []cardinalitypb.LabelMatcher{{Type: labels.MatchEqual, Name: "__name__", Value: "up"}}
			_, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, Matchers: up, StartMs: 0, EndMs: 8*hourMs - 1})
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
		})
	}
}

// SeriesCounts streams its answer, and says how many blocks it counted
// from the postings offsets table alone.
func TestBucketStore_SeriesCountsStreams(t *testing.T) {
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			store, b1, b2 := prepareSeriesCountsStore(t, opts...)
			srv := &seriesCountsServerMock{ctx: t.Context()}
			require.NoError(t, store.SeriesCounts(&storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, StartMs: 0, EndMs: 8 * hourMs, GroupBy: "pod"}, srv))
			require.Len(t, srv.sent, 1)
			assert.Equal(t, int64(2), srv.sent[0].BlocksFromOffsetsTable, "both blocks inside the window, no matchers")
			assert.ElementsMatch(t, []string{b1.String(), b2.String()}, srv.sent[0].BlockIds)

			srv = &seriesCountsServerMock{ctx: t.Context()}
			up := []cardinalitypb.LabelMatcher{{Type: labels.MatchEqual, Name: "__name__", Value: "up"}}
			require.NoError(t, store.SeriesCounts(&storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, StartMs: 0, EndMs: 4 * hourMs, GroupBy: "pod"}, srv))
			assert.Zero(t, srv.sent[0].BlocksFromOffsetsTable, "a matcher reads postings")

			// A refusal is sent as the call's error, with nothing streamed before it.
			srv = &seriesCountsServerMock{ctx: t.Context()}
			err := store.SeriesCounts(&storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: up, StartMs: 0, EndMs: 4 * hourMs, MaxIndexBytes: 1}, srv)
			require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
			assert.Empty(t, srv.sent)
		})
	}
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
			req := func(groupBy string, maxGroups int64, matchers ...cardinalitypb.LabelMatcher) *storegatewaypb.SeriesCountsRequest {
				return &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, Matchers: matchers, StartMs: 0, EndMs: 4 * hourMs, GroupBy: groupBy, MaxGroups: maxGroups}
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
			// so the cap is checked as the walk creates them.
			up := cardinalitypb.LabelMatcher{Type: labels.MatchEqual, Name: "__name__", Value: "up"}
			_, err = store.countSeries(ctx, req("__name__", 1, up))
			require.NoError(t, err, "one group")
			_, err = store.countSeries(ctx, req("pod", 2, up))
			require.Equal(t, codes.ResourceExhausted, status.Code(err), err)
			assert.ErrorContains(t, err, "the blocks hold at least 3 distinct values of pod, more than max_groups 2")

			// A window that cuts through the block isn't checked before
			// counting: its values may have no sample in the window.
			cut := req("__name__", 1)
			cut.EndMs = 2 * hourMs
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
		err := s.store.SeriesCounts(&storegatewaypb.SeriesCountsRequest{BlockIds: ids, StartMs: minT, EndMs: maxT, GroupBy: "__name__", MaxIndexBytes: maxIndexBytes}, srv)
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

	// max_index_bytes bounds the table before it is walked.
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

func TestCountIntersection(t *testing.T) {
	// naive is the quadratic algorithm countIntersection replaced, kept as the reference.
	naive := func(p []storage.SeriesRef, sorted []storage.SeriesRef, scale storage.SeriesRef) int64 {
		var n int64
		i := 0
		for _, v := range p {
			ref := v * scale
			for i < len(sorted) && sorted[i] < ref {
				i++
			}
			if i < len(sorted) && sorted[i] == ref {
				n++
				i++
			}
		}
		return n
	}
	sample := func(rnd *rand.Rand, universe int, keep float64, scale storage.SeriesRef) []storage.SeriesRef {
		var out []storage.SeriesRef
		for v := 1; v <= universe; v++ {
			if rnd.Float64() < keep {
				out = append(out, storage.SeriesRef(v)*scale)
			}
		}
		return out
	}

	rnd := rand.New(rand.NewSource(1))
	for _, scale := range []storage.SeriesRef{1, 16} {
		for iter := 0; iter < 500; iter++ {
			universe := 1 + rnd.Intn(2000)
			sorted := sample(rnd, universe, rnd.Float64(), scale)
			list := sample(rnd, universe, rnd.Float64(), 1)
			got, err := countIntersection(context.Background(), index.NewListPostings(list), sorted, scale)
			require.NoError(t, err)
			require.Equal(t, naive(list, sorted, scale), got, "scale %d iteration %d", scale, iter)
		}
	}
}

// cancelingPostings cancels its context after a number of Next calls.
type cancelingPostings struct {
	index.Postings
	cancel func()
	after  int
	nexts  int
}

func (p *cancelingPostings) Next() bool {
	p.nexts++
	if p.nexts == p.after {
		p.cancel()
	}
	return p.Postings.Next()
}

func TestCountIntersection_StopsOnCancel(t *testing.T) {
	refs := make([]storage.SeriesRef, 10*countIntersectionCheckEvery)
	for i := range refs {
		refs[i] = storage.SeriesRef(i + 1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := countIntersection(ctx, index.NewListPostings(refs), refs, 1)
	require.ErrorIs(t, err, context.Canceled)

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	p := &cancelingPostings{Postings: index.NewListPostings(refs), cancel: cancel, after: 2 * countIntersectionCheckEvery}
	_, err = countIntersection(ctx, p, refs, 1)
	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, p.nexts, len(refs), "stopped before reading the whole list")
}

// A group past max_groups is refused when it is created, not once the walk has built every group.
func TestSeriesCounts_MaxGroupsRefusesAtTheGroupOverTheCap(t *testing.T) {
	series := func(pod string) seriesChunkRefs {
		return seriesChunkRefs{lset: labels.FromStrings("__name__", "up", "pod", pod)}
	}
	c, err := newSeriesCountsState(&storegatewaypb.SeriesCountsRequest{StartMs: 0, EndMs: hourMs, GroupBy: "pod", MaxGroups: 2})
	require.NoError(t, err)
	require.NoError(t, c.add(series("p1")))
	require.NoError(t, c.add(series("p2")))
	require.NoError(t, c.add(series("p1")), "a series of a group already held")
	err = c.add(series("p3"))
	require.Equal(t, cardinality.LimitMaxGroups, cardinality.RefusedBy(err), err)
	assert.ErrorContains(t, err, "at least 3 distinct values of pod, more than max_groups 2")

	c, err = newSeriesCountsState(&storegatewaypb.SeriesCountsRequest{StartMs: 0, EndMs: hourMs, GroupBy: "pod", MaxGroups: 2})
	require.NoError(t, err)
	require.NoError(t, c.addCounts(map[string]int64{"p1": 1, "p2": 1}, 0))
	err = c.addCounts(map[string]int64{"p3": 1, "p4": 1, "p5": 1}, 0)
	require.Equal(t, cardinality.LimitMaxGroups, cardinality.RefusedBy(err), err)
	assert.Equal(t, 3, c.counter.GroupCount(), "the counts stop at the first group over the cap")
}

// Each group of hashes is its own set, so max_hashes is charged for the groups as well as the hashes.
func TestBucketStore_SeriesCountsMaxHashesChargesGroups(t *testing.T) {
	store, b1, _ := prepareSeriesCountsStore(t)
	ctx := t.Context()
	// The first block holds three pods, each a group of one or two hashes: four hashes.
	req := func(maxHashes int64) *storegatewaypb.SeriesCountsRequest {
		return &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, StartMs: 0, EndMs: 4 * hourMs, GroupBy: "pod", Hashes: true, MaxHashes: maxHashes}
	}
	_, err := store.countSeries(ctx, req(4+3*groupHashes-1))
	require.Equal(t, cardinality.LimitMaxHashes, cardinality.RefusedBy(err), err)
	n, ok := cardinality.HashesNeeded(err)
	require.True(t, ok, err)
	assert.Equal(t, 4+3*groupHashes, n)

	resp, err := store.countSeries(ctx, req(4+3*groupHashes))
	require.NoError(t, err)
	assert.Len(t, resp.Groups, 3)
}

// What a block resolved, its matching postings and its group-by offsets, is dropped once the block
// is counted rather than held until every block is.
func TestBucketStore_SeriesCountsReleasesBlockStateAfterTheWalk(t *testing.T) {
	store, b1, b2 := prepareSeriesCountsStore(t)
	ctx := t.Context()
	for name, req := range map[string]*storegatewaypb.SeriesCountsRequest{
		"series entries":   {Matchers: []cardinalitypb.LabelMatcher{{Type: labels.MatchEqual, Name: "__name__", Value: "up"}}, StartMs: 0, EndMs: 8 * hourMs, Hashes: true},
		"group-by offsets": {StartMs: 0, EndMs: 8 * hourMs},
	} {
		t.Run(name, func(t *testing.T) {
			req.BlockIds = []string{b1.String(), b2.String()}
			c, err := store.newCountRequest(req)
			require.NoError(t, err)
			blocks, err := store.openCountBlocks(req.BlockIds)
			require.NoError(t, err)
			defer func() {
				for _, o := range blocks {
					require.NoError(t, o.indexr.Close())
				}
			}()
			loaded, err := store.loadCountBlocks(ctx, c, blocks)
			require.NoError(t, err)
			require.NoError(t, store.resolveCountPostings(ctx, c, loaded))
			require.Len(t, loaded, 2)
			for _, o := range loaded {
				require.True(t, o.postings != nil || o.groupOffsets != nil, "the block resolved something to hold")
			}
			resp, err := store.walkCountBlocks(ctx, c, loaded)
			require.NoError(t, err)
			require.Len(t, resp.BlockIds, 2)
			for _, o := range loaded {
				assert.Nil(t, o.postings)
				assert.Nil(t, o.groupOffsets)
			}
		})
	}
}

// A matcher that is planned by walking its label's postings offsets table has that table priced and
// charged to max_index_bytes before the plan walks it.
func TestBucketStore_SeriesCountsChargesLazyMatcherTables(t *testing.T) {
	for name, opts := range countIndexHeaderVersions {
		t.Run(name, func(t *testing.T) {
			store, b1, _ := prepareSeriesCountsStore(t, opts...)
			ctx := t.Context()
			var walked []string
			var podTable int64
			store.blockSet.forEach(func(b *bucketBlock) {
				if b.meta.ULID == b1 {
					require.NoError(t, b.loadIndexHeader(ctx, newSafeQueryStats()))
					labelStats, err := b.indexHeaderReader.LabelValuesStats(ctx, "pod")
					require.NoError(t, err)
					podTable = labelStats.OffsetsTableBytes
				}
				b.indexHeaderReader = &interceptedIndexReader{Reader: b.indexHeaderReader, onLabelValuesOffsetsCalled: func(name string) error {
					walked = append(walked, name)
					return nil
				}}
			})
			require.Positive(t, podTable)
			// Hashes, so the group-by label's table is not walked.
			req := func(maxIndexBytes int64) *storegatewaypb.SeriesCountsRequest {
				return &storegatewaypb.SeriesCountsRequest{
					BlockIds: []string{b1.String()},
					Matchers: []cardinalitypb.LabelMatcher{{Type: labels.MatchRegexp, Name: "pod", Value: "p.+"}},
					StartMs:  0, EndMs: 4 * hourMs, Hashes: true, MaxIndexBytes: maxIndexBytes,
				}
			}

			_, err := store.countSeries(ctx, req(podTable-1))
			require.Equal(t, cardinality.LimitMaxIndexBytes, cardinality.RefusedBy(err), err)
			assert.ErrorContains(t, err, "pod postings offsets table")
			assert.Empty(t, walked, "refused before the table is walked")

			resp, err := store.countSeries(ctx, req(1<<20))
			require.NoError(t, err)
			assert.Equal(t, []string{"pod"}, walked)
			assert.Equal(t, podTable, resp.PostingsOffsetsTableBytes)
			assert.Equal(t, int64(4), resp.SeriesCounted)
		})
	}
}

// max_index_bytes charges the postings bytes fetched when they are more than the bytes touched: one
// fetched range also holds the gaps between the lists merged into it.
func TestSeriesCounts_IndexBytesChargeFetchedPostings(t *testing.T) {
	stats := newSafeQueryStats()
	stats.update(func(s *queryStats) {
		s.postingsTouchedSizeSum = 100
		s.postingsFetchedSizeSum = 160
		s.seriesProcessedSizeSum = 40
	})
	assert.Equal(t, int64(200), indexBytes(stats), "lists with gaps fetched as one range")

	stats.update(func(s *queryStats) { s.postingsTouchedSizeSum = 500 })
	assert.Equal(t, int64(540), indexBytes(stats), "lists read from the cache are touched and not fetched")
}

// The series a query shard holds are not known before the walk, so no share of the matching series
// is taken as a lower bound: a shard smaller than the even share would be refused for hashes it never holds.
func TestSeriesCounts_HashesLowerBoundWithAQueryShard(t *testing.T) {
	c, err := newSeriesCountsState(&storegatewaypb.SeriesCountsRequest{StartMs: 0, EndMs: 4 * hourMs, Hashes: true})
	require.NoError(t, err)
	meta := &block.Meta{}
	meta.MinTime, meta.MaxTime = 0, 4*hourMs
	loaded := []countBlock{{b: &bucketBlock{meta: meta}, seriesCount: 100}}
	assert.Equal(t, int64(100), c.hashesLowerBound(loaded))

	c.shard = &sharding.ShardSelector{ShardIndex: 0, ShardCount: 4}
	assert.Zero(t, c.hashesLowerBound(loaded), "a shard of four may hold fewer than 25 of the 100 series")
}

// With no matchers the series counts in the blocks' meta.json are enough to refuse a request over
// max_index_bytes or max_hashes, so it is refused without loading an index-header.
func TestBucketStore_SeriesCountsChecksLimitsBeforeLoadingIndexHeaders(t *testing.T) {
	store, b1, b2 := prepareSeriesCountsStore(t)
	ctx := t.Context()
	loads := 0
	store.blockSet.forEach(func(b *bucketBlock) {
		b.indexHeaderReader = &interceptedIndexReader{Reader: b.indexHeaderReader, onIndexVersionCalled: func() error {
			loads++
			return nil
		}}
	})
	// Hashes over two block ranges of four and two series: both blocks are walked.
	req := func() *storegatewaypb.SeriesCountsRequest {
		return &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String(), b2.String()}, StartMs: 0, EndMs: 8 * hourMs, Hashes: true}
	}

	overIndexBytes := req()
	overIndexBytes.MaxIndexBytes = 4*6 - 1
	_, err := store.countSeries(ctx, overIndexBytes)
	require.Equal(t, cardinality.LimitMaxIndexBytes, cardinality.RefusedBy(err), err)
	assert.Zero(t, loads, "refused on the all-postings lists alone, from meta.json")

	overHashes := req()
	overHashes.MaxHashes = 3
	_, err = store.countSeries(ctx, overHashes)
	n, ok := cardinality.HashesNeeded(err)
	require.True(t, ok, err)
	assert.Equal(t, int64(4), n, "the first block's four series")
	assert.Zero(t, loads)

	// With a matcher the index-headers have to be read first.
	withMatcher := req()
	withMatcher.MaxIndexBytes = 1
	withMatcher.Matchers = []cardinalitypb.LabelMatcher{{Type: labels.MatchEqual, Name: "__name__", Value: "up"}}
	_, err = store.countSeries(ctx, withMatcher)
	require.Equal(t, cardinality.LimitMaxIndexBytes, cardinality.RefusedBy(err), err)
	assert.Positive(t, loads)
}
