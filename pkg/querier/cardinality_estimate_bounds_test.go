// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

// boundsOf plans blocks, folds each counted block's counts and finishes the ranges.
func boundsOf(blocks bucketindex.Blocks, marked map[ulid.ULID]struct{}, counts map[ulid.ULID]map[string]int64) []*BoundsRange {
	ranges, byID := planBounds(blocks, marked)
	counted := map[ulid.ULID]bool{}
	markedCounts := map[ulid.ULID]map[string]int64{}
	for id, c := range counts {
		byID[id].fold(c, markedCounts)
		counted[id] = true
	}
	return finishBounds(ranges, counted, markedCounts)
}

// hourMs is an hour in milliseconds.
const hourMs = dayMs / 24

func shardBlock(id uint64, minT, maxT int64, shard string) *bucketindex.Block {
	return &bucketindex.Block{ID: ulid.MustNew(id, nil), MinTime: minT, MaxTime: maxT, CompactorShardID: shard}
}

func oooBlock(id uint64, minT, maxT int64, shard string) *bucketindex.Block {
	b := shardBlock(id, minT, maxT, shard)
	b.Labels = map[string]string{block.OutOfOrderExternalLabel: block.OutOfOrderExternalLabelValue}
	return b
}

func TestBounds_Arithmetic(t *testing.T) {
	s1, s2 := shardBlock(1, 0, dayMs, "1_of_2"), shardBlock(2, 0, dayMs, "2_of_2")

	t.Run("a range of disjoint shards alone is exact", func(t *testing.T) {
		got := boundsOf(bucketindex.Blocks{s1, s2}, nil, map[ulid.ULID]map[string]int64{
			s1.ID: {"up": 3, "go_threads": 1},
			s2.ID: {"up": 2},
		})
		require.Len(t, got, 1)
		r := got[0]
		assert.True(t, r.MainSet)
		assert.Equal(t, 2, r.MainBlocks)
		assert.Equal(t, map[string]int64{"up": 5, "go_threads": 1}, r.Lower)
		assert.Equal(t, r.Lower, r.Upper)
		assert.False(t, r.LowerBound)
		assert.Equal(t, int64(0), r.MinT)
		assert.Equal(t, dayMs, r.MaxT)
	})

	t.Run("out-of-order blocks of the same shard IDs add to upper only", func(t *testing.T) {
		// up{a,b} in shard 1, up{c} in shard 2; the out-of-order block repeats up{a} and adds up{d}.
		o1 := oooBlock(3, 0, dayMs, "1_of_2")
		got := boundsOf(bucketindex.Blocks{s1, s2, o1}, nil, map[ulid.ULID]map[string]int64{
			s1.ID: {"up": 2},
			s2.ID: {"up": 1},
			o1.ID: {"up": 2},
		})
		require.Len(t, got, 1)
		r := got[0]
		assert.True(t, r.MainSet, "the out-of-order block is not part of the main set")
		assert.Equal(t, 2, r.MainBlocks)
		assert.Equal(t, 1, r.OtherBlocks)
		const truth = 4 // a, b, c, d
		assert.LessOrEqual(t, r.Lower["up"], int64(truth))
		assert.GreaterOrEqual(t, r.Upper["up"], int64(truth))
		assert.Equal(t, int64(3), r.Lower["up"])
		assert.Equal(t, int64(5), r.Upper["up"])
	})

	t.Run("an out-of-order block known only by its compacted label is not a main block", func(t *testing.T) {
		o := oooBlock(3, 0, dayMs, "1_of_2")
		require.False(t, o.OutOfOrder, "the compactor keeps no hint")
		got := boundsOf(bucketindex.Blocks{s1, s2, o}, nil, map[ulid.ULID]map[string]int64{s1.ID: {"up": 1}, s2.ID: {"up": 1}, o.ID: {"up": 1}})
		assert.True(t, got[0].MainSet)
		assert.Equal(t, int64(2), got[0].Lower["up"])
	})

	t.Run("a range with no main set takes the largest block as lower and the sum as upper", func(t *testing.T) {
		for name, blocks := range map[string]bucketindex.Blocks{
			"blocks without shard IDs":                    {shardBlock(1, 0, dayMs, ""), shardBlock(2, 0, dayMs, "")},
			"a shard ID repeated among in-order blocks":   {shardBlock(1, 0, dayMs, "1_of_2"), shardBlock(2, 0, dayMs, "1_of_2")},
			"two ranges that overlap, merged into one":    {shardBlock(1, 0, 14*hourMs, ""), shardBlock(2, 10*hourMs, dayMs, "")},
			"only out-of-order blocks of the exact range": {oooBlock(1, 0, dayMs, "1_of_2"), oooBlock(2, 0, dayMs, "2_of_2")},
		} {
			ids := []ulid.ULID{blocks[0].ID, blocks[1].ID}
			got := boundsOf(blocks, nil, map[ulid.ULID]map[string]int64{ids[0]: {"up": 3, "x": 1}, ids[1]: {"up": 2, "x": 4}})
			require.Len(t, got, 1, name)
			r := got[0]
			assert.False(t, r.MainSet, name)
			assert.Equal(t, map[string]int64{"up": 3, "x": 4}, r.Lower, name)
			assert.Equal(t, map[string]int64{"up": 5, "x": 5}, r.Upper, name)
		}
	})

	t.Run("adjacent days stay two ranges", func(t *testing.T) {
		d2 := shardBlock(3, dayMs, 2*dayMs, "")
		got := boundsOf(bucketindex.Blocks{s1, s2, d2}, nil, map[ulid.ULID]map[string]int64{s1.ID: {"up": 1}, s2.ID: {"up": 1}, d2.ID: {"up": 7}})
		require.Len(t, got, 2)
		assert.Equal(t, int64(2), got[0].Upper["up"])
		assert.Equal(t, dayMs, got[1].MinT)
		assert.Equal(t, int64(7), got[1].Lower["up"])
		assert.True(t, got[1].MainSet, "a single block is a main set")
	})

	t.Run("a smaller range inside a larger one is folded into it", func(t *testing.T) {
		lvl1 := shardBlock(3, 2*hourMs, 4*hourMs, "")
		got := boundsOf(bucketindex.Blocks{s1, s2, lvl1}, nil, map[ulid.ULID]map[string]int64{s1.ID: {"up": 1}, s2.ID: {"up": 1}, lvl1.ID: {"up": 1}})
		require.Len(t, got, 1)
		assert.True(t, got[0].MainSet)
		assert.Equal(t, int64(2), got[0].Lower["up"])
		assert.Equal(t, int64(3), got[0].Upper["up"])
	})

	t.Run("a missing main shard flags the range, with the shard named", func(t *testing.T) {
		got := boundsOf(bucketindex.Blocks{s1}, nil, map[ulid.ULID]map[string]int64{s1.ID: {"up": 3}})
		require.Len(t, got, 1)
		assert.True(t, got[0].LowerBound)
		assert.Equal(t, []missingShard{{Shard: "2_of_2", MinTime: 0, MaxTime: dayMs}}, got[0].MissingShards)
		assert.Equal(t, int64(3), got[0].Lower["up"])
	})

	t.Run("an uncounted block flags the range", func(t *testing.T) {
		got := boundsOf(bucketindex.Blocks{s1, s2}, nil, map[ulid.ULID]map[string]int64{s1.ID: {"up": 3}})
		assert.True(t, got[0].LowerBound)
		assert.Equal(t, []ulid.ULID{s2.ID}, got[0].MissingBlocks)
	})
}

// Right after a compaction the bucket index lists the sources, marked for deletion, beside the
// output. Counted together they would about double upper.
func TestBounds_BlocksMarkedForDeletionAfterACompaction(t *testing.T) {
	// The output of the day's two shards, compacted from four 12h sources.
	out1, out2 := shardBlock(1, 0, dayMs, "1_of_2"), shardBlock(2, 0, dayMs, "2_of_2")
	src := bucketindex.Blocks{
		shardBlock(3, 0, 12*hourMs, "1_of_2"), shardBlock(4, 0, 12*hourMs, "2_of_2"),
		shardBlock(5, 12*hourMs, dayMs, "1_of_2"), shardBlock(6, 12*hourMs, dayMs, "2_of_2"),
	}
	marked := map[ulid.ULID]struct{}{}
	all := bucketindex.Blocks{out1, out2}
	for _, b := range src {
		marked[b.ID] = struct{}{}
		all = append(all, b)
	}
	// Five series of up over the day; the sources split them by half day, and up{e} is in both halves.
	counts := map[ulid.ULID]map[string]int64{
		out1.ID: {"up": 3}, out2.ID: {"up": 2},
		src[0].ID: {"up": 2}, src[1].ID: {"up": 1},
		src[2].ID: {"up": 2}, src[3].ID: {"up": 1},
	}

	t.Run("with the output counted, the sources are left out", func(t *testing.T) {
		got := boundsOf(all, marked, counts)
		require.Len(t, got, 1)
		assert.True(t, got[0].MainSet)
		assert.Equal(t, map[string]int64{"up": 5}, got[0].Lower)
		assert.Equal(t, map[string]int64{"up": 5}, got[0].Upper, "no doubling")
		assert.False(t, got[0].LowerBound)
	})

	t.Run("with the output not loaded yet, its sources stand in for it", func(t *testing.T) {
		notLoaded := map[ulid.ULID]map[string]int64{}
		for id, c := range counts {
			if id != out1.ID {
				notLoaded[id] = c
			}
		}
		got := boundsOf(all, marked, notLoaded)
		require.Len(t, got, 1)
		r := got[0]
		assert.False(t, r.LowerBound, "the counted sources cover the output not loaded")
		assert.Empty(t, r.MissingBlocks)
		const truth = 5
		assert.LessOrEqual(t, r.Lower["up"], int64(truth))
		assert.GreaterOrEqual(t, r.Upper["up"], int64(truth))
		// out2 and shard 1's sources.
		assert.Equal(t, int64(2+2+2), r.Upper["up"])
	})

	t.Run("an unsharded source of a split is left out only once every shard's output is counted", func(t *testing.T) {
		split := shardBlock(7, 0, dayMs, "")
		m := map[ulid.ULID]struct{}{split.ID: {}}
		got := boundsOf(bucketindex.Blocks{out1, out2, split}, m, map[ulid.ULID]map[string]int64{out1.ID: {"up": 3}, out2.ID: {"up": 2}, split.ID: {"up": 5}})
		assert.Equal(t, int64(5), got[0].Upper["up"])
		assert.False(t, got[0].LowerBound)
		got = boundsOf(bucketindex.Blocks{out1, out2, split}, m, map[ulid.ULID]map[string]int64{out1.ID: {"up": 3}, split.ID: {"up": 5}})
		assert.Equal(t, int64(8), got[0].Upper["up"], "shard 2's output is not counted, so the source is kept")
		assert.False(t, got[0].LowerBound, "the source holds shard 2's series")
		assert.Equal(t, int64(3), got[0].Lower["up"])
	})

	t.Run("a marked block that nothing covers adds to upper", func(t *testing.T) {
		got := boundsOf(bucketindex.Blocks{out1, src[1]}, marked, map[ulid.ULID]map[string]int64{out1.ID: {"up": 3}, src[1].ID: {"up": 1}})
		require.Len(t, got, 1)
		assert.Equal(t, int64(4), got[0].Upper["up"])
		assert.True(t, got[0].LowerBound, "shard 2 of the day is missing")
	})
}

func TestCoverageHoles(t *testing.T) {
	b := func(minT, maxT int64) *bucketindex.Block { return &bucketindex.Block{MinTime: minT, MaxTime: maxT} }
	assert.Empty(t, coverageHoles(bucketindex.Blocks{b(0, 10)}, 2, 8))
	assert.Equal(t, []estimateWindow{{Start: 0, End: 2}, {Start: 5, End: 7}, {Start: 9, End: 10}}, coverageHoles(bucketindex.Blocks{b(7, 9), b(2, 5)}, 0, 10), "leading, interior and trailing")
	assert.Equal(t, []estimateWindow{{Start: 0, End: 10}}, coverageHoles(nil, 0, 10))
}

// headMock answers the head rows from fixed in-memory and active counts.
type headMock struct {
	ingestersMock
	inMemory, active map[string]uint64
	ingestStorage    bool
}

func (m *headMock) LabelValuesCardinality(_ context.Context, names []model.LabelName, _ []*labels.Matcher, method cardinality.CountMethod) (uint64, *client.LabelValuesCardinalityResponse, error) {
	counts := m.inMemory
	if method == cardinality.ActiveMethod {
		counts = m.active
	}
	return 0, &client.LabelValuesCardinalityResponse{Items: []*client.LabelValueSeriesCount{{LabelName: string(names[0]), LabelValueSeries: counts}}}, nil
}

func (m *headMock) IngestStorageEnabled() bool { return m.ingestStorage }

// perBlockGateway answers a per-block count from fixed counts per block.
func perBlockGateway(addr string, counts map[ulid.ULID]map[string]int64, got **storegatewaypb.SeriesCountsRequest) *storeGatewayClientMock {
	return &storeGatewayClientMock{remoteAddr: addr, mockedSeriesCounts: func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
		if got != nil {
			*got = req
		}
		resp := &storegatewaypb.SeriesCountsResponse{BlockIds: req.BlockIds, BlocksFromOffsetsTable: int64(len(req.BlockIds))}
		for _, id := range req.BlockIds {
			for v, n := range counts[ulid.MustParse(id)] {
				resp.Groups = append(resp.Groups, &cardinalitypb.SeriesCountGroup{Value: v, Counts: []int64{n}, BlockId: id})
			}
		}
		return resp, nil
	}}
}

func TestCardinalityEstimateHandler_Bounds(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	now := time.UnixMilli(100 * hour)
	s1, s2 := shardBlock(1, 48*hour, 72*hour, "1_of_2"), shardBlock(2, 48*hour, 72*hour, "2_of_2")
	o1 := oooBlock(3, 48*hour, 72*hour, "1_of_2")
	blocks := bucketindex.Blocks{s1, s2, o1}
	counts := map[ulid.ULID]map[string]int64{s1.ID: {"up": 3, "rare": 1}, s2.ID: {"up": 2}, o1.ID: {"up": 1}}

	serve := func(t *testing.T, ing estimateIngesters, minT, maxT int64, query string) (*http.Response, boundsResponse, *storegatewaypb.SeriesCountsRequest) {
		var got *storegatewaypb.SeriesCountsRequest
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(blocks, &bucketindex.Metadata{}, nil)
		q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{perBlockGateway("1.1.1.1", counts, &got): {{s1.ID, s2.ID, o1.ID}}},
		}}}}, queryStoreAfter: 12 * time.Hour}
		e := NewCardinalityEstimator(q, ing, ingestersWithinLimit(13*time.Hour))
		e.now = func() time.Time { return now }
		h := NewCardinalityEstimateHandler(e, CardinalityEstimateConfig{}, false)
		rec := httpRecord(t, h, fmt.Sprintf("start=%d&end=%d&mode=bounds&%s", minT/1000, maxT/1000, query))
		var out boundsResponse
		if rec.StatusCode == http.StatusOK {
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		}
		return rec, out, got
	}

	t.Run("a window of blocks is widened to their range and answered from the offsets tables", func(t *testing.T) {
		rec, out, got := serve(t, nil, 50*hour, 60*hour, "")
		require.Equal(t, http.StatusOK, rec.StatusCode)
		require.NotNil(t, got)
		assert.True(t, got.PerBlock)
		assert.Empty(t, got.Matchers)
		require.Len(t, out.Ranges, 1)
		r := out.Ranges[0]
		assert.Equal(t, 48*hour, r.Start)
		assert.Equal(t, 72*hour, r.End)
		assert.True(t, r.MainSet)
		assert.Equal(t, []boundsGroup{{Value: "up", Lower: 5, Upper: 6}, {Value: "rare", Lower: 1, Upper: 1}}, r.Groups, "sorted by upper")
		assert.Equal(t, int64(6), r.LowerTotal)
		assert.Equal(t, int64(7), r.UpperTotal)
		assert.Nil(t, out.Head)
		assert.Empty(t, out.NotCovered)
	})

	t.Run("min_count drops by upper and limit cuts the row set", func(t *testing.T) {
		_, out, _ := serve(t, nil, 50*hour, 60*hour, "min_count=2")
		assert.Equal(t, []boundsGroup{{Value: "up", Lower: 5, Upper: 6}}, out.Ranges[0].Groups)
		assert.Equal(t, 2, out.Ranges[0].GroupCount, "group_count counts every group")
		_, out, _ = serve(t, nil, 50*hour, 60*hour, "limit=1")
		assert.Len(t, out.Ranges[0].Groups, 1)
		assert.True(t, out.Ranges[0].HasMore)
	})

	t.Run("a window that reaches the ingesters has head rows apart and the band after the blocks not covered", func(t *testing.T) {
		ing := &headMock{inMemory: map[string]uint64{"up": 9, "new": 4}, active: map[string]uint64{"up": 7}, ingestStorage: true}
		rec, out, _ := serve(t, ing, 50*hour, 100*hour, "")
		require.Equal(t, http.StatusOK, rec.StatusCode)
		require.NotNil(t, out.Head)
		assert.False(t, out.Head.Approximate)
		assert.Equal(t, []boundsHeadGroup{{Value: "up", InMemory: 9, Active: 7}, {Value: "new", InMemory: 4}}, out.Head.Groups)
		assert.Equal(t, int64(13), out.Head.InMemoryTotal)
		// The blocks end at 72h, query-store-after at 88h.
		assert.Equal(t, []estimateWindow{{Start: 72 * hour, End: 88 * hour}, {Start: 88 * hour, End: 100 * hour}}, out.NotCovered)
	})

	t.Run("without ingest storage the head rows are approximate", func(t *testing.T) {
		_, out, _ := serve(t, &headMock{inMemory: map[string]uint64{"up": 9}}, 50*hour, 100*hour, "")
		require.NotNil(t, out.Head)
		assert.True(t, out.Head.Approximate)
	})

	t.Run("matchers are refused", func(t *testing.T) {
		rec, _, got := serve(t, nil, 50*hour, 60*hour, `match[]={job="x"}`)
		assert.Equal(t, http.StatusBadRequest, rec.StatusCode)
		assert.Nil(t, got, "no store-gateway is asked")
		rec, _, _ = serve(t, nil, 50*hour, 60*hour, `match[]={__name__=~".%2B"}`)
		assert.Equal(t, http.StatusOK, rec.StatusCode, "a matcher every series passes is none")
	})

	t.Run("an unknown mode is refused", func(t *testing.T) {
		rec := httpRecord(t, NewCardinalityEstimateHandler(NewCardinalityEstimator(&BlocksStoreQueryable{}, nil, ingestersWithinLimit(time.Hour)), CardinalityEstimateConfig{}, false), "start=0&end=10&mode=sketch")
		assert.Equal(t, http.StatusBadRequest, rec.StatusCode)
	})

	t.Run("a store-gateway refusal is 422", func(t *testing.T) {
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(blocks, &bucketindex.Metadata{}, nil)
		refusing := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return nil, cardinality.MaxGroupsRefusal("block x holds", 11, "__name__", 10)
		}}
		q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{refusing: {{s1.ID, s2.ID, o1.ID}}},
		}}}}}
		e := NewCardinalityEstimator(q, nil, ingestersWithinLimit(13*time.Hour))
		e.now = func() time.Time { return now }
		rec := httpRecord(t, NewCardinalityEstimateHandler(e, CardinalityEstimateConfig{}, false), fmt.Sprintf("start=%d&end=%d&mode=bounds", 50*hour/1000, 60*hour/1000))
		assert.Equal(t, http.StatusUnprocessableEntity, rec.StatusCode)
	})

	t.Run("an older store-gateway is refused, not answered with exact counts", func(t *testing.T) {
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(blocks, &bucketindex.Metadata{}, nil)
		older := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return nil, status.Error(codes.Unimplemented, "unknown method")
		}}
		q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{older: {{s1.ID, s2.ID, o1.ID}}},
		}}}}}
		e := NewCardinalityEstimator(q, nil, ingestersWithinLimit(13*time.Hour))
		e.now = func() time.Time { return now }
		rec := httpRecord(t, NewCardinalityEstimateHandler(e, CardinalityEstimateConfig{}, false), fmt.Sprintf("start=%d&end=%d&mode=bounds", 50*hour/1000, 60*hour/1000))
		assert.Equal(t, http.StatusUnprocessableEntity, rec.StatusCode)
	})
}

// httpRecord serves query on h for tenant user-1 and returns the response.
func httpRecord(t *testing.T, h http.Handler, query string) *http.Response {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/prometheus/api/v1/cardinality/estimate?"+query, nil)
	r = r.WithContext(user.InjectOrgID(r.Context(), "user-1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec.Result()
}
