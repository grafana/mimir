// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

// A metric-name table over one block range of shards is counted by the
// store-gateways' SeriesCounts and added up.
func TestBlocksStoreQueryable_CardinalityEstimate_NameTable(t *testing.T) {
	const minT, maxT = int64(0), int64(24 * 60 * 60 * 1000)
	b1, b2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	shards := bucketindex.Blocks{
		{ID: b1, MinTime: minT, MaxTime: maxT, CompactorShardID: "1_of_2"},
		{ID: b2, MinTime: minT, MaxTime: maxT, CompactorShardID: "2_of_2"},
	}

	countsFor := func(id ulid.ULID, counts map[string]int64) func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
		return func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			if len(req.BlockIds) != 1 || req.BlockIds[0] != id.String() {
				return nil, errors.New("unexpected blocks")
			}
			resp := &storegatewaypb.SeriesCountsResponse{BlockIds: req.BlockIds, BlocksFromOffsetsTable: 1}
			for name, n := range counts {
				resp.Groups = append(resp.Groups, &cardinalitypb.SeriesCountGroup{Value: name, Counts: []int64{n}})
			}
			return resp, nil
		}
	}
	twoGateways := map[BlocksStoreClient][][]ulid.ULID{
		&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: countsFor(b1, map[string]int64{"up": 3, "errors_total": 1})}: {{b1}},
		&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesCounts: countsFor(b2, map[string]int64{"up": 2})}:                    {{b2}},
	}

	newQueryable := func(blocks bucketindex.Blocks, clients any) *BlocksStoreQueryable {
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT-1).Return(blocks, &bucketindex.Metadata{}, nil)
		return &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{clients}}}}}
	}
	estimate := func(q *BlocksStoreQueryable) (CardinalityEstimateResult, error) {
		return q.cardinalityEstimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: minT, MaxT: maxT, GroupBy: "__name__"})
	}

	t.Run("sums the shards across store-gateways", func(t *testing.T) {
		res, err := estimate(newQueryable(shards, twoGateways))
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {5}, "errors_total": {1}}, res.Counts)
		assert.ElementsMatch(t, []ulid.ULID{b1, b2}, res.Blocks)
		assert.Equal(t, 2, res.StoreGateways)
		assert.Equal(t, ReadIndexHeader, res.Read)
	})

	t.Run("a block no store-gateway counts makes the counts a lower bound", func(t *testing.T) {
		oneCounted := map[BlocksStoreClient][][]ulid.ULID{
			&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: countsFor(b1, map[string]int64{"up": 3})}: {{b1}},
			&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesCounts: func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
				return &storegatewaypb.SeriesCountsResponse{BlockIds: []string{}}, nil
			}}: {{b2}},
		}
		res, err := estimate(newQueryable(shards, oneCounted))
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {3}}, res.Counts)
		assert.Equal(t, []ulid.ULID{b1}, res.Blocks)
		assert.Equal(t, []ulid.ULID{b2}, res.MissingBlocks)
		assert.True(t, res.LowerBound)
	})

	t.Run("a range with fewer shard blocks than its shard count makes the counts a lower bound", func(t *testing.T) {
		oneShard := map[BlocksStoreClient][][]ulid.ULID{
			&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: countsFor(b1, map[string]int64{"up": 3})}: {{b1}},
		}
		res, err := estimate(newQueryable(shards[:1], oneShard))
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {3}}, res.Counts)
		assert.Empty(t, res.MissingBlocks)
		assert.Equal(t, []missingShard{{Shard: "2_of_2", MinTime: minT, MaxTime: maxT}}, res.MissingShards)
		assert.True(t, res.LowerBound)
	})

	t.Run("a shard the compactor recorded as empty is not missing", func(t *testing.T) {
		oneShard := map[BlocksStoreClient][][]ulid.ULID{
			&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: countsFor(b1, map[string]int64{"up": 3})}: {{b1}},
		}
		blocks := bucketindex.Blocks{{ID: b1, MinTime: minT, MaxTime: maxT, CompactorShardID: "1_of_2", EmptyCompactorShards: []uint64{1}}}
		res, err := estimate(newQueryable(blocks, oneShard))
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {3}}, res.Counts)
		assert.Empty(t, res.MissingBlocks)
		assert.Empty(t, res.MissingShards)
		assert.False(t, res.LowerBound)
	})

	t.Run("forwards the groups limit and reports a refusal as 422", func(t *testing.T) {
		var got *storegatewaypb.SeriesCountsRequest
		refusing := map[BlocksStoreClient][][]ulid.ULID{
			&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
				got = req
				return nil, cardinality.Refusalf(cardinality.LimitMaxGroups, "the blocks hold 5 distinct values of __name__, more than max_groups 2")
			}}: {{b1}},
		}
		q := newQueryable(shards[:1], refusing)
		_, err := q.cardinalityEstimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: minT, MaxT: maxT, GroupBy: "__name__", MaxGroups: 2})
		require.Error(t, err)
		assert.Equal(t, int64(2), got.MaxGroups)
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
	})

	t.Run("no blocks", func(t *testing.T) {
		res, err := estimate(newQueryable(bucketindex.Blocks{}, map[BlocksStoreClient][][]ulid.ULID{}))
		require.NoError(t, err)
		assert.Empty(t, res.Counts)
	})
}

// markedFinderMock is a finder that also knows which blocks carry a deletion mark.
type markedFinderMock struct {
	*blocksFinderMock
	marked map[ulid.ULID]struct{}
}

func (m *markedFinderMock) BlocksMarkedForDeletion(context.Context, string) (map[ulid.ULID]struct{}, error) {
	return m.marked, nil
}

func TestBlocksStoreQueryable_CardinalityEstimate_SkipsBlocksMarkedForDeletion(t *testing.T) {
	const minT, maxT = int64(0), int64(24 * 60 * 60 * 1000)
	// After a split-and-merge compaction the two sources are still listed,
	// marked for deletion, next to the two shards that replaced them.
	src1, src2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	shard1, shard2 := ulid.MustNew(3, nil), ulid.MustNew(4, nil)
	listed := bucketindex.Blocks{
		{ID: src1, MinTime: minT, MaxTime: maxT},
		{ID: src2, MinTime: minT, MaxTime: maxT},
		{ID: shard1, MinTime: minT, MaxTime: maxT, CompactorShardID: "1_of_2"},
		{ID: shard2, MinTime: minT, MaxTime: maxT, CompactorShardID: "2_of_2"},
	}
	var asked [][]string
	gateway := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
		asked = append(asked, req.BlockIds)
		return &storegatewaypb.SeriesCountsResponse{BlockIds: req.BlockIds, BlocksFromOffsetsTable: int64(len(req.BlockIds)), Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}}, nil
	}}
	newQueryable := func(marked map[ulid.ULID]struct{}) *BlocksStoreQueryable {
		inner := &blocksFinderMock{}
		inner.On("GetBlocks", mock.Anything, "user-1", minT, maxT-1).Return(listed, &bucketindex.Metadata{}, nil)
		clients := map[BlocksStoreClient][][]ulid.ULID{gateway: {{shard1, shard2}}}
		return &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: &markedFinderMock{inner, marked}, stores: &blocksStoreSetMock{mockedResponses: []interface{}{clients}}}}}
	}

	t.Run("the marked sources are left out and the shards are counted from the index-headers", func(t *testing.T) {
		asked = nil
		req := CardinalityEstimateRequest{MinT: minT, MaxT: maxT}
		res, err := newQueryable(map[ulid.ULID]struct{}{src1: {}, src2: {}}).cardinalityEstimate(context.Background(), "user-1", &req)
		require.NoError(t, err)
		assert.Equal(t, ReadIndexHeader, res.Read)
		assert.Equal(t, map[string][]int64{"up": {2}}, res.Counts, "one answer for the one request holding both shards")
		assert.ElementsMatch(t, []ulid.ULID{shard1, shard2}, res.Blocks)
		require.Len(t, asked, 1)
		assert.ElementsMatch(t, []string{shard1.String(), shard2.String()}, asked[0])
	})

}

// Right after a compaction the store-gateways still hold the sources and may not have loaded the output, so the sources are dropped only once the output is older than the upload grace period.
func TestBlocksStoreQueryable_CardinalityEstimate_KeepsMarkedSourcesOfRecentBlocks(t *testing.T) {
	const minT, maxT = int64(0), int64(24 * 60 * 60 * 1000)
	src1, src2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	shard1, shard2 := ulid.MustNew(3, nil), ulid.MustNew(4, nil)
	marked := map[ulid.ULID]struct{}{src1: {}, src2: {}}
	// The store set mock hands out the blocks it is given, whatever blocks it is asked for.
	estimate := func(shardsUploadedAt time.Time, loaded []ulid.ULID) (CardinalityEstimateResult, error) {
		listed := bucketindex.Blocks{
			{ID: src1, MinTime: minT, MaxTime: maxT, CompactionLevel: 1},
			{ID: src2, MinTime: minT, MaxTime: maxT, CompactionLevel: 1},
			{ID: shard1, MinTime: minT, MaxTime: maxT, CompactorShardID: "1_of_2", CompactionLevel: 2, UploadedAt: shardsUploadedAt.Unix()},
			{ID: shard2, MinTime: minT, MaxTime: maxT, CompactorShardID: "2_of_2", CompactionLevel: 2, UploadedAt: shardsUploadedAt.Unix()},
		}
		gateway := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			g := &cardinalitypb.SeriesCountGroup{Value: "up", Counts: []int64{2}}
			if req.Hashes {
				g = &cardinalitypb.SeriesCountGroup{Value: "up", Hashes: []uint64{1, 2}}
			}
			return &storegatewaypb.SeriesCountsResponse{BlockIds: convertULIDsToString(loaded), Groups: []*cardinalitypb.SeriesCountGroup{g}}, nil
		}}
		inner := &blocksFinderMock{}
		inner.On("GetBlocks", mock.Anything, "user-1", minT, maxT-1).Return(listed, &bucketindex.Metadata{}, nil)
		q := &BlocksStoreQueryable{
			consistency: NewBlocksConsistency(time.Hour, nil),
			compartments: []blocksStoreCompartment{{finder: &markedFinderMock{inner, marked}, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][][]ulid.ULID{gateway: {{src1, src2, shard1, shard2}}},
			}}}},
		}
		req := CardinalityEstimateRequest{MinT: minT, MaxT: maxT}
		res, err := q.cardinalityEstimate(context.Background(), "user-1", &req)
		return res, err
	}

	t.Run("shards the store-gateways have not loaded yet are counted from their sources", func(t *testing.T) {
		res, err := estimate(time.Now().Add(-time.Minute), []ulid.ULID{src1, src2})
		require.NoError(t, err)
		assert.True(t, res.Dedup)
		assert.Equal(t, map[string][]int64{"up": {2}}, res.Counts)
		assert.ElementsMatch(t, []ulid.ULID{src1, src2}, res.Blocks)
		assert.False(t, res.LowerBound, "the sources hold every series of the shards")
		assert.Empty(t, res.MissingBlocks)
	})

	t.Run("an uncounted source still makes the counts a lower bound", func(t *testing.T) {
		res, err := estimate(time.Now().Add(-time.Minute), []ulid.ULID{src1})
		require.NoError(t, err)
		assert.True(t, res.LowerBound)
		assert.Equal(t, []ulid.ULID{src2}, res.MissingBlocks)
	})

	t.Run("shards older than the grace period are counted alone", func(t *testing.T) {
		res, err := estimate(time.Now().Add(-2*time.Hour), []ulid.ULID{shard1, shard2})
		require.NoError(t, err)
		assert.False(t, res.Dedup)
		assert.ElementsMatch(t, []ulid.ULID{shard1, shard2}, res.Blocks)
		assert.False(t, res.LowerBound)
	})
}

func TestBlocksStoreQueryable_CardinalityEstimate_NameTableIndexBytes(t *testing.T) {
	b1, b2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	shards := bucketindex.Blocks{
		{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"},
		{ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: "2_of_2"},
	}
	// The store-gateways are asked concurrently and share the slice of requests.
	var gotMtx sync.Mutex
	gateway := func(addr string, table int64, got *[]*storegatewaypb.SeriesCountsRequest) *storeGatewayClientMock {
		return &storeGatewayClientMock{remoteAddr: addr, mockedSeriesCountsStream: func(req *storegatewaypb.SeriesCountsRequest) ([]*storegatewaypb.SeriesCountsResponse, error) {
			gotMtx.Lock()
			*got = append(*got, req)
			gotMtx.Unlock()
			return []*storegatewaypb.SeriesCountsResponse{
				{BlockIds: req.BlockIds, BlocksFromOffsetsTable: int64(len(req.BlockIds)), Groups: []*cardinalitypb.SeriesCountGroup{{Value: "a", Counts: []int64{1}}}, PostingsOffsetsTableBytes: table},
				{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "b", Counts: []int64{1}}}},
			}, nil
		}}
	}
	t.Run("max_index_bytes is split across the store-gateway calls", func(t *testing.T) {
		var got []*storegatewaypb.SeriesCountsRequest
		clients := map[BlocksStoreClient][][]ulid.ULID{gateway("1.1.1.1", 40, &got): {{b1}}, gateway("2.2.2.2", 60, &got): {{b2}}}
		req := CardinalityEstimateRequest{MinT: 0, MaxT: dayMs, GroupBy: "__name__", MaxIndexBytes: 1000}
		res, err := seriesCountsQueryable(0, dayMs, shards, clients).cardinalityEstimate(context.Background(), "user-1", &req)
		require.NoError(t, err)
		assert.Equal(t, ReadIndexHeader, res.Read)
		assert.Equal(t, map[string][]int64{"a": {2}, "b": {2}}, res.Counts)
		assert.Equal(t, int64(100), res.PostingsOffsetsTableBytes, "the tables both store-gateways walked")
		require.Len(t, got, 2)
		for _, r := range got {
			assert.Equal(t, int64(500), r.MaxIndexBytes)
		}
	})

	refusing := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCountsStream: func(*storegatewaypb.SeriesCountsRequest) ([]*storegatewaypb.SeriesCountsResponse, error) {
		return nil, cardinality.Refusalf(cardinality.LimitMaxIndexBytes, "the request would read about 900 bytes of the __name__ postings offsets table over 1 blocks, more than max_index_bytes 500")
	}}
	var got []*storegatewaypb.SeriesCountsRequest
	clients := map[BlocksStoreClient][][]ulid.ULID{refusing: {{b1}}, gateway("2.2.2.2", 60, &got): {{b2}}}
	req := CardinalityEstimateRequest{MinT: 0, MaxT: dayMs, GroupBy: "__name__", MaxIndexBytes: 1000}
	_, err := seriesCountsQueryable(0, dayMs, shards, clients).cardinalityEstimate(context.Background(), "user-1", &req)
	require.ErrorContains(t, err, "max_index_bytes 1000, split into 500 bytes for each of 2 store-gateway calls")
	assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))

	overNames := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCountsStream: func(*storegatewaypb.SeriesCountsRequest) ([]*storegatewaypb.SeriesCountsResponse, error) {
		return nil, cardinality.Refusalf(cardinality.LimitMaxGroups, "the blocks hold 12 distinct values of __name__, more than max_groups 10")
	}}
	clients = map[BlocksStoreClient][][]ulid.ULID{overNames: {{b1}}, gateway("2.2.2.2", 60, &got): {{b2}}}
	_, err = seriesCountsQueryable(0, dayMs, shards, clients).cardinalityEstimate(context.Background(), "user-1", &req)
	require.ErrorContains(t, err, "more than max_groups 10")
	assert.NotContains(t, err.Error(), "split into", "a max_groups refusal isn't blamed on max_index_bytes")
	assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
}

func TestMissingShards(t *testing.T) {
	var id uint64
	block := func(lo, hi int64, shard string) *bucketindex.Block {
		id++
		return &bucketindex.Block{ID: ulid.MustNew(id, nil), MinTime: lo, MaxTime: hi, CompactorShardID: shard}
	}
	assert.Empty(t, missingShards(nil))
	assert.Empty(t, missingShards(bucketindex.Blocks{block(0, 100, "")}), "a block with no shard ID is the whole range")
	assert.Empty(t, missingShards(bucketindex.Blocks{block(0, 100, "1_of_1")}))
	assert.Empty(t, missingShards(bucketindex.Blocks{block(0, 100, "1_of_2"), block(0, 100, "2_of_2")}))
	assert.Equal(t, []missingShard{{Shard: "2_of_2", MinTime: 0, MaxTime: 100}}, missingShards(bucketindex.Blocks{block(0, 100, "1_of_2")}))
	assert.Equal(t, []missingShard{{Shard: "2_of_2", MinTime: 0, MaxTime: 100}}, missingShards(bucketindex.Blocks{block(0, 100, "1_of_2"), block(0, 100, "1_of_2")}), "a shard listed twice is still one shard")
	assert.Equal(t,
		[]missingShard{{Shard: "1_of_4", MinTime: 0, MaxTime: 100}, {Shard: "3_of_4", MinTime: 0, MaxTime: 100}, {Shard: "2_of_2", MinTime: 100, MaxTime: 200}},
		missingShards(bucketindex.Blocks{block(0, 100, "2_of_4"), block(100, 200, "1_of_2"), block(0, 100, "4_of_4")}),
		"each range is checked on its own")
	assert.Empty(t, missingShards(bucketindex.Blocks{block(0, 100, "1_of_2"), block(0, 100, "1_of_4")}), "two shard counts overlap and are deduplicated, so neither set must be complete")
	assert.Empty(t, missingShards(bucketindex.Blocks{block(0, 100, "1_of_2"), block(0, 100, "")}), "a block with no shard ID may hold every series")
	assert.Empty(t, missingShards(bucketindex.Blocks{block(0, 100, "9_of_8")}), "an invalid shard ID is not checked")
	assert.Empty(t, missingShards(bucketindex.Blocks{block(0, 100, "1_of_4000000000")}), "an implausible shard count is not checked")
	// Mid-compaction: shard 1's merge into a 24-unit block is done, while
	// shard 2 is still in its 2-unit sources. Both are counted.
	assert.Empty(t, missingShards(bucketindex.Blocks{block(0, 24, "1_of_2"), block(0, 2, "1_of_2"), block(0, 2, "2_of_2"), block(2, 4, "2_of_2")}))
	assert.Empty(t, missingShards(bucketindex.Blocks{block(0, 24, "1_of_2"), block(0, 24, "2_of_2"), block(0, 2, "1_of_2")}), "a source still listed beside its merged shard")

	withEmpty := func(b *bucketindex.Block, empty ...uint64) *bucketindex.Block {
		b.EmptyCompactorShards = empty
		return b
	}
	assert.Empty(t, missingShards(bucketindex.Blocks{withEmpty(block(0, 100, "1_of_2"), 1)}), "the only block records the other shard as empty")
	assert.Empty(t, missingShards(bucketindex.Blocks{withEmpty(block(0, 100, "2_of_4"), 0, 2), withEmpty(block(0, 100, "4_of_4"), 0, 2)}))
	assert.Equal(t,
		[]missingShard{{Shard: "3_of_4", MinTime: 0, MaxTime: 100}},
		missingShards(bucketindex.Blocks{withEmpty(block(0, 100, "2_of_4"), 0, 2), withEmpty(block(0, 100, "4_of_4"), 0)}),
		"a shard is empty only if every block of the range records it",
	)
	assert.Equal(t,
		[]missingShard{{Shard: "1_of_4", MinTime: 0, MaxTime: 100}, {Shard: "3_of_4", MinTime: 0, MaxTime: 100}},
		missingShards(bucketindex.Blocks{block(0, 100, "2_of_4"), withEmpty(block(0, 100, "4_of_4"), 0, 2)}),
		"a block with no record, as from an older compactor, records no shard as empty",
	)
	assert.Equal(t,
		[]missingShard{{Shard: "2_of_2", MinTime: 100, MaxTime: 200}},
		missingShards(bucketindex.Blocks{withEmpty(block(0, 100, "1_of_2"), 1), block(100, 200, "1_of_2")}),
		"a record covers its own block range only",
	)
	assert.Equal(t,
		[]missingShard{{Shard: "2_of_3", MinTime: 0, MaxTime: 100}},
		missingShards(bucketindex.Blocks{withEmpty(block(0, 100, "1_of_3"), 2, 7)}),
		"a shard not recorded is still missing, and an index past the shard count is ignored",
	)
}

func TestCheckDisjointShards(t *testing.T) {
	b1, b2, b3 := ulid.MustNew(1, nil), ulid.MustNew(2, nil), ulid.MustNew(3, nil)
	blocks := func(shardIDs ...string) bucketindex.Blocks {
		out := bucketindex.Blocks{}
		for i, id := range shardIDs {
			out = append(out, &bucketindex.Block{ID: []ulid.ULID{b1, b2, b3}[i], MinTime: 0, MaxTime: dayMs, CompactorShardID: id})
		}
		return out
	}
	for name, tc := range map[string]struct {
		blocks  bucketindex.Blocks
		wantErr string
	}{
		"no block":                         {blocks: blocks()},
		"one block without a shard ID":     {blocks: blocks("")},
		"every shard of one count":         {blocks: blocks("1_of_3", "2_of_3", "3_of_3")},
		"some shards of one count":         {blocks: blocks("1_of_3", "3_of_3")},
		"a block without a shard ID":       {blocks: blocks("1_of_2", ""), wantErr: "has no shard ID"},
		"the same shard twice":             {blocks: blocks("1_of_2", "1_of_2"), wantErr: "shard 1_of_2 appears twice"},
		"two shard counts":                 {blocks: blocks("1_of_2", "2_of_4"), wantErr: "use shard counts 2 and 4"},
		"an unparseable shard ID":          {blocks: blocks("1_of_2", "two_of_2"), wantErr: b2.String()},
		"a shard index above its count":    {blocks: blocks("1_of_2", "3_of_2"), wantErr: b2.String()},
		"an unparseable shard ID, first":   {blocks: blocks("1-of-2", "2_of_2"), wantErr: b1.String()},
		"the same shard twice, of a third": {blocks: blocks("1_of_3", "2_of_3", "2_of_3"), wantErr: "shard 2_of_3 appears twice"},
	} {
		t.Run(name, func(t *testing.T) {
			err := checkDisjointShards(tc.blocks)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
			assert.Equal(t, http.StatusBadRequest, estimateHTTPStatus(err))
		})
	}
}

// Blocks of one range that checkDisjointShards does not prove share no
// series are deduplicated by hash: a series held by two blocks of the same
// shard counts once.
func TestBlocksStoreQueryable_SeriesCounts_UnprovenShardsAreDeduplicated(t *testing.T) {
	b1, b2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	// Each block holds one series. Blocks of the same shard hold the same
	// one, as after a compaction was retried.
	for name, tc := range map[string]struct {
		shards    [2]string
		hashes    [2]uint64
		wantDedup bool
		want      int64
	}{
		"two shards of one count are added up": {shards: [2]string{"1_of_2", "2_of_2"}, hashes: [2]uint64{1, 2}, want: 2},
		"the same shard twice":                 {shards: [2]string{"1_of_2", "1_of_2"}, hashes: [2]uint64{1, 1}, wantDedup: true, want: 1},
		"two shard counts":                     {shards: [2]string{"1_of_2", "2_of_4"}, hashes: [2]uint64{1, 1}, wantDedup: true, want: 1},
		"an unparseable shard ID":              {shards: [2]string{"1_of_2", "x"}, hashes: [2]uint64{1, 1}, wantDedup: true, want: 1},
	} {
		t.Run(name, func(t *testing.T) {
			blocks := bucketindex.Blocks{
				{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: tc.shards[0]},
				{ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: tc.shards[1]},
			}
			clients := map[BlocksStoreClient][][]ulid.ULID{
				gatewayHoldingSeries("1.1.1.1", tc.hashes[0]): {{b1}},
				gatewayHoldingSeries("2.2.2.2", tc.hashes[1]): {{b2}},
			}
			res, err := seriesCountsQueryable(0, dayMs, blocks, clients).seriesCountsInWindow(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs})
			require.NoError(t, err)
			assert.Equal(t, tc.wantDedup, res.Dedup)
			assert.Equal(t, map[string][]int64{"up": {tc.want}}, res.Counts)
		})
	}
}

// gatewayHoldingSeries is a store-gateway mock whose every block holds the
// series of the given label-set hashes in group "up". Like a real one it
// answers hashes when asked for them, and counts otherwise.
func gatewayHoldingSeries(addr string, hashes ...uint64) *storeGatewayClientMock {
	return &storeGatewayClientMock{remoteAddr: addr, mockedSeriesCounts: func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
		g := &cardinalitypb.SeriesCountGroup{Value: "up"}
		if req.Hashes {
			g.Hashes = hashes
		} else {
			g.Counts = []int64{int64(len(hashes))}
		}
		return &storegatewaypb.SeriesCountsResponse{BlockIds: req.BlockIds, Groups: []*cardinalitypb.SeriesCountGroup{g}}, nil
	}}
}
