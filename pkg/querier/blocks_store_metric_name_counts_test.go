// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

func TestBlocksStoreQueryable_MetricNameCounts(t *testing.T) {
	const minT, maxT = int64(0), int64(24 * 60 * 60 * 1000)
	b1, b2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	shards := bucketindex.Blocks{
		{ID: b1, MinTime: minT, MaxTime: maxT, CompactorShardID: "1_of_2"},
		{ID: b2, MinTime: minT, MaxTime: maxT, CompactorShardID: "2_of_2"},
	}

	countsFor := func(id ulid.ULID, counts map[string]int64) func(*storegatewaypb.MetricNameCountsRequest) (*storegatewaypb.MetricNameCountsResponse, error) {
		return func(req *storegatewaypb.MetricNameCountsRequest) (*storegatewaypb.MetricNameCountsResponse, error) {
			if len(req.BlockIds) != 1 || req.BlockIds[0] != id.String() {
				return nil, errors.New("unexpected blocks")
			}
			resp := &storegatewaypb.MetricNameCountsResponse{BlockIds: req.BlockIds}
			for name, n := range counts {
				resp.Counts = append(resp.Counts, &storegatewaypb.MetricNameCount{Name: name, Count: n})
			}
			return resp, nil
		}
	}
	twoGateways := map[BlocksStoreClient][][]ulid.ULID{
		&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedMetricNameCounts: countsFor(b1, map[string]int64{"up": 3, "errors_total": 1})}: {{b1}},
		&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedMetricNameCounts: countsFor(b2, map[string]int64{"up": 2})}:                    {{b2}},
	}

	newQueryable := func(blocks bucketindex.Blocks, clients any) *BlocksStoreQueryable {
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT-1).Return(blocks, &bucketindex.Metadata{}, nil)
		return &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{clients}}}}}
	}

	t.Run("sums the shards across store-gateways", func(t *testing.T) {
		res, err := newQueryable(shards, twoGateways).MetricNameCounts(context.Background(), "user-1", minT, maxT)
		require.NoError(t, err)
		assert.Equal(t, map[string]int64{"up": 5, "errors_total": 1}, res.Counts)
		assert.ElementsMatch(t, []ulid.ULID{b1, b2}, res.Blocks)
		assert.Equal(t, 2, res.StoreGateways)
	})

	t.Run("a block no store-gateway counts makes the counts a lower bound", func(t *testing.T) {
		oneCounted := map[BlocksStoreClient][][]ulid.ULID{
			&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedMetricNameCounts: countsFor(b1, map[string]int64{"up": 3})}: {{b1}},
			&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedMetricNameCounts: func(req *storegatewaypb.MetricNameCountsRequest) (*storegatewaypb.MetricNameCountsResponse, error) {
				return &storegatewaypb.MetricNameCountsResponse{}, nil
			}}: {{b2}},
		}
		res, err := newQueryable(shards, oneCounted).MetricNameCounts(context.Background(), "user-1", minT, maxT)
		require.NoError(t, err)
		assert.Equal(t, map[string]int64{"up": 3}, res.Counts)
		assert.Equal(t, []ulid.ULID{b1}, res.Blocks)
		assert.Equal(t, []ulid.ULID{b2}, res.MissingBlocks)
	})

	t.Run("a range with fewer shard blocks than its shard count makes the counts a lower bound", func(t *testing.T) {
		oneShard := map[BlocksStoreClient][][]ulid.ULID{
			&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedMetricNameCounts: countsFor(b1, map[string]int64{"up": 3})}: {{b1}},
		}
		res, err := newQueryable(shards[:1], oneShard).MetricNameCounts(context.Background(), "user-1", minT, maxT)
		require.NoError(t, err)
		assert.Equal(t, map[string]int64{"up": 3}, res.Counts)
		assert.Empty(t, res.MissingBlocks)
		assert.Equal(t, []MissingShard{{Shard: "2_of_2", MinTime: minT, MaxTime: maxT}}, res.MissingShards)
	})

	t.Run("forwards the metric-names limit and reports a refusal as 422", func(t *testing.T) {
		var got *storegatewaypb.MetricNameCountsRequest
		refusing := map[BlocksStoreClient][][]ulid.ULID{
			&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedMetricNameCounts: func(req *storegatewaypb.MetricNameCountsRequest) (*storegatewaypb.MetricNameCountsResponse, error) {
				got = req
				return nil, status.Error(codes.ResourceExhausted, "the blocks hold 5 distinct metric names, more than max_names 2")
			}}: {{b1}},
		}
		q := newQueryable(shards[:1], refusing)
		q.estimateCfg.MaxMetricNames = 2
		_, err := q.MetricNameCounts(context.Background(), "user-1", minT, maxT)
		require.Error(t, err)
		assert.Equal(t, int64(2), got.MaxNames)
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
	})

	t.Run("no blocks", func(t *testing.T) {
		res, err := newQueryable(bucketindex.Blocks{}, map[BlocksStoreClient][][]ulid.ULID{}).MetricNameCounts(context.Background(), "user-1", minT, maxT)
		require.NoError(t, err)
		assert.Empty(t, res.Counts)
	})
}

func TestCheckMetricNameCountsBlocks(t *testing.T) {
	const minT, maxT = int64(0), int64(100)
	block := func(lo, hi int64, shard string) *bucketindex.Block {
		return &bucketindex.Block{ID: ulid.MustNew(uint64(lo+hi), nil), MinTime: lo, MaxTime: hi, CompactorShardID: shard}
	}
	assert.NoError(t, checkMetricNameCountsBlocks(bucketindex.Blocks{block(0, 100, "")}, minT, maxT))
	assert.NoError(t, checkMetricNameCountsBlocks(bucketindex.Blocks{block(0, 100, "1_of_2"), block(0, 100, "2_of_2")}, minT, maxT))
	assert.ErrorContains(t, checkMetricNameCountsBlocks(bucketindex.Blocks{block(0, 50, ""), block(50, 100, "")}, minT, maxT), "more than one block range")
	assert.ErrorContains(t, checkMetricNameCountsBlocks(bucketindex.Blocks{block(0, 200, "")}, minT, maxT), "cuts through")
	assert.ErrorContains(t, checkMetricNameCountsBlocks(bucketindex.Blocks{block(0, 100, ""), block(0, 100, "1_of_2")}, minT, maxT), "may share series")
	assert.ErrorContains(t, checkMetricNameCountsBlocks(bucketindex.Blocks{block(0, 100, "1_of_2"), block(0, 100, "1_of_4")}, minT, maxT), "shard counts")
	assert.ErrorContains(t, checkMetricNameCountsBlocks(bucketindex.Blocks{block(0, 100, "1_of_2"), block(0, 100, "1_of_2")}, minT, maxT), "appears twice")
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
	gateway := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedMetricNameCounts: func(req *storegatewaypb.MetricNameCountsRequest) (*storegatewaypb.MetricNameCountsResponse, error) {
		asked = append(asked, req.BlockIds)
		return &storegatewaypb.MetricNameCountsResponse{BlockIds: req.BlockIds, Counts: []*storegatewaypb.MetricNameCount{{Name: "up", Count: 2}}}, nil
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
		res, err := newQueryable(map[ulid.ULID]struct{}{src1: {}, src2: {}}).CardinalityEstimate(context.Background(), "user-1", &req)
		require.NoError(t, err)
		assert.Equal(t, readIndexHeader, res.Read)
		assert.Equal(t, map[string][]int64{"up": {2}}, res.Counts, "one answer for the one request holding both shards")
		assert.ElementsMatch(t, []ulid.ULID{shard1, shard2}, res.Blocks)
		require.Len(t, asked, 1)
		assert.ElementsMatch(t, []string{shard1.String(), shard2.String()}, asked[0])
	})

	t.Run("without marks the blocks may share series and the request is refused", func(t *testing.T) {
		req := CardinalityEstimateRequest{MinT: minT, MaxT: maxT, Step: maxT}
		_, err := newQueryable(nil).CardinalityEstimate(context.Background(), "user-1", &req)
		require.Error(t, err)
		assert.Equal(t, http.StatusBadRequest, estimateHTTPStatus(err))
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
	gateway := func(addr string, table int64, got *[]*storegatewaypb.MetricNameCountsRequest) *storeGatewayClientMock {
		return &storeGatewayClientMock{remoteAddr: addr, mockedMetricNameCountsStream: func(req *storegatewaypb.MetricNameCountsRequest) ([]*storegatewaypb.MetricNameCountsResponse, error) {
			gotMtx.Lock()
			*got = append(*got, req)
			gotMtx.Unlock()
			return []*storegatewaypb.MetricNameCountsResponse{
				{BlockIds: req.BlockIds, Counts: []*storegatewaypb.MetricNameCount{{Name: "a", Count: 1}}, PostingsOffsetsTableBytes: table},
				{BlockIds: req.BlockIds, Counts: []*storegatewaypb.MetricNameCount{{Name: "b", Count: 1}}},
			}, nil
		}}
	}
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("streamed name table: %t", stream), func(t *testing.T) {
			var got []*storegatewaypb.MetricNameCountsRequest
			clients := map[BlocksStoreClient][][]ulid.ULID{gateway("1.1.1.1", 40, &got): {{b1}}, gateway("2.2.2.2", 60, &got): {{b2}}}
			req := CardinalityEstimateRequest{MinT: 0, MaxT: dayMs, GroupBy: "__name__", MaxIndexBytes: 1000, StreamNames: stream}
			res, err := seriesCountsQueryable(0, dayMs, shards, clients).CardinalityEstimate(context.Background(), "user-1", &req)
			require.NoError(t, err)
			if res.names != nil {
				defer res.names.close()
			}
			assert.Equal(t, readIndexHeader, res.Read)
			assert.Equal(t, int64(100), res.PostingsOffsetsTableBytes, "the tables both store-gateways walked")
			require.Len(t, got, 2)
			for _, r := range got {
				assert.Equal(t, int64(500), r.MaxIndexBytes, "max_index_bytes is split across the calls")
			}
		})
	}

	refusing := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedMetricNameCountsStream: func(*storegatewaypb.MetricNameCountsRequest) ([]*storegatewaypb.MetricNameCountsResponse, error) {
		return nil, status.Error(codes.ResourceExhausted, "the request would read about 900 bytes of the __name__ postings offsets table over 1 blocks, more than max_index_bytes 500")
	}}
	var got []*storegatewaypb.MetricNameCountsRequest
	clients := map[BlocksStoreClient][][]ulid.ULID{refusing: {{b1}}, gateway("2.2.2.2", 60, &got): {{b2}}}
	req := CardinalityEstimateRequest{MinT: 0, MaxT: dayMs, GroupBy: "__name__", MaxIndexBytes: 1000}
	_, err := seriesCountsQueryable(0, dayMs, shards, clients).CardinalityEstimate(context.Background(), "user-1", &req)
	require.ErrorContains(t, err, "max_index_bytes 1000, split into 500 bytes for each of 2 store-gateway calls")
	assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))

	overNames := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedMetricNameCountsStream: func(*storegatewaypb.MetricNameCountsRequest) ([]*storegatewaypb.MetricNameCountsResponse, error) {
		return nil, status.Error(codes.ResourceExhausted, "the blocks hold 12 distinct metric names, more than max_names 10")
	}}
	clients = map[BlocksStoreClient][][]ulid.ULID{overNames: {{b1}}, gateway("2.2.2.2", 60, &got): {{b2}}}
	_, err = seriesCountsQueryable(0, dayMs, shards, clients).CardinalityEstimate(context.Background(), "user-1", &req)
	require.ErrorContains(t, err, "more than max_names 10")
	assert.NotContains(t, err.Error(), "split into", "a max_names refusal isn't blamed on max_index_bytes")
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
	assert.Equal(t, []MissingShard{{Shard: "2_of_2", MinTime: 0, MaxTime: 100}}, missingShards(bucketindex.Blocks{block(0, 100, "1_of_2")}))
	assert.Equal(t, []MissingShard{{Shard: "2_of_2", MinTime: 0, MaxTime: 100}}, missingShards(bucketindex.Blocks{block(0, 100, "1_of_2"), block(0, 100, "1_of_2")}), "a shard listed twice is still one shard")
	assert.Equal(t,
		[]MissingShard{{Shard: "1_of_4", MinTime: 0, MaxTime: 100}, {Shard: "3_of_4", MinTime: 0, MaxTime: 100}, {Shard: "2_of_2", MinTime: 100, MaxTime: 200}},
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
}
