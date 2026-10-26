// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

const dayMs = int64(24 * 60 * 60 * 1000)

func seriesCountsQueryable(minT, maxT int64, blocks bucketindex.Blocks, clients map[BlocksStoreClient][][]ulid.ULID) *BlocksStoreQueryable {
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT-1).Return(blocks, &bucketindex.Metadata{}, nil)
	return &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{clients}}}}}
}

// gatewayAnswering returns a store-gateway mock that records the request it
// got and answers with resp. Unless resp sets BlockIds, every requested
// block is reported as counted.
func gatewayAnswering(addr string, got **storegatewaypb.SeriesCountsRequest, resp storegatewaypb.SeriesCountsResponse) *storeGatewayClientMock {
	return &storeGatewayClientMock{remoteAddr: addr, mockedSeriesCounts: func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
		if got != nil {
			*got = req
		}
		out := resp
		if out.BlockIds == nil {
			out.BlockIds = req.BlockIds
		}
		return &out, nil
	}}
}

func TestBlocksStoreQueryable_SeriesCounts(t *testing.T) {
	b1, b2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	up := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")}

	t.Run("adds up shards of one range, per bucket", func(t *testing.T) {
		shards := bucketindex.Blocks{
			{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"},
			{ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: "2_of_2"},
		}
		var got *storegatewaypb.SeriesCountsRequest
		clients := map[BlocksStoreClient][][]ulid.ULID{
			gatewayAnswering("1.1.1.1", &got, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{1, 2}}}, SeriesFetchedBytes: 10}): {{b1}},
			gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{3, 0}}}, SeriesFetchedBytes: 5}):   {{b2}},
		}
		q := seriesCountsQueryable(0, 2*60*60*1000, shards, clients)
		res, err := q.SeriesCounts(context.Background(), "user-1", CardinalityEstimateRequest{Matchers: up, MinT: 0, MaxT: 2 * 60 * 60 * 1000, Step: 60 * 60 * 1000, GroupBy: "__name__"})
		require.NoError(t, err)
		assert.False(t, res.Dedup)
		assert.Equal(t, map[string][]int64{"up": {4, 2}}, res.Counts)
		assert.Equal(t, int64(15), res.SeriesFetchedBytes)
		assert.Equal(t, 2, res.StoreGateways)
		require.NotNil(t, got)
		assert.False(t, got.Hashes)
		assert.Equal(t, int64(60*60*1000), got.StepMs)
		require.Len(t, got.Matchers, 1)
		assert.Equal(t, "up", got.Matchers[0].Value)
	})

	t.Run("sends a window wider than the step and refuses a store-gateway that ignored it", func(t *testing.T) {
		shards := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
		const hour = 60 * 60 * 1000
		req := CardinalityEstimateRequest{Matchers: up, MinT: 0, MaxT: 3 * hour, Step: hour, Window: 2 * hour, GroupBy: "__name__"}
		var got *storegatewaypb.SeriesCountsRequest
		honours := gatewayAnswering("1.1.1.1", &got, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{1, 2}}}, WindowMs: 2 * hour})
		res, err := seriesCountsQueryable(0, 3*hour, shards, map[BlocksStoreClient][][]ulid.ULID{honours: {{b1}}}).SeriesCounts(context.Background(), "user-1", req)
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {1, 2}}, res.Counts)
		require.NotNil(t, got)
		assert.Equal(t, int64(2*hour), got.WindowMs)

		ignores := gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{1, 2, 3}}}})
		_, err = seriesCountsQueryable(0, 3*hour, shards, map[BlocksStoreClient][][]ulid.ULID{ignores: {{b1}}}).SeriesCounts(context.Background(), "user-1", req)
		require.ErrorContains(t, err, "a store-gateway counted with window_ms 0 instead of the 7200000 asked for")
		assert.NotContains(t, err.Error(), "2.2.2.2", "the caller sees the kind of component, not its address")
		assert.Equal(t, "2.2.2.2", estimateSourceAddr(err), "the address is kept for the log")
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
		assert.ErrorIs(t, err, errCountsNotSupported)

		req.Window = hour
		got = nil
		_, err = seriesCountsQueryable(0, 3*hour, shards, map[BlocksStoreClient][][]ulid.ULID{gatewayAnswering("3.3.3.3", &got, storegatewaypb.SeriesCountsResponse{}): {{b1}}}).SeriesCounts(context.Background(), "user-1", req)
		require.NoError(t, err)
		assert.Zero(t, got.WindowMs, "a window equal to the step is sent as none, which every version counts the same way")
	})

	t.Run("refuses a store-gateway that ignored step_ms", func(t *testing.T) {
		shards := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
		const hour = 60 * 60 * 1000
		// Three buckets of one hour. A store-gateway that doesn't know step_ms
		// answers one count per group.
		req := CardinalityEstimateRequest{Matchers: up, MinT: 0, MaxT: 3 * hour, Step: hour, GroupBy: "__name__"}
		ignores := gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{6}}}})
		_, err := seriesCountsQueryable(0, 3*hour, shards, map[BlocksStoreClient][][]ulid.ULID{ignores: {{b1}}}).SeriesCounts(context.Background(), "user-1", req)
		require.ErrorContains(t, err, "a store-gateway returned 1 buckets for \"up\" instead of the 3 asked for with step_ms")
		assert.NotContains(t, err.Error(), "2.2.2.2")
		assert.Equal(t, "2.2.2.2", estimateSourceAddr(err))
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
		assert.ErrorIs(t, err, errCountsNotSupported)

		// Without a step one count per group is right.
		req.Step = 0
		res, err := seriesCountsQueryable(0, 3*hour, shards, map[BlocksStoreClient][][]ulid.ULID{ignores: {{b1}}}).SeriesCounts(context.Background(), "user-1", req)
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {6}}, res.Counts)
	})

	t.Run("unions hashes across block ranges", func(t *testing.T) {
		days := bucketindex.Blocks{
			{ID: b1, MinTime: 0, MaxTime: dayMs},
			{ID: b2, MinTime: dayMs, MaxTime: 2 * dayMs},
		}
		var got *storegatewaypb.SeriesCountsRequest
		clients := map[BlocksStoreClient][][]ulid.ULID{
			gatewayAnswering("1.1.1.1", &got, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2}}}}):                                   {{b1}},
			gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{2, 3}}, {Value: "x", Hashes: []uint64{9}}}}): {{b2}},
		}
		res, err := seriesCountsQueryable(0, 2*dayMs, days, clients).SeriesCounts(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: 2 * dayMs})
		require.NoError(t, err)
		assert.True(t, res.Dedup)
		assert.Equal(t, map[string][]int64{"up": {3}, "x": {1}}, res.Counts, "series 2 is in both days")
		assert.True(t, got.Hashes)
	})

	t.Run("refuses buckets across block ranges", func(t *testing.T) {
		days := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}, {ID: b2, MinTime: dayMs, MaxTime: 2 * dayMs}}
		_, err := seriesCountsQueryable(0, 2*dayMs, days, nil).SeriesCounts(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: 2 * dayMs, Step: dayMs})
		require.ErrorIs(t, err, errStepNeedsOneSource, "the estimator answers it bucket by bucket instead")
	})

	t.Run("a sketch from one store-gateway absorbs the other's hashes", func(t *testing.T) {
		days := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}, {ID: b2, MinTime: dayMs, MaxTime: 2 * dayMs}}
		sk, err := hyperloglog.NewSketch(storegateway.SeriesCountsSketchPrecision, false)
		require.NoError(t, err)
		for _, h := range []uint64{0x1111111111111111, 0x2222222222222222, 0x3333333333333333} {
			sk.InsertHash(h)
		}
		sketch, err := sk.MarshalBinary()
		require.NoError(t, err)
		var got *storegatewaypb.SeriesCountsRequest
		clients := map[BlocksStoreClient][][]ulid.ULID{
			gatewayAnswering("1.1.1.1", &got, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Sketch: sketch}}}): {{b1}},
			gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{
				{Value: "up", Hashes: []uint64{0x3333333333333333, 0x4444444444444444}},
				{Value: "down", Hashes: []uint64{0x5555555555555555}},
			}}): {{b2}},
		}
		res, err := seriesCountsQueryable(0, 2*dayMs, days, clients).SeriesCounts(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: 2 * dayMs, SketchAbove: 2})
		require.NoError(t, err)
		assert.Equal(t, int64(2), got.SketchAbove)
		assert.Equal(t, map[string][]int64{"up": {4}, "down": {1}}, res.Counts, "four distinct up series, one shared")
		assert.Equal(t, map[string]bool{"up": true}, res.Estimated)
		assert.Equal(t, int64(len(sketch)), res.SketchBytes)
		assert.Equal(t, int64(24), res.HashBytes, "three hashes of 8 bytes")
	})

	t.Run("max_index_bytes is split across the store-gateway calls", func(t *testing.T) {
		blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"}, {ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: "2_of_2"}}
		var got1, got2 *storegatewaypb.SeriesCountsRequest
		clients := map[BlocksStoreClient][][]ulid.ULID{
			gatewayAnswering("1.1.1.1", &got1, storegatewaypb.SeriesCountsResponse{}): {{b1}},
			gatewayAnswering("2.2.2.2", &got2, storegatewaypb.SeriesCountsResponse{}): {{b2}},
		}
		_, err := seriesCountsQueryable(0, dayMs, blocks, clients).SeriesCounts(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs, MaxIndexBytes: 1000})
		require.NoError(t, err)
		assert.Equal(t, int64(500), got1.MaxIndexBytes)
		assert.Equal(t, int64(500), got2.MaxIndexBytes)
	})

	t.Run("a store-gateway over max_index_bytes fails the request", func(t *testing.T) {
		blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
		over := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "the request would read about 5000 index bytes, more than max_index_bytes 1000")
		}}
		_, err := seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{over: {{b1}}}).SeriesCounts(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs, MaxIndexBytes: 1000})
		require.ErrorContains(t, err, "max_index_bytes 1000, split into 1000 bytes for each of 1 store-gateway calls: store-gateway: the request would read about 5000 index bytes, more than max_index_bytes 1000")
		assert.Equal(t, "1.1.1.1", estimateSourceAddr(err))
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
	})

	t.Run("fails if a block is not counted", func(t *testing.T) {
		blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
		clients := map[BlocksStoreClient][][]ulid.ULID{
			gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{BlockIds: []string{}}): {{b1}},
		}
		_, err := seriesCountsQueryable(0, dayMs, blocks, clients).SeriesCounts(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs})
		require.ErrorContains(t, err, "none of the 1 blocks for this window was counted")
	})

	t.Run("retries an unavailable store-gateway's blocks on another one", func(t *testing.T) {
		blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"}, {ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: "2_of_2"}}
		down := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return nil, status.Error(codes.Unavailable, "connection refused")
		}}
		up := gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}, IndexBytes: 100, PostingsOffsetsTableBytes: 50})
		q := seriesCountsQueryable(0, dayMs, blocks, nil)
		q.dynamicReplication = storegateway.NewNopDynamicReplication(3)
		var gotRetry *storegatewaypb.SeriesCountsRequest
		replica := gatewayAnswering("3.3.3.3", &gotRetry, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{5}}}, IndexBytes: 100})
		q.compartments[0].stores = &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{down: {{b1}}, up: {{b2}}},
			map[BlocksStoreClient][][]ulid.ULID{replica: {{b1}}},
		}}
		res, err := q.SeriesCounts(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs, MaxIndexBytes: 1000})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {7}}, res.Counts, "b1 counted once, by the replica")
		assert.False(t, res.LowerBound)
		assert.Equal(t, 1, res.Retries)
		assert.Equal(t, 3, res.StoreGateways)
		require.NotNil(t, gotRetry)
		assert.Equal(t, []string{b1.String()}, gotRetry.BlockIds)
		assert.Equal(t, int64(850), gotRetry.MaxIndexBytes, "the retry gets what the first attempt left of max_index_bytes, postings offsets table included")
	})

	t.Run("a block no replica counts makes the counts a lower bound", func(t *testing.T) {
		blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"}, {ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: "2_of_2"}}
		down := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return nil, status.Error(codes.Unavailable, "connection refused")
		}}
		up := gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}})
		q := seriesCountsQueryable(0, dayMs, blocks, nil)
		q.dynamicReplication = storegateway.NewNopDynamicReplication(2)
		q.compartments[0].stores = &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{down: {{b1}}, up: {{b2}}},
			errors.New("no store-gateway left"),
		}}
		res, err := q.SeriesCounts(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {2}}, res.Counts)
		assert.True(t, res.LowerBound)
		assert.Equal(t, []ulid.ULID{b1}, res.MissingBlocks)
		assert.Equal(t, []ulid.ULID{b2}, res.Blocks)
	})

	t.Run("a range with fewer shard blocks than its shard count is summed as a lower bound", func(t *testing.T) {
		blocks := bucketindex.Blocks{
			{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_3"},
			{ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: "3_of_3"},
		}
		var got *storegatewaypb.SeriesCountsRequest
		clients := map[BlocksStoreClient][][]ulid.ULID{
			gatewayAnswering("1.1.1.1", &got, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}}): {{b1}},
			gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{3}}}}):  {{b2}},
		}
		res, err := seriesCountsQueryable(0, dayMs, blocks, clients).SeriesCounts(context.Background(), "user-1", CardinalityEstimateRequest{Matchers: up, MinT: 0, MaxT: dayMs})
		require.NoError(t, err)
		assert.False(t, res.Dedup, "the shards present share no series, so they are added up")
		require.NotNil(t, got)
		assert.False(t, got.Hashes)
		assert.Equal(t, map[string][]int64{"up": {5}}, res.Counts)
		assert.True(t, res.LowerBound)
		assert.Equal(t, []MissingShard{{Shard: "2_of_3", MinTime: 0, MaxTime: dayMs}}, res.MissingShards)
		assert.Empty(t, res.MissingBlocks)
	})
}

// A store-gateway whose stream fails partway is retried on another one, and
// what it sent before failing is not counted.
func TestBlocksStoreQueryable_SeriesCounts_StreamFailingPartway(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	partway := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCountsStream: func(req *storegatewaypb.SeriesCountsRequest) ([]*storegatewaypb.SeriesCountsResponse, error) {
		return []*storegatewaypb.SeriesCountsResponse{{BlockIds: req.BlockIds, Groups: []*storegatewaypb.SeriesCountGroup{{Value: "a", Counts: []int64{5}}}}}, status.Error(codes.Unavailable, "connection reset")
	}}
	whole := &storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedSeriesCountsStream: func(req *storegatewaypb.SeriesCountsRequest) ([]*storegatewaypb.SeriesCountsResponse, error) {
		return []*storegatewaypb.SeriesCountsResponse{
			{BlockIds: req.BlockIds, Groups: []*storegatewaypb.SeriesCountGroup{{Value: "a", Counts: []int64{2}}}},
			{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "b", Counts: []int64{1}}}},
		}, nil
	}}
	q := seriesCountsQueryable(0, dayMs, blocks, nil)
	q.dynamicReplication = storegateway.NewNopDynamicReplication(2)
	q.compartments[0].stores = &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][][]ulid.ULID{partway: {{b1}}},
		map[BlocksStoreClient][][]ulid.ULID{whole: {{b1}}},
	}}
	res, err := q.SeriesCounts(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: dayMs})
	require.NoError(t, err)
	assert.Equal(t, map[string][]int64{"a": {2}, "b": {1}}, res.Counts, "the groups of every message, and none of the failed stream's")
	assert.Equal(t, 1, res.Retries)
}

func TestBlockRangeHolding(t *testing.T) {
	day := func(lo int64, shard string) *bucketindex.Block {
		return &bucketindex.Block{ID: ulid.MustNew(uint64(lo+1), nil), MinTime: lo, MaxTime: lo + dayMs, CompactorShardID: shard}
	}
	lo, hi, err := blockRangeHolding(bucketindex.Blocks{day(0, "1_of_2"), day(0, "2_of_2")}, 3, 50)
	require.NoError(t, err)
	assert.Equal(t, []int64{0, dayMs}, []int64{lo, hi})

	_, _, err = blockRangeHolding(bucketindex.Blocks{day(0, ""), day(dayMs, "")}, dayMs-10, dayMs+10)
	require.ErrorContains(t, err, "more than one block range")

	lo, hi, err = blockRangeHolding(nil, 3, 50)
	require.NoError(t, err)
	assert.Equal(t, []int64{3, 50}, []int64{lo, hi}, "nothing to snap to")
}

func TestParseSeriesCountsRequest(t *testing.T) {
	parse := func(query string) (CardinalityEstimateRequest, int, error) {
		r := httptest.NewRequest(http.MethodGet, "/x?"+query, nil)
		require.NoError(t, r.ParseForm())
		req, opts, err := parseCardinalityEstimateRequest(r)
		return req, opts.limit, err
	}
	req, limit, err := parse(`start=0&end=86400&match[]={__name__="up"}&group_by=pod&step=1h&max_index_bytes=100&limit=5`)
	require.NoError(t, err)
	assert.Equal(t, int64(86400000), req.MaxT)
	assert.Equal(t, "pod", req.GroupBy)
	assert.Equal(t, int64(3600000), req.Step)
	assert.Equal(t, int64(100), req.MaxIndexBytes)
	assert.Equal(t, 5, limit)
	require.Len(t, req.Matchers, 1)

	req, _, err = parse("start=0&end=10")
	require.NoError(t, err)
	assert.Equal(t, int64(10_000), req.SketchAbove, "the design's default")
	assert.Equal(t, "__name__", req.GroupBy)
	assert.Empty(t, req.Matchers)

	// The buckets still end every step from start+step; the window read starts one window before the first end.
	req, _, err = parse("start=7200&end=14400&step=1h&window=2h")
	require.NoError(t, err)
	assert.Equal(t, int64(3600000), req.MinT)
	assert.Equal(t, int64(14400000), req.MaxT)
	assert.Equal(t, int64(7200000), req.Window)
	n, err := cardinality.Buckets(req.MinT, req.MaxT, req.Step, req.Window)
	require.NoError(t, err)
	assert.Equal(t, 2, n, "the same buckets as step=1h alone")

	// A snapped window is widened to a block range before its buckets are cut.
	_, _, err = parse("start=0&end=9000&step=1h&snap=true")
	require.NoError(t, err)

	// Every series has a name, so a matcher on any name is no matcher.
	req, _, err = parse(`start=0&end=10&match[]={__name__=~".%2B"}`)
	require.NoError(t, err)
	assert.Nil(t, req.Matchers)
	req, _, err = parse(`start=0&end=10&match[]={__name__=~".*",job="x"}`)
	require.NoError(t, err)
	assert.Equal(t, []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "x")}, req.Matchers)
	for _, every := range []string{`{__name__!=""}`, `{__name__!~""}`} {
		req, _, err = parse("start=0&end=10&match[]=" + every)
		require.NoError(t, err)
		assert.Nil(t, req.Matchers, every)
	}
	req, _, err = parse(`start=0&end=10&match[]={__name__=~".%2Bx"}`)
	require.NoError(t, err)
	assert.Len(t, req.Matchers, 1, "a regex that matches only some names stays")

	for _, bad := range []string{
		"start=0&end=7200&window=1h",
		"start=0&end=7200&step=1h&window=30m",
		"start=0&end=7200&step=1h&window=0s",
		"start=0&end=7200&step=1h&window=2h&snap=true",
		"start=0&end=5400&step=1h&window=2h",
		"start=0&end=7200&step=1h&window=2h",
		"start=x&end=10",
		"start=10&end=5",
		"start=0&end=10&match[]=up{&",
		"start=0&end=10&match[]=a&match[]=b",
		"start=0&end=10&step=0s",
		"start=0&end=9000&step=1h",     // the step doesn't divide the window, with no window parameter
		"start=0&end=3600&step=2h",     // a step longer than the window
		"start=0&end=86400000&step=1s", // more buckets than the most a request may have
		"start=0&end=10&max_index_bytes=-1",
		"start=0&end=10&sketch_above=-1",
		"start=0&end=10&limit=x",
	} {
		_, _, err := parse(bad)
		assert.Error(t, err, bad)
	}
}

func TestCardinalityEstimateHandler(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	clients := map[BlocksStoreClient][][]ulid.ULID{
		gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{
			{Value: "a", Counts: []int64{1}}, {Value: "b", Counts: []int64{4}}, {Value: "c", Counts: []int64{2}},
		}, IndexBytes: 300, PostingsOffsetsTableBytes: 40}): {{b1}},
	}
	q := seriesCountsQueryable(0, dayMs, blocks, clients)
	var logs bytes.Buffer
	q.logger = log.NewLogfmtLogger(&logs)
	reg := prometheus.NewPedanticRegistry()
	q.metrics = newBlocksStoreQueryableMetrics(compartments.Config{}, reg)
	h := WithCardinalityEstimateRoute(http.NotFoundHandler(), "/prometheus", NewCardinalityEstimator(q, nil, nil), CardinalityEstimateConfig{}, false)

	// A matcher sends it to the full index.
	r := httptest.NewRequest(http.MethodGet, `/prometheus/api/v1/cardinality/estimate?start=0&end=86400&limit=2&match[]={job="x"}`, nil)
	r = r.WithContext(user.InjectOrgID(r.Context(), "user-1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out estimateResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, readFullIndex, out.Read)
	assert.Equal(t, 3, out.GroupCount)
	assert.Equal(t, int64(7), out.Total)
	assert.Equal(t, []estimateGroup{{Value: "b", Count: 4}, {Value: "c", Count: 2}}, out.Groups)
	// The field names are the API contract's: total, and the window counted.
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	assert.JSONEq(t, `7`, string(raw["total"]))
	assert.JSONEq(t, `{"start":0,"end":86400000}`, string(raw["window"]))
	for _, old := range []string{"series", "min_time", "max_time", "snapped"} {
		assert.NotContains(t, raw, old)
	}
	// What the estimate cost is in the log line and the metrics, not in the
	// response, which the query-frontend caches.
	for _, cost := range []string{"elapsed_ms", "index_bytes", "postings_fetched_bytes", "series_fetched_bytes", "postings_offsets_table_bytes", "series_counted", "hash_bytes", "sketch_bytes", "blocks_from_postings", "retries", "store_gateways", "blocks"} {
		assert.NotContains(t, raw, cost)
	}
	line := logs.String()
	assert.Contains(t, line, `msg="cardinality estimate"`)
	assert.Contains(t, line, "index_bytes=300")
	assert.Contains(t, line, "postings_offsets_table_bytes=40")
	assert.Contains(t, line, "blocks=1")
	assert.Contains(t, line, "read=full_index")
	assert.Equal(t, 1, strings.Count(line, "\n"), "one line per request")
	assert.Equal(t, float64(300), testutil.ToFloat64(q.metrics.estimateBytes.WithLabelValues(readFullIndex, "index")))
	assert.Equal(t, float64(40), testutil.ToFloat64(q.metrics.estimateBytes.WithLabelValues(readFullIndex, "postings_offsets_table")))
	assert.Equal(t, float64(1), testutil.ToFloat64(q.metrics.estimateBlocks.WithLabelValues(readFullIndex)))
	assert.Equal(t, sourceBlocks, out.Source)
	assert.True(t, out.HasMore, "limit=2 cut one of three groups")
	assert.False(t, out.LowerBound)
	assert.Empty(t, out.MissingBlocks)
	assert.Empty(t, rec.Header().Get("Cache-Control"), "a complete answer can be cached")

	// The blocks cover only the first day of a two-day window: the answer is
	// a lower bound, which the query-frontend must not cache.
	q.compartments[0].finder.(*blocksFinderMock).On("GetBlocks", mock.Anything, "user-1", int64(0), 2*dayMs-1).Return(blocks, &bucketindex.Metadata{}, nil)
	q.compartments[0].stores.(*blocksStoreSetMock).mockedResponses = append(q.compartments[0].stores.(*blocksStoreSetMock).mockedResponses, clients)
	r = httptest.NewRequest(http.MethodGet, `/prometheus/api/v1/cardinality/estimate?start=0&end=172800&match[]={job="x"}`, nil)
	r = r.WithContext(user.InjectOrgID(r.Context(), "user-1"))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"lower_bound":true`)
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))

	// Other paths go to next.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/prometheus/api/v1/query", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// A block range missing one of its shard blocks is answered from the
// shards present, as a lower bound that names the missing shard.
func TestCardinalityEstimateHandler_MissingShard(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"}}
	gateway := &storeGatewayClientMock{
		remoteAddr: "1.1.1.1",
		mockedMetricNameCounts: func(req *storegatewaypb.MetricNameCountsRequest) (*storegatewaypb.MetricNameCountsResponse, error) {
			return &storegatewaypb.MetricNameCountsResponse{BlockIds: req.BlockIds, Counts: []*storegatewaypb.MetricNameCount{{Name: "up", Count: 3}}}, nil
		},
	}
	q := seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{gateway: {{b1}}})
	h := WithCardinalityEstimateRoute(http.NotFoundHandler(), "/prometheus", NewCardinalityEstimator(q, nil, nil), CardinalityEstimateConfig{}, false)

	r := httptest.NewRequest(http.MethodGet, `/prometheus/api/v1/cardinality/estimate?start=0&end=86400`, nil)
	r = r.WithContext(user.InjectOrgID(r.Context(), "user-1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	assert.JSONEq(t, `"index_header"`, string(raw["read"]))
	assert.JSONEq(t, `true`, string(raw["lower_bound"]))
	assert.JSONEq(t, `[{"shard":"2_of_2","start":0,"end":86400000}]`, string(raw["missing_shards"]))
	assert.NotContains(t, raw, "missing_blocks")
	assert.JSONEq(t, `3`, string(raw["total"]))
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
}

// The design spells "every metric name" as {__name__=~".+"}, which is read
// as no matcher and answered from the index-headers.
func TestCardinalityEstimateHandler_EveryNameIsNoMatcher(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	gateway := &storeGatewayClientMock{
		remoteAddr: "1.1.1.1",
		mockedMetricNameCounts: func(req *storegatewaypb.MetricNameCountsRequest) (*storegatewaypb.MetricNameCountsResponse, error) {
			return &storegatewaypb.MetricNameCountsResponse{BlockIds: req.BlockIds, Counts: []*storegatewaypb.MetricNameCount{{Name: "up", Count: 3}}}, nil
		},
	}
	q := seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{gateway: {{b1}}})
	h := WithCardinalityEstimateRoute(http.NotFoundHandler(), "/prometheus", NewCardinalityEstimator(q, nil, nil), CardinalityEstimateConfig{}, false)
	r := httptest.NewRequest(http.MethodGet, `/prometheus/api/v1/cardinality/estimate?start=0&end=86400&match[]={__name__=~".%2B"}`, nil)
	r = r.WithContext(user.InjectOrgID(r.Context(), "user-1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"read":"index_header"`)
}

// Per-bucket answers carry a missing shard once, whichever read answered
// each bucket: the name table for a bucket that is the whole block range,
// series entries for one inside it.
func TestCardinalityEstimator_PerBucketMissingShard(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	day1, day2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	all := bucketindex.Blocks{
		{ID: day1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"},
		{ID: day2, MinTime: dayMs, MaxTime: 2 * dayMs},
	}
	gateway := &storeGatewayClientMock{
		remoteAddr: "1.1.1.1",
		mockedMetricNameCounts: func(req *storegatewaypb.MetricNameCountsRequest) (*storegatewaypb.MetricNameCountsResponse, error) {
			return &storegatewaypb.MetricNameCountsResponse{BlockIds: req.BlockIds, Counts: []*storegatewaypb.MetricNameCount{{Name: "up", Count: 3}}}, nil
		},
		mockedSeriesCounts: func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return &storegatewaypb.SeriesCountsResponse{BlockIds: req.BlockIds, Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}}, nil
		},
	}
	for name, step := range map[string]int64{"name table": 24 * hour, "series entries": 12 * hour} {
		t.Run(name, func(t *testing.T) {
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", mock.Anything, "user-1", int64(0), 2*dayMs-1).Return(all, &bucketindex.Metadata{}, nil)
			for lo := int64(0); lo < 2*dayMs; lo += step {
				blocks := all[:1]
				if lo >= dayMs {
					blocks = all[1:]
				}
				finder.On("GetBlocks", mock.Anything, "user-1", lo, lo+step-1).Return(blocks, &bucketindex.Metadata{}, nil)
			}
			clients := map[BlocksStoreClient][][]ulid.ULID{gateway: {{day1, day2}}}
			stores := &blocksStoreSetMock{}
			for range 4 {
				stores.mockedResponses = append(stores.mockedResponses, clients)
			}
			q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: stores}}}
			req := CardinalityEstimateRequest{MinT: 0, MaxT: 2 * dayMs, Step: step}
			res, err := NewCardinalityEstimator(q, nil, nil).Estimate(context.Background(), "user-1", &req)
			require.NoError(t, err)
			assert.Equal(t, readPerBucket, res.Read)
			assert.True(t, res.LowerBound)
			assert.Equal(t, []MissingShard{{Shard: "2_of_2", MinTime: 0, MaxTime: dayMs}}, res.MissingShards)
		})
	}
}

// The whole-head fallback counts every series the heads hold, whatever the
// window, so the answer changes as the heads do and must not be cached.
func TestCardinalityEstimateHandler_HeadWholeNotCached(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	ing := &ingestersMock{
		resp: &client.LabelValuesCardinalityResponse{Items: []*client.LabelValueSeriesCount{
			{LabelName: "__name__", LabelValueSeries: map[string]uint64{"up": 4}},
		}},
		seriesCountsErr: status.Error(codes.ResourceExhausted, "more than max_index_bytes 1000"),
	}
	e := NewCardinalityEstimator(&BlocksStoreQueryable{queryStoreAfter: 12 * time.Hour}, ing, ingestersWithinLimit(13*time.Hour))
	e.now = func() time.Time { return time.UnixMilli(100 * hour) }
	h := WithCardinalityEstimateRoute(http.NotFoundHandler(), "/prometheus", e, CardinalityEstimateConfig{}, false)

	r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/prometheus/api/v1/cardinality/estimate?start=%d&end=%d&max_index_bytes=1000", 99*3600, 100*3600), nil)
	r = r.WithContext(user.InjectOrgID(r.Context(), "user-1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"head_whole":true`)
	assert.Contains(t, rec.Body.String(), `"lower_bound":false`)
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
}

func TestBlocksStoreQueryable_CardinalityEstimate_PicksTheRead(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	day := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	gateway := &storeGatewayClientMock{
		remoteAddr: "1.1.1.1",
		mockedMetricNameCounts: func(req *storegatewaypb.MetricNameCountsRequest) (*storegatewaypb.MetricNameCountsResponse, error) {
			return &storegatewaypb.MetricNameCountsResponse{BlockIds: req.BlockIds, Counts: []*storegatewaypb.MetricNameCount{{Name: "up", Count: 3}}}, nil
		},
		mockedSeriesCounts: func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return &storegatewaypb.SeriesCountsResponse{BlockIds: req.BlockIds, Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}}, nil
		},
	}
	queryable := func(windows ...[2]int64) *BlocksStoreQueryable {
		finder := &blocksFinderMock{}
		for _, w := range windows {
			finder.On("GetBlocks", mock.Anything, "user-1", w[0], w[1]-1).Return(day, &bucketindex.Metadata{}, nil)
		}
		clients := map[BlocksStoreClient][][]ulid.ULID{gateway: {{b1}}}
		return &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{clients, clients}}}}}
	}
	up := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")}

	for name, tc := range map[string]struct {
		req     CardinalityEstimateRequest
		read    string
		counts  map[string][]int64
		snapped bool
	}{
		"every name over one block range": {CardinalityEstimateRequest{MinT: 0, MaxT: dayMs, GroupBy: "__name__"}, readIndexHeader, map[string][]int64{"up": {3}}, false},
		"a matcher":                       {CardinalityEstimateRequest{MinT: 0, MaxT: dayMs, Matchers: up}, readFullIndex, map[string][]int64{"up": {2}}, false},
		"a window inside the block":       {CardinalityEstimateRequest{MinT: 0, MaxT: dayMs / 2}, readFullIndex, map[string][]int64{"up": {2}}, false},
		"snapped to the block":            {CardinalityEstimateRequest{MinT: 0, MaxT: dayMs / 2, Snap: true}, readIndexHeader, map[string][]int64{"up": {3}}, true},
		"grouped by another label":        {CardinalityEstimateRequest{MinT: 0, MaxT: dayMs, GroupBy: "job"}, readFullIndex, map[string][]int64{"up": {2}}, false},
	} {
		t.Run(name, func(t *testing.T) {
			req := tc.req
			res, err := queryable([2]int64{req.MinT, req.MaxT}, [2]int64{0, dayMs}).CardinalityEstimate(context.Background(), "user-1", &req)
			require.NoError(t, err)
			assert.Equal(t, tc.read, res.Read)
			assert.Equal(t, tc.counts, res.Counts)
			assert.Equal(t, tc.snapped, res.Snapped)
			if tc.snapped {
				assert.Equal(t, dayMs, req.MaxT, "the request says what was counted")
			}
		})
	}
}

func TestSplitIndexBytes(t *testing.T) {
	assert.Equal(t, int64(0), splitIndexBytes(0, 4), "unlimited stays unlimited")
	assert.Equal(t, int64(1000), splitIndexBytes(1000, 1))
	assert.Equal(t, int64(333), splitIndexBytes(1000, 3))
	assert.Equal(t, int64(1), splitIndexBytes(3, 8), "a positive limit never becomes unlimited")
}

// With snap, the buckets are known only once the window is widened to the
// block range. A step that doesn't cut that range into whole buckets fails
// the request, rather than turning off the check that every group carries
// the buckets asked for, which catches a store-gateway that ignored step_ms.
func TestBlocksStoreQueryable_CardinalityEstimate_SnapWithStep(t *testing.T) {
	const minute = int64(time.Minute / time.Millisecond)
	b1 := ulid.MustNew(1, nil)
	day := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	// A store-gateway that doesn't know step_ms answers one count per group.
	var got *storegatewaypb.SeriesCountsRequest
	ignoresStep := gatewayAnswering("1.1.1.1", &got, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{6}}}})
	queryable := func() *BlocksStoreQueryable {
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", int64(0), 60*minute-1).Return(day, &bucketindex.Metadata{}, nil)
		finder.On("GetBlocks", mock.Anything, "user-1", int64(0), dayMs-1).Return(day, &bucketindex.Metadata{}, nil)
		clients := map[BlocksStoreClient][][]ulid.ULID{ignoresStep: {{b1}}}
		return &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{clients}}}}}
	}

	req := CardinalityEstimateRequest{MinT: 0, MaxT: 60 * minute, Step: 7 * minute, Snap: true}
	_, err := queryable().CardinalityEstimate(context.Background(), "user-1", &req)
	require.ErrorContains(t, err, "doesn't divide [0, 86400000)", "the snapped window is checked")
	assert.Equal(t, http.StatusBadRequest, estimateHTTPStatus(err))
	assert.Nil(t, got, "refused before any store-gateway is asked")

	req = CardinalityEstimateRequest{MinT: 0, MaxT: 60 * minute, Step: 60 * minute, Snap: true}
	_, err = queryable().CardinalityEstimate(context.Background(), "user-1", &req)
	require.ErrorContains(t, err, "instead of the 24 asked for with step_ms")
	assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
}
