// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
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
		res, err := q.SeriesCounts(context.Background(), "user-1", SeriesCountsRequest{Matchers: up, MinT: 0, MaxT: 2 * 60 * 60 * 1000, Step: 60 * 60 * 1000, GroupBy: "__name__"})
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
		res, err := seriesCountsQueryable(0, 2*dayMs, days, clients).SeriesCounts(context.Background(), "user-1", SeriesCountsRequest{MinT: 0, MaxT: 2 * dayMs})
		require.NoError(t, err)
		assert.True(t, res.Dedup)
		assert.Equal(t, map[string][]int64{"up": {3}, "x": {1}}, res.Counts, "series 2 is in both days")
		assert.True(t, got.Hashes)
	})

	t.Run("refuses buckets across block ranges", func(t *testing.T) {
		days := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}, {ID: b2, MinTime: dayMs, MaxTime: 2 * dayMs}}
		_, err := seriesCountsQueryable(0, 2*dayMs, days, nil).SeriesCounts(context.Background(), "user-1", SeriesCountsRequest{MinT: 0, MaxT: 2 * dayMs, Step: dayMs})
		require.ErrorContains(t, err, "inside one block range")
	})

	t.Run("lower bound when a store-gateway hits its budget", func(t *testing.T) {
		blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
		clients := map[BlocksStoreClient][][]ulid.ULID{
			gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{BlockIds: []string{}, LowerBound: true, SeriesCounted: 5, Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{5}}}}): {{b1}},
		}
		res, err := seriesCountsQueryable(0, dayMs, blocks, clients).SeriesCounts(context.Background(), "user-1", SeriesCountsRequest{MinT: 0, MaxT: dayMs, MaxSeries: 5})
		require.NoError(t, err, "an unfinished block is expected under a budget")
		assert.True(t, res.LowerBound)
		assert.Equal(t, int64(5), res.SeriesCounted)
	})

	t.Run("fails if a block is not counted", func(t *testing.T) {
		blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
		clients := map[BlocksStoreClient][][]ulid.ULID{
			gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{BlockIds: []string{}}): {{b1}},
		}
		_, err := seriesCountsQueryable(0, dayMs, blocks, clients).SeriesCounts(context.Background(), "user-1", SeriesCountsRequest{MinT: 0, MaxT: dayMs})
		require.ErrorContains(t, err, "was not counted")
	})
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
	parse := func(query string) (SeriesCountsRequest, int, error) {
		r := httptest.NewRequest(http.MethodGet, "/x?"+query, nil)
		require.NoError(t, r.ParseForm())
		return parseSeriesCountsRequest(r)
	}
	req, limit, err := parse(`start=0&end=86400&match[]={__name__="up"}&group_by=pod&step=1h&budget=100&limit=5`)
	require.NoError(t, err)
	assert.Equal(t, int64(86400000), req.MaxT)
	assert.Equal(t, "pod", req.GroupBy)
	assert.Equal(t, int64(3600000), req.Step)
	assert.Equal(t, int64(100), req.MaxSeries)
	assert.Equal(t, 5, limit)
	require.Len(t, req.Matchers, 1)

	req, _, err = parse("start=0&end=10")
	require.NoError(t, err)
	assert.Equal(t, "__name__", req.GroupBy)
	assert.Empty(t, req.Matchers)

	for _, bad := range []string{
		"start=x&end=10",
		"start=10&end=5",
		"start=0&end=10&match[]=up{&",
		"start=0&end=10&match[]=a&match[]=b",
		"start=0&end=10&step=0s",
		"start=0&end=10&budget=-1",
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
		}}): {{b1}},
	}
	h := WithCardinalityEstimateRoute(http.NotFoundHandler(), "/prometheus", seriesCountsQueryable(0, dayMs, blocks, clients))

	// A matcher sends it to the full index.
	r := httptest.NewRequest(http.MethodGet, `/prometheus/api/v1/cardinality/estimate?start=0&end=86400&limit=2&match[]={job="x"}`, nil)
	r = r.WithContext(user.InjectOrgID(r.Context(), "user-1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out estimateResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, readFullIndex, out.Read)
	assert.Equal(t, 3, out.Groups)
	assert.Equal(t, int64(7), out.Series)
	assert.Equal(t, []estimateGroup{{Value: "b", Count: 4}, {Value: "c", Count: 2}}, out.Counts)

	// Other paths go to next.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/prometheus/api/v1/query", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)
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
		req     SeriesCountsRequest
		read    string
		counts  map[string][]int64
		snapped bool
	}{
		"every name over one block range": {SeriesCountsRequest{MinT: 0, MaxT: dayMs, GroupBy: "__name__"}, readIndexHeader, map[string][]int64{"up": {3}}, false},
		"a matcher":                       {SeriesCountsRequest{MinT: 0, MaxT: dayMs, Matchers: up}, readFullIndex, map[string][]int64{"up": {2}}, false},
		"a window inside the block":       {SeriesCountsRequest{MinT: 0, MaxT: dayMs / 2}, readFullIndex, map[string][]int64{"up": {2}}, false},
		"snapped to the block":            {SeriesCountsRequest{MinT: 0, MaxT: dayMs / 2, Snap: true}, readIndexHeader, map[string][]int64{"up": {3}}, true},
		"grouped by another label":        {SeriesCountsRequest{MinT: 0, MaxT: dayMs, GroupBy: "job"}, readFullIndex, map[string][]int64{"up": {2}}, false},
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
