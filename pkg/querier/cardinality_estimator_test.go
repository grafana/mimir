// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

type ingestersMock struct {
	gotLabelNames []model.LabelName
	gotMatchers   []*labels.Matcher
	gotMethod     cardinality.CountMethod
	resp          *client.LabelValuesCardinalityResponse

	gotSeriesCounts *client.SeriesCountsRequest
	seriesCounts    distributor.SeriesCountsResult
	seriesCountsErr error
}

func (m *ingestersMock) SeriesCounts(_ context.Context, _ []*labels.Matcher, req *client.SeriesCountsRequest) (distributor.SeriesCountsResult, error) {
	m.gotSeriesCounts = req
	return m.seriesCounts, m.seriesCountsErr
}

func (m *ingestersMock) LabelValuesCardinality(_ context.Context, labelNames []model.LabelName, matchers []*labels.Matcher, countMethod cardinality.CountMethod) (uint64, *client.LabelValuesCardinalityResponse, error) {
	m.gotLabelNames, m.gotMatchers, m.gotMethod = labelNames, matchers, countMethod
	return 0, m.resp, nil
}

type ingestersWithinLimit time.Duration

func (l ingestersWithinLimit) QueryIngestersWithin(string) time.Duration { return time.Duration(l) }

func TestCardinalityEstimator_Routing(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	now := time.UnixMilli(100 * hour)
	b1 := ulid.MustNew(1, nil)
	// One 24h block ending 24h before now; the ingesters hold the last 13h.
	day := bucketindex.Blocks{{ID: b1, MinTime: 52 * hour, MaxTime: 76 * hour}}

	newEstimator := func(blocks bucketindex.Blocks, ing *ingestersMock) *CardinalityEstimator {
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(blocks, &bucketindex.Metadata{}, nil)
		gw := gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{3}}}})
		q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
		}}}}, queryStoreAfter: 12 * time.Hour}
		var e *CardinalityEstimator
		if ing == nil {
			e = NewCardinalityEstimator(q, nil, nil)
		} else {
			e = NewCardinalityEstimator(q, ing, ingestersWithinLimit(13*time.Hour))
		}
		e.now = func() time.Time { return now }
		return e
	}
	headResp := &client.LabelValuesCardinalityResponse{Items: []*client.LabelValueSeriesCount{
		{LabelName: "__name__", LabelValueSeries: map[string]uint64{"up": 4, "errors_total": 2}},
	}}

	t.Run("a window the blocks cover is counted from the blocks", func(t *testing.T) {
		ing := &ingestersMock{resp: headResp}
		up := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")}
		res, err := newEstimator(day, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{Matchers: up, MinT: 52 * hour, MaxT: 76 * hour})
		require.NoError(t, err)
		assert.Equal(t, sourceBlocks, res.Source)
		assert.Equal(t, map[string][]int64{"up": {3}}, res.Counts)
		assert.Nil(t, ing.gotLabelNames, "the ingesters are not asked")
	})

	t.Run("a recent window with no block is counted from the heads' chunk metas", func(t *testing.T) {
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Responses: []*client.SeriesCountsResponse{
			{Groups: []*client.SeriesCountGroup{{Value: "up", Counts: []int64{1, 2}}}, SeriesCounted: 2, MinTimeMs: 98 * hour},
			{Groups: []*client.SeriesCountGroup{{Value: "up", Counts: []int64{0, 1}}, {Value: "errors_total", Counts: []int64{1, 1}}}, SeriesCounted: 2, MinTimeMs: 97 * hour},
		}}}
		job := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "a")}
		res, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{Matchers: job, MinT: 98 * hour, MaxT: 100 * hour, Step: hour, GroupBy: "__name__", MaxIndexBytes: 1000})
		require.NoError(t, err)
		assert.Equal(t, sourceHead, res.Source)
		assert.Equal(t, readHeadChunkMetas, res.Read)
		assert.False(t, res.HeadWhole)
		assert.False(t, res.LowerBound, "every head starts by the window start")
		assert.Equal(t, map[string][]int64{"up": {1, 3}, "errors_total": {1, 1}}, res.Counts)
		require.NotNil(t, ing.gotSeriesCounts)
		assert.Equal(t, hour, ing.gotSeriesCounts.StepMs)
		assert.Equal(t, "__name__", ing.gotSeriesCounts.GroupBy)
		assert.Equal(t, int64(1000), ing.gotSeriesCounts.MaxIndexBytes)
		assert.Equal(t, []*client.LabelMatcher{{Type: client.EQUAL, Name: "job", Value: "a"}}, ing.gotSeriesCounts.Matchers)
	})

	t.Run("a block that claims the recent range is not trusted over the ingesters", func(t *testing.T) {
		// A block being built for the current range can have its metadata
		// cover the whole range before all of its data is in.
		building := bucketindex.Blocks{{ID: b1, MinTime: 98 * hour, MaxTime: 102 * hour}}
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Responses: []*client.SeriesCountsResponse{
			{Groups: []*client.SeriesCountGroup{{Value: "up", Counts: []int64{7}}}, MinTimeMs: 98 * hour},
		}}}
		res, err := newEstimator(building, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.Equal(t, sourceHead, res.Source)
		assert.Equal(t, map[string][]int64{"up": {7}}, res.Counts)
	})

	t.Run("hashes from replicas in one zone are unioned", func(t *testing.T) {
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
			{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2}}}},
			{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: []uint64{2, 3}}}},
		}}}
		res, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.True(t, res.Dedup)
		assert.Equal(t, map[string][]int64{"up": {3}}, res.Counts)
	})

	t.Run("a tenant with no data before the window's start is not a lower bound", func(t *testing.T) {
		// The ingesters keep the last 13h, so nothing missing from them after
		// 87h was ever written.
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Responses: []*client.SeriesCountsResponse{
			{Groups: []*client.SeriesCountGroup{{Value: "up", Counts: []int64{1}}}, MinTimeMs: 99 * hour},
			{}, // an ingester without the tenant
		}}}
		res, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 90 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.False(t, res.LowerBound)
		assert.Zero(t, res.HeadMinTime)
	})

	t.Run("a window starting before what the ingesters keep is a lower bound", func(t *testing.T) {
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Responses: []*client.SeriesCountsResponse{
			{Groups: []*client.SeriesCountGroup{{Value: "up", Counts: []int64{1}}}, MinTimeMs: 95 * hour},
		}}}
		e := newEstimator(nil, ing)
		// The ingesters are asked only for the last 10h, less than
		// query-store-after, so this head-only window starts before them.
		e.limits = ingestersWithinLimit(10 * time.Hour)
		res, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 89 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.Equal(t, sourceHead, res.Source)
		assert.True(t, res.LowerBound)
		assert.Equal(t, 90*hour, res.HeadMinTime)
	})

	t.Run("a head walk over max_index_bytes falls back to the whole head's postings", func(t *testing.T) {
		ing := &ingestersMock{resp: headResp, seriesCountsErr: status.Error(codes.ResourceExhausted, "the request would walk 9 head series, about 4608 bytes at 512 bytes each, more than max_index_bytes 1000")}
		res, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour, MaxIndexBytes: 1000})
		require.NoError(t, err)
		assert.Equal(t, readHeadPostings, res.Read)
		assert.True(t, res.HeadWhole)
		assert.Contains(t, res.HeadFallback, "max_index_bytes 1000")
		assert.Equal(t, map[string][]int64{"up": {4}, "errors_total": {2}}, res.Counts)
		assert.Equal(t, cardinality.InMemoryMethod, ing.gotMethod)
	})

	t.Run("with a step the refusal stands", func(t *testing.T) {
		ing := &ingestersMock{seriesCountsErr: status.Error(codes.ResourceExhausted, "more than max_index_bytes 1000")}
		_, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour, Step: hour, MaxIndexBytes: 1000})
		require.Error(t, err)
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
	})

	t.Run("an invalid request on the ingesters is 400", func(t *testing.T) {
		ing := &ingestersMock{seriesCountsErr: status.Error(codes.InvalidArgument, "step 7 doesn't divide the window")}
		_, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour, Step: 7})
		assert.Equal(t, http.StatusBadRequest, estimateHTTPStatus(err))
	})

	t.Run("a window held partly by blocks and partly by the ingesters unions both sources' hashes", func(t *testing.T) {
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(day, &bucketindex.Metadata{}, nil)
		var gotBlocks *storegatewaypb.SeriesCountsRequest
		gw := gatewayAnswering("1.1.1.1", &gotBlocks, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2}}}})
		q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
		}}}}, queryStoreAfter: 12 * time.Hour}
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
			{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: []uint64{2, 3}}, {Value: "new", Hashes: []uint64{9}}}, MinTimeMs: 76 * hour},
		}}}
		e := NewCardinalityEstimator(q, ing, ingestersWithinLimit(13*time.Hour))
		e.now = func() time.Time { return now }
		res, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 52 * hour, MaxT: 100 * hour, MaxIndexBytes: 1000})
		require.NoError(t, err)
		assert.Equal(t, sourceBoth, res.Source)
		assert.True(t, res.Dedup)
		assert.Equal(t, map[string][]int64{"up": {3}, "new": {1}}, res.Counts, "series 2 is in a block and an ingester")
		assert.False(t, res.LowerBound, "the block ends where the ingesters' data starts")
		require.NotNil(t, gotBlocks)
		assert.True(t, gotBlocks.Hashes)
		assert.Equal(t, int64(500), gotBlocks.MaxIndexBytes, "max_index_bytes is split between the two sources")
		assert.True(t, ing.gotSeriesCounts.Hashes)
		assert.Equal(t, int64(500), ing.gotSeriesCounts.MaxIndexBytes)
	})

	t.Run("a gap between the blocks and the ingesters is a lower bound", func(t *testing.T) {
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
			{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: []uint64{9}}}, MinTimeMs: 90 * hour},
		}}}
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(day, &bucketindex.Metadata{}, nil)
		gw := gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1}}}})
		q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
		}}}}, queryStoreAfter: 12 * time.Hour}
		e := NewCardinalityEstimator(q, ing, ingestersWithinLimit(13*time.Hour))
		e.now = func() time.Time { return now }
		res, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 52 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {2}}, res.Counts)
		assert.True(t, res.LowerBound)
		assert.True(t, res.CoverageGap, "nothing holds [76h, 90h)")
	})

	t.Run("per-bucket counts across blocks and ingesters are refused", func(t *testing.T) {
		_, err := newEstimator(day, &ingestersMock{}).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 52 * hour, MaxT: 100 * hour, Step: 24 * hour})
		require.ErrorContains(t, err, "per-bucket counts over a window held partly by blocks")
		assert.Equal(t, http.StatusBadRequest, estimateHTTPStatus(err))
	})

	t.Run("a window older than the ingesters with no block is empty", func(t *testing.T) {
		res, err := newEstimator(nil, &ingestersMock{resp: headResp}).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 0, MaxT: 24 * hour})
		require.NoError(t, err)
		assert.Empty(t, res.Counts)
		assert.Equal(t, sourceBlocks, res.Source)
	})

	t.Run("without ingesters a partly covered window is a lower bound from the blocks", func(t *testing.T) {
		res, err := newEstimator(day, nil).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 52 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.Equal(t, sourceBlocks, res.Source)
		assert.True(t, res.LowerBound)
		assert.True(t, res.CoverageGap)
	})
}

func TestBlocksCover(t *testing.T) {
	b := func(minT, maxT int64) *bucketindex.Block { return &bucketindex.Block{MinTime: minT, MaxTime: maxT} }
	for name, tc := range map[string]struct {
		blocks     bucketindex.Blocks
		minT, maxT int64
		want       bool
	}{
		"one block holds the window":        {bucketindex.Blocks{b(0, 10)}, 2, 8, true},
		"two adjacent blocks":               {bucketindex.Blocks{b(5, 10), b(0, 5)}, 0, 10, true},
		"a gap between blocks":              {bucketindex.Blocks{b(0, 4), b(5, 10)}, 0, 10, false},
		"blocks stop before the window end": {bucketindex.Blocks{b(0, 8)}, 0, 10, false},
		"blocks start after the window":     {bucketindex.Blocks{b(2, 10)}, 0, 10, false},
		"overlapping blocks":                {bucketindex.Blocks{b(0, 6), b(3, 10)}, 0, 10, true},
		"no blocks":                         {nil, 0, 10, false},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, blocksCover(tc.blocks, tc.minT, tc.maxT))
		})
	}
}

