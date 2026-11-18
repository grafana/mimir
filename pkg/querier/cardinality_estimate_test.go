// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
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
	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/validation"
)

type ingestersMock struct {
	gotSeriesCounts *client.SeriesCountsRequest
	// gotMatchers are the matchers the distributor was given to pick the ingesters with.
	gotMatchers     []*labels.Matcher
	seriesCounts    distributor.SeriesCountsResult
	seriesCountsErr error
	// seriesCountsFunc, when set, answers SeriesCounts instead.
	seriesCountsFunc func(*client.SeriesCountsRequest) (distributor.SeriesCountsResult, error)
	// seriesCountsCtxFunc, when set, answers SeriesCounts instead, with the call's context.
	seriesCountsCtxFunc func(context.Context, *client.SeriesCountsRequest) (distributor.SeriesCountsResult, error)
}

func (m *ingestersMock) SeriesCounts(ctx context.Context, matchers []*labels.Matcher, req *client.SeriesCountsRequest) (distributor.SeriesCountsResult, error) {
	m.gotSeriesCounts = req
	m.gotMatchers = matchers
	if m.seriesCountsCtxFunc != nil {
		return m.seriesCountsCtxFunc(ctx, req)
	}
	if m.seriesCountsFunc != nil {
		return m.seriesCountsFunc(req)
	}
	return m.seriesCounts, m.seriesCountsErr
}

type ingestersWithinLimit time.Duration

func (l ingestersWithinLimit) QueryIngestersWithin(string) time.Duration { return time.Duration(l) }

func (ingestersWithinLimit) CardinalityEstimateEnabled(string) bool { return true }

func (ingestersWithinLimit) MaxQueryLookback(string) time.Duration { return 0 }

func (ingestersWithinLimit) CompactorBlocksRetentionPeriod(string) time.Duration { return 0 }

func (ingestersWithinLimit) CardinalityEstimateMaxIndexBytes(string) int64 { return 0 }

func (ingestersWithinLimit) CardinalityEstimateMaxMemoryBytes(string) uint64 { return 0 }

func (ingestersWithinLimit) CardinalityEstimateMaxGroups(string) int64 { return 0 }

// maxGroupsLimit is ingestersWithinLimit with a limit on the groups an
// estimate may answer.
type maxGroupsLimit struct {
	ingestersWithinLimit
	groups int64
}

func (l maxGroupsLimit) CardinalityEstimateMaxGroups(string) int64 { return l.groups }

func (ingestersWithinLimit) MaxLabelNameLength(string) int { return 1024 }

// indexBytesLimit is ingestersWithinLimit with a limit on index bytes per
// estimate.
type indexBytesLimit struct {
	ingestersWithinLimit
	bytes int64
}

func (l indexBytesLimit) CardinalityEstimateMaxIndexBytes(string) int64 { return l.bytes }

func (ingestersWithinLimit) MaxEstimatedMemoryConsumptionPerQuery(string) uint64 { return 0 }

// memoryLimit is ingestersWithinLimit with a memory limit per query.
type memoryLimit struct {
	ingestersWithinLimit
	bytes uint64
}

func (l memoryLimit) MaxEstimatedMemoryConsumptionPerQuery(string) uint64 { return l.bytes }

func TestCardinalityEstimator_Routing(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	now := time.UnixMilli(100 * hour)
	b1 := ulid.MustNew(1, nil)
	// One 24h block ending 24h before now; the ingesters hold the last 13h.
	day := bucketindex.Blocks{{ID: b1, MinTime: 52 * hour, MaxTime: 76 * hour}}

	newEstimator := func(blocks bucketindex.Blocks, ing *ingestersMock) *CardinalityEstimator {
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(blocks, &bucketindex.Metadata{}, nil)
		gw := gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{3}}}})
		q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
		}}}}, queryStoreAfter: 12 * time.Hour}
		var e *CardinalityEstimator
		if ing == nil {
			e = NewCardinalityEstimator(q, nil, ingestersWithinLimit(13*time.Hour))
		} else {
			e = NewCardinalityEstimator(q, ing, ingestersWithinLimit(13*time.Hour))
		}
		e.now = func() time.Time { return now }
		return e
	}
	t.Run("a window the blocks cover is counted from the blocks", func(t *testing.T) {
		ing := &ingestersMock{}
		up := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")}
		res, err := newEstimator(day, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{Matchers: up, MinT: 52 * hour, MaxT: 76 * hour})
		require.NoError(t, err)
		assert.Equal(t, SourceBlocks, res.Source)
		assert.Equal(t, map[string][]int64{"up": {3}}, res.Counts)
		assert.Nil(t, ing.gotSeriesCounts, "the ingesters are not asked")
	})

	t.Run("a recent window with no block is counted from the heads' chunk metas", func(t *testing.T) {
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Responses: []*client.SeriesCountsResponse{
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}, SeriesCounted: 2, MinTimeMs: 98 * hour},
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{1}}, {Value: "errors_total", Counts: []int64{1}}}, SeriesCounted: 2, MinTimeMs: 97 * hour},
		}}}
		job := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "a")}
		res, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{Matchers: job, MinT: 98 * hour, MaxT: 100 * hour, GroupBy: "__name__", MaxIndexBytes: 1000})
		require.NoError(t, err)
		assert.Equal(t, SourceHead, res.Source)
		assert.Equal(t, ReadHeadChunkMetas, res.Read)
		assert.False(t, res.LowerBound, "every head starts by the window start")
		assert.Equal(t, map[string][]int64{"up": {3}, "errors_total": {1}}, res.Counts)
		require.NotNil(t, ing.gotSeriesCounts)
		assert.Equal(t, "__name__", ing.gotSeriesCounts.GroupBy)
		assert.Equal(t, int64(1000), ing.gotSeriesCounts.MaxIndexBytes)
		assert.Equal(t, []cardinalitypb.LabelMatcher{{Type: labels.MatchEqual, Name: "job", Value: "a"}}, ing.gotSeriesCounts.Matchers)
	})

	t.Run("a block that claims the recent range is not trusted over the ingesters", func(t *testing.T) {
		// A block being built for the current range can have its metadata
		// cover the whole range before all of its data is in.
		building := bucketindex.Blocks{{ID: b1, MinTime: 98 * hour, MaxTime: 102 * hour}}
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Responses: []*client.SeriesCountsResponse{
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{7}}}, MinTimeMs: 98 * hour},
		}}}
		res, err := newEstimator(building, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.Equal(t, SourceHead, res.Source)
		assert.Equal(t, map[string][]int64{"up": {7}}, res.Counts)
	})

	t.Run("hashes from replicas in one zone are unioned", func(t *testing.T) {
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2}}}},
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{2, 3}}}},
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
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{1}}}, MinTimeMs: 99 * hour},
			{}, // an ingester without the tenant
		}}}
		res, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 90 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.False(t, res.LowerBound)
		assert.Zero(t, res.HeadMinTime)
	})

	t.Run("a window starting before what the ingesters keep is a lower bound", func(t *testing.T) {
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Responses: []*client.SeriesCountsResponse{
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{1}}}, MinTimeMs: 95 * hour},
		}}}
		e := newEstimator(nil, ing)
		// The ingesters are asked only for the last 10h, less than
		// query-store-after, so this head-only window starts before them.
		e.limits = ingestersWithinLimit(10 * time.Hour)
		res, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 89 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.Equal(t, SourceHead, res.Source)
		assert.True(t, res.LowerBound)
		assert.Equal(t, 90*hour, res.HeadMinTime)
	})

	t.Run("the ingesters cover the window from the latest start they report", func(t *testing.T) {
		// Both start before what the ingesters are asked for (90h), so the
		// cover reaches back to the later of the two, not the earlier.
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Responses: []*client.SeriesCountsResponse{
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{1}}}, MinTimeMs: 85 * hour},
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{1}}}, MinTimeMs: 88*hour + hour/2},
			{}, // holds nothing for the tenant
		}}}
		e := newEstimator(nil, ing)
		e.limits = ingestersWithinLimit(10 * time.Hour)
		// The window starts at query-store-after, so the head alone answers.
		res, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 88 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.Equal(t, SourceHead, res.Source)
		assert.True(t, res.LowerBound)
		assert.Equal(t, 88*hour+hour/2, res.HeadMinTime)
		res, err = e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 89 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.False(t, res.LowerBound)
	})

	t.Run("a head walk over max_index_bytes is refused", func(t *testing.T) {
		ing := &ingestersMock{seriesCountsErr: cardinality.Refusalf(cardinality.LimitMaxIndexBytes, "the request would walk 9 head series, about 4608 bytes at 512 bytes each, more than max_index_bytes 1000")}
		_, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour, MaxIndexBytes: 1000})
		require.ErrorContains(t, err, "more than max_index_bytes 1000")
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
		assert.Equal(t, refusedByMaxIndexBytes, estimateRefusalLimit(err))
	})

	t.Run("the ingesters are sent the max_groups cap and their refusal on it is 422", func(t *testing.T) {
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Responses: []*client.SeriesCountsResponse{
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{1}}}, MinTimeMs: 99 * hour},
		}}}
		e := newEstimator(nil, ing)
		e.limits = maxGroupsLimit{ingestersWithinLimit: ingestersWithinLimit(13 * time.Hour), groups: 5}
		_, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		require.NotNil(t, ing.gotSeriesCounts)
		assert.Equal(t, int64(5), ing.gotSeriesCounts.MaxGroups, "head-only, from the tenant's limit")
		hashed := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{2, 3}}}, MinTimeMs: 76 * hour},
		}}}
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(day, &bucketindex.Metadata{}, nil)
		gw := gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2}}}})
		q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
		}}}}, queryStoreAfter: 12 * time.Hour}
		e = NewCardinalityEstimator(q, hashed, maxGroupsLimit{ingestersWithinLimit: ingestersWithinLimit(13 * time.Hour), groups: 5})
		e.now = func() time.Time { return now }
		_, err = e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 52 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.Equal(t, int64(5), hashed.gotSeriesCounts.MaxGroups, "blocks and head")

		refusing := &ingestersMock{seriesCountsErr: cardinality.Refusalf(cardinality.LimitMaxGroups, "the head and local blocks hold at least 6 distinct values of __name__, more than max_groups 5")}
		e = newEstimator(nil, refusing)
		_, err = e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour, MaxGroups: 5})
		require.Error(t, err)
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
		assert.Equal(t, refusedByMaxGroups, estimateRefusalLimit(err))
	})

	t.Run("an invalid request on the ingesters is 400", func(t *testing.T) {
		ing := &ingestersMock{seriesCountsErr: status.Error(codes.InvalidArgument, "the window end must be after its start")}
		_, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour})
		assert.Equal(t, http.StatusBadRequest, estimateHTTPStatus(err))
	})

	t.Run("a window held partly by blocks and partly by the ingesters unions both sources' hashes", func(t *testing.T) {
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(day, &bucketindex.Metadata{}, nil)
		var gotBlocks *storegatewaypb.SeriesCountsRequest
		gw := gatewayAnswering("1.1.1.1", &gotBlocks, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2}}}})
		q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
		}}}}, queryStoreAfter: 12 * time.Hour}
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{2, 3}}, {Value: "new", Hashes: []uint64{9}}}, MinTimeMs: 76 * hour},
		}}}
		e := NewCardinalityEstimator(q, ing, ingestersWithinLimit(13*time.Hour))
		e.now = func() time.Time { return now }
		res, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 52 * hour, MaxT: 100 * hour, MaxIndexBytes: 1000})
		require.NoError(t, err)
		assert.Equal(t, SourceBlocksAndHead, res.Source)
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
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{9}}}, MinTimeMs: 90 * hour},
		}}}
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(day, &bucketindex.Metadata{}, nil)
		gw := gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1}}}})
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

	t.Run("with query-ingesters-within 0 the ingesters cover only what they report", func(t *testing.T) {
		// The ingesters are always asked, but nothing says how far back
		// they keep data, so a window they and the blocks only partly hold
		// is a lower bound with a coverage gap.
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{9}}}, MinTimeMs: 87 * hour},
		}}}
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(day, &bucketindex.Metadata{}, nil)
		gw := gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1}}}})
		q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
			map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
		}}}}, queryStoreAfter: 12 * time.Hour}
		e := NewCardinalityEstimator(q, ing, ingestersWithinLimit(0))
		e.now = func() time.Time { return now }
		res, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 28 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.Equal(t, SourceBlocksAndHead, res.Source)
		assert.True(t, res.LowerBound, "nothing holds [28h, 52h) or [76h, 87h)")
		assert.True(t, res.CoverageGap)
		assert.Equal(t, 87*hour, res.HeadMinTime)

		// A head-only window starting before what they report is a lower
		// bound too.
		ing.seriesCounts = distributor.SeriesCountsResult{Responses: []*client.SeriesCountsResponse{
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{1}}}, MinTimeMs: 95 * hour},
		}}
		res, err = e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 89 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.Equal(t, SourceHead, res.Source)
		assert.True(t, res.LowerBound)
		assert.Equal(t, 95*hour, res.HeadMinTime)

		// One starting at what they report is complete.
		res, err = e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 95 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.False(t, res.LowerBound)
		assert.Zero(t, res.HeadMinTime)
	})

	t.Run("a window older than the ingesters with no block is empty and flagged", func(t *testing.T) {
		res, err := newEstimator(nil, &ingestersMock{}).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 0, MaxT: 24 * hour})
		require.NoError(t, err)
		assert.Empty(t, res.Counts)
		assert.Equal(t, SourceBlocks, res.Source)
		assert.True(t, res.LowerBound, "no block may only mean the bucket index does not list one yet")
		assert.True(t, res.CoverageGap)

	})

	t.Run("ingesters that don't know the count RPC are refused", func(t *testing.T) {
		ing := &ingestersMock{seriesCountsErr: status.Error(codes.Unimplemented, "unknown method SeriesCounts")}
		_, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99*hour + hour/2, MaxT: 100 * hour})
		require.Error(t, err)
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
		assert.ErrorIs(t, err, errCountsNotSupported)
		assert.ErrorContains(t, err, "ingesters does not support cardinality estimates yet")
	})

	t.Run("head counts added up across partitions are an upper value", func(t *testing.T) {
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{UpperBound: true, Responses: []*client.SeriesCountsResponse{
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}, SeriesCounted: 2, MinTimeMs: 98 * hour},
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{1}}}, SeriesCounted: 1, MinTimeMs: 98 * hour},
		}}}
		res, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 98 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int64{"up": {3}}, res.Counts)
		assert.True(t, res.UpperBound)
		assert.False(t, res.LowerBound)
		assert.False(t, res.Dedup)
	})

	t.Run("an ingester answer without one count per group fails the estimate", func(t *testing.T) {
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Responses: []*client.SeriesCountsResponse{
			{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{3, 1}}}, SeriesCounted: 3, MinTimeMs: 98 * hour},
		}}}
		_, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 98 * hour, MaxT: 100 * hour, GroupBy: "__name__"})
		require.ErrorContains(t, err, "ingester returned 2 counts for \"up\" instead of one")
		assert.Equal(t, http.StatusInternalServerError, estimateHTTPStatus(err))
	})

	t.Run("an ingester over its max_hashes share is refused", func(t *testing.T) {
		ing := &ingestersMock{seriesCountsErr: cardinality.MaxHashesRefusal(42, 10)}
		e := newEstimator(nil, ing)
		e.limits = estimateMemoryLimits{ingestersWithinLimit: ingestersWithinLimit(13 * time.Hour), estimate: 10 * cardinality.HashBudgetBytes}
		res, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99*hour + hour/2, MaxT: 100 * hour})
		require.Error(t, err)
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
		assert.Equal(t, refusedByMaxHashes, estimateRefusalLimit(err))
		require.NotNil(t, ing.gotSeriesCounts)
		assert.Equal(t, int64(10), ing.gotSeriesCounts.MaxHashes, "the whole limit, which the distributor splits across the ingesters")
		n, ok := estimateHashesNeeded(res, err)
		require.True(t, ok)
		assert.Equal(t, int64(42), n, "the log line says what the walk would have hashed")
	})

	t.Run("an ingester failure leaves the transport error's address out of the error text", func(t *testing.T) {
		ing := &ingestersMock{seriesCountsErr: status.Error(codes.Unavailable, "connection error: dial tcp 10.9.9.9:9095: connect: connection refused")}
		_, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour})
		require.ErrorContains(t, err, "ingesters: the call failed with Unavailable")
		assert.NotContains(t, err.Error(), "10.9.9.9")
		assert.Contains(t, estimateFailureDetail(err), "10.9.9.9", "the log line keeps it")
		assert.Equal(t, http.StatusInternalServerError, estimateHTTPStatus(err))
	})

	t.Run("without ingesters a partly covered window is a lower bound from the blocks", func(t *testing.T) {
		res, err := newEstimator(day, nil).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 52 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.Equal(t, SourceBlocks, res.Source)
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

// A window held partly by blocks and partly by the ingesters asks both at once, and the first side to fail ends the other.
func TestCardinalityEstimator_BlocksAndHeadAskedConcurrently(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	b1 := ulid.MustNew(1, nil)
	up := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")}
	estimate := func(gateway func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error), ingesters func(context.Context, *client.SeriesCountsRequest) (distributor.SeriesCountsResult, error)) (CardinalityEstimateResult, error) {
		day := bucketindex.Blocks{{ID: b1, MinTime: 72 * hour, MaxTime: 96 * hour}}
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(day, &bucketindex.Metadata{}, nil)
		gw := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: gateway}
		q := &BlocksStoreQueryable{
			compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
			}}}},
			queryStoreAfter: 12 * time.Hour,
		}
		e := NewCardinalityEstimator(q, &ingestersMock{seriesCountsCtxFunc: ingesters}, ingestersWithinLimit(13*time.Hour))
		e.now = func() time.Time { return time.UnixMilli(105 * hour) }
		return e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{Matchers: up, MinT: 72 * hour, MaxT: 96 * hour})
	}
	errNeverAsked := errors.New("the other side was not asked while this one was waiting")

	t.Run("each side is asked while the other has not answered", func(t *testing.T) {
		gatewayAsked, ingestersAsked := make(chan struct{}), make(chan struct{})
		res, err := estimate(func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			close(gatewayAsked)
			select {
			case <-ingestersAsked:
			case <-time.After(5 * time.Second):
				return nil, errNeverAsked
			}
			return &storegatewaypb.SeriesCountsResponse{BlockIds: req.BlockIds, Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2, 3}}}}, nil
		}, func(context.Context, *client.SeriesCountsRequest) (distributor.SeriesCountsResult, error) {
			close(ingestersAsked)
			select {
			case <-gatewayAsked:
			case <-time.After(5 * time.Second):
				return distributor.SeriesCountsResult{}, errNeverAsked
			}
			return distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
				{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{3, 4}}}, MinTimeMs: 72 * hour},
			}}, nil
		})
		require.NoError(t, err)
		assert.Equal(t, SourceBlocksAndHead, res.Source)
		assert.Equal(t, map[string][]int64{"up": {4}}, res.Counts, "a series in both sides counts once")
	})

	t.Run("a store-gateway's refusal ends the ingesters' call and is the error returned", func(t *testing.T) {
		_, err := estimate(func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return nil, cardinality.Refusalf(cardinality.LimitMaxIndexBytes, "more than max_index_bytes 10")
		}, func(ctx context.Context, _ *client.SeriesCountsRequest) (distributor.SeriesCountsResult, error) {
			select {
			case <-ctx.Done():
				return distributor.SeriesCountsResult{}, ctx.Err()
			case <-time.After(5 * time.Second):
				return distributor.SeriesCountsResult{}, errNeverAsked
			}
		})
		require.Error(t, err)
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err), err)
	})

	t.Run("the ingesters' refusal ends the store-gateways' calls and is the error returned", func(t *testing.T) {
		ingestersAnswered := make(chan struct{})
		_, err := estimate(func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			select {
			case <-ingestersAnswered:
			case <-time.After(5 * time.Second):
				return nil, errNeverAsked
			}
			return &storegatewaypb.SeriesCountsResponse{BlockIds: req.BlockIds}, nil
		}, func(context.Context, *client.SeriesCountsRequest) (distributor.SeriesCountsResult, error) {
			defer close(ingestersAnswered)
			return distributor.SeriesCountsResult{}, cardinality.Refusalf(cardinality.LimitMaxHashes, "more than max_hashes 10")
		})
		require.Error(t, err)
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err), err)
		assert.Equal(t, refusedByMaxHashes, estimateRefusalLimit(err))
	})
}

// The ingesters' answers stay charged to the memory tracker only until the estimate merged them or failed.
func TestCardinalityEstimator_ReleasesIngesterAnswers(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	b1 := ulid.MustNew(1, nil)
	answersHashes := func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
		return &storegatewaypb.SeriesCountsResponse{BlockIds: req.BlockIds, Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2, 3}}}}, nil
	}
	// estimate answers a window that starts at minT and ends in the ingesters' range, with the ingesters'
	// answer charged answerBytes. It also returns what the tracker holds once the estimate returned.
	estimate := func(t *testing.T, minT int64, answerBytes uint64, gateway func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error), ingestersAnswered chan struct{}) (CardinalityEstimateResult, uint64, error) {
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(bucketindex.Blocks{{ID: b1, MinTime: 72 * hour, MaxTime: 96 * hour}}, &bucketindex.Metadata{}, nil)
		gw := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: gateway}
		q := &BlocksStoreQueryable{
			compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
			}}}},
			queryStoreAfter: 12 * time.Hour,
		}
		tracker := limiter.NewUnlimitedMemoryConsumptionTracker(context.Background())
		ctx := limiter.AddMemoryTrackerToContext(context.Background(), tracker)
		ing := &ingestersMock{seriesCountsFunc: func(req *client.SeriesCountsRequest) (distributor.SeriesCountsResult, error) {
			if ingestersAnswered != nil {
				defer close(ingestersAnswered)
			}
			require.NoError(t, tracker.IncreaseMemoryConsumption(answerBytes, limiter.CardinalityEstimateGroups))
			release := sync.OnceFunc(func() { tracker.DecreaseMemoryConsumption(answerBytes, limiter.CardinalityEstimateGroups) })
			group := &cardinalitypb.SeriesCountGroup{Value: "up", Counts: []int64{2}}
			if req.Hashes {
				group = &cardinalitypb.SeriesCountGroup{Value: "up", Hashes: []uint64{3, 4}}
			}
			return distributor.NewSeriesCountsResult([]*client.SeriesCountsResponse{{Groups: []*cardinalitypb.SeriesCountGroup{group}, MinTimeMs: 72 * hour}}, req.Hashes, release), nil
		}}
		e := NewCardinalityEstimator(q, ing, ingestersWithinLimit(13*time.Hour))
		e.now = func() time.Time { return time.UnixMilli(105 * hour) }
		res, err := e.Estimate(ctx, "user-1", &CardinalityEstimateRequest{MinT: minT, MaxT: 105 * hour})
		return res, tracker.CurrentEstimatedMemoryConsumptionBytes(), err
	}

	t.Run("the store-gateways fail after the ingesters answered", func(t *testing.T) {
		ingestersAnswered := make(chan struct{})
		_, held, err := estimate(t, 72*hour, 1000, func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			<-ingestersAnswered
			return nil, cardinality.Refusalf(cardinality.LimitMaxGroups, "more than max_groups 1")
		}, ingestersAnswered)
		require.Error(t, err)
		assert.Zero(t, held, "the ingesters' answers are given back")
	})

	// Once merged, the tracker holds what it holds for answers that were never charged: the merged groups.
	for name, tc := range map[string]struct {
		minT       int64
		wantSource string
	}{
		"merged with the store-gateways' answers": {minT: 72 * hour, wantSource: SourceBlocksAndHead},
		"merged alone": {minT: 94 * hour, wantSource: SourceHead},
	} {
		t.Run(name, func(t *testing.T) {
			_, baseline, err := estimate(t, tc.minT, 0, answersHashes, nil)
			require.NoError(t, err)
			require.Positive(t, baseline)
			res, held, err := estimate(t, tc.minT, 1000, answersHashes, nil)
			require.NoError(t, err)
			require.Equal(t, tc.wantSource, res.Source)
			assert.Equal(t, baseline, held, "the ingesters' answers are given back")
		})
	}
}

func TestCardinalityEstimator_MemoryLimit(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	now := time.UnixMilli(100 * hour)
	hashes := make([]uint64, 100)
	for i := range hashes {
		hashes[i] = uint64(i + 1)
	}
	ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
		{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: hashes}}},
	}}}
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(bucketindex.Blocks(nil), &bucketindex.Metadata{}, nil)
	q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder}}, queryStoreAfter: 12 * time.Hour}

	for name, tc := range map[string]struct {
		limit   uint64
		refused bool
	}{
		"under the limit": {limit: 1 << 20},
		"over the limit":  {limit: 1000, refused: true},
		"no limit":        {limit: 0},
	} {
		t.Run(name, func(t *testing.T) {
			e := NewCardinalityEstimator(q, ing, memoryLimit{ingestersWithinLimit(13 * time.Hour), tc.limit})
			e.now = func() time.Time { return now }
			res, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour})
			if tc.refused {
				require.ErrorContains(t, err, "the groups and hashes this estimate holds in the querier")
				assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, map[string][]int64{"up": {100}}, res.Counts)
		})
	}
}

func TestCardinalityEstimator_TenantIndexBytesLimit(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	now := time.UnixMilli(100 * hour)
	b1 := ulid.MustNew(1, nil)
	day := bucketindex.Blocks{{ID: b1, MinTime: 52 * hour, MaxTime: 76 * hour}}

	for name, tc := range map[string]struct {
		asked, limit, want int64
	}{
		"a request without max_index_bytes gets the tenant's limit": {asked: 0, limit: 1000, want: 1000},
		"a larger request is cut to the tenant's limit":             {asked: 5000, limit: 1000, want: 1000},
		"a smaller request is kept":                                 {asked: 600, limit: 1000, want: 600},
		"without a tenant limit the request is unbounded":           {asked: 0, limit: 0, want: 0},
	} {
		t.Run(name, func(t *testing.T) {
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(day, &bucketindex.Metadata{}, nil)
			var got *storegatewaypb.SeriesCountsRequest
			gw := gatewayAnswering("1.1.1.1", &got, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1}}}})
			q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
			}}}}, queryStoreAfter: 12 * time.Hour}
			ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
				{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2}}}, MinTimeMs: 87 * hour},
			}}}
			e := NewCardinalityEstimator(q, ing, indexBytesLimit{ingestersWithinLimit(13 * time.Hour), tc.limit})
			e.now = func() time.Time { return now }

			// The window reaches both sources, which share the bound evenly.
			req := &CardinalityEstimateRequest{MinT: 52 * hour, MaxT: 100 * hour, MaxIndexBytes: tc.asked}
			_, err := e.Estimate(context.Background(), "user-1", req)
			require.NoError(t, err)
			assert.Equal(t, tc.want, req.MaxIndexBytes)
			require.NotNil(t, got)
			require.NotNil(t, ing.gotSeriesCounts)
			assert.Equal(t, tc.want/2, got.MaxIndexBytes, "the store-gateway's share")
			assert.Equal(t, tc.want/2, ing.gotSeriesCounts.MaxIndexBytes, "the ingesters' share")
		})
	}
}

// estimateMemoryLimits is ingestersWithinLimit with both memory limits.
type estimateMemoryLimits struct {
	ingestersWithinLimit
	estimate, query uint64
}

func (l estimateMemoryLimits) CardinalityEstimateMaxMemoryBytes(string) uint64 { return l.estimate }

func (l estimateMemoryLimits) MaxEstimatedMemoryConsumptionPerQuery(string) uint64 { return l.query }

func TestCardinalityEstimator_EstimateMemoryLimit(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	now := time.UnixMilli(100 * hour)
	// 100 hashes held cost 2,400 bytes plus the group.
	hashes := make([]uint64, 100)
	for i := range hashes {
		hashes[i] = uint64(i + 1)
	}
	ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
		{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: hashes}}},
	}}}

	for name, tc := range map[string]struct {
		estimate, query uint64
		refused         bool
	}{
		"the estimate's limit applies with no query limit":        {estimate: 1000, refused: true},
		"the estimate's limit applies when the query's is higher": {estimate: 1000, query: 1 << 20, refused: true},
		"a lower query limit applies":                             {estimate: 1 << 20, query: 1000, refused: true},
		"under both":                                              {estimate: 1 << 20, query: 1 << 20},
		"neither":                                                 {},
	} {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(bucketindex.Blocks(nil), &bucketindex.Metadata{}, nil)
			q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder}}, queryStoreAfter: 12 * time.Hour, metrics: newBlocksStoreQueryableMetrics(compartments.Config{}, reg)}
			e := NewCardinalityEstimator(q, ing, estimateMemoryLimits{ingestersWithinLimit(13 * time.Hour), tc.estimate, tc.query})
			e.now = func() time.Time { return now }
			_, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour})
			want := 0
			if tc.refused {
				require.ErrorContains(t, err, "the groups and hashes this estimate holds in the querier")
				assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
				want = 1
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, float64(want), testutil.ToFloat64(q.metrics.estimateRefusals.WithLabelValues(refusedByMemory)))
		})
	}

	t.Run("the default limits bound an estimate", func(t *testing.T) {
		var defaults validation.Limits
		flagext.DefaultValues(&defaults)
		assert.Equal(t, uint64(256<<20), defaults.CardinalityEstimateMaxMemoryBytes, "about 8M hashes at the 32 bytes each is budgeted")
		assert.Zero(t, defaults.MaxEstimatedMemoryConsumptionPerQuery)
		assert.Equal(t, uint64(256<<20), estimateMemoryLimit(defaults.CardinalityEstimateMaxMemoryBytes, defaults.MaxEstimatedMemoryConsumptionPerQuery))
	})
}

// The hashes a deduplicating estimate may hold, and so the shares its
// sources get, follow the tenant's memory limit for an estimate, or the
// per-query limit when that is lower; with neither there is no limit.
func TestEstimateHashBudget(t *testing.T) {
	// A hash is budgeted 32 bytes: the 24 it holds merged and the 8 it holds in its source's answer until then.
	assert.Equal(t, int64(256<<20/32), estimateHashBudget(256<<20, 0))
	assert.Equal(t, int64(1<<30/32), estimateHashBudget(1<<30, 0), "a raised tenant limit")
	assert.Equal(t, int64(64<<20/32), estimateHashBudget(256<<20, 64<<20), "the per-query limit when lower")
	assert.Equal(t, int64(256<<20/32), estimateHashBudget(0, 256<<20), "the per-query limit alone")
	assert.Zero(t, estimateHashBudget(0, 0), "no limit")

	const hour = int64(time.Hour / time.Millisecond)
	for name, tc := range map[string]struct {
		estimate, query uint64
		want            int64
	}{
		"raised":   {estimate: 3200, want: 100},
		"lowered":  {estimate: 3200, query: 640, want: 20},
		"no limit": {want: 0},
	} {
		t.Run(name, func(t *testing.T) {
			ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{}}
			e := NewCardinalityEstimator(&BlocksStoreQueryable{queryStoreAfter: 12 * time.Hour}, ing, estimateMemoryLimits{ingestersWithinLimit(13 * time.Hour), tc.estimate, tc.query})
			e.now = func() time.Time { return time.UnixMilli(100 * hour) }
			_, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour})
			require.NoError(t, err)
			require.NotNil(t, ing.gotSeriesCounts)
			assert.Equal(t, tc.want, ing.gotSeriesCounts.MaxHashes)
		})
	}
}

// Blocks plus head share one budget, as they share max_index_bytes.
func TestCardinalityEstimator_HashBudgetSplits(t *testing.T) {
	assert.Equal(t, int64(1), estimateHashBudget(10, 0), "a limit under one hash still allows one")
	const hour = int64(time.Hour / time.Millisecond)
	day := bucketindex.Blocks{{ID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: 96 * hour}}
	var gotGateway *storegatewaypb.SeriesCountsRequest
	gw := gatewayAnswering("1.1.1.1", &gotGateway, storegatewaypb.SeriesCountsResponse{})
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(day, &bucketindex.Metadata{}, nil)
	q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][][]ulid.ULID{gw: {{day[0].ID}}},
	}}}}, queryStoreAfter: 12 * time.Hour}
	ing := &ingestersMock{}
	e := NewCardinalityEstimator(q, ing, estimateMemoryLimits{ingestersWithinLimit(13 * time.Hour), 100 * cardinality.HashBudgetBytes, 0})
	e.now = func() time.Time { return time.UnixMilli(100 * hour) }
	_, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 80 * hour, MaxT: 100 * hour})
	require.NoError(t, err)
	require.NotNil(t, gotGateway)
	require.NotNil(t, ing.gotSeriesCounts)
	assert.Equal(t, int64(50), gotGateway.MaxHashes, "the blocks' half")
	assert.Equal(t, int64(50), ing.gotSeriesCounts.MaxHashes, "the ingesters' half")
}

// Ingester answers over the memory limit, as the distributor charges them, are a refusal by memory.
func TestClassifyIngesterError_OverMemoryLimit(t *testing.T) {
	err := classifyIngesterError(fmt.Errorf("%w: over", distributor.ErrSeriesCountsOverMemoryLimit))
	assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
	assert.Equal(t, refusedByMemory, estimateRefusalLimit(err))
}
