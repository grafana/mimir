// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/util/validation"
)

type ingestersMock struct {
	gotLabelNames []model.LabelName
	gotMatchers   []*labels.Matcher
	gotMethod     cardinality.CountMethod
	resp          *client.LabelValuesCardinalityResponse
	respErr       error

	gotSeriesCounts *client.SeriesCountsRequest
	seriesCounts    distributor.SeriesCountsResult
	seriesCountsErr error
	// seriesCountsFunc, when set, answers SeriesCounts instead.
	seriesCountsFunc func(*client.SeriesCountsRequest) (distributor.SeriesCountsResult, error)
}

func (m *ingestersMock) SeriesCounts(_ context.Context, _ []*labels.Matcher, req *client.SeriesCountsRequest) (distributor.SeriesCountsResult, error) {
	m.gotSeriesCounts = req
	if m.seriesCountsFunc != nil {
		return m.seriesCountsFunc(req)
	}
	return m.seriesCounts, m.seriesCountsErr
}

func (m *ingestersMock) LabelValuesCardinality(_ context.Context, labelNames []model.LabelName, matchers []*labels.Matcher, countMethod cardinality.CountMethod) (uint64, *client.LabelValuesCardinalityResponse, error) {
	m.gotLabelNames, m.gotMatchers, m.gotMethod = labelNames, matchers, countMethod
	return 0, m.resp, m.respErr
}

type ingestersWithinLimit time.Duration

func (l ingestersWithinLimit) QueryIngestersWithin(string) time.Duration { return time.Duration(l) }

func (ingestersWithinLimit) CardinalityEstimateEnabled(string) bool { return true }

func (ingestersWithinLimit) CardinalityEstimateMaxIndexBytes(string) int64 { return 0 }

func (ingestersWithinLimit) CardinalityEstimateMaxMemoryBytes(string) uint64 { return 0 }

func (ingestersWithinLimit) CardinalityEstimateMaxBucketsAnsweredSeparately(string) int { return 48 }

// maxBucketsLimit is ingestersWithinLimit with a limit on the buckets
// answered one by one.
type maxBucketsLimit struct {
	ingestersWithinLimit
	buckets int
}

func (l maxBucketsLimit) CardinalityEstimateMaxBucketsAnsweredSeparately(string) int {
	return l.buckets
}

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

	t.Run("a window older than the head always holds is refused, not answered from the head alone", func(t *testing.T) {
		ing := &ingestersMock{resp: headResp, seriesCountsErr: status.Error(codes.ResourceExhausted, "more than max_index_bytes 1000")}
		_, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 97 * hour, MaxT: 100 * hour, MaxIndexBytes: 1000})
		require.Error(t, err, "the last 3h reach the ingesters' local blocks, which the head postings don't read")
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
		assert.Nil(t, ing.gotLabelNames)
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

	t.Run("a window older than the ingesters with no block is empty and flagged", func(t *testing.T) {
		res, err := newEstimator(nil, &ingestersMock{resp: headResp}).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 0, MaxT: 24 * hour})
		require.NoError(t, err)
		assert.Empty(t, res.Counts)
		assert.Equal(t, sourceBlocks, res.Source)
		assert.True(t, res.LowerBound, "no block may only mean the bucket index does not list one yet")
		assert.True(t, res.CoverageGap)

		// The handler must not let the query-frontend keep the empty table.
		rec := httptest.NewRecorder()
		setEstimateCacheControl(rec, &estimateResponse{LowerBound: res.LowerBound, CoverageGap: res.CoverageGap})
		assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	})

	t.Run("ingesters that don't know the count RPC are refused, not counted from the whole head", func(t *testing.T) {
		ing := &ingestersMock{resp: headResp, seriesCountsErr: status.Error(codes.Unimplemented, "unknown method SeriesCounts")}
		// A window inside what every head holds, where a request over the index
		// bytes limit would fall back to the whole-head count.
		_, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99*hour + hour/2, MaxT: 100 * hour})
		require.Error(t, err)
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
		assert.ErrorIs(t, err, errCountsNotSupported)
		assert.ErrorContains(t, err, "ingesters does not support cardinality estimates yet")
		assert.Nil(t, ing.gotLabelNames, "the whole-head count is not asked")
	})

	t.Run("an ingester that ignored step_ms is refused", func(t *testing.T) {
		// Two buckets of one hour. An ingester that doesn't know step_ms
		// answers one count per group.
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Responses: []*client.SeriesCountsResponse{
			{Groups: []*client.SeriesCountGroup{{Value: "up", Counts: []int64{3}}}, SeriesCounted: 3, MinTimeMs: 98 * hour},
		}}}
		_, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 98 * hour, MaxT: 100 * hour, Step: hour, GroupBy: "__name__"})
		require.ErrorContains(t, err, "ingester returned 1 buckets for \"up\" instead of the 2 asked for with step_ms")
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
		assert.ErrorIs(t, err, errCountsNotSupported)
	})

	t.Run("a step that doesn't cut a head window sent with snap is refused before the ingesters are asked", func(t *testing.T) {
		// The handler leaves the buckets of a request with snap to be checked
		// once the window is widened, which a head window never is.
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Responses: []*client.SeriesCountsResponse{
			{Groups: []*client.SeriesCountGroup{{Value: "up", Counts: []int64{3}}}, SeriesCounted: 3, MinTimeMs: 98 * hour},
		}}}
		_, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 98 * hour, MaxT: 100 * hour, Step: 7 * time.Minute.Milliseconds(), Snap: true, GroupBy: "__name__"})
		require.ErrorContains(t, err, "step: ")
		assert.Equal(t, http.StatusBadRequest, estimateHTTPStatus(err))
		assert.Nil(t, ing.gotSeriesCounts)
	})

	t.Run("an ingester over its max_hashes share is answered from the whole head", func(t *testing.T) {
		ing := &ingestersMock{resp: headResp, seriesCountsErr: status.Error(codes.ResourceExhausted, cardinality.MaxHashesRefusal(42, 10))}
		e := newEstimator(nil, ing)
		e.blocks.estimateCfg.MaxDedupSeries = 10
		res, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99*hour + hour/2, MaxT: 100 * hour})
		require.NoError(t, err)
		require.NotNil(t, ing.gotSeriesCounts)
		assert.Equal(t, int64(10), ing.gotSeriesCounts.MaxHashes, "the distributor splits it across the ingesters")
		assert.True(t, res.HeadWhole)
		assert.Contains(t, res.HeadFallback, "max_hashes 10")
		n, ok := estimateHashesNeeded(res, nil)
		require.True(t, ok)
		assert.Equal(t, int64(42), n, "the log line says what the walk would have hashed")
	})

	t.Run("a failed whole-head fallback answers with the refusal it stood in for", func(t *testing.T) {
		ing := &ingestersMock{
			seriesCountsErr: status.Error(codes.ResourceExhausted, "the request would walk 9000 series, more than max_index_bytes 1000"),
			respErr:         status.Error(codes.Unavailable, "dial tcp 10.9.9.9:9095: connect: connection refused"),
		}
		_, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99*hour + hour/2, MaxT: 100 * hour})
		require.ErrorContains(t, err, "more than max_index_bytes 1000; the whole-head count it falls back to failed too")
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err), "a refusal, not a failure")
		assert.NotContains(t, err.Error(), "10.9.9.9")
		assert.Contains(t, estimateFailureDetail(err), "10.9.9.9", "the log line keeps the fallback's failure")
		require.NotNil(t, ing.gotLabelNames, "the whole-head count was asked")
	})

	t.Run("an ingester failure leaves the transport error's address out of the error text", func(t *testing.T) {
		ing := &ingestersMock{resp: headResp, seriesCountsErr: status.Error(codes.Unavailable, "connection error: dial tcp 10.9.9.9:9095: connect: connection refused")}
		_, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour, Step: hour / 2})
		require.ErrorContains(t, err, "ingesters: the call failed with Unavailable")
		assert.NotContains(t, err.Error(), "10.9.9.9")
		assert.Contains(t, estimateFailureDetail(err), "10.9.9.9", "the log line keeps it")
		assert.Equal(t, http.StatusInternalServerError, estimateHTTPStatus(err))
	})

	t.Run("an older ingester found by the distributor is refused, not counted from the whole head", func(t *testing.T) {
		ing := &ingestersMock{resp: headResp, seriesCountsErr: fmt.Errorf("ingester 1.2.3.4 counted with window_ms 0 instead of the 7200000 asked for; %w", distributor.ErrSeriesCountsOlderIngester)}
		// A window inside what every head holds, where a limit refusal would
		// fall back to the whole-head count.
		_, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99*hour + hour/2, MaxT: 100 * hour})
		require.ErrorContains(t, err, "counted with window_ms 0 instead of the 7200000 asked for; it may run an older version, retry once every ingester is updated")
		assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
		assert.ErrorIs(t, err, errCountsNotSupported)
		assert.Nil(t, ing.gotLabelNames, "the whole-head count is not asked")
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

func TestCardinalityEstimator_TrustsCompactedBlocks(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	b1 := ulid.MustNew(1, nil)
	up := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")}

	// Yesterday is [72h, 96h); now is 09:00 the next day, inside both
	// query-store-after (12h) and query-ingesters-within (13h).
	estimate := func(level int, now time.Time, trust time.Duration) (CardinalityEstimateResult, *ingestersMock, error) {
		day := bucketindex.Blocks{{ID: b1, MinTime: 72 * hour, MaxTime: 96 * hour, CompactionLevel: level}}
		finder := &blocksFinderMock{}
		finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(day, &bucketindex.Metadata{}, nil)
		gw := gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{3}, Hashes: []uint64{1, 2, 3}}}})
		q := &BlocksStoreQueryable{
			compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
			}}}},
			queryStoreAfter: 12 * time.Hour,
			estimateCfg:     CardinalityEstimateConfig{TrustCompactedBlocksAfter: trust},
		}
		ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
			{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: []uint64{3, 4}}}, MinTimeMs: 92 * hour},
		}}}
		e := NewCardinalityEstimator(q, ing, ingestersWithinLimit(13*time.Hour))
		e.now = func() time.Time { return now }
		res, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{Matchers: up, MinT: 72 * hour, MaxT: 96 * hour})
		return res, ing, err
	}
	nineAM := time.UnixMilli(105 * hour)

	t.Run("a compacted day that ended long enough ago is counted from its blocks alone", func(t *testing.T) {
		res, ing, err := estimate(2, nineAM, time.Hour)
		require.NoError(t, err)
		assert.Equal(t, sourceBlocks, res.Source)
		assert.Equal(t, map[string][]int64{"up": {3}}, res.Counts)
		assert.Nil(t, ing.gotSeriesCounts, "the ingesters are not asked")
	})

	t.Run("level-1 blocks are not trusted", func(t *testing.T) {
		res, ing, err := estimate(1, nineAM, time.Hour)
		require.NoError(t, err)
		assert.Equal(t, sourceBoth, res.Source)
		assert.NotNil(t, ing.gotSeriesCounts)
	})

	t.Run("a window that ended too recently is not trusted", func(t *testing.T) {
		res, _, err := estimate(2, time.UnixMilli(96*hour+30*60*1000), time.Hour)
		require.NoError(t, err)
		assert.Equal(t, sourceBoth, res.Source)
	})

	t.Run("0 turns it off", func(t *testing.T) {
		res, _, err := estimate(2, nineAM, 0)
		require.NoError(t, err)
		assert.Equal(t, sourceBoth, res.Source)
	})
}

func TestCardinalityEstimator_MemoryLimit(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	now := time.UnixMilli(100 * hour)
	hashes := make([]uint64, 100)
	for i := range hashes {
		hashes[i] = uint64(i + 1)
	}
	ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
		{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: hashes}}},
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

func TestCardinalityEstimator_PerBucketAcrossSources(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	now := time.UnixMilli(100 * hour)
	b1 := ulid.MustNew(1, nil)
	day := bucketindex.Blocks{{ID: b1, MinTime: 52 * hour, MaxTime: 76 * hour}}

	finder := &blocksFinderMock{}
	// The first bucket is the day's block; the second has no block before
	// query-store-after (88h) and is held by the ingesters.
	finder.On("GetBlocks", mock.Anything, "user-1", 52*hour, 76*hour-1).Return(day, &bucketindex.Metadata{}, nil)
	finder.On("GetBlocks", mock.Anything, "user-1", 76*hour, 88*hour-1).Return(bucketindex.Blocks(nil), &bucketindex.Metadata{}, nil)
	var got *storegatewaypb.SeriesCountsRequest
	gw := gatewayAnswering("1.1.1.1", &got, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{3}}}})
	q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
	}}}}, queryStoreAfter: 12 * time.Hour}
	ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
		{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: []uint64{7, 8}}}, MinTimeMs: 76 * hour},
	}}}
	e := NewCardinalityEstimator(q, ing, ingestersWithinLimit(13*time.Hour))
	e.now = func() time.Time { return now }

	up := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")}
	res, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{Matchers: up, MinT: 52 * hour, MaxT: 100 * hour, Step: 24 * hour, MaxIndexBytes: 1000})
	require.NoError(t, err)
	assert.Equal(t, readPerBucket, res.Read)
	assert.Equal(t, "blocks,blocks+head", res.Source)
	assert.Equal(t, map[string][]int64{"up": {3, 2}}, res.Counts)
	require.NotNil(t, got)
	assert.False(t, got.Hashes, "the day's bucket is one source, a plain count")
	assert.Equal(t, int64(500), got.MaxIndexBytes, "the buckets share max_index_bytes")
	require.NotNil(t, ing.gotSeriesCounts)
	assert.Equal(t, 76*hour, ing.gotSeriesCounts.StartTimestampMs)

	_, err = e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{Matchers: up, MinT: 52 * hour, MaxT: 100 * hour, Step: hour / 2})
	require.ErrorContains(t, err, "more than 48 (the -querier.cardinality-estimate-max-buckets-answered-separately limit)")
	assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
}

// The buckets a request may answer one by one are a per-tenant limit: a
// raised limit answers a request the default refuses, and a lowered one
// refuses a request the default answers.
func TestCardinalityEstimator_PerBucketMaxBucketsAnsweredSeparately(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	const minute = int64(time.Minute / time.Millisecond)
	now := time.UnixMilli(100 * hour)
	q := &BlocksStoreQueryable{queryStoreAfter: 12 * time.Hour}
	var got []*client.SeriesCountsRequest
	ing := &ingestersMock{seriesCountsFunc: func(req *client.SeriesCountsRequest) (distributor.SeriesCountsResult, error) {
		got = append(got, req)
		if req.StepMs > 0 {
			return distributor.SeriesCountsResult{}, fmt.Errorf("partition 1: %w", distributor.ErrSeriesCountsNeedOneIngesterPerZone)
		}
		return distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
			{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2}}}, MinTimeMs: 90 * hour},
		}}, nil
	}}
	up := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")}
	// One hour at a one-minute step is 60 buckets.
	sixty := func() *CardinalityEstimateRequest {
		return &CardinalityEstimateRequest{Matchers: up, MinT: 96 * hour, MaxT: 97 * hour, Step: minute}
	}
	estimate := func(limits estimateLimits, req *CardinalityEstimateRequest) (CardinalityEstimateResult, error) {
		got = nil
		e := NewCardinalityEstimator(q, ing, limits)
		e.now = func() time.Time { return now }
		return e.Estimate(context.Background(), "user-1", req)
	}

	_, err := estimate(ingestersWithinLimit(13*time.Hour), sixty())
	require.ErrorContains(t, err, "its 60 buckets would be answered one by one, more than 48 (the -querier.cardinality-estimate-max-buckets-answered-separately limit); use a larger step")
	assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))

	res, err := estimate(maxBucketsLimit{ingestersWithinLimit(13 * time.Hour), 60}, sixty())
	require.NoError(t, err)
	assert.Equal(t, readPerBucket, res.Read)
	require.Len(t, res.Counts["up"], 60)
	assert.Len(t, got, 61, "the step request, then one request per bucket")

	two := &CardinalityEstimateRequest{Matchers: up, MinT: 96 * hour, MaxT: 98 * hour, Step: hour}
	_, err = estimate(ingestersWithinLimit(13*time.Hour), two)
	require.NoError(t, err)
	_, err = estimate(maxBucketsLimit{ingestersWithinLimit(13 * time.Hour), 1}, &CardinalityEstimateRequest{Matchers: up, MinT: 96 * hour, MaxT: 98 * hour, Step: hour})
	require.ErrorContains(t, err, "its 2 buckets would be answered one by one, more than 1 (the -querier.cardinality-estimate-max-buckets-answered-separately limit)")
	assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
	assert.Len(t, got, 1, "no bucket is asked once the limit refuses")

	_, err = estimate(maxBucketsLimit{ingestersWithinLimit(13 * time.Hour), 0}, sixty())
	require.NoError(t, err, "0 means no limit")

	assert.Equal(t, 48, (&CardinalityEstimator{}).maxPerBucketEstimates("user-1"), "without per-tenant limits the default applies")
	assert.Equal(t, 48, validation.MockDefaultLimits().CardinalityEstimateMaxBucketsAnsweredSeparately, "the limit's default")
}

// Under ingest storage with several partitions the distributor can't add
// up per-bucket counts, since a series can be in two partitions; the
// estimator answers each bucket as its own window, deduplicated by hash.
func TestCardinalityEstimator_PerBucketHeadOverPartitions(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	now := time.UnixMilli(100 * hour)
	q := &BlocksStoreQueryable{queryStoreAfter: 12 * time.Hour}
	// Series 2 moved from partition 0 to partition 1 in the first bucket,
	// so both partitions' heads hold it.
	byBucket := map[int64][]*client.SeriesCountsResponse{
		96 * hour: {
			{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2}}}, MinTimeMs: 90 * hour},
			{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: []uint64{2, 3}}}, MinTimeMs: 90 * hour},
		},
		97 * hour: {
			{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: []uint64{1}}}, MinTimeMs: 90 * hour},
			{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: []uint64{2, 3}}}, MinTimeMs: 90 * hour},
		},
	}
	var got []*client.SeriesCountsRequest
	ing := &ingestersMock{seriesCountsFunc: func(req *client.SeriesCountsRequest) (distributor.SeriesCountsResult, error) {
		got = append(got, req)
		if req.StepMs > 0 {
			return distributor.SeriesCountsResult{}, fmt.Errorf("partition 1: %w", distributor.ErrSeriesCountsNeedOneIngesterPerZone)
		}
		return distributor.SeriesCountsResult{Hashed: true, Responses: byBucket[req.StartTimestampMs]}, nil
	}}
	e := NewCardinalityEstimator(q, ing, ingestersWithinLimit(13*time.Hour))
	e.now = func() time.Time { return now }

	up := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")}
	res, err := e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{Matchers: up, MinT: 96 * hour, MaxT: 98 * hour, Step: hour})
	require.NoError(t, err)
	assert.Equal(t, readPerBucket, res.Read)
	assert.Equal(t, sourceHead, res.Source)
	assert.True(t, res.Dedup)
	assert.Equal(t, map[string][]int64{"up": {3, 3}}, res.Counts)
	require.Len(t, got, 3)
	assert.Equal(t, int64(0), got[1].StepMs)
	assert.Equal(t, int64(0), got[2].StepMs)

	// With buckets two hours wide, each bucket answered on its own is its own two-hour window.
	byBucket[95*hour] = byBucket[96*hour]
	byBucket[96*hour] = byBucket[97*hour]
	got = nil
	res, err = e.Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{Matchers: up, MinT: 95 * hour, MaxT: 98 * hour, Step: hour, Window: 2 * hour})
	require.NoError(t, err)
	assert.Equal(t, map[string][]int64{"up": {3, 3}}, res.Counts)
	require.Len(t, got, 3)
	assert.Equal(t, 2*hour, got[0].WindowMs, "the per-bucket request carries the window")
	for i, start := range []int64{95 * hour, 96 * hour} {
		assert.Equal(t, start, got[i+1].StartTimestampMs)
		assert.Equal(t, start+2*hour, got[i+1].EndTimestampMs)
		assert.Zero(t, got[i+1].StepMs)
		assert.Zero(t, got[i+1].WindowMs)
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
			gw := gatewayAnswering("1.1.1.1", &got, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1}}}})
			q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
			}}}}, queryStoreAfter: 12 * time.Hour}
			ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
				{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2}}}, MinTimeMs: 87 * hour},
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
		{Groups: []*client.SeriesCountGroup{{Value: "up", Hashes: hashes}}},
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
		assert.Equal(t, uint64(256<<20), defaults.CardinalityEstimateMaxMemoryBytes, "sized for the design's 10M held hashes at 24 bytes each")
		assert.Zero(t, defaults.MaxEstimatedMemoryConsumptionPerQuery)
		assert.Equal(t, uint64(256<<20), estimateMemoryLimit(defaults.CardinalityEstimateMaxMemoryBytes, defaults.MaxEstimatedMemoryConsumptionPerQuery))
	})
}
