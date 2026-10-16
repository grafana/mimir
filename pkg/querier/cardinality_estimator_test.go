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

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

type ingestersMock struct {
	gotLabelNames []model.LabelName
	gotMatchers   []*labels.Matcher
	gotMethod     cardinality.CountMethod
	resp          *client.LabelValuesCardinalityResponse
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
		}}}}}
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

	t.Run("a recent window with no block is counted from the heads", func(t *testing.T) {
		ing := &ingestersMock{resp: headResp}
		job := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "a")}
		res, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{Matchers: job, MinT: 99 * hour, MaxT: 100 * hour})
		require.NoError(t, err)
		assert.Equal(t, sourceHead, res.Source)
		assert.Equal(t, readHeadPostings, res.Read)
		assert.True(t, res.HeadWhole)
		assert.Equal(t, map[string][]int64{"up": {4}, "errors_total": {2}}, res.Counts)
		assert.Equal(t, int64(6), res.SeriesCounted)
		assert.Equal(t, []model.LabelName{"__name__"}, ing.gotLabelNames)
		assert.Equal(t, job, ing.gotMatchers)
		assert.Equal(t, cardinality.InMemoryMethod, ing.gotMethod)
	})

	t.Run("group_by is sent as the label name", func(t *testing.T) {
		ing := &ingestersMock{resp: &client.LabelValuesCardinalityResponse{Items: []*client.LabelValueSeriesCount{{LabelName: "pod", LabelValueSeries: map[string]uint64{"p1": 1}}}}}
		res, err := newEstimator(nil, ing).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour, GroupBy: "pod"})
		require.NoError(t, err)
		assert.Equal(t, []model.LabelName{"pod"}, ing.gotLabelNames)
		assert.Equal(t, map[string][]int64{"p1": {1}}, res.Counts)
	})

	t.Run("per-bucket counts on the heads are refused", func(t *testing.T) {
		_, err := newEstimator(nil, &ingestersMock{resp: headResp}).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 99 * hour, MaxT: 100 * hour, Step: hour})
		require.ErrorContains(t, err, "per-bucket counts on the ingesters")
		assert.Equal(t, http.StatusBadRequest, estimateHTTPStatus(err))
	})

	t.Run("a window held partly by blocks and partly by the ingesters is refused", func(t *testing.T) {
		_, err := newEstimator(day, &ingestersMock{resp: headResp}).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 52 * hour, MaxT: 100 * hour})
		require.ErrorContains(t, err, "needs deduplication")
		assert.Equal(t, http.StatusBadRequest, estimateHTTPStatus(err))
	})

	t.Run("a window older than the ingesters with no block is empty", func(t *testing.T) {
		res, err := newEstimator(nil, &ingestersMock{resp: headResp}).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 0, MaxT: 24 * hour})
		require.NoError(t, err)
		assert.Empty(t, res.Counts)
		assert.Equal(t, sourceBlocks, res.Source)
	})

	t.Run("without ingesters a partly covered window is refused", func(t *testing.T) {
		_, err := newEstimator(day, nil).Estimate(context.Background(), "user-1", &CardinalityEstimateRequest{MinT: 52 * hour, MaxT: 100 * hour})
		require.ErrorContains(t, err, "cover only part of the window")
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
