// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/distributor_queryable_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/chunk"
	"github.com/grafana/mimir/pkg/chunk/encoding"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/prom1/storage/metric"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/chunkcompat"
)

func TestDistributorQuerier_SelectShouldHonorQueryIngestersWithin(t *testing.T) {
	now := time.Now()

	tests := map[string]struct {
		querySeries          bool
		queryIngestersWithin time.Duration
		queryMinT            int64
		queryMaxT            int64
		expectedMinT         int64
		expectedMaxT         int64
	}{
		"should not manipulate query time range if queryIngestersWithin is disabled": {
			queryIngestersWithin: 0,
			queryMinT:            util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:            util.TimeToMillis(now.Add(-30 * time.Minute)),
			expectedMinT:         util.TimeToMillis(now.Add(-100 * time.Minute)),
			expectedMaxT:         util.TimeToMillis(now.Add(-30 * time.Minute)),
		},
		"should not manipulate query time range if queryIngestersWithin is enabled but query min time is newer": {
			queryIngestersWithin: time.Hour,
			queryMinT:            util.TimeToMillis(now.Add(-50 * time.Minute)),
			queryMaxT:            util.TimeToMillis(now.Add(-30 * time.Minute)),
			expectedMinT:         util.TimeToMillis(now.Add(-50 * time.Minute)),
			expectedMaxT:         util.TimeToMillis(now.Add(-30 * time.Minute)),
		},
		"should manipulate query time range if queryIngestersWithin is enabled and query min time is older": {
			queryIngestersWithin: time.Hour,
			queryMinT:            util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:            util.TimeToMillis(now.Add(-30 * time.Minute)),
			expectedMinT:         util.TimeToMillis(now.Add(-60 * time.Minute)),
			expectedMaxT:         util.TimeToMillis(now.Add(-30 * time.Minute)),
		},
		"should skip the query if the query max time is older than queryIngestersWithin": {
			queryIngestersWithin: time.Hour,
			queryMinT:            util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:            util.TimeToMillis(now.Add(-90 * time.Minute)),
			expectedMinT:         0,
			expectedMaxT:         0,
		},
		"should manipulate query time range if queryIngestersWithin is enabled and query max time is older, but the query is for /series": {
			querySeries:          true,
			queryIngestersWithin: time.Hour,
			queryMinT:            util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:            util.TimeToMillis(now.Add(-30 * time.Minute)),
			expectedMinT:         util.TimeToMillis(now.Add(-60 * time.Minute)),
			expectedMaxT:         util.TimeToMillis(now.Add(-30 * time.Minute)),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			distributor := &mockDistributor{}
			distributor.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(model.Matrix{}, nil)
			distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&client.QueryStreamResponse{}, nil)
			distributor.On("MetricsForLabelMatchers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]metric.Metric{}, nil)

			ctx := user.InjectOrgID(context.Background(), "test")
			queryable := newDistributorQueryable(distributor, nil, testData.queryIngestersWithin, true, log.NewNopLogger())
			querier, err := queryable.Querier(ctx, testData.queryMinT, testData.queryMaxT)
			require.NoError(t, err)

			hints := &storage.SelectHints{Start: testData.queryMinT, End: testData.queryMaxT}
			if testData.querySeries {
				hints.Func = "series"
			}

			seriesSet := querier.Select(true, hints)
			require.NoError(t, seriesSet.Err())

			if testData.expectedMinT == 0 && testData.expectedMaxT == 0 {
				assert.Len(t, distributor.Calls, 0)
			} else {
				require.Len(t, distributor.Calls, 1)
				assert.InDelta(t, testData.expectedMinT, int64(distributor.Calls[0].Arguments.Get(1).(model.Time)), float64(5*time.Second.Milliseconds()))
				assert.Equal(t, testData.expectedMaxT, int64(distributor.Calls[0].Arguments.Get(2).(model.Time)))
			}
		})
	}
}

func TestDistributorQueryableFilter(t *testing.T) {
	d := &mockDistributor{}
	dq := newDistributorQueryable(d, nil, 1*time.Hour, true, log.NewNopLogger())

	now := time.Now()

	queryMinT := util.TimeToMillis(now.Add(-5 * time.Minute))
	queryMaxT := util.TimeToMillis(now)

	require.True(t, dq.UseQueryable(now, queryMinT, queryMaxT))
	require.True(t, dq.UseQueryable(now.Add(time.Hour), queryMinT, queryMaxT))

	// Same query, hour+1ms later, is not sent to ingesters.
	require.False(t, dq.UseQueryable(now.Add(time.Hour).Add(1*time.Millisecond), queryMinT, queryMaxT))
}

func TestIngesterStreaming(t *testing.T) {
	const mint, maxt = 0, 10

	// We need to make sure that there is at least one chunk present,
	// else no series will be selected.
	promChunk, err := encoding.NewForEncoding(encoding.PrometheusXorChunk)
	require.NoError(t, err)

	// Ensure at least 1 sample is appended to the chunk otherwise it can't be marshalled.
	promChunk.Add(model.SamplePair{Timestamp: mint, Value: 0})

	clientChunks, err := chunkcompat.ToChunks([]chunk.Chunk{
		chunk.NewChunk("", 0, nil, promChunk, model.Earliest, model.Earliest),
	})
	require.NoError(t, err)

	d := &mockDistributor{}
	d.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		&client.QueryStreamResponse{
			Chunkseries: []client.TimeSeriesChunk{
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "bar", Value: "baz"},
					},
					Chunks: clientChunks,
				},
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "foo", Value: "bar"},
					},
					Chunks: clientChunks,
				},
			},
		},
		nil)

	ctx := user.InjectOrgID(context.Background(), "0")
	queryable := newDistributorQueryable(d, mergeChunks, 0, true, log.NewNopLogger())
	querier, err := queryable.Querier(ctx, mint, maxt)
	require.NoError(t, err)

	seriesSet := querier.Select(true, &storage.SelectHints{Start: mint, End: maxt})
	require.NoError(t, seriesSet.Err())

	require.True(t, seriesSet.Next())
	series := seriesSet.At()
	require.Equal(t, labels.Labels{{Name: "bar", Value: "baz"}}, series.Labels())

	require.True(t, seriesSet.Next())
	series = seriesSet.At()
	require.Equal(t, labels.Labels{{Name: "foo", Value: "bar"}}, series.Labels())

	require.False(t, seriesSet.Next())
	require.NoError(t, seriesSet.Err())
}

func TestIngesterStreamingMixedResults(t *testing.T) {
	const (
		mint = 0
		maxt = 10000
	)
	s1 := []mimirpb.Sample{
		{Value: 1, TimestampMs: 1000},
		{Value: 2, TimestampMs: 2000},
		{Value: 3, TimestampMs: 3000},
		{Value: 4, TimestampMs: 4000},
		{Value: 5, TimestampMs: 5000},
	}
	s2 := []mimirpb.Sample{
		{Value: 1, TimestampMs: 1000},
		{Value: 2.5, TimestampMs: 2500},
		{Value: 3, TimestampMs: 3000},
		{Value: 5.5, TimestampMs: 5500},
	}

	mergedSamplesS1S2 := []mimirpb.Sample{
		{Value: 1, TimestampMs: 1000},
		{Value: 2, TimestampMs: 2000},
		{Value: 2.5, TimestampMs: 2500},
		{Value: 3, TimestampMs: 3000},
		{Value: 4, TimestampMs: 4000},
		{Value: 5, TimestampMs: 5000},
		{Value: 5.5, TimestampMs: 5500},
	}

	d := &mockDistributor{}
	d.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		&client.QueryStreamResponse{
			Chunkseries: []client.TimeSeriesChunk{
				{
					Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "one"}},
					Chunks: convertToChunks(t, s1),
				},
				{
					Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "two"}},
					Chunks: convertToChunks(t, s1),
				},
			},

			Timeseries: []mimirpb.TimeSeries{
				{
					Labels:  []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "two"}},
					Samples: s2,
				},
				{
					Labels:  []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "three"}},
					Samples: s1,
				},
			},
		},
		nil)

	ctx := user.InjectOrgID(context.Background(), "0")
	queryable := newDistributorQueryable(d, mergeChunks, 0, true, log.NewNopLogger())
	querier, err := queryable.Querier(ctx, mint, maxt)
	require.NoError(t, err)

	seriesSet := querier.Select(true, &storage.SelectHints{Start: mint, End: maxt}, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*"))
	require.NoError(t, seriesSet.Err())

	require.True(t, seriesSet.Next())
	verifySeries(t, seriesSet.At(), labels.Labels{{Name: labels.MetricName, Value: "one"}}, s1)

	require.True(t, seriesSet.Next())
	verifySeries(t, seriesSet.At(), labels.Labels{{Name: labels.MetricName, Value: "three"}}, s1)

	require.True(t, seriesSet.Next())
	verifySeries(t, seriesSet.At(), labels.Labels{{Name: labels.MetricName, Value: "two"}}, mergedSamplesS1S2)

	require.False(t, seriesSet.Next())
	require.NoError(t, seriesSet.Err())
}

func TestDistributorQuerier_LabelNames(t *testing.T) {
	const mint, maxt = 0, 10

	someMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
	labelNames := []string{"foo", "job"}

	t.Run("with matchers", func(t *testing.T) {
		t.Run("queryLabelNamesWithMatchers=true", func(t *testing.T) {
			d := &mockDistributor{}
			d.On("LabelNames", mock.Anything, model.Time(mint), model.Time(maxt), someMatchers).
				Return(labelNames, nil)

			queryable := newDistributorQueryable(d, nil, 0, true, log.NewNopLogger())
			querier, err := queryable.Querier(context.Background(), mint, maxt)
			require.NoError(t, err)

			names, warnings, err := querier.LabelNames(someMatchers...)
			require.NoError(t, err)
			assert.Empty(t, warnings)
			assert.Equal(t, labelNames, names)
		})

		t.Run("queryLabelNamesWithMatchers=false", func(t *testing.T) {
			metrics := []metric.Metric{
				{Metric: model.Metric{"foo": "bar"}},
				{Metric: model.Metric{"job": "baz"}},
				{Metric: model.Metric{"job": "baz", "foo": "boom"}},
			}
			d := &mockDistributor{}
			d.On("MetricsForLabelMatchers", mock.Anything, model.Time(mint), model.Time(maxt), someMatchers).
				Return(metrics, nil)

			queryable := newDistributorQueryable(d, nil, 0, false, log.NewNopLogger())
			querier, err := queryable.Querier(context.Background(), mint, maxt)
			require.NoError(t, err)

			names, warnings, err := querier.LabelNames(someMatchers...)
			require.NoError(t, err)
			assert.Empty(t, warnings)
			assert.Equal(t, labelNames, names)
		})
	})
}

func BenchmarkDistributorQueryable_Select(b *testing.B) {
	const (
		numSeries          = 10000
		numLabelsPerSeries = 20
	)

	// We need to make sure that there is at least one chunk present,
	// else no series will be selected.
	promChunk, err := encoding.NewForEncoding(encoding.PrometheusXorChunk)
	require.NoError(b, err)

	clientChunks, err := chunkcompat.ToChunks([]chunk.Chunk{
		chunk.NewChunk("", 0, nil, promChunk, model.Earliest, model.Earliest),
	})
	require.NoError(b, err)

	// Generate fixtures for series that are going to be returned by the mocked QueryStream().
	commonLabelsBuilder := labels.NewBuilder(nil)
	for i := 0; i < numLabelsPerSeries-1; i++ {
		commonLabelsBuilder.Set(fmt.Sprintf("label_%d", i), fmt.Sprintf("value_%d", i))
	}
	commonLabels := commonLabelsBuilder.Labels()

	response := &client.QueryStreamResponse{Chunkseries: make([]client.TimeSeriesChunk, 0, numSeries)}
	for i := 0; i < numSeries; i++ {
		lbls := labels.NewBuilder(commonLabels)
		lbls.Set("series_id", strconv.Itoa(i))

		response.Chunkseries = append(response.Chunkseries, client.TimeSeriesChunk{
			Labels: mimirpb.FromLabelsToLabelAdapters(lbls.Labels()),
			Chunks: clientChunks,
		})
	}

	d := &mockDistributor{}
	d.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(response, nil)

	ctx := user.InjectOrgID(context.Background(), "0")
	queryable := newDistributorQueryable(d, mergeChunks, 0, true, log.NewNopLogger())
	querier, err := queryable.Querier(ctx, math.MinInt64, math.MaxInt64)
	require.NoError(b, err)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		seriesSet := querier.Select(true, &storage.SelectHints{Start: math.MinInt64, End: math.MaxInt64})
		if seriesSet.Err() != nil {
			b.Fatal(seriesSet.Err())
		}
	}
}

func verifySeries(t *testing.T, series storage.Series, l labels.Labels, samples []mimirpb.Sample) {
	require.Equal(t, l, series.Labels())

	it := series.Iterator()
	for _, s := range samples {
		require.True(t, it.Next())
		require.Nil(t, it.Err())
		ts, v := it.At()
		require.Equal(t, s.Value, v)
		require.Equal(t, s.TimestampMs, ts)
	}
	require.False(t, it.Next())
	require.Nil(t, it.Err())
}

func convertToChunks(t *testing.T, samples []mimirpb.Sample) []client.Chunk {
	// We need to make sure that there is at least one chunk present,
	// else no series will be selected.
	promChunk, err := encoding.NewForEncoding(encoding.PrometheusXorChunk)
	require.NoError(t, err)

	for _, s := range samples {
		c, err := promChunk.Add(model.SamplePair{Value: model.SampleValue(s.Value), Timestamp: model.Time(s.TimestampMs)})
		require.NoError(t, err)
		require.Nil(t, c)
	}

	clientChunks, err := chunkcompat.ToChunks([]chunk.Chunk{
		chunk.NewChunk("", 0, nil, promChunk, model.Time(samples[0].TimestampMs), model.Time(samples[len(samples)-1].TimestampMs)),
	})
	require.NoError(t, err)

	return clientChunks
}

type mockDistributor struct {
	mock.Mock
}

func (m *mockDistributor) QueryExemplars(ctx context.Context, from, to model.Time, matchers ...[]*labels.Matcher) (*client.ExemplarQueryResponse, error) {
	args := m.Called(ctx, from, to, matchers)
	return args.Get(0).(*client.ExemplarQueryResponse), args.Error(1)
}
func (m *mockDistributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error) {
	args := m.Called(ctx, from, to, matchers)
	return args.Get(0).(*client.QueryStreamResponse), args.Error(1)
}
func (m *mockDistributor) LabelValuesForLabelName(ctx context.Context, from, to model.Time, lbl model.LabelName, matchers ...*labels.Matcher) ([]string, error) {
	args := m.Called(ctx, from, to, lbl, matchers)
	return args.Get(0).([]string), args.Error(1)
}
func (m *mockDistributor) LabelNames(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) ([]string, error) {
	args := m.Called(ctx, from, to, matchers)
	return args.Get(0).([]string), args.Error(1)
}
func (m *mockDistributor) MetricsForLabelMatchers(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	args := m.Called(ctx, from, to, matchers)
	return args.Get(0).([]metric.Metric), args.Error(1)
}

func (m *mockDistributor) MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error) {
	args := m.Called(ctx)
	return args.Get(0).([]scrape.MetricMetadata), args.Error(1)
}

func (m *mockDistributor) LabelNamesAndValues(ctx context.Context, matchers []*labels.Matcher) (*client.LabelNamesAndValuesResponse, error) {
	args := m.Called(ctx, matchers)
	return args.Get(0).(*client.LabelNamesAndValuesResponse), args.Error(1)
}

func (m *mockDistributor) LabelValuesCardinality(ctx context.Context, labelNames []model.LabelName, matchers []*labels.Matcher) (uint64, *client.LabelValuesCardinalityResponse, error) {
	args := m.Called(ctx, labelNames, matchers)
	return args.Get(0).(uint64), args.Get(1).(*client.LabelValuesCardinalityResponse), args.Error(2)
}
