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
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestDistributorQuerier_Select_ShouldHonorQueryIngestersWithin(t *testing.T) {
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
			distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(client.CombinedQueryStreamResponse{}, nil)
			distributor.On("MetricsForLabelMatchers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]labels.Labels{}, nil)

			const tenantID = "test"
			ctx := user.InjectOrgID(context.Background(), tenantID)
			configProvider := newMockConfigProvider(testData.queryIngestersWithin)
			queryable := NewDistributorQueryable(distributor, configProvider, nil, log.NewNopLogger())
			querier, err := queryable.Querier(testData.queryMinT, testData.queryMaxT)
			require.NoError(t, err)

			hints := &storage.SelectHints{Start: testData.queryMinT, End: testData.queryMaxT}
			if testData.querySeries {
				hints.Func = "series"
			}

			seriesSet := querier.Select(ctx, true, hints)
			require.NoError(t, seriesSet.Err())
			require.Equal(t, []string{tenantID}, configProvider.seenUserIDs)

			if testData.expectedMinT == 0 && testData.expectedMaxT == 0 {
				assert.Len(t, distributor.Calls, 0)
			} else if testData.querySeries {
				require.Len(t, distributor.Calls, 1)
				assert.InDelta(t, testData.expectedMinT, int64(distributor.Calls[0].Arguments.Get(1).(model.Time)), float64(5*time.Second.Milliseconds()))
				assert.Equal(t, testData.expectedMaxT, int64(distributor.Calls[0].Arguments.Get(2).(model.Time)))
			} else {
				require.Len(t, distributor.Calls, 1)
				assert.InDelta(t, testData.expectedMinT, int64(distributor.Calls[0].Arguments.Get(2).(model.Time)), float64(5*time.Second.Milliseconds()))
				assert.Equal(t, testData.expectedMaxT, int64(distributor.Calls[0].Arguments.Get(3).(model.Time)))
			}
		})
	}
}

func TestDistributorQuerier_Select(t *testing.T) {
	const mint, maxt = 0, 10

	// We need to make sure that there is at least one chunk present,
	// else no series will be selected.
	promChunk, err := chunk.NewForEncoding(chunk.PrometheusXorChunk)
	require.NoError(t, err)

	// Ensure at least 1 sample is appended to the chunk otherwise it can't be marshalled.
	_, err = promChunk.Add(model.SamplePair{Timestamp: mint, Value: 0})
	require.NoError(t, err)

	clientChunks, err := client.ToChunks([]chunk.Chunk{
		chunk.NewChunk(labels.EmptyLabels(), promChunk, model.Earliest, model.Earliest),
	})
	require.NoError(t, err)

	testCases := map[string]struct {
		response client.CombinedQueryStreamResponse
	}{
		"chunk series": {
			response: client.CombinedQueryStreamResponse{
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
		},
		"streaming series": {
			response: client.CombinedQueryStreamResponse{
				StreamingSeries: []client.StreamingSeries{
					{
						Labels: labels.FromStrings("bar", "baz"),
					},
					{
						Labels: labels.FromStrings("foo", "bar"),
					},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			d := &mockDistributor{}
			d.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(testCase.response, nil)

			ctx := user.InjectOrgID(context.Background(), "0")
			queryable := NewDistributorQueryable(d, newMockConfigProvider(0), nil, log.NewNopLogger())
			querier, err := queryable.Querier(mint, maxt)
			require.NoError(t, err)

			seriesSet := querier.Select(ctx, true, &storage.SelectHints{Start: mint, End: maxt})
			require.NoError(t, seriesSet.Err())

			require.True(t, seriesSet.Next())
			series := seriesSet.At()
			require.Equal(t, labels.FromStrings("bar", "baz"), series.Labels())

			require.True(t, seriesSet.Next())
			series = seriesSet.At()
			require.Equal(t, labels.FromStrings("foo", "bar"), series.Labels())

			require.False(t, seriesSet.Next())
			require.NoError(t, seriesSet.Err())
		})
	}
}

func TestDistributorQuerier_Select_MixedChunkseriesTimeseriesAndStreamingResults(t *testing.T) {
	const (
		mint = 0
		maxt = 10000
	)
	s1 := []mimirpb.Sample{
		{TimestampMs: 1000, Value: 1},
		{TimestampMs: 2000, Value: 2},
		{TimestampMs: 3000, Value: 3},
		{TimestampMs: 4000, Value: 4},
		{TimestampMs: 5000, Value: 5},
	}
	s2 := []mimirpb.Sample{
		{TimestampMs: 1000, Value: 1},
		{TimestampMs: 2500, Value: 2.5},
		{TimestampMs: 3000, Value: 3},
		{TimestampMs: 5500, Value: 5.5},
	}
	s3 := []mimirpb.Sample{
		{TimestampMs: 3000, Value: 3},
		{TimestampMs: 6000, Value: 6},
		{TimestampMs: 7000, Value: 7},
	}
	s4 := []mimirpb.Sample{
		{TimestampMs: 8000, Value: 8},
		{TimestampMs: 9000, Value: 9},
	}

	mergedSamplesS1S2S3 := []interface{}{
		mimirpb.Sample{TimestampMs: 1000, Value: 1},
		mimirpb.Sample{TimestampMs: 2000, Value: 2},
		mimirpb.Sample{TimestampMs: 2500, Value: 2.5},
		mimirpb.Sample{TimestampMs: 3000, Value: 3},
		mimirpb.Sample{TimestampMs: 4000, Value: 4},
		mimirpb.Sample{TimestampMs: 5000, Value: 5},
		mimirpb.Sample{TimestampMs: 5500, Value: 5.5},
		mimirpb.Sample{TimestampMs: 6000, Value: 6},
		mimirpb.Sample{TimestampMs: 7000, Value: 7},
	}

	streamReader := createTestStreamReader([]client.QueryStreamSeriesChunks{
		{SeriesIndex: 0, Chunks: convertToChunks(t, samplesToInterface(s4))},
		{SeriesIndex: 1, Chunks: convertToChunks(t, samplesToInterface(s3))},
	})

	d := &mockDistributor{}
	d.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		client.CombinedQueryStreamResponse{
			Chunkseries: []client.TimeSeriesChunk{
				{
					Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "one"}},
					Chunks: convertToChunks(t, samplesToInterface(s1)),
				},
				{
					Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "two"}},
					Chunks: convertToChunks(t, samplesToInterface(s1)),
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

			StreamingSeries: []client.StreamingSeries{
				{
					Labels: labels.FromStrings(labels.MetricName, "four"),
					Sources: []client.StreamingSeriesSource{
						{SeriesIndex: 0, StreamReader: streamReader},
					},
				},
				{
					Labels: labels.FromStrings(labels.MetricName, "two"),
					Sources: []client.StreamingSeriesSource{
						{SeriesIndex: 1, StreamReader: streamReader},
					},
				},
			},
		},
		nil)

	ctx := user.InjectOrgID(context.Background(), "0")
	queryable := NewDistributorQueryable(d, newMockConfigProvider(0), stats.NewQueryMetrics(prometheus.NewPedanticRegistry()), log.NewNopLogger())
	querier, err := queryable.Querier(mint, maxt)
	require.NoError(t, err)

	seriesSet := querier.Select(ctx, true, &storage.SelectHints{Start: mint, End: maxt}, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*"))
	require.NoError(t, seriesSet.Err())

	require.True(t, seriesSet.Next())
	verifySeries(t, seriesSet.At(), labels.FromStrings(labels.MetricName, "four"), samplesToInterface(s4))

	require.True(t, seriesSet.Next())
	verifySeries(t, seriesSet.At(), labels.FromStrings(labels.MetricName, "one"), samplesToInterface(s1))

	require.True(t, seriesSet.Next())
	verifySeries(t, seriesSet.At(), labels.FromStrings(labels.MetricName, "three"), samplesToInterface(s1))

	require.True(t, seriesSet.Next())
	verifySeries(t, seriesSet.At(), labels.FromStrings(labels.MetricName, "two"), mergedSamplesS1S2S3)

	require.False(t, seriesSet.Next())
	require.NoError(t, seriesSet.Err())
}

func genTestHistogram(timestamp int64, value int) mimirpb.Histogram {
	return mimirpb.FromHistogramToHistogramProto(timestamp, test.GenerateTestHistogram(value))
}

func genTestFloatHistogram(timestamp int64, value int) mimirpb.Histogram {
	return mimirpb.FromFloatHistogramToHistogramProto(timestamp, test.GenerateTestFloatHistogram(value))
}

func TestDistributorQuerier_Select_MixedFloatAndIntegerHistograms(t *testing.T) {
	const (
		mint = 0
		maxt = 10000
	)

	s1 := []mimirpb.Histogram{
		genTestHistogram(1000, 1),
		genTestHistogram(2000, 2),
		genTestHistogram(3000, 3),
		genTestFloatHistogram(4000, 4),
		genTestHistogram(5000, 5),
	}
	s2 := []mimirpb.Histogram{
		genTestHistogram(1000, 1),
		genTestFloatHistogram(2500, 25),
		genTestHistogram(3000, 3),
		genTestHistogram(5500, 55),
	}

	mergedSamplesS1S2 := []interface{}{
		genTestHistogram(1000, 1),
		genTestHistogram(2000, 2),
		genTestFloatHistogram(2500, 25),
		genTestHistogram(3000, 3),
		genTestFloatHistogram(4000, 4),
		genTestHistogram(5000, 5),
		genTestHistogram(5500, 55),
	}

	d := &mockDistributor{}
	d.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		client.CombinedQueryStreamResponse{
			Chunkseries: []client.TimeSeriesChunk{
				{
					Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "one"}},
					Chunks: convertToChunks(t, histogramsToInterface(s1)),
				},
				{
					Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "two"}},
					Chunks: convertToChunks(t, histogramsToInterface(s1)),
				},
			},

			Timeseries: []mimirpb.TimeSeries{
				{
					Labels:     []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "two"}},
					Histograms: s2,
				},
				{
					Labels:     []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "three"}},
					Histograms: s1,
				},
			},
		},
		nil)

	ctx := user.InjectOrgID(context.Background(), "0")
	queryable := NewDistributorQueryable(d, newMockConfigProvider(0), nil, log.NewNopLogger())
	querier, err := queryable.Querier(mint, maxt)
	require.NoError(t, err)

	seriesSet := querier.Select(ctx, true, &storage.SelectHints{Start: mint, End: maxt}, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*"))
	require.NoError(t, seriesSet.Err())

	require.True(t, seriesSet.Next())
	verifySeries(t, seriesSet.At(), labels.FromStrings(labels.MetricName, "one"), histogramsToInterface(s1))

	require.True(t, seriesSet.Next())
	verifySeries(t, seriesSet.At(), labels.FromStrings(labels.MetricName, "three"), histogramsToInterface(s1))

	require.True(t, seriesSet.Next())
	verifySeries(t, seriesSet.At(), labels.FromStrings(labels.MetricName, "two"), mergedSamplesS1S2)

	require.False(t, seriesSet.Next())
	require.NoError(t, seriesSet.Err())
}

func TestDistributorQuerier_Select_MixedHistogramsAndFloatSamples(t *testing.T) {
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
	h1 := []mimirpb.Histogram{
		genTestFloatHistogram(5500, 55),
		genTestFloatHistogram(6000, 60),
		genTestFloatHistogram(8000, 80),
	}
	s2 := []mimirpb.Sample{
		{Value: 1, TimestampMs: 1000},
		{Value: 2.5, TimestampMs: 2500},
		{Value: 3, TimestampMs: 3000},
		{Value: 5.5, TimestampMs: 5500},
	}
	h2 := []mimirpb.Histogram{
		genTestFloatHistogram(5500, 55),
		genTestFloatHistogram(6000, 60),
		genTestFloatHistogram(7000, 70),
		genTestFloatHistogram(8000, 80),
	}

	mergedSamples := []interface{}{
		mimirpb.Sample{Value: 1, TimestampMs: 1000},
		mimirpb.Sample{Value: 2, TimestampMs: 2000},
		mimirpb.Sample{Value: 2.5, TimestampMs: 2500},
		mimirpb.Sample{Value: 3, TimestampMs: 3000},
		mimirpb.Sample{Value: 4, TimestampMs: 4000},
		mimirpb.Sample{Value: 5, TimestampMs: 5000},
		// {Value: 5.5, TimestampMs: 5500},  masked by histograms
		genTestFloatHistogram(5500, 55),
		genTestFloatHistogram(6000, 60),
		genTestFloatHistogram(7000, 70),
		genTestFloatHistogram(8000, 80),
	}

	d := &mockDistributor{}
	d.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		client.CombinedQueryStreamResponse{
			Chunkseries: []client.TimeSeriesChunk{
				{
					Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "one"}},
					Chunks: convertToChunks(t, append(samplesToInterface(s1), histogramsToInterface(h1)...)),
				},
				{
					Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "two"}},
					Chunks: convertToChunks(t, append(samplesToInterface(s1), histogramsToInterface(h1)...)),
				},
			},

			Timeseries: []mimirpb.TimeSeries{
				{
					Labels:     []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "two"}},
					Samples:    s2,
					Histograms: h2,
				},
				{
					Labels:     []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "three"}},
					Samples:    s1,
					Histograms: h1,
				},
			},
		},
		nil)

	ctx := user.InjectOrgID(context.Background(), "0")
	queryable := NewDistributorQueryable(d, newMockConfigProvider(0), nil, log.NewNopLogger())
	querier, err := queryable.Querier(mint, maxt)
	require.NoError(t, err)

	seriesSet := querier.Select(ctx, true, &storage.SelectHints{Start: mint, End: maxt}, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*"))
	require.NoError(t, seriesSet.Err())

	require.True(t, seriesSet.Next())
	verifySeries(t, seriesSet.At(), labels.FromStrings(labels.MetricName, "one"), append(samplesToInterface(s1), histogramsToInterface(h1)...))

	require.True(t, seriesSet.Next())
	verifySeries(t, seriesSet.At(), labels.FromStrings(labels.MetricName, "three"), append(samplesToInterface(s1), histogramsToInterface(h1)...))

	require.True(t, seriesSet.Next())
	verifySeries(t, seriesSet.At(), labels.FromStrings(labels.MetricName, "two"), mergedSamples)

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
			ctx := user.InjectOrgID(context.Background(), "0")
			queryable := NewDistributorQueryable(d, newMockConfigProvider(0), nil, log.NewNopLogger())
			querier, err := queryable.Querier(mint, maxt)
			require.NoError(t, err)

			names, warnings, err := querier.LabelNames(ctx, &storage.LabelHints{}, someMatchers...)
			require.NoError(t, err)
			assert.Empty(t, warnings)
			assert.Equal(t, labelNames, names)
		})
	})
}

func BenchmarkDistributorQuerier_Select(b *testing.B) {
	const (
		numSeries          = 10000
		numLabelsPerSeries = 20
	)

	// We need to make sure that there is at least one chunk present,
	// else no series will be selected.
	promChunk, err := chunk.NewForEncoding(chunk.PrometheusXorChunk)
	require.NoError(b, err)

	overflowChunk, err := promChunk.Add(model.SamplePair{Timestamp: model.Now(), Value: 1})
	require.NoError(b, err)
	require.Nil(b, overflowChunk)

	clientChunks, err := client.ToChunks([]chunk.Chunk{
		chunk.NewChunk(labels.EmptyLabels(), promChunk, model.Earliest, model.Earliest),
	})
	require.NoError(b, err)

	// Generate fixtures for series that are going to be returned by the mocked QueryStream().
	commonLabelsBuilder := labels.NewScratchBuilder(numLabelsPerSeries - 1)
	for i := 0; i < numLabelsPerSeries-1; i++ {
		commonLabelsBuilder.Add(fmt.Sprintf("label_%d", i), fmt.Sprintf("value_%d", i))
	}
	commonLabelsBuilder.Sort()
	commonLabels := commonLabelsBuilder.Labels()

	response := client.CombinedQueryStreamResponse{Chunkseries: make([]client.TimeSeriesChunk, 0, numSeries)}
	for i := 0; i < numSeries; i++ {
		lbls := labels.NewBuilder(commonLabels)
		lbls.Set("series_id", strconv.Itoa(i))

		response.Chunkseries = append(response.Chunkseries, client.TimeSeriesChunk{
			Labels: mimirpb.FromLabelsToLabelAdapters(lbls.Labels()),
			Chunks: clientChunks,
		})
	}

	d := &mockDistributor{}
	d.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(response, nil)

	ctx := user.InjectOrgID(context.Background(), "0")
	queryable := NewDistributorQueryable(d, newMockConfigProvider(0), nil, log.NewNopLogger())
	querier, err := queryable.Querier(math.MinInt64, math.MaxInt64)
	require.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		seriesSet := querier.Select(ctx, true, &storage.SelectHints{Start: math.MinInt64, End: math.MaxInt64})
		if seriesSet.Err() != nil {
			b.Fatal(seriesSet.Err())
		}

		var it chunkenc.Iterator
		for seriesSet.Next() {
			ss := seriesSet.At()
			it = ss.Iterator(it)
			if it.Err() != nil {
				b.Fatal(it.Err())
			}
		}
	}
}

func samplesToInterface(samples []mimirpb.Sample) []interface{} {
	r := make([]interface{}, 0, len(samples))
	for _, s := range samples {
		r = append(r, s)
	}
	return r
}

func histogramsToInterface(samples []mimirpb.Histogram) []interface{} {
	r := make([]interface{}, 0, len(samples))
	for _, s := range samples {
		r = append(r, s)
	}
	return r
}

func verifySeries(t *testing.T, series storage.Series, l labels.Labels, samples []interface{}) {
	require.Equal(t, l, series.Labels())

	it := series.Iterator(nil)
	for _, s := range samples {
		valType := it.Next()
		require.NotEqual(t, chunkenc.ValNone, valType)
		require.Nil(t, it.Err())
		switch s := s.(type) {
		case mimirpb.Sample:
			test.RequireIteratorFloat(t, s.TimestampMs, s.Value, it, valType)
		case mimirpb.Histogram:
			if s.IsFloatHistogram() {
				test.RequireIteratorFloatHistogram(t, s.Timestamp, mimirpb.FromFloatHistogramProtoToFloatHistogram(&s), it, valType)
			} else {
				test.RequireIteratorHistogram(t, s.Timestamp, mimirpb.FromHistogramProtoToHistogram(&s), it, valType)
			}
		default:
			t.Errorf("verifyHistogramSeries - internal error, unhandled expected type: %T", s)
		}
	}
	require.Equal(t, chunkenc.ValNone, it.Next())
	require.Nil(t, it.Err())
}

func convertToChunks(t *testing.T, samples []interface{}) []client.Chunk {
	var (
		overflow chunk.EncodedChunk
		err      error
	)

	chunks := []chunk.Chunk{}
	ensureChunk := func(enc chunk.Encoding, ts int64) {
		if len(chunks) == 0 || chunks[len(chunks)-1].Data.Encoding() != enc {
			c, err := chunk.NewForEncoding(enc)
			require.NoError(t, err)
			chunks = append(chunks, chunk.NewChunk(labels.EmptyLabels(), c, model.Time(ts), model.Time(ts)))
			return
		}
		chunks[len(chunks)-1].Through = model.Time(ts)
	}

	for _, s := range samples {
		switch s := s.(type) {
		case mimirpb.Sample:
			ensureChunk(chunk.PrometheusXorChunk, s.TimestampMs)
			overflow, err = chunks[len(chunks)-1].Data.Add(model.SamplePair{Value: model.SampleValue(s.Value), Timestamp: model.Time(s.TimestampMs)})
		case mimirpb.Histogram:
			if s.IsFloatHistogram() {
				ensureChunk(chunk.PrometheusFloatHistogramChunk, s.Timestamp)
				overflow, err = chunks[len(chunks)-1].Data.AddFloatHistogram(s.Timestamp, mimirpb.FromFloatHistogramProtoToFloatHistogram(&s))
			} else {
				ensureChunk(chunk.PrometheusHistogramChunk, s.Timestamp)
				overflow, err = chunks[len(chunks)-1].Data.AddHistogram(s.Timestamp, mimirpb.FromHistogramProtoToHistogram(&s))
			}
		default:
			t.Errorf("convertToChunks - unhandled type: %T", s)
		}
		require.NoError(t, err)
		require.Nil(t, overflow)
	}

	clientChunks, err := client.ToChunks(chunks)
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
func (m *mockDistributor) QueryStream(ctx context.Context, queryMetrics *stats.QueryMetrics, from, to model.Time, matchers ...*labels.Matcher) (client.CombinedQueryStreamResponse, error) {
	args := m.Called(ctx, queryMetrics, from, to, matchers)
	return args.Get(0).(client.CombinedQueryStreamResponse), args.Error(1)
}
func (m *mockDistributor) LabelValuesForLabelName(ctx context.Context, from, to model.Time, lbl model.LabelName, matchers ...*labels.Matcher) ([]string, error) {
	args := m.Called(ctx, from, to, lbl, matchers)
	return args.Get(0).([]string), args.Error(1)
}
func (m *mockDistributor) LabelNames(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) ([]string, error) {
	args := m.Called(ctx, from, to, matchers)
	return args.Get(0).([]string), args.Error(1)
}
func (m *mockDistributor) MetricsForLabelMatchers(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) ([]labels.Labels, error) {
	args := m.Called(ctx, from, to, matchers)
	return args.Get(0).([]labels.Labels), args.Error(1)
}

func (m *mockDistributor) MetricsMetadata(ctx context.Context, req *client.MetricsMetadataRequest) ([]scrape.MetricMetadata, error) {
	args := m.Called(ctx, req)
	return args.Get(0).([]scrape.MetricMetadata), args.Error(1)
}

func (m *mockDistributor) LabelNamesAndValues(ctx context.Context, matchers []*labels.Matcher, countMethod cardinality.CountMethod) (*client.LabelNamesAndValuesResponse, error) {
	args := m.Called(ctx, matchers, countMethod)
	return args.Get(0).(*client.LabelNamesAndValuesResponse), args.Error(1)
}

func (m *mockDistributor) LabelValuesCardinality(ctx context.Context, labelNames []model.LabelName, matchers []*labels.Matcher, countMethod cardinality.CountMethod) (uint64, *client.LabelValuesCardinalityResponse, error) {
	args := m.Called(ctx, labelNames, matchers, countMethod)
	return args.Get(0).(uint64), args.Get(1).(*client.LabelValuesCardinalityResponse), args.Error(2)
}

func (m *mockDistributor) ActiveSeries(ctx context.Context, matchers []*labels.Matcher) ([]labels.Labels, error) {
	args := m.Called(ctx, matchers)
	return args.Get(0).([]labels.Labels), args.Error(1)
}

func (m *mockDistributor) ActiveNativeHistogramMetrics(ctx context.Context, matchers []*labels.Matcher) (*cardinality.ActiveNativeHistogramMetricsResponse, error) {
	args := m.Called(ctx, matchers)
	return args.Get(0).(*cardinality.ActiveNativeHistogramMetricsResponse), args.Error(1)
}

type mockConfigProvider struct {
	queryIngestersWithin time.Duration
	seenUserIDs          []string
}

func newMockConfigProvider(duration time.Duration) *mockConfigProvider {
	return &mockConfigProvider{queryIngestersWithin: duration}
}

func (p *mockConfigProvider) QueryIngestersWithin(userID string) time.Duration {
	p.seenUserIDs = append(p.seenUserIDs, userID)
	return p.queryIngestersWithin
}
