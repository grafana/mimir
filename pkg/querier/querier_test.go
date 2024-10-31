// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/querier_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	promtestutil "github.com/prometheus/prometheus/util/testutil"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestMain(m *testing.M) {
	test.VerifyNoLeakTestMain(m)
}

const (
	chunkOffset     = 1 * time.Hour
	chunkLength     = 3 * time.Hour
	sampleRate      = 15 * time.Second
	samplesPerChunk = chunkLength / sampleRate
)

type query struct {
	query   string
	labels  labels.Labels
	samples func(from, through time.Time, step time.Duration) int
	step    time.Duration

	valueType    func(ts model.Time) chunkenc.ValueType
	assertFPoint func(t testing.TB, ts int64, point promql.FPoint)
	assertHPoint func(t testing.TB, ts int64, point promql.HPoint)
}

func TestQuerier(t *testing.T) {
	var cfg Config
	flagext.DefaultValues(&cfg)

	const chunks = 24
	secondChunkStart := model.Time(0).Add(chunkOffset)

	queries := map[string]query{
		// Windowed rates with small step;  This will cause BufferedIterator to read
		// all the samples.
		"float: windowed rates with small step": {
			query:  "rate(foo[1m])",
			step:   sampleRate * 4,
			labels: labels.Labels{},
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from) / step)
			},
			valueType: func(_ model.Time) chunkenc.ValueType { return chunkenc.ValFloat },
			assertFPoint: func(t testing.TB, ts int64, point promql.FPoint) {
				require.Equal(t, promql.FPoint{
					T: ts + int64((sampleRate*4)/time.Millisecond),
					F: 1000.0,
				}, point)
			},
		},

		// Very simple single-point gets, with low step.  Performance should be
		// similar to above.
		"float: single point gets with low step": {
			query:  "foo",
			step:   sampleRate * 4,
			labels: labels.FromStrings(model.MetricNameLabel, "foo"),
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from)/step) + 1
			},
			valueType: func(_ model.Time) chunkenc.ValueType { return chunkenc.ValFloat },
			assertFPoint: func(t testing.TB, ts int64, point promql.FPoint) {
				require.Equal(t, promql.FPoint{
					T: ts,
					F: float64(ts),
				}, point)
			},
		},

		// Rates with large step; excersise everything.
		"float: rate with large step": {
			query:  "rate(foo[1m])",
			step:   sampleRate * 4 * 10,
			labels: labels.Labels{},
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from) / step)
			},
			valueType: func(_ model.Time) chunkenc.ValueType { return chunkenc.ValFloat },
			assertFPoint: func(t testing.TB, ts int64, point promql.FPoint) {
				require.Equal(t, promql.FPoint{
					T: ts + int64((sampleRate*4)/time.Millisecond)*10,
					F: 1000.0,
				}, point)
			},
		},

		// Single points gets with large step; excersise Seek performance.
		"float: single point gets with large step": {
			query:  "foo",
			step:   sampleRate * 4 * 10,
			labels: labels.FromStrings(model.MetricNameLabel, "foo"),
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from)/step) + 1
			},
			valueType: func(_ model.Time) chunkenc.ValueType { return chunkenc.ValFloat },
			assertFPoint: func(t testing.TB, ts int64, point promql.FPoint) {
				require.Equal(t, promql.FPoint{
					T: ts,
					F: float64(ts),
				}, point)
			},
		},

		"integer histogram: single point gets with low step": {
			query:  "foo",
			step:   sampleRate * 4,
			labels: labels.FromStrings(model.MetricNameLabel, "foo"),
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from)/step) + 1
			},
			valueType: func(_ model.Time) chunkenc.ValueType { return chunkenc.ValFloatHistogram },
			assertHPoint: func(t testing.TB, ts int64, point promql.HPoint) {
				require.Equal(t, ts, point.T)
				// The TSDB head uses heuristic to determine the chunk sizes, which
				// can alter where chunks are started, which can alter where
				// unknown and no reset hints are. We really don't care as the
				// data doesn't have resets and is not a product of a merge.
				point.H.CounterResetHint = histogram.UnknownCounterReset
				test.RequireFloatHistogramEqual(t, test.GenerateTestHistogram(int(ts)).ToFloat(nil), point.H)
			},
		},

		"float histogram: single point gets with low step": {
			query:  "foo",
			step:   sampleRate * 4,
			labels: labels.FromStrings(model.MetricNameLabel, "foo"),
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from)/step) + 1
			},
			valueType: func(_ model.Time) chunkenc.ValueType { return chunkenc.ValFloatHistogram },
			assertHPoint: func(t testing.TB, ts int64, point promql.HPoint) {
				require.Equal(t, ts, point.T)
				// The TSDB head uses heuristic to determine the chunk sizes, which
				// can alter where chunks are started, which can alter where
				// unknown and no reset hints are. We really don't care as the
				// data doesn't have resets and is not a product of a merge.
				point.H.CounterResetHint = histogram.UnknownCounterReset
				test.RequireFloatHistogramEqual(t, test.GenerateTestFloatHistogram(int(ts)), point.H)
			},
		},

		"float histogram: with large step": {
			query:  "foo",
			step:   sampleRate * 4 * 10,
			labels: labels.FromStrings(model.MetricNameLabel, "foo"),
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from)/step) + 1
			},
			valueType: func(_ model.Time) chunkenc.ValueType { return chunkenc.ValFloatHistogram },
			assertHPoint: func(t testing.TB, ts int64, point promql.HPoint) {
				require.Equal(t, ts, point.T)
				// The TSDB head uses heuristic to determine the chunk sizes, which
				// can alter where chunks are started, which can alter where
				// unknown and no reset hints are. We really don't care as the
				// data doesn't have resets and is not a product of a merge.
				point.H.CounterResetHint = histogram.UnknownCounterReset
				test.RequireFloatHistogramEqual(t, test.GenerateTestFloatHistogram(int(ts)), point.H)
			},
		},

		"float to histogram: check transition with low steps": {
			query:  "foo",
			step:   sampleRate * 4,
			labels: labels.FromStrings(model.MetricNameLabel, "foo"),
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from)/step) + 1
			},
			valueType: func(ts model.Time) chunkenc.ValueType {
				if ts.After(secondChunkStart) { // New type starts new chunk anyway, so use the chunkoffset
					return chunkenc.ValFloatHistogram
				}
				return chunkenc.ValFloat
			},
			assertFPoint: func(t testing.TB, ts int64, point promql.FPoint) {
				require.True(t, ts <= int64(secondChunkStart))
				require.Equal(t, promql.FPoint{
					T: ts,
					F: float64(ts),
				}, point)
			},
			assertHPoint: func(t testing.TB, ts int64, point promql.HPoint) {
				require.True(t, ts > int64(secondChunkStart))
				require.Equal(t, ts, point.T)
				// The TSDB head uses heuristic to determine the chunk sizes, which
				// can alter where chunks are started, which can alter where
				// unknown and no reset hints are. We really don't care as the
				// data doesn't have resets and is not a product of a merge.
				point.H.CounterResetHint = histogram.UnknownCounterReset
				test.RequireFloatHistogramEqual(t, test.GenerateTestFloatHistogram(int(ts)), point.H)
			},
		},
	}

	for qName, q := range queries {
		t.Run(qName, func(t *testing.T) {
			// Generate TSDB head used to simulate querying the long-term storage.
			db, through := mockTSDB(t, model.Time(0), int(chunks*samplesPerChunk), sampleRate, chunkOffset, int(samplesPerChunk), q.valueType)
			dbQueryable := TimeRangeQueryable{
				Queryable: db,
				IsApplicable: func(_ string, _ time.Time, _, _ int64) bool {
					return true
				},
			}

			// No samples returned by ingesters.
			distributor := &mockDistributor{}
			distributor.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&client.QueryResponse{}, nil)
			distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(client.CombinedQueryStreamResponse{}, nil)

			overrides, err := validation.NewOverrides(defaultLimitsConfig(), nil)
			require.NoError(t, err)

			queryable, _, _, err := New(cfg, overrides, distributor, []TimeRangeQueryable{dbQueryable}, nil, log.NewNopLogger(), nil)
			require.NoError(t, err)

			testRangeQuery(t, queryable, through, q)
		})
	}
}

// This test ensures the PromQL engine works correct if Select() function returns samples outside
// the queried range because the underlying queryable doesn't trim chunks based on the query start/end time.
func TestQuerier_QueryableReturnsChunksOutsideQueriedRange(t *testing.T) {
	var (
		logger     = log.NewNopLogger()
		queryStart = mustParseTime("2021-11-01T06:00:00Z")
		queryEnd   = mustParseTime("2021-11-01T06:05:00Z")
		queryStep  = time.Minute
	)

	var cfg Config
	flagext.DefaultValues(&cfg)

	// Mock distributor to return chunks containing samples outside the queried range.
	distributor := &mockDistributor{}
	distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		client.CombinedQueryStreamResponse{
			Chunkseries: []client.TimeSeriesChunk{
				// Series with data points only before queryStart.
				{
					Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "one"}},
					Chunks: convertToChunks(t, []interface{}{
						mimirpb.Sample{TimestampMs: queryStart.Add(-9*time.Minute).Unix() * 1000, Value: 1},
						mimirpb.Sample{TimestampMs: queryStart.Add(-8*time.Minute).Unix() * 1000, Value: 1},
						mimirpb.Sample{TimestampMs: queryStart.Add(-7*time.Minute).Unix() * 1000, Value: 1},
					}),
				},
				// Series with data points before and after queryStart, but before queryEnd.
				{
					Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "one"}},
					Chunks: convertToChunks(t, []interface{}{
						mimirpb.Sample{TimestampMs: queryStart.Add(-9*time.Minute).Unix() * 1000, Value: 1},
						mimirpb.Sample{TimestampMs: queryStart.Add(-8*time.Minute).Unix() * 1000, Value: 3},
						mimirpb.Sample{TimestampMs: queryStart.Add(-7*time.Minute).Unix() * 1000, Value: 5},
						mimirpb.Sample{TimestampMs: queryStart.Add(-6*time.Minute).Unix() * 1000, Value: 7},
						mimirpb.Sample{TimestampMs: queryStart.Add(-5*time.Minute).Unix() * 1000, Value: 11},
						mimirpb.Sample{TimestampMs: queryStart.Add(-4*time.Minute).Unix() * 1000, Value: 13},
						mimirpb.Sample{TimestampMs: queryStart.Add(-3*time.Minute).Unix() * 1000, Value: 17},
						mimirpb.Sample{TimestampMs: queryStart.Add(-2*time.Minute).Unix() * 1000, Value: 19},
						mimirpb.Sample{TimestampMs: queryStart.Add(-1*time.Minute).Unix() * 1000, Value: 23},
						mimirpb.Sample{TimestampMs: queryStart.Add(+0*time.Minute).Unix() * 1000, Value: 29},
						mimirpb.Sample{TimestampMs: queryStart.Add(+1*time.Minute).Unix() * 1000, Value: 31},
						mimirpb.Sample{TimestampMs: queryStart.Add(+2*time.Minute).Unix() * 1000, Value: 37},
					}),
				},
				// Series with data points after queryEnd.
				{
					Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "one"}},
					Chunks: convertToChunks(t, []interface{}{
						mimirpb.Sample{TimestampMs: queryStart.Add(+4*time.Minute).Unix() * 1000, Value: 41},
						mimirpb.Sample{TimestampMs: queryStart.Add(+5*time.Minute).Unix() * 1000, Value: 43},
						mimirpb.Sample{TimestampMs: queryStart.Add(+6*time.Minute).Unix() * 1000, Value: 47},
						mimirpb.Sample{TimestampMs: queryStart.Add(+7*time.Minute).Unix() * 1000, Value: 53},
					}),
				},
			},
		},
		nil)

	limits := defaultLimitsConfig()
	limits.QueryIngestersWithin = 0 // Always query ingesters in this test.
	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:     logger,
		MaxSamples: 1e6,
		Timeout:    1 * time.Minute,
	})

	queryable, _, _, err := New(cfg, overrides, distributor, nil, nil, logger, nil)
	require.NoError(t, err)

	ctx := user.InjectOrgID(context.Background(), "user-1")
	query, err := engine.NewRangeQuery(ctx, queryable, nil, `sum({__name__=~".+"})`, queryStart, queryEnd, queryStep)
	require.NoError(t, err)

	r := query.Exec(ctx)
	m, err := r.Matrix()
	require.NoError(t, err)

	require.Equal(t, 1, m.Len())
	assert.Equal(t, []promql.FPoint{
		{T: 1635746400000, F: 29},
		{T: 1635746460000, F: 31},
		{T: 1635746520000, F: 37},
		{T: 1635746580000, F: 37},
		{T: 1635746640000, F: 78},
		{T: 1635746700000, F: 80},
	}, m[0].Floats)
}

// TestBatchMergeChunks is a regression test to catch one particular case
// when the Batch merger iterator was corrupting memory by not copying
// Batches by value because the Batch itself was not possible to copy
// by value.
func TestBatchMergeChunks(t *testing.T) {
	var (
		logger     = log.NewNopLogger()
		queryStart = mustParseTime("2021-11-01T06:00:00Z")
		queryEnd   = mustParseTime("2021-11-01T06:01:00Z")
		queryStep  = time.Second
	)

	var cfg Config
	flagext.DefaultValues(&cfg)

	limits := defaultLimitsConfig()
	limits.QueryIngestersWithin = 0 // Always query ingesters in this test.
	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	s1 := []mimirpb.Sample{}
	s2 := []mimirpb.Sample{}

	for i := 0; i < 12; i++ {
		s1 = append(s1, mimirpb.Sample{Value: float64(i * 15000), TimestampMs: queryStart.Add(time.Duration(i) * time.Second).UnixMilli()})
		if i != 9 { // let series 3 miss a point
			s2 = append(s2, mimirpb.Sample{Value: float64(i * 15000), TimestampMs: queryStart.Add(time.Duration(i) * time.Second).UnixMilli()})
		}
	}

	c1 := convertToChunks(t, samplesToInterface(s1))
	c2 := convertToChunks(t, samplesToInterface(s2))
	chunks12 := []client.Chunk{}
	chunks12 = append(chunks12, c1...)
	chunks12 = append(chunks12, c2...)

	chunks21 := []client.Chunk{}
	chunks21 = append(chunks21, c2...)
	chunks21 = append(chunks21, c1...)

	// Mock distributor to return chunks that need merging.
	distributor := &mockDistributor{}
	distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		client.CombinedQueryStreamResponse{
			Chunkseries: []client.TimeSeriesChunk{
				// Series with chunks in the 1,2 order, that need merge
				{
					Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "one"}, {Name: labels.InstanceName, Value: "foo"}},
					Chunks: chunks12,
				},
				// Series with chunks in the 2,1 order, that need merge
				{
					Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "one"}, {Name: labels.InstanceName, Value: "bar"}},
					Chunks: chunks21,
				},
			},
		},
		nil)

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:     logger,
		MaxSamples: 1e6,
		Timeout:    1 * time.Minute,
	})

	queryable, _, _, err := New(cfg, overrides, distributor, nil, nil, logger, nil)
	require.NoError(t, err)

	ctx := user.InjectOrgID(context.Background(), "user-1")
	query, err := engine.NewRangeQuery(ctx, queryable, nil, `rate({__name__=~".+"}[10s])`, queryStart, queryEnd, queryStep)
	require.NoError(t, err)

	r := query.Exec(ctx)
	m, err := r.Matrix()
	require.NoError(t, err)

	require.Equal(t, 2, m.Len())
	require.ElementsMatch(t, m[0].Floats, m[1].Floats)
	require.ElementsMatch(t, m[0].Histograms, m[1].Histograms)
}

func BenchmarkQueryExecute(b *testing.B) {
	var (
		logger    = log.NewNopLogger()
		queryStep = time.Second
	)

	var cfg Config
	flagext.DefaultValues(&cfg)

	limits := defaultLimitsConfig()
	limits.QueryIngestersWithin = 0 // Always query ingesters in this test.
	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(b, err)

	scenarios := []struct {
		numChunks          int
		numSamplesPerChunk int
		duplicationFactor  int
	}{
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 1},
		{numChunks: 1000, numSamplesPerChunk: 100, duplicationFactor: 3},
		{numChunks: 100, numSamplesPerChunk: 100, duplicationFactor: 1},
		{numChunks: 100, numSamplesPerChunk: 100, duplicationFactor: 3},
		{numChunks: 1, numSamplesPerChunk: 100, duplicationFactor: 1},
		{numChunks: 1, numSamplesPerChunk: 100, duplicationFactor: 3},
	}

	for _, scenario := range scenarios {
		for _, encoding := range []chunk.Encoding{
			chunk.PrometheusXorChunk,
			chunk.PrometheusHistogramChunk,
			chunk.PrometheusFloatHistogramChunk,
		} {
			name := fmt.Sprintf("chunks: %d samples per chunk: %d duplication factor: %d encoding: %s", scenario.numChunks, scenario.numSamplesPerChunk, scenario.duplicationFactor, encoding)
			queryStart := time.Now().Add(-time.Second * time.Duration(scenario.numChunks*scenario.numSamplesPerChunk))
			queryEnd := time.Now()
			chunks := createChunks(b, scenario.numChunks, scenario.numSamplesPerChunk, scenario.duplicationFactor, queryStart, queryStep, encoding)
			// Mock distributor to return chunks that need merging.
			distributor := &mockDistributor{}
			distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
				client.CombinedQueryStreamResponse{
					Chunkseries: []client.TimeSeriesChunk{
						{
							Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "one"}, {Name: labels.InstanceName, Value: "foo"}},
							Chunks: chunks,
						},
					},
				},
				nil)

			engine := promql.NewEngine(promql.EngineOpts{
				Logger:     logger,
				MaxSamples: 1e6,
				Timeout:    1 * time.Minute,
			})

			queryable, _, _, err := New(cfg, overrides, distributor, nil, nil, logger, nil)
			require.NoError(b, err)

			ctx := user.InjectOrgID(context.Background(), "user-1")

			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					query, err := engine.NewRangeQuery(ctx, queryable, nil, `rate({__name__=~".+"}[10s])`, queryStart, queryEnd, queryStep)
					require.NoError(b, err)

					r := query.Exec(ctx)
					m, err := r.Matrix()
					require.NoError(b, err)

					require.Equal(b, 1, m.Len())
				}
			})
		}
	}
}

func mockTSDB(t *testing.T, mint model.Time, samples int, step, chunkOffset time.Duration, samplesPerChunk int, valueType func(model.Time) chunkenc.ValueType) (storage.Queryable, model.Time) {
	dir := t.TempDir()

	opts := tsdb.DefaultHeadOptions()
	opts.EnableNativeHistograms.Store(true)
	opts.ChunkDirRoot = dir
	// We use TSDB head only. By using full TSDB DB, and appending samples to it, closing it would cause unnecessary HEAD compaction, which slows down the test.
	head, err := tsdb.NewHead(nil, nil, nil, nil, opts, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = head.Close()
	})

	app := head.Appender(context.Background())

	l := labels.FromStrings(model.MetricNameLabel, "foo")

	cnt := 0
	chunkStartTs := mint
	ts := chunkStartTs
	for i := 0; i < samples; i++ {
		valType := valueType(ts)
		switch valType {
		case chunkenc.ValFloat:
			_, err := app.Append(0, l, int64(ts), float64(ts))
			require.NoError(t, err)
		case chunkenc.ValHistogram:
			_, err := app.AppendHistogram(0, l, int64(ts), test.GenerateTestHistogram(int(ts)), nil)
			require.NoError(t, err)
		case chunkenc.ValFloatHistogram:
			_, err := app.AppendHistogram(0, l, int64(ts), nil, test.GenerateTestFloatHistogram(int(ts)))
			require.NoError(t, err)
		default:
			t.Errorf("Unknown chunk type %v", valType)
		}

		cnt++

		ts = ts.Add(step)

		if cnt%samplesPerChunk == 0 {
			// Simulate next chunk, restart timestamp.
			chunkStartTs = chunkStartTs.Add(chunkOffset)
			ts = chunkStartTs
		}
	}

	require.NoError(t, app.Commit())
	queryable := storage.QueryableFunc(func(mint, maxt int64) (storage.Querier, error) {
		return tsdb.NewBlockQuerier(head, mint, maxt)
	})

	return queryable, ts
}

func createChunks(b require.TestingT, numChunks, numSamplesPerChunk, duplicationFactor int, queryStart time.Time, step time.Duration, enc chunk.Encoding) []client.Chunk {
	result := make([]chunk.Chunk, 0, numChunks)

	for d := 0; d < duplicationFactor; d++ {
		for c := 0; c < numChunks; c++ {
			minTime := queryStart.Add(step * time.Duration(c*numSamplesPerChunk))
			maxTime := minTime.Add(step * time.Duration(numSamplesPerChunk))
			result = append(result, mkChunk(b, model.TimeFromUnixNano(minTime.UnixNano()), model.TimeFromUnixNano(maxTime.UnixNano()), step, enc))
		}
	}

	chunks, err := client.ToChunks(result)
	require.NoError(b, err)
	return chunks
}

func TestQuerier_QueryIngestersWithinConfig(t *testing.T) {
	testCases := []struct {
		name                 string
		mint, maxt           time.Time
		hitIngester          bool
		queryIngestersWithin time.Duration
	}{
		{
			name:                 "hit-test1",
			mint:                 time.Now().Add(-5 * time.Hour),
			maxt:                 time.Now().Add(1 * time.Hour),
			hitIngester:          true,
			queryIngestersWithin: 1 * time.Hour,
		},
		{
			name:                 "hit-test2",
			mint:                 time.Now().Add(-5 * time.Hour),
			maxt:                 time.Now().Add(-59 * time.Minute),
			hitIngester:          true,
			queryIngestersWithin: 1 * time.Hour,
		},
		{ // Skipping ingester is disabled.
			name:                 "hit-test2",
			mint:                 time.Now().Add(-5 * time.Hour),
			maxt:                 time.Now().Add(-50 * time.Minute),
			hitIngester:          true,
			queryIngestersWithin: 0,
		},
		{
			name:                 "dont-hit-test1",
			mint:                 time.Now().Add(-5 * time.Hour),
			maxt:                 time.Now().Add(-100 * time.Minute),
			hitIngester:          false,
			queryIngestersWithin: 1 * time.Hour,
		},
		{
			name:                 "dont-hit-test2",
			mint:                 time.Now().Add(-5 * time.Hour),
			maxt:                 time.Now().Add(-61 * time.Minute),
			hitIngester:          false,
			queryIngestersWithin: 1 * time.Hour,
		},
	}

	dir := t.TempDir()
	queryTracker := promql.NewActiveQueryTracker(dir, 10, log.NewNopLogger())

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:             log.NewNopLogger(),
		ActiveQueryTracker: queryTracker,
		MaxSamples:         1e6,
		Timeout:            1 * time.Minute,
	})
	cfg := Config{QueryEngine: prometheusEngine}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			distributor := &errDistributor{}

			limits := defaultLimitsConfig()
			limits.QueryIngestersWithin = model.Duration(c.queryIngestersWithin)
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			queryable, _, _, err := New(cfg, overrides, distributor, nil, nil, log.NewNopLogger(), nil)
			require.NoError(t, err)
			ctx := user.InjectOrgID(context.Background(), "0")
			query, err := engine.NewRangeQuery(ctx, queryable, nil, "dummy", c.mint, c.maxt, 1*time.Minute)
			require.NoError(t, err)

			r := query.Exec(ctx)
			_, err = r.Matrix()

			if c.hitIngester {
				// If the ingester was hit, the distributor always returns errDistributorError. Prometheus
				// wrap any Select() error into "expanding series", so we do wrap it as well to have a match.
				require.Error(t, err)
				require.Equal(t, errors.Wrap(errDistributorError, "expanding series").Error(), err.Error())
			} else {
				// If the ingester was hit, there would have been an error from errDistributor.
				require.NoError(t, err)
			}
		})
	}
}

func TestQuerier_ValidateQueryTimeRange(t *testing.T) {
	const engineLookbackDelta = 5 * time.Minute

	now := time.Now()

	tests := map[string]struct {
		queryStartTime    time.Time
		queryEndTime      time.Time
		expectedStartTime time.Time
		expectedEndTime   time.Time
	}{
		"should manipulate query if end time is after the limit": {
			queryStartTime:    now.Add(-5 * time.Hour),
			queryEndTime:      now.Add(1 * time.Hour),
			expectedStartTime: now.Add(-5 * time.Hour).Add(-engineLookbackDelta),
			expectedEndTime:   now.Add(1 * time.Hour),
		},
		"should not manipulate query if end time is far in the future": {
			queryStartTime:    now.Add(-5 * time.Hour),
			queryEndTime:      now.Add(100 * time.Hour),
			expectedStartTime: now.Add(-5 * time.Hour).Add(-engineLookbackDelta),
			expectedEndTime:   now.Add(100 * time.Hour),
		},
		"should manipulate query if start time is far in the future": {
			queryStartTime:    now.Add(50 * time.Minute),
			queryEndTime:      now.Add(60 * time.Minute),
			expectedStartTime: now.Add(50 * time.Minute).Add(-engineLookbackDelta),
			expectedEndTime:   now.Add(60 * time.Minute),
		},
	}

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:        log.NewNopLogger(),
		MaxSamples:    1e6,
		Timeout:       1 * time.Minute,
		LookbackDelta: engineLookbackDelta,
	})

	cfg := Config{}
	flagext.DefaultValues(&cfg)

	for name, c := range tests {
		t.Run(name, func(t *testing.T) {
			// We don't need to query any data for this test, so an empty store is fine.
			distributor := &mockDistributor{}
			distributor.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(model.Matrix{}, nil)
			distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(client.CombinedQueryStreamResponse{}, nil)

			overrides, err := validation.NewOverrides(defaultLimitsConfig(), nil)
			require.NoError(t, err)

			queryable, _, _, err := New(cfg, overrides, distributor, nil, nil, log.NewNopLogger(), nil)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "0")
			query, err := engine.NewRangeQuery(ctx, queryable, nil, "dummy", c.queryStartTime, c.queryEndTime, time.Minute)
			require.NoError(t, err)

			r := query.Exec(ctx)
			require.Nil(t, r.Err)

			_, err = r.Matrix()
			require.Nil(t, err)

			// Assert on the time range of the actual executed query (5s delta).
			delta := float64(5000)
			require.Len(t, distributor.Calls, 1)
			assert.InDelta(t, util.TimeToMillis(c.expectedStartTime), int64(distributor.Calls[0].Arguments.Get(2).(model.Time)), delta)
			assert.InDelta(t, util.TimeToMillis(c.expectedEndTime), int64(distributor.Calls[0].Arguments.Get(3).(model.Time)), delta)
		})
	}
}

func TestQuerier_ValidateQueryTimeRange_MaxQueryLength(t *testing.T) {
	const maxQueryLength = 30 * 24 * time.Hour

	tests := map[string]struct {
		query          string
		queryStartTime time.Time
		queryEndTime   time.Time
		expected       error
	}{
		"should allow query on short time range and rate time window close to the limit": {
			query:          "rate(foo[29d])",
			queryStartTime: time.Now().Add(-time.Hour),
			queryEndTime:   time.Now(),
			expected:       nil,
		},
		"should allow query on large time range close to the limit and short rate time window": {
			query:          "rate(foo[1m])",
			queryStartTime: time.Now().Add(-maxQueryLength).Add(time.Hour),
			queryEndTime:   time.Now(),
			expected:       nil,
		},
		"should forbid query on short time range and rate time window over the limit": {
			query:          "rate(foo[31d])",
			queryStartTime: time.Now().Add(-time.Hour),
			queryEndTime:   time.Now(),
			expected:       errors.Errorf("expanding series: %s", NewMaxQueryLengthError(745*time.Hour, 720*time.Hour)),
		},
		"should forbid query on large time range over the limit and short rate time window": {
			query:          "rate(foo[1m])",
			queryStartTime: time.Now().Add(-maxQueryLength).Add(-time.Hour),
			queryEndTime:   time.Now(),
			expected:       errors.Errorf("expanding series: %s", NewMaxQueryLengthError((721*time.Hour)+time.Minute, 720*time.Hour)),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			var cfg Config
			flagext.DefaultValues(&cfg)

			limits := defaultLimitsConfig()
			limits.MaxPartialQueryLength = model.Duration(maxQueryLength)
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			// We don't need to query any data for this test, so an empty distributor is fine.
			distributor := &emptyDistributor{}
			queryable, _, _, err := New(cfg, overrides, distributor, nil, nil, log.NewNopLogger(), nil)
			require.NoError(t, err)

			// Create the PromQL engine to execute the query.
			engine := promql.NewEngine(promql.EngineOpts{
				Logger:             log.NewNopLogger(),
				ActiveQueryTracker: nil,
				MaxSamples:         1e6,
				Timeout:            1 * time.Minute,
			})

			ctx := user.InjectOrgID(context.Background(), "test")
			query, err := engine.NewRangeQuery(ctx, queryable, nil, testData.query, testData.queryStartTime, testData.queryEndTime, time.Minute)
			require.NoError(t, err)

			r := query.Exec(ctx)

			if testData.expected != nil {
				require.NotNil(t, r.Err)
				assert.Equal(t, testData.expected.Error(), r.Err.Error())
			} else {
				assert.Nil(t, r.Err)
			}
		})
	}
}

func TestQuerier_ValidateQueryTimeRange_MaxQueryLookback(t *testing.T) {
	const (
		engineLookbackDelta = 5 * time.Minute
		thirtyDays          = 30 * 24 * time.Hour
	)

	now := time.Now()

	tests := map[string]struct {
		maxQueryLookback          model.Duration
		query                     string
		queryStartTime            time.Time
		queryEndTime              time.Time
		expectedSkipped           bool
		expectedQueryStartTime    time.Time
		expectedQueryEndTime      time.Time
		expectedMetadataStartTime time.Time
		expectedMetadataEndTime   time.Time
	}{
		"should not manipulate time range for a query on short time range and rate time window close to the limit": {
			maxQueryLookback:          model.Duration(thirtyDays),
			query:                     "rate(foo[29d])",
			queryStartTime:            now.Add(-time.Hour),
			queryEndTime:              now,
			expectedQueryStartTime:    now.Add(-time.Hour).Add(-29 * 24 * time.Hour),
			expectedQueryEndTime:      now,
			expectedMetadataStartTime: now.Add(-time.Hour),
			expectedMetadataEndTime:   now,
		},
		"should not manipulate a query on large time range close to the limit and short rate time window": {
			maxQueryLookback:          model.Duration(thirtyDays),
			query:                     "rate(foo[1m])",
			queryStartTime:            now.Add(-thirtyDays).Add(time.Hour),
			queryEndTime:              now,
			expectedQueryStartTime:    now.Add(-thirtyDays).Add(time.Hour).Add(-time.Minute),
			expectedQueryEndTime:      now,
			expectedMetadataStartTime: now.Add(-thirtyDays).Add(time.Hour),
			expectedMetadataEndTime:   now,
		},
		"should manipulate a query on short time range and rate time window over the limit": {
			maxQueryLookback:          model.Duration(thirtyDays),
			query:                     "rate(foo[31d])",
			queryStartTime:            now.Add(-time.Hour),
			queryEndTime:              now,
			expectedQueryStartTime:    now.Add(-thirtyDays),
			expectedQueryEndTime:      now,
			expectedMetadataStartTime: now.Add(-time.Hour),
			expectedMetadataEndTime:   now,
		},
		"should manipulate a query on large time range over the limit and short rate time window": {
			maxQueryLookback:          model.Duration(thirtyDays),
			query:                     "rate(foo[1m])",
			queryStartTime:            now.Add(-thirtyDays).Add(-100 * time.Hour),
			queryEndTime:              now,
			expectedQueryStartTime:    now.Add(-thirtyDays),
			expectedQueryEndTime:      now,
			expectedMetadataStartTime: now.Add(-thirtyDays),
			expectedMetadataEndTime:   now,
		},
		"should skip executing a query outside the allowed time range": {
			maxQueryLookback: model.Duration(thirtyDays),
			query:            "rate(foo[1m])",
			queryStartTime:   now.Add(-thirtyDays).Add(-100 * time.Hour),
			queryEndTime:     now.Add(-thirtyDays).Add(-90 * time.Hour),
			expectedSkipped:  true,
		},
	}

	logger := log.NewNopLogger()
	// Create the PromQL engine to execute the queries.
	engine := promql.NewEngine(promql.EngineOpts{
		Logger:             logger,
		ActiveQueryTracker: nil,
		MaxSamples:         1e6,
		LookbackDelta:      engineLookbackDelta,
		Timeout:            1 * time.Minute,
	})

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "test")

			var cfg Config
			flagext.DefaultValues(&cfg)

			limits := defaultLimitsConfig()
			limits.MaxQueryLookback = testData.maxQueryLookback
			limits.QueryIngestersWithin = 0 // Always query ingesters in this test.
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			t.Run("query range", func(t *testing.T) {
				distributor := &mockDistributor{}
				distributor.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(model.Matrix{}, nil)
				distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(client.CombinedQueryStreamResponse{}, nil)

				queryable, _, _, err := New(cfg, overrides, distributor, nil, nil, logger, nil)
				require.NoError(t, err)

				query, err := engine.NewRangeQuery(ctx, queryable, nil, testData.query, testData.queryStartTime, testData.queryEndTime, time.Minute)
				require.NoError(t, err)

				r := query.Exec(ctx)
				require.Nil(t, r.Err)

				_, err = r.Matrix()
				require.Nil(t, err)

				if !testData.expectedSkipped {
					// Assert on the time range of the actual executed query (5s delta).
					delta := float64(5000)
					require.Len(t, distributor.Calls, 1)
					assert.InDelta(t, util.TimeToMillis(testData.expectedQueryStartTime), int64(distributor.Calls[0].Arguments.Get(2).(model.Time)), delta)
					assert.InDelta(t, util.TimeToMillis(testData.expectedQueryEndTime), int64(distributor.Calls[0].Arguments.Get(3).(model.Time)), delta)
				} else {
					// Ensure no query has been executed (because skipped).
					assert.Len(t, distributor.Calls, 0)
				}
			})

			t.Run("series", func(t *testing.T) {
				distributor := &mockDistributor{}
				distributor.On("MetricsForLabelMatchers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]labels.Labels{}, nil)

				queryable, _, _, err := New(cfg, overrides, distributor, nil, nil, logger, nil)
				require.NoError(t, err)

				q, err := queryable.Querier(util.TimeToMillis(testData.queryStartTime), util.TimeToMillis(testData.queryEndTime))
				require.NoError(t, err)

				hints := &storage.SelectHints{
					Start: util.TimeToMillis(testData.queryStartTime),
					End:   util.TimeToMillis(testData.queryEndTime),
					Func:  "series",
				}
				matcher := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test")

				set := q.Select(ctx, false, hints, matcher)
				require.False(t, set.Next()) // Expected to be empty.
				require.NoError(t, set.Err())

				if !testData.expectedSkipped {
					// Assert on the time range of the actual executed query (5s delta).
					delta := float64(5000)
					require.Len(t, distributor.Calls, 1)
					assert.Equal(t, "MetricsForLabelMatchers", distributor.Calls[0].Method)
					assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataStartTime), int64(distributor.Calls[0].Arguments.Get(1).(model.Time)), delta)
					assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataEndTime), int64(distributor.Calls[0].Arguments.Get(2).(model.Time)), delta)
				} else {
					// Ensure no query has been executed (because skipped).
					assert.Len(t, distributor.Calls, 0)
				}
			})

			t.Run("label names", func(t *testing.T) {
				matchers := []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchNotEqual, "route", "get_user"),
				}
				distributor := &mockDistributor{}
				distributor.On("LabelNames", mock.Anything, mock.Anything, mock.Anything, matchers).Return([]string{}, nil)

				queryable, _, _, err := New(cfg, overrides, distributor, nil, nil, logger, nil)
				require.NoError(t, err)

				q, err := queryable.Querier(util.TimeToMillis(testData.queryStartTime), util.TimeToMillis(testData.queryEndTime))
				require.NoError(t, err)

				_, _, err = q.LabelNames(ctx, &storage.LabelHints{}, matchers...)
				require.NoError(t, err)

				if !testData.expectedSkipped {
					// Assert on the time range of the actual executed query (5s delta).
					delta := float64(5000)
					require.Len(t, distributor.Calls, 1)
					assert.Equal(t, "LabelNames", distributor.Calls[0].Method)
					args := distributor.Calls[0].Arguments
					assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataStartTime), int64(args.Get(1).(model.Time)), delta)
					assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataEndTime), int64(args.Get(2).(model.Time)), delta)
					assert.Equal(t, matchers, args.Get(3).([]*labels.Matcher))
				} else {
					// Ensure no query has been executed (because skipped).
					assert.Len(t, distributor.Calls, 0)
				}
			})

			t.Run("label values", func(t *testing.T) {
				distributor := &mockDistributor{}
				distributor.On("LabelValuesForLabelName", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]string{}, nil)

				queryable, _, _, err := New(cfg, overrides, distributor, nil, nil, logger, nil)
				require.NoError(t, err)

				q, err := queryable.Querier(util.TimeToMillis(testData.queryStartTime), util.TimeToMillis(testData.queryEndTime))
				require.NoError(t, err)

				_, _, err = q.LabelValues(ctx, labels.MetricName, &storage.LabelHints{})
				require.NoError(t, err)

				if !testData.expectedSkipped {
					// Assert on the time range of the actual executed query (5s delta).
					delta := float64(5000)
					require.Len(t, distributor.Calls, 1)
					assert.Equal(t, "LabelValuesForLabelName", distributor.Calls[0].Method)
					assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataStartTime), int64(distributor.Calls[0].Arguments.Get(1).(model.Time)), delta)
					assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataEndTime), int64(distributor.Calls[0].Arguments.Get(2).(model.Time)), delta)
				} else {
					// Ensure no query has been executed (because skipped).
					assert.Len(t, distributor.Calls, 0)
				}
			})
		})
	}
}

// Check that time range of /series is restricted by maxLabelsQueryLength.
// LabelName and LabelValues are checked in TestBlocksStoreQuerier_MaxLabelsQueryRange(),
// because the implementation of those makes it really hard to do in Querier.
func TestQuerier_ValidateQueryTimeRange_MaxLabelsQueryRange(t *testing.T) {
	const (
		thirtyDays = 30 * 24 * time.Hour
	)

	now := time.Now()

	tests := map[string]struct {
		maxLabelsQueryLength      model.Duration
		queryStartTime            time.Time
		queryEndTime              time.Time
		expectedMetadataStartTime time.Time
		expectedMetadataEndTime   time.Time
	}{
		"should manipulate series query on large time range over the limit": {
			maxLabelsQueryLength:      model.Duration(thirtyDays),
			queryStartTime:            now.Add(-thirtyDays).Add(-100 * time.Hour),
			queryEndTime:              now,
			expectedMetadataStartTime: now.Add(-thirtyDays),
			expectedMetadataEndTime:   now,
		},
		"should not manipulate query short time range within the limit": {
			maxLabelsQueryLength:      model.Duration(thirtyDays),
			queryStartTime:            now.Add(-time.Hour),
			queryEndTime:              now,
			expectedMetadataStartTime: now.Add(-time.Hour),
			expectedMetadataEndTime:   now,
		},
		"should manipulate the start of a query without start time": {
			maxLabelsQueryLength:      model.Duration(thirtyDays),
			queryStartTime:            v1.MinTime,
			queryEndTime:              now,
			expectedMetadataStartTime: now.Add(-thirtyDays),
			expectedMetadataEndTime:   now,
		},
		"should not manipulate query without end time, we allow querying arbitrarily into the future": {
			maxLabelsQueryLength:      model.Duration(thirtyDays),
			queryStartTime:            now.Add(-time.Hour),
			queryEndTime:              v1.MaxTime,
			expectedMetadataStartTime: now.Add(-time.Hour),
			expectedMetadataEndTime:   v1.MaxTime,
		},
		"should manipulate the start of a query without start or end time, we allow querying arbitrarily into the future, but not the past": {
			maxLabelsQueryLength:      model.Duration(thirtyDays),
			queryStartTime:            v1.MinTime,
			queryEndTime:              v1.MaxTime,
			expectedMetadataStartTime: now.Add(-thirtyDays),
			expectedMetadataEndTime:   v1.MaxTime,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "test")

			var cfg Config
			flagext.DefaultValues(&cfg)

			limits := defaultLimitsConfig()
			limits.MaxQueryLookback = model.Duration(thirtyDays * 2)
			limits.MaxLabelsQueryLength = testData.maxLabelsQueryLength
			limits.MaxPartialQueryLength = testData.maxLabelsQueryLength
			limits.MaxTotalQueryLength = testData.maxLabelsQueryLength
			limits.QueryIngestersWithin = 0 // Always query ingesters in this test.
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			distributor := &mockDistributor{}
			distributor.On("MetricsForLabelMatchers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]labels.Labels{}, nil)

			queryable, _, _, err := New(cfg, overrides, distributor, nil, nil, log.NewNopLogger(), nil)
			require.NoError(t, err)

			q, err := queryable.Querier(util.TimeToMillis(testData.queryStartTime), util.TimeToMillis(testData.queryEndTime))
			require.NoError(t, err)

			hints := &storage.SelectHints{
				Start: util.TimeToMillis(testData.queryStartTime),
				End:   util.TimeToMillis(testData.queryEndTime),
				Func:  "series",
			}
			matcher := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test")

			set := q.Select(ctx, false, hints, matcher)
			require.False(t, set.Next()) // Expected to be empty.
			require.NoError(t, set.Err())

			// Assert on the time range of the actual executed query (5s delta).
			delta := float64(5000)
			require.Len(t, distributor.Calls, 1)
			assert.Equal(t, "MetricsForLabelMatchers", distributor.Calls[0].Method)
			assert.Equal(t, "MetricsForLabelMatchers", distributor.Calls[0].Method)
			gotStartMillis := int64(distributor.Calls[0].Arguments.Get(1).(model.Time))
			assert.InDeltaf(t, util.TimeToMillis(testData.expectedMetadataStartTime), gotStartMillis, delta, "expected start %s, got %s", testData.expectedMetadataStartTime.UTC(), util.TimeFromMillis(gotStartMillis).UTC())
			gotEndMillis := int64(distributor.Calls[0].Arguments.Get(2).(model.Time))
			assert.InDeltaf(t, util.TimeToMillis(testData.expectedMetadataEndTime), gotEndMillis, delta, "expected end %s, got %s", testData.expectedMetadataEndTime.UTC(), util.TimeFromMillis(gotEndMillis).UTC())
		})
	}
}

func testRangeQuery(t testing.TB, queryable storage.Queryable, end model.Time, q query) *promql.Result {
	dir := t.TempDir()
	queryTracker := promql.NewActiveQueryTracker(dir, 10, log.NewNopLogger())

	from, through, step := time.Unix(0, 0), end.Time(), q.step
	engine := promql.NewEngine(promql.EngineOpts{
		Logger:             log.NewNopLogger(),
		ActiveQueryTracker: queryTracker,
		MaxSamples:         1e6,
		Timeout:            1 * time.Minute,
	})
	ctx := user.InjectOrgID(context.Background(), "0")
	query, err := engine.NewRangeQuery(ctx, queryable, nil, q.query, from, through, step)
	require.NoError(t, err)

	r := query.Exec(ctx)
	m, err := r.Matrix()
	require.NoError(t, err)

	require.Len(t, m, 1)
	series := m[0]
	promtestutil.RequireEqual(t, q.labels, series.Metric)
	require.Equal(t, q.samples(from, through, step), len(series.Floats)+len(series.Histograms))
	var ts int64
	for _, point := range series.Floats {
		q.assertFPoint(t, ts, point)
		ts += int64(step / time.Millisecond)
	}
	for _, point := range series.Histograms {
		q.assertHPoint(t, ts, point)
		ts += int64(step / time.Millisecond)
	}
	return r
}

type errDistributor struct{}

func (m *errDistributor) LabelNamesAndValues(_ context.Context, _ []*labels.Matcher, _ cardinality.CountMethod) (*client.LabelNamesAndValuesResponse, error) {
	return nil, errors.New("method is not implemented")
}

var errDistributorError = fmt.Errorf("errDistributorError")

func (m *errDistributor) QueryStream(context.Context, *stats.QueryMetrics, model.Time, model.Time, ...*labels.Matcher) (client.CombinedQueryStreamResponse, error) {
	return client.CombinedQueryStreamResponse{}, errDistributorError
}
func (m *errDistributor) QueryExemplars(context.Context, model.Time, model.Time, ...[]*labels.Matcher) (*client.ExemplarQueryResponse, error) {
	return nil, errDistributorError
}
func (m *errDistributor) LabelValuesForLabelName(context.Context, model.Time, model.Time, model.LabelName, ...*labels.Matcher) ([]string, error) {
	return nil, errDistributorError
}
func (m *errDistributor) LabelNames(context.Context, model.Time, model.Time, ...*labels.Matcher) ([]string, error) {
	return nil, errDistributorError
}
func (m *errDistributor) MetricsForLabelMatchers(context.Context, model.Time, model.Time, ...*labels.Matcher) ([]labels.Labels, error) {
	return nil, errDistributorError
}

func (m *errDistributor) MetricsMetadata(context.Context, *client.MetricsMetadataRequest) ([]scrape.MetricMetadata, error) {
	return nil, errDistributorError
}

func (m *errDistributor) LabelValuesCardinality(context.Context, []model.LabelName, []*labels.Matcher, cardinality.CountMethod) (uint64, *client.LabelValuesCardinalityResponse, error) {
	return 0, nil, errDistributorError
}

func (m *errDistributor) ActiveSeries(context.Context, []*labels.Matcher) ([]labels.Labels, error) {
	return nil, errDistributorError
}

func (m *errDistributor) ActiveNativeHistogramMetrics(context.Context, []*labels.Matcher) (*cardinality.ActiveNativeHistogramMetricsResponse, error) {
	return nil, errDistributorError
}

type emptyDistributor struct{}

func (d *emptyDistributor) LabelNamesAndValues(_ context.Context, _ []*labels.Matcher, _ cardinality.CountMethod) (*client.LabelNamesAndValuesResponse, error) {
	return nil, errors.New("method is not implemented")
}

func (d *emptyDistributor) QueryStream(context.Context, *stats.QueryMetrics, model.Time, model.Time, ...*labels.Matcher) (client.CombinedQueryStreamResponse, error) {
	return client.CombinedQueryStreamResponse{}, nil
}

func (d *emptyDistributor) QueryExemplars(context.Context, model.Time, model.Time, ...[]*labels.Matcher) (*client.ExemplarQueryResponse, error) {
	return nil, nil
}

func (d *emptyDistributor) LabelValuesForLabelName(context.Context, model.Time, model.Time, model.LabelName, ...*labels.Matcher) ([]string, error) {
	return nil, nil
}

func (d *emptyDistributor) LabelNames(context.Context, model.Time, model.Time, ...*labels.Matcher) ([]string, error) {
	return nil, nil
}

func (d *emptyDistributor) MetricsForLabelMatchers(context.Context, model.Time, model.Time, ...*labels.Matcher) ([]labels.Labels, error) {
	return nil, nil
}

func (d *emptyDistributor) MetricsMetadata(context.Context, *client.MetricsMetadataRequest) ([]scrape.MetricMetadata, error) {
	return nil, nil
}

func (d *emptyDistributor) LabelValuesCardinality(context.Context, []model.LabelName, []*labels.Matcher, cardinality.CountMethod) (uint64, *client.LabelValuesCardinalityResponse, error) {
	return 0, nil, nil
}

func (d *emptyDistributor) ActiveSeries(context.Context, []*labels.Matcher) ([]labels.Labels, error) {
	return nil, nil
}

func (d *emptyDistributor) ActiveNativeHistogramMetrics(context.Context, []*labels.Matcher) (*cardinality.ActiveNativeHistogramMetricsResponse, error) {
	return &cardinality.ActiveNativeHistogramMetricsResponse{}, nil
}

func TestQuerier_QueryStoreAfterConfig(t *testing.T) {
	testCases := []struct {
		name                 string
		mint, maxt           time.Time
		queryIngestersWithin time.Duration
		queryStoreAfter      time.Duration
		expectedHitIngester  bool
		expectedHitStorage   bool
	}{
		{
			name:                 "hit only ingester",
			mint:                 time.Now().Add(-5 * time.Minute),
			maxt:                 time.Now(),
			expectedHitIngester:  true,
			expectedHitStorage:   false,
			queryIngestersWithin: 1 * time.Hour,
			queryStoreAfter:      time.Hour,
		},
		{
			name:                 "hit both",
			mint:                 time.Now().Add(-5 * time.Hour),
			maxt:                 time.Now(),
			expectedHitIngester:  true,
			expectedHitStorage:   true,
			queryIngestersWithin: 1 * time.Hour,
			queryStoreAfter:      time.Hour,
		},
		{
			name:                 "hit only storage",
			mint:                 time.Now().Add(-5 * time.Hour),
			maxt:                 time.Now().Add(-2 * time.Hour),
			expectedHitIngester:  false,
			expectedHitStorage:   true,
			queryIngestersWithin: 1 * time.Hour,
			queryStoreAfter:      0,
		},
	}

	dir := t.TempDir()
	queryTracker := promql.NewActiveQueryTracker(dir, 10, log.NewNopLogger())

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:             log.NewNopLogger(),
		ActiveQueryTracker: queryTracker,
		MaxSamples:         1e6,
		Timeout:            1 * time.Minute,
	})

	cfg := Config{}
	flagext.DefaultValues(&cfg)

	for _, c := range testCases {
		cfg.QueryStoreAfter = c.queryStoreAfter
		t.Run(c.name, func(t *testing.T) {
			distributor := &errDistributor{}

			limits := defaultLimitsConfig()
			limits.QueryIngestersWithin = model.Duration(c.queryIngestersWithin)
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			// Mock the blocks storage to return an empty SeriesSet (we just need to check whether
			// it was hit or not).
			expectedMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "metric")}
			querier := &mockBlocksStorageQuerier{}
			querier.On("Select", mock.Anything, true, mock.Anything, expectedMatchers).Return(storage.EmptySeriesSet())

			querierQueryables := []TimeRangeQueryable{
				NewStoreGatewayTimeRangeQueryable(newMockBlocksStorageQueryable(querier), cfg),
			}

			queryable, _, _, err := New(cfg, overrides, distributor, querierQueryables, nil, log.NewNopLogger(), nil)
			require.NoError(t, err)
			ctx := user.InjectOrgID(context.Background(), "0")
			query, err := engine.NewRangeQuery(ctx, queryable, nil, "metric", c.mint, c.maxt, 1*time.Minute)
			require.NoError(t, err)

			r := query.Exec(ctx)
			_, err = r.Matrix()

			if c.expectedHitIngester {
				// If the ingester was hit, the distributor always returns errDistributorError. Prometheus
				// wrap any Select() error into "expanding series", so we do wrap it as well to have a match.
				require.Error(t, err)
				require.Equal(t, errors.Wrap(errDistributorError, "expanding series").Error(), err.Error())
			} else {
				// If the ingester was hit, there would have been an error from errDistributor.
				require.NoError(t, err)
			}

			// Verify if the test did/did not hit the LTS
			time.Sleep(30 * time.Millisecond) // NOTE: Since this is a lazy querier there is a race condition between the response and chunk store being called

			if c.expectedHitStorage {
				querier.AssertCalled(t, "Select", mock.Anything, true, mock.Anything, expectedMatchers)
			} else {
				querier.AssertNotCalled(t, "Select", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			}
		})
	}
}

func TestConfig_ValidateLimits(t *testing.T) {
	tests := map[string]struct {
		setup    func(cfg *Config, limits *validation.Limits)
		expected error
	}{
		"should pass with default config": {
			setup: func(*Config, *validation.Limits) {},
		},
		"should pass if 'query store after' is enabled and shuffle-sharding is disabled": {
			setup: func(cfg *Config, _ *validation.Limits) {
				cfg.QueryStoreAfter = time.Hour
			},
		},
		"should pass if both 'query store after' and 'query ingesters within' are set and 'query store after' < 'query ingesters within'": {
			setup: func(cfg *Config, limits *validation.Limits) {
				cfg.QueryStoreAfter = time.Hour
				limits.QueryIngestersWithin = model.Duration(2 * time.Hour)
			},
		},
		"should fail if both 'query store after' and 'query ingesters within' are set and 'query store after' > 'query ingesters within'": {
			setup: func(cfg *Config, limits *validation.Limits) {
				cfg.QueryStoreAfter = 3 * time.Hour
				limits.QueryIngestersWithin = model.Duration(2 * time.Hour)
			},
			expected: errBadLookbackConfigs,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := &Config{}
			flagext.DefaultValues(cfg)
			limits := defaultLimitsConfig()
			testData.setup(cfg, &limits)
			assert.Equal(t, testData.expected, cfg.ValidateLimits(limits))
		})
	}
}

func TestClampMaxTime(t *testing.T) {
	logger := spanlogger.FromContext(context.Background(), log.NewNopLogger())

	now := time.Now()

	testCases := []struct {
		testName            string
		originalMaxT        int64
		referenceT          int64
		limitDelta          time.Duration
		expectedClampedMaxT int64
	}{
		{
			testName:            "no clamp for maxT in past when a limit is disabled by setting to 0",
			originalMaxT:        now.Add(-2 * time.Hour).UnixMilli(),
			referenceT:          now.UnixMilli(),
			limitDelta:          0,
			expectedClampedMaxT: now.Add(-2 * time.Hour).UnixMilli(),
		},
		{
			testName:            "no clamp for maxT in the future when a limit is disabled by setting to 0",
			originalMaxT:        now.Add(2 * time.Hour).UnixMilli(),
			referenceT:          now.UnixMilli(),
			limitDelta:          0,
			expectedClampedMaxT: now.Add(2 * time.Hour).UnixMilli(),
		},
		{
			// scenario:
			// * limit set to truncate any query if the query maxT more than 1 hour into the future
			// * originalMinT is the original query maxT, now + 2 hours
			// * referenceT is now
			// * since the query maxT can never be more than 1 hour into the future,
			// the original maxT will be clamped backwards in time to now + 1 hour
			testName:            "clamp maxT due to max query into future",
			originalMaxT:        now.Add(2 * time.Hour).UnixMilli(),
			referenceT:          now.UnixMilli(),
			limitDelta:          1 * time.Hour,
			expectedClampedMaxT: now.Add(1 * time.Hour).UnixMilli(),
		},
		{
			// scenario:
			// * limit set to only query the block store if the query range is previous to the last 4 hours
			// * originalMinT is the original query maxT, now - 3 hours, sent to the block store querier
			// * since the block store querier should only be queried for data older than the last 4 hours,
			// the original maxT will be clamped backwards in time to now - 4 hours
			testName:            "clamp maxT due to query store after",
			originalMaxT:        now.Add(-3 * time.Hour).UnixMilli(),
			referenceT:          now.UnixMilli(),
			limitDelta:          -4 * time.Hour,
			expectedClampedMaxT: now.Add(-4 * time.Hour).UnixMilli(),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.testName, func(t *testing.T) {
			clampedMaxT := clampMaxTime(logger, testCase.originalMaxT, testCase.referenceT, testCase.limitDelta, "")
			assert.Equal(t, testCase.expectedClampedMaxT, clampedMaxT)
		})
	}
}

func TestClampMinTime(t *testing.T) {
	logger := spanlogger.FromContext(context.Background(), log.NewNopLogger())

	now := time.Now()

	testCases := []struct {
		testName            string
		originalMinT        int64
		referenceT          int64
		limitDelta          time.Duration
		expectedClampedMinT int64
	}{
		{
			testName:            "no clamp for minT when a limit is disabled by setting to 0",
			originalMinT:        now.Add(-2 * time.Hour).UnixMilli(),
			referenceT:          now.UnixMilli(),
			limitDelta:          0,
			expectedClampedMinT: now.Add(-2 * time.Hour).UnixMilli(),
		},
		{
			// scenario:
			// * limit set to truncate any query if the query minT is older than 24 hours
			// * originalMinT is the original query minT, now - 48 hours
			// * referenceT is now
			// * since the query minT can never be older than 24 hours,
			// the original minT will be clamped forwards in time to now - 24 hours
			testName:            "clamp minT due to max query lookback",
			originalMinT:        now.Add(-48 * time.Hour).UnixMilli(),
			referenceT:          now.UnixMilli(),
			limitDelta:          -24 * time.Hour,
			expectedClampedMinT: now.Add(-24 * time.Hour).UnixMilli(),
		},
		{
			// scenario:
			// * limit set to truncate the label query range if the query range is longer than 6 hours
			// * originalMinT is the original query minT, now - 12 hours
			// * referenceT is the query maxT, now - 3 hours
			// * since the entire label query range cannot be longer than 6 hours
			// the original minT will be clamped forwards in time to query maxT - 6 hours
			testName:            "clamp minT due to max label query length",
			originalMinT:        now.Add(-12 * time.Hour).UnixMilli(),
			referenceT:          now.Add(-3 * time.Hour).UnixMilli(),
			limitDelta:          -6 * time.Hour,
			expectedClampedMinT: now.Add(-9 * time.Hour).UnixMilli(),
		},
		{
			// scenario:
			// * limit set to only query the ingesters if the query range is within the last 6 hours
			// * originalMinT is the original query minT, now - 9 hours, sent to the ingester querier
			// * since the ingester querier should only be queried for data within than the last 6 hours,
			// the original minT will be clamped forwards in time to now - 6 hours
			testName:            "clamp minT due to query ingesters within",
			originalMinT:        now.Add(-9 * time.Hour).UnixMilli(),
			referenceT:          now.UnixMilli(),
			limitDelta:          -6 * time.Hour,
			expectedClampedMinT: now.Add(-6 * time.Hour).UnixMilli(),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.testName, func(t *testing.T) {
			clampedMaxT := clampMinTime(logger, testCase.originalMinT, testCase.referenceT, testCase.limitDelta, "")
			assert.Equal(t, testCase.expectedClampedMinT, clampedMaxT)
		})
	}
}

func defaultLimitsConfig() validation.Limits {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	return limits
}

func mustParseTime(input string) time.Time {
	parsed, err := time.Parse(time.RFC3339, input)
	if err != nil {
		panic(err)
	}
	return parsed
}

type mockBlocksStorageQueryable struct {
	querier storage.Querier
}

func newMockBlocksStorageQueryable(querier storage.Querier) *mockBlocksStorageQueryable {
	return &mockBlocksStorageQueryable{
		querier: querier,
	}
}

// Querier implements storage.Queryable.
func (m *mockBlocksStorageQueryable) Querier(int64, int64) (storage.Querier, error) {
	return m.querier, nil
}

type mockBlocksStorageQuerier struct {
	mock.Mock
}

func (m *mockBlocksStorageQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	args := m.Called(ctx, sortSeries, hints, matchers)
	return args.Get(0).(storage.SeriesSet)
}

func (m *mockBlocksStorageQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	args := m.Called(ctx, name, hints, matchers)
	return args.Get(0).([]string), args.Get(1).(annotations.Annotations), args.Error(2)
}

func (m *mockBlocksStorageQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	args := m.Called(ctx, matchers, hints)
	return args.Get(0).([]string), args.Get(1).(annotations.Annotations), args.Error(2)
}

func (m *mockBlocksStorageQuerier) Close() error {
	return nil
}

func TestTenantQueryLimitsProvider(t *testing.T) {
	tenantLimits := &staticTenantLimits{
		limits: map[string]*validation.Limits{
			"user-1": {
				MaxEstimatedMemoryConsumptionPerQuery: 1000,
			},
			"user-2": {
				MaxEstimatedMemoryConsumptionPerQuery: 10,
			},
			"user-3": {
				MaxEstimatedMemoryConsumptionPerQuery: 3000,
			},
			"unlimited-user": {
				MaxEstimatedMemoryConsumptionPerQuery: 0,
			},
		},
	}

	overrides, err := validation.NewOverrides(defaultLimitsConfig(), tenantLimits)
	require.NoError(t, err)

	provider := &tenantQueryLimitsProvider{
		limits: overrides,
	}

	testCases := map[string]struct {
		ctx           context.Context
		expectedLimit uint64
		expectedError error
	}{
		"no tenant ID provided": {
			ctx:           context.Background(),
			expectedError: user.ErrNoOrgID,
		},
		"single tenant ID provided, has limit": {
			ctx:           user.InjectOrgID(context.Background(), "user-1"),
			expectedLimit: 1000,
		},
		"single tenant ID provided, unlimited": {
			ctx:           user.InjectOrgID(context.Background(), "unlimited-user"),
			expectedLimit: 0,
		},
		"multiple tenant IDs provided, all have limits": {
			ctx:           user.InjectOrgID(context.Background(), "user-1|user-2|user-3"),
			expectedLimit: 4010,
		},
		"multiple tenant IDs provided, one unlimited": {
			ctx:           user.InjectOrgID(context.Background(), "user-1|unlimited-user|user-3"),
			expectedLimit: 0,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actualLimit, actualErr := provider.GetMaxEstimatedMemoryConsumptionPerQuery(testCase.ctx)

			if testCase.expectedError == nil {
				require.NoError(t, actualErr)
				require.Equal(t, testCase.expectedLimit, actualLimit)
			} else {
				require.ErrorIs(t, actualErr, testCase.expectedError)
			}
		})
	}
}

type staticTenantLimits struct {
	limits map[string]*validation.Limits
}

func (s *staticTenantLimits) ByUserID(userID string) *validation.Limits {
	return s.limits[userID]
}

func (s *staticTenantLimits) AllByUserID() map[string]*validation.Limits {
	return s.limits
}
