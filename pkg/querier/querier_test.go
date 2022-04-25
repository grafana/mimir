// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/querier_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/mock"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util"
)

const (
	chunkOffset     = 1 * time.Hour
	chunkLength     = 3 * time.Hour
	sampleRate      = 15 * time.Second
	samplesPerChunk = chunkLength / sampleRate
)

type query struct {
	query    string
	labels   labels.Labels
	samples  func(from, through time.Time, step time.Duration) int
	expected func(t int64) (int64, float64)
	step     time.Duration
}

func TestQuerier(t *testing.T) {
	var cfg Config
	flagext.DefaultValues(&cfg)

	const chunks = 24

	queries := []query{
		// Windowed rates with small step;  This will cause BufferedIterator to read
		// all the samples.
		{
			query:  "rate(foo[1m])",
			step:   sampleRate * 4,
			labels: labels.Labels{},
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from) / step)
			},
			expected: func(t int64) (int64, float64) {
				return t + int64((sampleRate*4)/time.Millisecond), 1000.0
			},
		},

		// Very simple single-point gets, with low step.  Performance should be
		// similar to above.
		{
			query: "foo",
			step:  sampleRate * 4,
			labels: labels.Labels{
				labels.Label{Name: model.MetricNameLabel, Value: "foo"},
			},
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from)/step) + 1
			},
			expected: func(t int64) (int64, float64) {
				return t, float64(t)
			},
		},

		// Rates with large step; excersise everything.
		{
			query:  "rate(foo[1m])",
			step:   sampleRate * 4 * 10,
			labels: labels.Labels{},
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from) / step)
			},
			expected: func(t int64) (int64, float64) {
				return t + int64((sampleRate*4)/time.Millisecond)*10, 1000.0
			},
		},

		// Single points gets with large step; excersise Seek performance.
		{
			query: "foo",
			step:  sampleRate * 4 * 10,
			labels: labels.Labels{
				labels.Label{Name: model.MetricNameLabel, Value: "foo"},
			},
			samples: func(from, through time.Time, step time.Duration) int {
				return int(through.Sub(from)/step) + 1
			},
			expected: func(t int64) (int64, float64) {
				return t, float64(t)
			},
		},
	}

	// Generate TSDB head used to simulate querying the long-term storage.
	db, through := mockTSDB(t, model.Time(0), int(chunks*samplesPerChunk), sampleRate, chunkOffset, int(samplesPerChunk))

	for _, query := range queries {
		for _, iterators := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/iterators=%t", query.query, iterators), func(t *testing.T) {
				cfg.Iterators = iterators

				// No samples returned by ingesters.
				distributor := &mockDistributor{}
				distributor.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&client.QueryResponse{}, nil)
				distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&client.QueryStreamResponse{}, nil)

				overrides, err := validation.NewOverrides(defaultLimitsConfig(), nil)
				require.NoError(t, err)

				queryables := []QueryableWithFilter{UseAlwaysQueryable(db)}
				queryable, _, _ := New(cfg, overrides, distributor, queryables, nil, log.NewNopLogger(), nil)
				testRangeQuery(t, queryable, through, query)
			})
		}
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
	cfg.QueryIngestersWithin = 0 // Always query ingesters in this test.

	// Mock distributor to return chunks containing samples outside the queried range.
	distributor := &mockDistributor{}
	distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		&client.QueryStreamResponse{
			Chunkseries: []client.TimeSeriesChunk{
				// Series with data points only before queryStart.
				{
					Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "one"}},
					Chunks: convertToChunks(t, []mimirpb.Sample{
						{TimestampMs: queryStart.Add(-9*time.Minute).Unix() * 1000, Value: 1},
						{TimestampMs: queryStart.Add(-8*time.Minute).Unix() * 1000, Value: 1},
						{TimestampMs: queryStart.Add(-7*time.Minute).Unix() * 1000, Value: 1},
					}),
				},
				// Series with data points before and after queryStart, but before queryEnd.
				{
					Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "one"}},
					Chunks: convertToChunks(t, []mimirpb.Sample{
						{TimestampMs: queryStart.Add(-9*time.Minute).Unix() * 1000, Value: 1},
						{TimestampMs: queryStart.Add(-8*time.Minute).Unix() * 1000, Value: 3},
						{TimestampMs: queryStart.Add(-7*time.Minute).Unix() * 1000, Value: 5},
						{TimestampMs: queryStart.Add(-6*time.Minute).Unix() * 1000, Value: 7},
						{TimestampMs: queryStart.Add(-5*time.Minute).Unix() * 1000, Value: 11},
						{TimestampMs: queryStart.Add(-4*time.Minute).Unix() * 1000, Value: 13},
						{TimestampMs: queryStart.Add(-3*time.Minute).Unix() * 1000, Value: 17},
						{TimestampMs: queryStart.Add(-2*time.Minute).Unix() * 1000, Value: 19},
						{TimestampMs: queryStart.Add(-1*time.Minute).Unix() * 1000, Value: 23},
						{TimestampMs: queryStart.Add(+0*time.Minute).Unix() * 1000, Value: 29},
						{TimestampMs: queryStart.Add(+1*time.Minute).Unix() * 1000, Value: 31},
						{TimestampMs: queryStart.Add(+2*time.Minute).Unix() * 1000, Value: 37},
					}),
				},
				// Series with data points after queryEnd.
				{
					Labels: []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "one"}},
					Chunks: convertToChunks(t, []mimirpb.Sample{
						{TimestampMs: queryStart.Add(+4*time.Minute).Unix() * 1000, Value: 41},
						{TimestampMs: queryStart.Add(+5*time.Minute).Unix() * 1000, Value: 43},
						{TimestampMs: queryStart.Add(+6*time.Minute).Unix() * 1000, Value: 47},
						{TimestampMs: queryStart.Add(+7*time.Minute).Unix() * 1000, Value: 53},
					}),
				},
			},
		},
		nil)

	overrides, err := validation.NewOverrides(defaultLimitsConfig(), nil)
	require.NoError(t, err)

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:     logger,
		MaxSamples: 1e6,
		Timeout:    1 * time.Minute,
	})

	queryable, _, _ := New(cfg, overrides, distributor, nil, nil, logger, nil)
	query, err := engine.NewRangeQuery(queryable, nil, `sum({__name__=~".+"})`, queryStart, queryEnd, queryStep)
	require.NoError(t, err)

	ctx := user.InjectOrgID(context.Background(), "user-1")
	r := query.Exec(ctx)
	m, err := r.Matrix()
	require.NoError(t, err)

	require.Equal(t, 1, m.Len())
	assert.Equal(t, []promql.Point{
		{T: 1635746400000, V: 29},
		{T: 1635746460000, V: 31},
		{T: 1635746520000, V: 37},
		{T: 1635746580000, V: 37},
		{T: 1635746640000, V: 78},
		{T: 1635746700000, V: 80},
	}, m[0].Points)
}

func mockTSDB(t *testing.T, mint model.Time, samples int, step, chunkOffset time.Duration, samplesPerChunk int) (storage.Queryable, model.Time) {
	dir := t.TempDir()

	opts := tsdb.DefaultHeadOptions()
	opts.ChunkDirRoot = dir
	// We use TSDB head only. By using full TSDB DB, and appending samples to it, closing it would cause unnecessary HEAD compaction, which slows down the test.
	head, err := tsdb.NewHead(nil, nil, nil, nil, opts, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = head.Close()
	})

	app := head.Appender(context.Background())

	l := labels.Labels{
		{Name: model.MetricNameLabel, Value: "foo"},
	}

	cnt := 0
	chunkStartTs := mint
	ts := chunkStartTs
	for i := 0; i < samples; i++ {
		_, err := app.Append(0, l, int64(ts), float64(ts))
		require.NoError(t, err)
		cnt++

		ts = ts.Add(step)

		if cnt%samplesPerChunk == 0 {
			// Simulate next chunk, restart timestamp.
			chunkStartTs = chunkStartTs.Add(chunkOffset)
			ts = chunkStartTs
		}
	}

	require.NoError(t, app.Commit())
	queryable := storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return tsdb.NewBlockQuerier(head, mint, maxt)
	})

	return queryable, ts
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
	cfg := Config{}
	for _, c := range testCases {
		cfg.QueryIngestersWithin = c.queryIngestersWithin
		t.Run(c.name, func(t *testing.T) {
			distributor := &errDistributor{}

			overrides, err := validation.NewOverrides(defaultLimitsConfig(), nil)
			require.NoError(t, err)

			// We don't have to actually query the storage in this test, so we initialize the querier
			// with no store queryable.
			var storeQueryables []QueryableWithFilter

			queryable, _, _ := New(cfg, overrides, distributor, storeQueryables, nil, log.NewNopLogger(), nil)
			query, err := engine.NewRangeQuery(queryable, nil, "dummy", c.mint, c.maxt, 1*time.Minute)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "0")
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

func TestQuerier_ValidateQueryTimeRange_MaxQueryIntoFuture(t *testing.T) {
	const engineLookbackDelta = 5 * time.Minute

	now := time.Now()

	tests := map[string]struct {
		maxQueryIntoFuture time.Duration
		queryStartTime     time.Time
		queryEndTime       time.Time
		expectedSkipped    bool
		expectedStartTime  time.Time
		expectedEndTime    time.Time
	}{
		"should manipulate query if end time is after the limit": {
			maxQueryIntoFuture: 10 * time.Minute,
			queryStartTime:     now.Add(-5 * time.Hour),
			queryEndTime:       now.Add(1 * time.Hour),
			expectedStartTime:  now.Add(-5 * time.Hour).Add(-engineLookbackDelta),
			expectedEndTime:    now.Add(10 * time.Minute),
		},
		"should not manipulate query if end time is far in the future but limit is disabled": {
			maxQueryIntoFuture: 0,
			queryStartTime:     now.Add(-5 * time.Hour),
			queryEndTime:       now.Add(100 * time.Hour),
			expectedStartTime:  now.Add(-5 * time.Hour).Add(-engineLookbackDelta),
			expectedEndTime:    now.Add(100 * time.Hour),
		},
		"should not manipulate query if end time is in the future but below the limit": {
			maxQueryIntoFuture: 10 * time.Minute,
			queryStartTime:     now.Add(-100 * time.Minute),
			queryEndTime:       now.Add(5 * time.Minute),
			expectedStartTime:  now.Add(-100 * time.Minute).Add(-engineLookbackDelta),
			expectedEndTime:    now.Add(5 * time.Minute),
		},
		"should skip executing a query outside the allowed time range": {
			maxQueryIntoFuture: 10 * time.Minute,
			queryStartTime:     now.Add(50 * time.Minute),
			queryEndTime:       now.Add(60 * time.Minute),
			expectedSkipped:    true,
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
		cfg.MaxQueryIntoFuture = c.maxQueryIntoFuture
		t.Run(name, func(t *testing.T) {
			// We don't need to query any data for this test, so an empty store is fine.
			distributor := &mockDistributor{}
			distributor.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(model.Matrix{}, nil)
			distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&client.QueryStreamResponse{}, nil)

			overrides, err := validation.NewOverrides(defaultLimitsConfig(), nil)
			require.NoError(t, err)

			queryable, _, _ := New(cfg, overrides, distributor, nil, nil, log.NewNopLogger(), nil)
			query, err := engine.NewRangeQuery(queryable, nil, "dummy", c.queryStartTime, c.queryEndTime, time.Minute)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "0")
			r := query.Exec(ctx)
			require.Nil(t, r.Err)

			_, err = r.Matrix()
			require.Nil(t, err)

			if !c.expectedSkipped {
				// Assert on the time range of the actual executed query (5s delta).
				delta := float64(5000)
				require.Len(t, distributor.Calls, 1)
				assert.InDelta(t, util.TimeToMillis(c.expectedStartTime), int64(distributor.Calls[0].Arguments.Get(1).(model.Time)), delta)
				assert.InDelta(t, util.TimeToMillis(c.expectedEndTime), int64(distributor.Calls[0].Arguments.Get(2).(model.Time)), delta)
			} else {
				// Ensure no query has been executed executed (because skipped).
				assert.Len(t, distributor.Calls, 0)
			}
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
			expected:       errors.New("expanding series: the query time range exceeds the limit (query length: 745h0m0s, limit: 720h0m0s)"),
		},
		"should forbid query on large time range over the limit and short rate time window": {
			query:          "rate(foo[1m])",
			queryStartTime: time.Now().Add(-maxQueryLength).Add(-time.Hour),
			queryEndTime:   time.Now(),
			expected:       errors.New("expanding series: the query time range exceeds the limit (query length: 721h1m0s, limit: 720h0m0s)"),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			var cfg Config
			flagext.DefaultValues(&cfg)

			limits := defaultLimitsConfig()
			limits.MaxQueryLength = model.Duration(maxQueryLength)
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			// We don't need to query any data for this test, so an empty distributor is fine.
			distributor := &emptyDistributor{}
			queryable, _, _ := New(cfg, overrides, distributor, nil, nil, log.NewNopLogger(), nil)

			// Create the PromQL engine to execute the query.
			engine := promql.NewEngine(promql.EngineOpts{
				Logger:             log.NewNopLogger(),
				ActiveQueryTracker: nil,
				MaxSamples:         1e6,
				Timeout:            1 * time.Minute,
			})

			query, err := engine.NewRangeQuery(queryable, nil, testData.query, testData.queryStartTime, testData.queryEndTime, time.Minute)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "test")
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

	// Create the PromQL engine to execute the queries.
	engine := promql.NewEngine(promql.EngineOpts{
		Logger:             log.NewNopLogger(),
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
			cfg.QueryIngestersWithin = 0 // Always query ingesters in this test.

			limits := defaultLimitsConfig()
			limits.MaxQueryLookback = testData.maxQueryLookback
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			t.Run("query range", func(t *testing.T) {
				distributor := &mockDistributor{}
				distributor.On("Query", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(model.Matrix{}, nil)
				distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&client.QueryStreamResponse{}, nil)

				queryable, _, _ := New(cfg, overrides, distributor, nil, nil, log.NewNopLogger(), nil)
				require.NoError(t, err)

				query, err := engine.NewRangeQuery(queryable, nil, testData.query, testData.queryStartTime, testData.queryEndTime, time.Minute)
				require.NoError(t, err)

				r := query.Exec(ctx)
				require.Nil(t, r.Err)

				_, err = r.Matrix()
				require.Nil(t, err)

				if !testData.expectedSkipped {
					// Assert on the time range of the actual executed query (5s delta).
					delta := float64(5000)
					require.Len(t, distributor.Calls, 1)
					assert.InDelta(t, util.TimeToMillis(testData.expectedQueryStartTime), int64(distributor.Calls[0].Arguments.Get(1).(model.Time)), delta)
					assert.InDelta(t, util.TimeToMillis(testData.expectedQueryEndTime), int64(distributor.Calls[0].Arguments.Get(2).(model.Time)), delta)
				} else {
					// Ensure no query has been executed executed (because skipped).
					assert.Len(t, distributor.Calls, 0)
				}
			})

			t.Run("series", func(t *testing.T) {
				distributor := &mockDistributor{}
				distributor.On("MetricsForLabelMatchers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]model.Metric{}, nil)

				queryable, _, _ := New(cfg, overrides, distributor, nil, nil, log.NewNopLogger(), nil)
				q, err := queryable.Querier(ctx, util.TimeToMillis(testData.queryStartTime), util.TimeToMillis(testData.queryEndTime))
				require.NoError(t, err)

				hints := &storage.SelectHints{
					Start: util.TimeToMillis(testData.queryStartTime),
					End:   util.TimeToMillis(testData.queryEndTime),
					Func:  "series",
				}
				matcher := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test")

				set := q.Select(false, hints, matcher)
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
					// Ensure no query has been executed executed (because skipped).
					assert.Len(t, distributor.Calls, 0)
				}
			})

			t.Run("label names", func(t *testing.T) {
				matchers := []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchNotEqual, "route", "get_user"),
				}
				distributor := &mockDistributor{}
				distributor.On("LabelNames", mock.Anything, mock.Anything, mock.Anything, matchers).Return([]string{}, nil)

				queryable, _, _ := New(cfg, overrides, distributor, nil, nil, log.NewNopLogger(), nil)
				q, err := queryable.Querier(ctx, util.TimeToMillis(testData.queryStartTime), util.TimeToMillis(testData.queryEndTime))
				require.NoError(t, err)

				_, _, err = q.LabelNames(matchers...)
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
					// Ensure no query has been executed executed (because skipped).
					assert.Len(t, distributor.Calls, 0)
				}
			})

			t.Run("label values", func(t *testing.T) {
				distributor := &mockDistributor{}
				distributor.On("LabelValuesForLabelName", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]string{}, nil)

				queryable, _, _ := New(cfg, overrides, distributor, nil, nil, log.NewNopLogger(), nil)
				q, err := queryable.Querier(ctx, util.TimeToMillis(testData.queryStartTime), util.TimeToMillis(testData.queryEndTime))
				require.NoError(t, err)

				_, _, err = q.LabelValues(labels.MetricName)
				require.NoError(t, err)

				if !testData.expectedSkipped {
					// Assert on the time range of the actual executed query (5s delta).
					delta := float64(5000)
					require.Len(t, distributor.Calls, 1)
					assert.Equal(t, "LabelValuesForLabelName", distributor.Calls[0].Method)
					assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataStartTime), int64(distributor.Calls[0].Arguments.Get(1).(model.Time)), delta)
					assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataEndTime), int64(distributor.Calls[0].Arguments.Get(2).(model.Time)), delta)
				} else {
					// Ensure no query has been executed executed (because skipped).
					assert.Len(t, distributor.Calls, 0)
				}
			})
		})
	}
}

// Check that time range of /series is restricted by maxLabelsQueryLength.
// LabelName and LabelValues are checked in TestBlocksStoreQuerier_MaxLabelsQueryRange(),
// because the implementation of those makes it really hard to do in Querier.
func TestQuerier_MaxLabelsQueryRange(t *testing.T) {
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
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "test")

			var cfg Config
			flagext.DefaultValues(&cfg)
			cfg.QueryIngestersWithin = 0 // Always query ingesters in this test.

			limits := defaultLimitsConfig()
			limits.MaxQueryLookback = model.Duration(thirtyDays * 2)
			limits.MaxLabelsQueryLength = testData.maxLabelsQueryLength
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			// We don't need to query any data for this test, so an empty store is fine.
			var storeQueryable []QueryableWithFilter

			t.Run("series", func(t *testing.T) {
				distributor := &mockDistributor{}
				distributor.On("MetricsForLabelMatchers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]model.Metric{}, nil)

				queryable, _, _ := New(cfg, overrides, distributor, storeQueryable, nil, log.NewNopLogger(), nil)
				q, err := queryable.Querier(ctx, util.TimeToMillis(testData.queryStartTime), util.TimeToMillis(testData.queryEndTime))
				require.NoError(t, err)

				hints := &storage.SelectHints{
					Start: util.TimeToMillis(testData.queryStartTime),
					End:   util.TimeToMillis(testData.queryEndTime),
					Func:  "series",
				}
				matcher := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test")

				set := q.Select(false, hints, matcher)
				require.False(t, set.Next()) // Expected to be empty.
				require.NoError(t, set.Err())

				// Assert on the time range of the actual executed query (5s delta).
				delta := float64(5000)
				require.Len(t, distributor.Calls, 1)
				assert.Equal(t, "MetricsForLabelMatchers", distributor.Calls[0].Method)
				assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataStartTime), int64(distributor.Calls[0].Arguments.Get(1).(model.Time)), delta)
				assert.InDelta(t, util.TimeToMillis(testData.expectedMetadataEndTime), int64(distributor.Calls[0].Arguments.Get(2).(model.Time)), delta)
			})

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
	query, err := engine.NewRangeQuery(queryable, nil, q.query, from, through, step)
	require.NoError(t, err)

	ctx := user.InjectOrgID(context.Background(), "0")
	r := query.Exec(ctx)
	m, err := r.Matrix()
	require.NoError(t, err)

	require.Len(t, m, 1)
	series := m[0]
	require.Equal(t, q.labels, series.Metric)
	require.Equal(t, q.samples(from, through, step), len(series.Points))
	var ts int64
	for i, point := range series.Points {
		expectedTime, expectedValue := q.expected(ts)
		require.Equal(t, expectedTime, point.T, strconv.Itoa(i))
		require.Equal(t, expectedValue, point.V, strconv.Itoa(i))
		ts += int64(step / time.Millisecond)
	}
	return r
}

type errDistributor struct{}

func (m *errDistributor) LabelNamesAndValues(_ context.Context, _ []*labels.Matcher) (*client.LabelNamesAndValuesResponse, error) {
	return nil, errors.New("method is not implemented")
}

var errDistributorError = fmt.Errorf("errDistributorError")

func (m *errDistributor) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error) {
	return nil, errDistributorError
}
func (m *errDistributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error) {
	return nil, errDistributorError
}
func (m *errDistributor) QueryExemplars(ctx context.Context, from, to model.Time, matchers ...[]*labels.Matcher) (*client.ExemplarQueryResponse, error) {
	return nil, errDistributorError
}
func (m *errDistributor) LabelValuesForLabelName(context.Context, model.Time, model.Time, model.LabelName, ...*labels.Matcher) ([]string, error) {
	return nil, errDistributorError
}
func (m *errDistributor) LabelNames(context.Context, model.Time, model.Time, ...*labels.Matcher) ([]string, error) {
	return nil, errDistributorError
}
func (m *errDistributor) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]model.Metric, error) {
	return nil, errDistributorError
}

func (m *errDistributor) MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error) {
	return nil, errDistributorError
}

func (m *errDistributor) LabelValuesCardinality(ctx context.Context, labelNames []model.LabelName, matchers []*labels.Matcher) (uint64, *client.LabelValuesCardinalityResponse, error) {
	return 0, nil, errDistributorError
}

type emptyDistributor struct{}

func (d *emptyDistributor) LabelNamesAndValues(_ context.Context, _ []*labels.Matcher) (*client.LabelNamesAndValuesResponse, error) {
	return nil, errors.New("method is not implemented")
}

func (d *emptyDistributor) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error) {
	return nil, nil
}

func (d *emptyDistributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error) {
	return &client.QueryStreamResponse{}, nil
}

func (d *emptyDistributor) QueryExemplars(ctx context.Context, from, to model.Time, matchers ...[]*labels.Matcher) (*client.ExemplarQueryResponse, error) {
	return nil, nil
}

func (d *emptyDistributor) LabelValuesForLabelName(context.Context, model.Time, model.Time, model.LabelName, ...*labels.Matcher) ([]string, error) {
	return nil, nil
}

func (d *emptyDistributor) LabelNames(context.Context, model.Time, model.Time, ...*labels.Matcher) ([]string, error) {
	return nil, nil
}

func (d *emptyDistributor) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]model.Metric, error) {
	return nil, nil
}

func (d *emptyDistributor) MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error) {
	return nil, nil
}

func (d *emptyDistributor) LabelValuesCardinality(ctx context.Context, labelNames []model.LabelName, matchers []*labels.Matcher) (uint64, *client.LabelValuesCardinalityResponse, error) {
	return 0, nil, nil
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
		cfg.QueryIngestersWithin = c.queryIngestersWithin
		cfg.QueryStoreAfter = c.queryStoreAfter
		t.Run(c.name, func(t *testing.T) {
			distributor := &errDistributor{}

			overrides, err := validation.NewOverrides(defaultLimitsConfig(), nil)
			require.NoError(t, err)

			// Mock the blocks storage to return an empty SeriesSet (we just need to check whether
			// it was hit or not).
			expectedMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "metric")}
			querier := &mockBlocksStorageQuerier{}
			querier.On("Select", true, mock.Anything, expectedMatchers).Return(storage.EmptySeriesSet())

			queryable, _, _ := New(cfg, overrides, distributor, []QueryableWithFilter{UseAlwaysQueryable(newMockBlocksStorageQueryable(querier))}, nil, log.NewNopLogger(), nil)
			query, err := engine.NewRangeQuery(queryable, nil, "metric", c.mint, c.maxt, 1*time.Minute)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), "0")
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
				querier.AssertCalled(t, "Select", true, mock.Anything, expectedMatchers)
			} else {
				querier.AssertNotCalled(t, "Select", mock.Anything, mock.Anything, mock.Anything)
			}
		})
	}
}

func TestUseAlwaysQueryable(t *testing.T) {
	m := &mockQueryableWithFilter{}
	qwf := UseAlwaysQueryable(m)

	require.True(t, qwf.UseQueryable(time.Now(), 0, 0))
	require.False(t, m.useQueryableCalled)
}

func TestUseBeforeTimestamp(t *testing.T) {
	m := &mockQueryableWithFilter{}
	now := time.Now()
	qwf := UseBeforeTimestampQueryable(m, now.Add(-1*time.Hour))

	require.False(t, qwf.UseQueryable(now, util.TimeToMillis(now.Add(-5*time.Minute)), util.TimeToMillis(now)))
	require.False(t, m.useQueryableCalled)

	require.False(t, qwf.UseQueryable(now, util.TimeToMillis(now.Add(-1*time.Hour)), util.TimeToMillis(now)))
	require.False(t, m.useQueryableCalled)

	require.True(t, qwf.UseQueryable(now, util.TimeToMillis(now.Add(-1*time.Hour).Add(-time.Millisecond)), util.TimeToMillis(now)))
	require.False(t, m.useQueryableCalled) // UseBeforeTimestampQueryable wraps Queryable, and not QueryableWithFilter.
}

func TestStoreQueryable(t *testing.T) {
	m := &mockQueryableWithFilter{}
	now := time.Now()
	sq := storeQueryable{m, time.Hour}

	require.False(t, sq.UseQueryable(now, util.TimeToMillis(now.Add(-5*time.Minute)), util.TimeToMillis(now)))
	require.False(t, m.useQueryableCalled)

	require.False(t, sq.UseQueryable(now, util.TimeToMillis(now.Add(-1*time.Hour).Add(time.Millisecond)), util.TimeToMillis(now)))
	require.False(t, m.useQueryableCalled)

	require.True(t, sq.UseQueryable(now, util.TimeToMillis(now.Add(-1*time.Hour)), util.TimeToMillis(now)))
	require.True(t, m.useQueryableCalled) // storeQueryable wraps QueryableWithFilter, so it must call its UseQueryable method.
}

func TestConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		setup    func(cfg *Config)
		expected error
	}{
		"should pass with default config": {
			setup: func(cfg *Config) {},
		},
		"should pass if 'query store after' is enabled and shuffle-sharding is disabled": {
			setup: func(cfg *Config) {
				cfg.QueryStoreAfter = time.Hour
			},
		},
		"should pass if 'query store after' is enabled and shuffle-sharding is enabled with greater value": {
			setup: func(cfg *Config) {
				cfg.QueryStoreAfter = time.Hour
				cfg.ShuffleShardingIngestersLookbackPeriod = 2 * time.Hour
			},
		},
		"should fail if 'query store after' is enabled and shuffle-sharding is enabled with lesser value": {
			setup: func(cfg *Config) {
				cfg.QueryStoreAfter = time.Hour
				cfg.ShuffleShardingIngestersLookbackPeriod = time.Minute
			},
			expected: errShuffleShardingLookbackLessThanQueryStoreAfter,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := &Config{}
			flagext.DefaultValues(cfg)
			testData.setup(cfg)

			assert.Equal(t, testData.expected, cfg.Validate())
		})
	}
}

type mockQueryableWithFilter struct {
	useQueryableCalled bool
}

func (m *mockQueryableWithFilter) Querier(_ context.Context, _, _ int64) (storage.Querier, error) {
	return nil, nil
}

func (m *mockQueryableWithFilter) UseQueryable(_ time.Time, _, _ int64) bool {
	m.useQueryableCalled = true
	return true
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
func (m *mockBlocksStorageQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return m.querier, nil
}

type mockBlocksStorageQuerier struct {
	mock.Mock
}

func (m *mockBlocksStorageQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	args := m.Called(sortSeries, hints, matchers)
	return args.Get(0).(storage.SeriesSet)
}

func (m *mockBlocksStorageQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	args := m.Called(name, matchers)
	return args.Get(0).([]string), args.Get(1).(storage.Warnings), args.Error(2)
}

func (m *mockBlocksStorageQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	args := m.Called(matchers)
	return args.Get(0).([]string), args.Get(1).(storage.Warnings), args.Error(2)
}

func (m *mockBlocksStorageQuerier) Close() error {
	return nil
}
