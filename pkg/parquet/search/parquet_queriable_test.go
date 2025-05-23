// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus-community/parquet-common/blob/382b6ec8ae40fb5dcdcabd8019f69a4be1cd8869/search/parquet_queryable_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package search

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/parquet/schema"
	"github.com/grafana/mimir/pkg/parquet/storage"
)

func TestPromQLAcceptance(t *testing.T) {
	if os.Getenv("RUN_PARQUET_PROMQL_ACCEPTANCE") != "true" {
		t.SkipNow()
	}

	opts := promql.EngineOpts{
		Timeout:                  1 * time.Hour,
		MaxSamples:               1e10,
		EnableNegativeOffset:     true,
		EnableAtModifier:         true,
		NoStepSubqueryIntervalFn: func(_ int64) int64 { return 30 * time.Second.Milliseconds() },
		LookbackDelta:            5 * time.Minute,
		EnableDelayedNameRemoval: true,
	}

	engine := promql.NewEngine(opts)
	t.Cleanup(func() { _ = engine.Close() })

	promqltest.RunBuiltinTestsWithStorage(&parallelTest{T: t}, engine, func(tt testutil.T) prom_storage.Storage {
		return &acceptanceTestStorage{t: t, st: teststorage.New(tt)}
	})
}

type parallelTest struct {
	*testing.T
}

func (s *parallelTest) Run(name string, t func(*testing.T)) bool {
	return s.T.Run(name+"-concurrent", func(tt *testing.T) {
		tt.Parallel()
		s.T.Run(name, t)
	})
}

type acceptanceTestStorage struct {
	t  *testing.T
	st *teststorage.TestStorage
}

func (st *acceptanceTestStorage) Appender(ctx context.Context) prom_storage.Appender {
	return st.st.Appender(ctx)
}

func (st *acceptanceTestStorage) ChunkQuerier(int64, int64) (prom_storage.ChunkQuerier, error) {
	return nil, errors.New("unimplemented")
}

func (st *acceptanceTestStorage) Querier(from, to int64) (prom_storage.Querier, error) {
	if st.st.Head().NumSeries() == 0 {
		// parquet-go panics when writing an empty parquet file
		return st.st.Querier(from, to)
	}
	bkt, err := filesystem.NewBucket(st.t.TempDir())
	if err != nil {
		st.t.Fatalf("unable to create bucket: %s", err)
	}
	st.t.Cleanup(func() { _ = bkt.Close() })

	h := st.st.Head()
	data := testData{minTime: h.MinTime(), maxTime: h.MaxTime()}
	block := convertToParquet(st.t, context.Background(), bkt, data, h)

	q, err := createQueryable(block)
	if err != nil {
		st.t.Fatalf("unable to create queryable: %s", err)
	}
	return q.Querier(from, to)
}

func (st *acceptanceTestStorage) Close() error {
	return st.st.Close()
}

func (st *acceptanceTestStorage) StartTime() (int64, error) {
	return st.st.StartTime()
}

func TestQueryable(t *testing.T) {
	st := teststorage.New(t)
	ctx := context.Background()
	t.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	cfg := defaultTestConfig()
	data := generateTestData(t, st, ctx, cfg)

	ir, err := st.Head().Index()
	require.NoError(t, err)

	// Convert to Parquet
	shard := convertToParquet(t, ctx, bkt, data, st.Head())

	t.Run("QueryByUniqueLabel", func(t *testing.T) {
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "unique", "unique_0")}
		sFound := queryWithQueryable(t, data.minTime, data.maxTime, shard, nil, matchers...)
		totalFound := 0
		for _, series := range sFound {
			require.Equal(t, series.Labels().Get("unique"), "unique_0")
			require.Contains(t, data.seriesHash, series.Labels().Hash())
			totalFound++
		}
		require.Equal(t, cfg.totalMetricNames, totalFound)
	})

	t.Run("QueryByMetricName", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			name := fmt.Sprintf("metric_%d", rand.Int()%cfg.totalMetricNames)
			matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, name)}
			sFound := queryWithQueryable(t, data.minTime, data.maxTime, shard, nil, matchers...)
			totalFound := 0
			for _, series := range sFound {
				totalFound++
				require.Equal(t, series.Labels().Get(labels.MetricName), name)
				require.Contains(t, data.seriesHash, series.Labels().Hash())
			}
			require.Equal(t, cfg.metricsPerMetricName, totalFound)
		}
	})

	t.Run("QueryByUniqueLabel and SkipChunks=true", func(t *testing.T) {
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "unique", "unique_0")}
		hints := &prom_storage.SelectHints{
			Func: "series",
		}
		sFound := queryWithQueryable(t, data.minTime, data.maxTime, shard, hints, matchers...)
		totalFound := 0
		for _, series := range sFound {
			totalFound++
			require.Equal(t, series.Labels().Get("unique"), "unique_0")
			require.Contains(t, data.seriesHash, series.Labels().Hash())
		}
		require.Equal(t, cfg.totalMetricNames, totalFound)
	})

	t.Run("LabelNames", func(t *testing.T) {
		queryable, err := createQueryable(shard)
		require.NoError(t, err)
		querier, err := queryable.Querier(data.minTime, data.maxTime)
		require.NoError(t, err)

		t.Run("Without Matchers", func(t *testing.T) {
			lNames, _, err := querier.LabelNames(context.Background(), nil)
			require.NoError(t, err)
			require.NotEmpty(t, lNames)
			expectedLabelNames, err := ir.LabelNames(context.Background())
			require.NoError(t, err)
			require.Equal(t, expectedLabelNames, lNames)
		})

		t.Run("With Matchers", func(t *testing.T) {
			lNames, _, err := querier.LabelNames(context.Background(), nil, labels.MustNewMatcher(labels.MatchEqual, "random_name_0", "random_value_0"))
			require.NoError(t, err)
			require.NotEmpty(t, lNames)
			expectedLabelNames, err := ir.LabelNames(context.Background(), labels.MustNewMatcher(labels.MatchEqual, "random_name_0", "random_value_0"))
			require.NoError(t, err)
			require.Equal(t, expectedLabelNames, lNames)
		})
	})

	t.Run("LabelValues", func(t *testing.T) {
		queryable, err := createQueryable(shard)
		require.NoError(t, err)
		querier, err := queryable.Querier(data.minTime, data.maxTime)
		require.NoError(t, err)
		t.Run("Without Matchers", func(t *testing.T) {
			lValues, _, err := querier.LabelValues(context.Background(), labels.MetricName, nil)
			require.NoError(t, err)
			expectedLabelValues, err := ir.SortedLabelValues(context.Background(), labels.MetricName, nil)
			require.NoError(t, err)
			require.Equal(t, expectedLabelValues, lValues)
		})

		t.Run("With Matchers", func(t *testing.T) {
			lValues, _, err := querier.LabelValues(context.Background(), labels.MetricName, nil, labels.MustNewMatcher(labels.MatchEqual, "random_name_0", "random_value_0"))
			require.NoError(t, err)
			expectedLabelValues, err := ir.SortedLabelValues(context.Background(), labels.MetricName, nil, labels.MustNewMatcher(labels.MatchEqual, "random_name_0", "random_value_0"))
			require.NoError(t, err)
			require.Equal(t, expectedLabelValues, lValues)
		})
	})
}

func TestQueryableWithEmptyMatcher(t *testing.T) {
	opts := promql.EngineOpts{
		Timeout:                  1 * time.Hour,
		MaxSamples:               1e10,
		EnableNegativeOffset:     true,
		EnableAtModifier:         true,
		NoStepSubqueryIntervalFn: func(_ int64) int64 { return 30 * time.Second.Milliseconds() },
		LookbackDelta:            5 * time.Minute,
		EnableDelayedNameRemoval: true,
	}

	engine := promql.NewEngine(opts)
	t.Cleanup(func() { _ = engine.Close() })

	load := `load 30s
			    http_requests_total{pod="nginx-1", route="/"} 0+1x5
			    http_requests_total{pod="nginx-2"} 0+2x5
			    http_requests_total{pod="nginx-3", route="/"} 0+3x5
			    http_requests_total{pod="nginx-4"} 0+4x5

eval instant at 60s http_requests_total{route=""}
	{__name__="http_requests_total", pod="nginx-2"} 4
	{__name__="http_requests_total", pod="nginx-4"} 8

eval instant at 60s http_requests_total{route=~""}
	{__name__="http_requests_total", pod="nginx-2"} 4
	{__name__="http_requests_total", pod="nginx-4"} 8

eval instant at 60s http_requests_total{route!~".+"}
	{__name__="http_requests_total", pod="nginx-2"} 4
	{__name__="http_requests_total", pod="nginx-4"} 8

eval instant at 60s http_requests_total{route!=""}
	{__name__="http_requests_total", pod="nginx-1", route="/"} 2
	{__name__="http_requests_total", pod="nginx-3", route="/"} 6

eval instant at 60s http_requests_total{route!~""}
	{__name__="http_requests_total", pod="nginx-1", route="/"} 2
	{__name__="http_requests_total", pod="nginx-3", route="/"} 6

eval instant at 60s http_requests_total{route=~".+"}
	{__name__="http_requests_total", pod="nginx-1", route="/"} 2
	{__name__="http_requests_total", pod="nginx-3", route="/"} 6
`

	promqltest.RunTestWithStorage(t, load, engine, func(tt testutil.T) prom_storage.Storage {
		return &acceptanceTestStorage{t: t, st: teststorage.New(tt)}
	})
}

func queryWithQueryable(t *testing.T, mint, maxt int64, shard *storage.ParquetShard, hints *prom_storage.SelectHints, matchers ...*labels.Matcher) []prom_storage.Series {
	ctx := context.Background()
	queryable, err := createQueryable(shard)
	require.NoError(t, err)
	querier, err := queryable.Querier(mint, maxt)
	require.NoError(t, err)
	ss := querier.Select(ctx, true, hints, matchers...)

	found := make([]prom_storage.Series, 0, 100)
	for ss.Next() {
		found = append(found, ss.At())
	}
	return found
}

func createQueryable(shard *storage.ParquetShard) (prom_storage.Queryable, error) {
	d := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
	return NewParquetQueryable(d, func(ctx context.Context, mint, maxt int64) ([]*storage.ParquetShard, error) {
		return []*storage.ParquetShard{shard}, nil
	})
}
