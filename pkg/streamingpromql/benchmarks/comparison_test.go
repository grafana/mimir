// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/bench_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package benchmarks

import (
	"context"
	"math"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/util/validation"
)

// This is based on the benchmarks from https://github.com/prometheus/prometheus/blob/main/promql/bench_test.go.
func BenchmarkQuery(b *testing.B) {
	// Important: the setup below must remain in sync with the setup done in tools/benchmark-query-engine.
	q := createBenchmarkQueryable(b, MetricSizes)
	cases := TestCases(MetricSizes)

	opts := streamingpromql.NewTestEngineOpts()
	prometheusEngine := promql.NewEngine(opts.CommonOpts)
	mimirEngine, err := streamingpromql.NewEngine(opts, streamingpromql.NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(b, err)

	// Important: the names below must remain in sync with the names used in tools/benchmark-query-engine.
	engines := map[string]promql.QueryEngine{
		"Prometheus": prometheusEngine,
		"Mimir":      mimirEngine,
	}

	ctx := user.InjectOrgID(context.Background(), UserID)

	// Don't compare results when we're running under tools/benchmark-query-engine, as that will skew peak memory utilisation.
	skipCompareResults := os.Getenv("MIMIR_PROMQL_ENGINE_BENCHMARK_SKIP_COMPARE_RESULTS") == "true"

	for _, c := range cases {
		start := time.Unix(int64((NumIntervals-c.Steps)*intervalSeconds), 0)
		end := time.Unix(int64(NumIntervals*intervalSeconds), 0)

		b.Run(c.Name(), func(b *testing.B) {
			if !skipCompareResults {
				// Check both engines produce the same result before running the benchmark.
				prometheusResult, prometheusClose := c.Run(ctx, b, start, end, interval, prometheusEngine, q)
				mimirResult, mimirClose := c.Run(ctx, b, start, end, interval, mimirEngine, q)

				testutils.RequireEqualResults(b, c.Expr, prometheusResult, mimirResult, false)

				prometheusClose()
				mimirClose()
			}

			for name, engine := range engines {
				b.Run("engine="+name, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						res, cleanup := c.Run(ctx, b, start, end, interval, engine, q)

						if res != nil {
							cleanup()
						}
					}
				})
			}
		})
	}
}

func TestBothEnginesReturnSameResultsForBenchmarkQueries(t *testing.T) {
	metricSizes := []int{1, 100} // Don't bother with 2000 series test here: these test cases take a while and they're most interesting as benchmarks, not correctness tests.
	q := createBenchmarkQueryable(t, metricSizes)
	cases := TestCases(metricSizes)

	opts := streamingpromql.NewTestEngineOpts()
	prometheusEngine := promql.NewEngine(opts.CommonOpts)
	mimirEngine, err := streamingpromql.NewEngine(opts, streamingpromql.NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)

	ctx := user.InjectOrgID(context.Background(), UserID)

	for _, c := range cases {
		t.Run(c.Name(), func(t *testing.T) {
			start := time.Unix(int64((NumIntervals-c.Steps)*intervalSeconds), 0)
			end := time.Unix(int64(NumIntervals*intervalSeconds), 0)

			prometheusResult, prometheusClose := c.Run(ctx, t, start, end, interval, prometheusEngine, q)
			mimirResult, mimirClose := c.Run(ctx, t, start, end, interval, mimirEngine, q)

			testutils.RequireEqualResults(t, c.Expr, prometheusResult, mimirResult, false)

			prometheusClose()
			mimirClose()
		})
	}
}

// This test checks that the way we set up the ingester and PromQL engine does what we expect
// (ie. that we can query the data we write to the ingester)
func TestBenchmarkSetup(t *testing.T) {
	q := createBenchmarkQueryable(t, []int{1})

	opts := streamingpromql.NewTestEngineOpts()
	mimirEngine, err := streamingpromql.NewEngine(opts, streamingpromql.NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)

	ctx := user.InjectOrgID(context.Background(), UserID)
	query, err := mimirEngine.NewRangeQuery(ctx, q, nil, "a_1", time.Unix(0, 0), time.Unix(int64((NumIntervals-1)*intervalSeconds), 0), interval)
	require.NoError(t, err)

	t.Cleanup(query.Close)
	result := query.Exec(ctx)
	require.NoError(t, result.Err)

	matrix, err := result.Matrix()
	require.NoError(t, err)

	require.Len(t, matrix, 1)
	series := matrix[0]
	require.Equal(t, labels.FromStrings("__name__", "a_1"), series.Metric)
	require.Len(t, series.Histograms, 0)

	intervalMilliseconds := interval.Milliseconds()
	expectedPoints := make([]promql.FPoint, NumIntervals)

	for i := range expectedPoints {
		expectedPoints[i].T = int64(i) * intervalMilliseconds
		expectedPoints[i].F = float64(i)
	}

	require.Equal(t, expectedPoints, series.Floats)

	// Check native histograms are set up correctly
	query, err = mimirEngine.NewRangeQuery(ctx, q, nil, "nh_1", time.Unix(0, 0), time.Unix(int64(15*intervalSeconds), 0), interval)
	require.NoError(t, err)

	t.Cleanup(query.Close)
	result = query.Exec(ctx)
	require.NoError(t, result.Err)

	matrix, err = result.Matrix()
	require.NoError(t, err)

	require.Len(t, matrix, 1)
	series = matrix[0]
	require.Equal(t, labels.FromStrings("__name__", "nh_1"), series.Metric)
	require.Len(t, series.Floats, 0)
	require.Len(t, series.Histograms, 16)

	// Check one histogram point is as expected
	require.Equal(t, int64(0), series.Histograms[0].T)
	require.Equal(t, 12.0, series.Histograms[0].H.Count)
	require.Equal(t, 18.4, series.Histograms[0].H.Sum)
}

func createBenchmarkQueryable(t testing.TB, metricSizes []int) storage.Queryable {
	addr := os.Getenv("MIMIR_PROMQL_ENGINE_BENCHMARK_INGESTER_ADDR")

	if addr == "" {
		var err error
		var cleanup func()
		addr, cleanup, err = StartIngesterAndLoadData(t.TempDir(), metricSizes)
		require.NoError(t, err)
		t.Cleanup(cleanup)
	}

	return createIngesterQueryable(t, addr)
}

func createIngesterQueryable(t testing.TB, address string) storage.Queryable {
	logger := log.NewNopLogger()
	kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), logger, nil)
	t.Cleanup(func() { require.NoError(t, closer.Close()) })

	err := kvStore.CAS(context.Background(), ingester.IngesterRingKey, func(_ interface{}) (interface{}, bool, error) {
		ingesters := map[string]ring.InstanceDesc{}
		ingesters[address] = ring.InstanceDesc{
			Addr:                address,
			Zone:                "benchmark-zone-a",
			State:               ring.ACTIVE,
			Timestamp:           time.Now().Unix(),
			RegisteredTimestamp: time.Now().Add(-2 * time.Hour).Unix(),
			Tokens:              []uint32{0},
		}

		return &ring.Desc{Ingesters: ingesters}, true, nil
	})
	require.NoError(t, err)

	ingestersRing, err := ring.New(ring.Config{
		KVStore: kv.Config{
			Mock: kvStore,
		},
		HeartbeatTimeout:  60 * time.Minute,
		ReplicationFactor: 1,
	}, ingester.IngesterRingKey, ingester.IngesterRingKey, logger, nil)

	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingestersRing))
	t.Cleanup(ingestersRing.StopAsync)

	test.Poll(t, time.Second, 1, func() interface{} { return ingestersRing.InstancesCount() })

	distributorCfg := distributor.Config{}
	clientCfg := client.Config{}
	querierCfg := querier.Config{}
	flagext.DefaultValues(&distributorCfg, &clientCfg, &querierCfg)

	// The default value for this option is defined in the querier config and applied to the distributor config struct,
	// so we have to copy it over ourselves.
	distributorCfg.StreamingChunksPerIngesterSeriesBufferSize = querierCfg.StreamingChunksPerIngesterSeriesBufferSize

	limits := defaultLimitsTestConfig()
	limits.NativeHistogramsIngestionEnabled = true

	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	d, err := distributor.New(distributorCfg, clientCfg, overrides, nil, nil, ingestersRing, nil, false, nil, logger)
	require.NoError(t, err)

	queryMetrics := stats.NewQueryMetrics(nil)

	return querier.NewDistributorQueryable(d, alwaysQueryIngestersConfigProvider{}, queryMetrics, log.NewNopLogger())
}

type alwaysQueryIngestersConfigProvider struct{}

func (a alwaysQueryIngestersConfigProvider) QueryIngestersWithin(string) time.Duration {
	return time.Duration(math.MaxInt64)
}
