// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/bench_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package benchmarks

import (
	"context"
	"math"
	"os"
	"slices"
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
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/util/validation"
)

// This is based on the benchmarks from https://github.com/prometheus/prometheus/blob/main/promql/bench_test.go.
func BenchmarkQuery(b *testing.B) {
	// Important: the setup below must remain in sync with the setup done in tools/benchmark-query-engine.
	q := createBenchmarkQueryable(b, MetricSizes)
	cases := TestCases(MetricSizes)

	opts := streamingpromql.NewTestEngineOpts()
	standardEngine := promql.NewEngine(opts)
	streamingEngine, err := streamingpromql.NewEngine(opts)
	require.NoError(b, err)

	// Important: the names below must remain in sync with the names used in tools/benchmark-query-engine.
	engines := map[string]promql.QueryEngine{
		"standard":  standardEngine,
		"streaming": streamingEngine,
	}

	ctx := user.InjectOrgID(context.Background(), UserID)

	// Don't compare results when we're running under tools/benchmark-query-engine, as that will skew peak memory utilisation.
	skipCompareResults := os.Getenv("STREAMING_PROMQL_ENGINE_BENCHMARK_SKIP_COMPARE_RESULTS") == "true"

	for _, c := range cases {
		start := time.Unix(int64((NumIntervals-c.Steps)*10), 0)
		end := time.Unix(int64(NumIntervals*10), 0)
		interval := time.Second * 10

		b.Run(c.Name(), func(b *testing.B) {
			if !skipCompareResults {
				// Check both engines produce the same result before running the benchmark.
				standardResult, standardClose := c.Run(ctx, b, start, end, interval, standardEngine, q)
				streamingResult, streamingClose := c.Run(ctx, b, start, end, interval, streamingEngine, q)

				requireEqualResults(b, standardResult, streamingResult)

				standardClose()
				streamingClose()
			}

			for name, engine := range engines {
				b.Run(name, func(b *testing.B) {
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

func TestBenchmarkQueries(t *testing.T) {
	metricSizes := []int{1, 100} // Don't bother with 2000 series test here: these test cases take a while and they're most interesting as benchmarks, not correctness tests.
	q := createBenchmarkQueryable(t, metricSizes)
	cases := TestCases(metricSizes)

	opts := streamingpromql.NewTestEngineOpts()
	standardEngine := promql.NewEngine(opts)
	streamingEngine, err := streamingpromql.NewEngine(opts)
	require.NoError(t, err)

	ctx := user.InjectOrgID(context.Background(), UserID)

	for _, c := range cases {
		t.Run(c.Name(), func(t *testing.T) {
			start := time.Unix(int64((NumIntervals-c.Steps)*10), 0)
			end := time.Unix(int64(NumIntervals*10), 0)
			interval := time.Second * 10

			standardResult, standardClose := c.Run(ctx, t, start, end, interval, standardEngine, q)
			streamingResult, streamingClose := c.Run(ctx, t, start, end, interval, streamingEngine, q)

			requireEqualResults(t, standardResult, streamingResult)

			standardClose()
			streamingClose()
		})
	}
}

// Why do we do this rather than require.Equal(t, expected, actual)?
// It's possible that floating point values are slightly different due to imprecision, but require.Equal doesn't allow us to set an allowable difference.
func requireEqualResults(t testing.TB, expected, actual *promql.Result) {
	require.Equal(t, expected.Err, actual.Err)

	// Ignore warnings until they're supported by the streaming engine.
	// require.Equal(t, expected.Warnings, actual.Warnings)

	require.Equal(t, expected.Value.Type(), actual.Value.Type())

	switch expected.Value.Type() {
	case parser.ValueTypeVector:
		expectedVector, err := expected.Vector()
		require.NoError(t, err)
		actualVector, err := actual.Vector()
		require.NoError(t, err)

		// Instant queries don't guarantee any particular sort order, so sort results here so that we can easily compare them.
		sortVector(expectedVector)
		sortVector(actualVector)

		require.Len(t, actualVector, len(expectedVector))

		for i, expectedSample := range expectedVector {
			actualSample := actualVector[i]

			require.Equal(t, expectedSample.Metric, actualSample.Metric)
			require.Equal(t, expectedSample.T, actualSample.T)
			require.Equal(t, expectedSample.H, actualSample.H)
			require.InEpsilon(t, expectedSample.F, actualSample.F, 1e-10)
		}
	case parser.ValueTypeMatrix:
		expectedMatrix, err := expected.Matrix()
		require.NoError(t, err)
		actualMatrix, err := actual.Matrix()
		require.NoError(t, err)

		require.Len(t, actualMatrix, len(expectedMatrix))

		for i, expectedSeries := range expectedMatrix {
			actualSeries := actualMatrix[i]

			require.Equal(t, expectedSeries.Metric, actualSeries.Metric)
			require.Equal(t, expectedSeries.Histograms, actualSeries.Histograms)

			for j, expectedPoint := range expectedSeries.Floats {
				actualPoint := actualSeries.Floats[j]

				require.Equal(t, expectedPoint.T, actualPoint.T)
				require.InEpsilonf(t, expectedPoint.F, actualPoint.F, 1e-10, "expected series %v to have points %v, but result is %v", expectedSeries.Metric.String(), expectedSeries.Floats, actualSeries.Floats)
			}
		}
	default:
		require.Fail(t, "unexpected value type", "type: %v", expected.Value.Type())
	}
}

func createBenchmarkQueryable(t testing.TB, metricSizes []int) storage.Queryable {
	addr := os.Getenv("STREAMING_PROMQL_ENGINE_BENCHMARK_INGESTER_ADDR")

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
	flagext.DefaultValues(&distributorCfg, &clientCfg)

	limits := defaultLimitsTestConfig()
	limits.NativeHistogramsIngestionEnabled = true

	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	d, err := distributor.New(distributorCfg, clientCfg, overrides, nil, ingestersRing, nil, false, nil, logger)
	require.NoError(t, err)

	queryMetrics := stats.NewQueryMetrics(nil)

	return querier.NewDistributorQueryable(d, alwaysQueryIngestersConfigProvider{}, queryMetrics, log.NewNopLogger())
}

type alwaysQueryIngestersConfigProvider struct{}

func (a alwaysQueryIngestersConfigProvider) QueryIngestersWithin(string) time.Duration {
	return time.Duration(math.MaxInt64)
}

func sortVector(v promql.Vector) {
	slices.SortFunc(v, func(a, b promql.Sample) int {
		return labels.Compare(a.Metric, b.Metric)
	})
}
