// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/promql_test.go
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/querysharding_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package sharding

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/shardingtest"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
)

func createEngine(t *testing.T, shardCount int) (promql.QueryEngine, *prometheus.Registry) {
	reg := prometheus.NewPedanticRegistry()
	opts := streamingpromql.NewTestEngineOpts()
	opts.CommonOpts.Reg = reg
	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	if shardCount > 0 {
		limits := &mockLimits{totalShards: shardCount}
		planner.RegisterASTOptimizationPass(NewOptimizationPass(limits, 0, reg, log.NewNopLogger()))
	}

	engine, err := streamingpromql.NewEngine(opts, stats.NewQueryMetrics(reg), planner)
	require.NoError(t, err)

	return engine, reg
}

func TestQuerySharding_Correctness(t *testing.T) {
	shardingtest.RunCorrectnessTests(t, func(t *testing.T, testData shardingtest.CorrectnessTestCase, queryable storage.Queryable) {
		generators := map[string]func(t *testing.T, ctx context.Context, engine promql.QueryEngine) promql.Query{
			"instant query": func(t *testing.T, ctx context.Context, engine promql.QueryEngine) promql.Query {
				q, err := engine.NewInstantQuery(ctx, queryable, nil, testData.Query, shardingtest.End)
				require.NoError(t, err)
				return q
			},
		}

		if !testData.NoRangeQuery {
			generators["range query"] = func(t *testing.T, ctx context.Context, engine promql.QueryEngine) promql.Query {
				q, err := engine.NewRangeQuery(ctx, queryable, nil, testData.Query, shardingtest.Start, shardingtest.End, shardingtest.Step)
				require.NoError(t, err)
				return q
			}
		}

		for name, generator := range generators {
			t.Run(name, func(t *testing.T) {
				ctx := user.InjectOrgID(context.Background(), "test-user")

				// Run the query without sharding.
				unshardedEngine, _ := createEngine(t, 0)
				unshardedQuery := generator(t, ctx, unshardedEngine)
				unshardedResult := unshardedQuery.Exec(ctx)
				require.NoError(t, unshardedResult.Err)

				// Ensure the query produces some results.
				require.NotEmpty(t, unshardedResult.Value)
				requireValidSamples(t, unshardedResult.Value)

				for _, numShards := range []int{2, 4, 8, 16} {
					t.Run(fmt.Sprintf("shards=%d", numShards), func(t *testing.T) {
						shardedEngine, reg := createEngine(t, numShards)

						// Run the query with sharding.
						shardedQuery := generator(t, ctx, shardedEngine)
						shardedResult := shardedQuery.Exec(ctx)
						require.NoError(t, shardedResult.Err)

						// Ensure the two results match (float precision can slightly differ, there's no guarantee in PromQL engine too
						// if you rerun the same query twice).
						testutils.RequireEqualResults(t, testData.Query, unshardedResult, shardedResult, false)

						// Ensure the query has been sharded/not sharded as expected.
						shardingtest.AssertShardingMetrics(t, reg, testData.ExpectedShardedQueries, numShards)
					})
				}
			})
		}
	})
}

func TestQuerySharding_FunctionCorrectness(t *testing.T) {
	shardingtest.RunFunctionCorrectnessTests(t, func(t *testing.T, expr string, numShards int, allowedErr error, queryable storage.Queryable) {
		ctx := user.InjectOrgID(context.Background(), "test-user")

		// Run the query without sharding.
		unshardedEngine, _ := createEngine(t, 0)
		unshardedQuery, err := unshardedEngine.NewRangeQuery(ctx, queryable, nil, expr, shardingtest.Start, shardingtest.End, shardingtest.Step)
		require.NoError(t, err)

		unshardedResult := unshardedQuery.Exec(ctx)

		// MQE currently doesn't support every experimental function, so it's expected for it to return an error in some cases.
		if err != nil && allowedErr != nil {
			require.ErrorAs(t, err, &allowedErr)
			return
		}

		require.NoError(t, unshardedResult.Err)
		require.IsTypef(t, promql.Matrix{}, unshardedResult.Value, "expected Matrix result, got %T", unshardedResult.Value)
		require.NotEmpty(t, unshardedResult.Value)

		shardedEngine, _ := createEngine(t, numShards)

		// Run the query with sharding.
		shardedQuery, err := shardedEngine.NewRangeQuery(ctx, queryable, nil, expr, shardingtest.Start, shardingtest.End, shardingtest.Step)
		require.NoError(t, err)
		shardedResult := shardedQuery.Exec(ctx)
		require.NoError(t, shardedResult.Err)

		// Ensure the two results match (float precision can slightly differ, there's no guarantee in PromQL engine too
		// if you rerun the same query twice).
		testutils.RequireEqualResults(t, expr, unshardedResult, shardedResult, false)
	})
}

// requireValidSamples ensures the query produces some results which are not NaN.
func requireValidSamples(t *testing.T, result parser.Value) {
	t.Helper()

	switch result := result.(type) {
	case promql.Matrix:
		for _, series := range result {
			for _, f := range series.Floats {
				if !math.IsNaN(f.F) {
					return
				}
			}

			for _, h := range series.Histograms {
				if !math.IsNaN(h.H.Sum) {
					return
				}
			}
		}

	case promql.Vector:
		for _, series := range result {
			if series.H != nil && !math.IsNaN(series.H.Sum) {
				return
			}

			if series.H == nil && !math.IsNaN(series.F) {
				return
			}
		}

	case promql.Scalar:
		if !math.IsNaN(result.V) {
			return
		}

	case promql.String:
		return

	default:
		require.Fail(t, "unexpected result type", "expected Matrix or Vector, got %T", result)
	}

	t.Fatalf("Result should have some not-NaN samples")
}
