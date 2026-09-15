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
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/shardingtest"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/testdatagen"
	"github.com/grafana/mimir/pkg/querier/stats"
	storagesharding "github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

func createEngine(t *testing.T, shardCount int) (promql.QueryEngine, *prometheus.Registry) {
	reg := prometheus.NewPedanticRegistry()
	opts := streamingpromql.NewTestEngineOpts()
	opts.CommonOpts.Reg = reg
	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	if shardCount > 0 {
		limits := &mockLimits{totalShards: shardCount}
		planner.RegisterASTOptimizationPass(NewOptimizationPass(limits, 0, nil, reg, log.NewNopLogger()))
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

func TestQuerySharding_AvgStats(t *testing.T) {
	// Verify that sharded avg() reports the same sample count as unsharded avg().
	// Without the __sharded_avg__ correction, sharded avg() reports 2x the samples
	// because both the sum and count legs process the same underlying data.

	start := time.Unix(0, 0)
	end := time.Unix(300, 0)
	step := 30 * time.Second

	queryable := testdatagen.StorageSeriesQueryable([]storage.Series{
		testdatagen.NewSeries(labels.FromStrings("__name__", "foo", "group", "a", "idx", "0"), start, end, step, testdatagen.Factor(5)),
		testdatagen.NewSeries(labels.FromStrings("__name__", "foo", "group", "a", "idx", "1"), start, end, step, testdatagen.Factor(7)),
		testdatagen.NewSeries(labels.FromStrings("__name__", "foo", "group", "b", "idx", "0"), start, end, step, testdatagen.Factor(12)),
		testdatagen.NewSeries(labels.FromStrings("__name__", "foo", "group", "b", "idx", "1"), start, end, step, testdatagen.Factor(11)),
	})

	ctx := user.InjectOrgID(context.Background(), "test-user")

	execQuery := func(t *testing.T, engine promql.QueryEngine, expr string) (int64, int64) {
		q, err := engine.NewRangeQuery(ctx, queryable, nil, expr, start, end, step)
		require.NoError(t, err)
		defer q.Close()
		res := q.Exec(ctx)
		require.NoError(t, res.Err)
		samples := q.Stats().Samples
		return samples.TotalSamples, samples.SamplesRead
	}

	expr := `avg(foo)`
	unshardedEngine, _ := createEngine(t, 0)
	unshardedSamplesProcessed, unshardedSamplesRead := execQuery(t, unshardedEngine, expr)
	require.Greater(t, unshardedSamplesProcessed, int64(0), "unsharded query should have processed some samples")
	require.Greater(t, unshardedSamplesRead, int64(0), "unsharded query should have read some samples")

	for _, numShards := range []int{2, 4} {
		t.Run(fmt.Sprintf("shards=%d", numShards), func(t *testing.T) {
			shardedEngine, _ := createEngine(t, numShards)
			shardedSamplesProcessed, shardedSamplesRead := execQuery(t, shardedEngine, expr)
			require.Equal(t, unshardedSamplesProcessed, shardedSamplesProcessed, "sharded avg() should report the same 'samples processed' count as unsharded avg()")
			require.Equal(t, unshardedSamplesRead, shardedSamplesRead, "sharded avg() should report the same 'samples read' count as unsharded avg()")
		})
	}
}

// TestSubsetLabelSharding_Correctness replicates the correctness result from the subset-label query
// sharding experiment (docs/internal SUBSET_LABEL_SHARDING_EXPERIMENT_RESULTS.md): for the supported
// histogram_quantile(sum by (le, <labels>) (rate(...))) shape, subset-label sharding returns the same
// series and samples as an unsharded query and as classic sharding.
//
// Subset sharding pushes the whole histogram_quantile down into each shard and concatenates the
// per-shard outputs. That is only correct because every series in a given subset group (here,
// span_name) is owned by exactly one shard. The mock queryable in testdatagen reproduces that
// ownership by hashing over the subset labels, exactly like the ingester and store-gateway do at
// query time.
func TestSubsetLabelSharding_Correctness(t *testing.T) {
	const metric = "traces_spanmetrics_latency_bucket"

	// Cumulative classic-histogram buckets. Larger le must have a >= count, so use a per-bucket factor
	// that grows with the bucket index to keep the buckets monotonic at every timestamp.
	buckets := []string{"0.1", "0.5", "1", "2.5", "5", "10", "+Inf"}
	from := shardingtest.Start.Add(-5 * time.Minute) // Enough history for the [5m] rate at the range start.
	to := shardingtest.End

	// histogramSeries returns the le-bucket series for one classic histogram identified by the given
	// distinguishing labels (e.g. span_name, cluster). Passing none models samples that are missing the
	// subset label entirely, which must still be owned by exactly one shard (the empty-subset-hash shard).
	histogramSeries := func(distinguishing ...labels.Label) []storage.Series {
		out := make([]storage.Series, 0, len(buckets))
		for i, le := range buckets {
			b := labels.NewBuilder(labels.EmptyLabels())
			b.Set("__name__", metric)
			b.Set("le", le)
			for _, l := range distinguishing {
				b.Set(l.Name, l.Value)
			}
			out = append(out, testdatagen.NewSeries(b.Labels(), from, to, shardingtest.Step, testdatagen.Factor(float64(i+1))))
		}
		return out
	}

	singleLabelSeries := func() []storage.Series {
		var s []storage.Series
		for i := range 8 {
			s = append(s, histogramSeries(labels.Label{Name: "span_name", Value: "span_" + strconv.Itoa(i)})...)
		}
		return s
	}

	multiLabelSeries := func() []storage.Series {
		var s []storage.Series
		for c := range 2 {
			for i := range 6 {
				s = append(s, histogramSeries(
					labels.Label{Name: "cluster", Value: "cluster_" + strconv.Itoa(c)},
					labels.Label{Name: "span_name", Value: "span_" + strconv.Itoa(i)},
				)...)
			}
		}
		return s
	}

	missingLabelSeries := func() []storage.Series {
		s := histogramSeries() // One histogram with no span_name at all.
		for i := range 6 {
			s = append(s, histogramSeries(labels.Label{Name: "span_name", Value: "span_" + strconv.Itoa(i)})...)
		}
		return s
	}

	scenarios := []struct {
		name     string
		query    string
		byLabels []string // Subset labels expected in the shard selector, sorted as the pass emits them.
		series   []storage.Series
	}{
		{
			name:     "single subset label",
			query:    `histogram_quantile(0.9, sum by (le, span_name) (rate(traces_spanmetrics_latency_bucket[5m])))`,
			byLabels: []string{"span_name"},
			series:   singleLabelSeries(),
		},
		{
			// Multiple subset labels take the store-gateway/ingester late-filter path (the postings
			// prefilter is single-label only); ownership is a hash over both labels together.
			name:     "multiple subset labels",
			query:    `histogram_quantile(0.9, sum by (le, cluster, span_name) (rate(traces_spanmetrics_latency_bucket[5m])))`,
			byLabels: []string{"cluster", "span_name"},
			series:   multiLabelSeries(),
		},
		{
			// Some series are missing span_name entirely. They hash as the empty subset value and must be
			// owned by exactly one shard, mirroring the ingester/store-gateway HashForLabels behaviour.
			name:     "series missing the subset label",
			query:    `histogram_quantile(0.9, sum by (le, span_name) (rate(traces_spanmetrics_latency_bucket[5m])))`,
			byLabels: []string{"span_name"},
			series:   missingLabelSeries(),
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			queryable := testdatagen.StorageSeriesQueryable(scenario.series)

			generators := map[string]func(t *testing.T, ctx context.Context, engine promql.QueryEngine) promql.Query{
				"instant query": func(t *testing.T, ctx context.Context, engine promql.QueryEngine) promql.Query {
					q, err := engine.NewInstantQuery(ctx, queryable, nil, scenario.query, shardingtest.End)
					require.NoError(t, err)
					return q
				},
				"range query": func(t *testing.T, ctx context.Context, engine promql.QueryEngine) promql.Query {
					q, err := engine.NewRangeQuery(ctx, queryable, nil, scenario.query, shardingtest.Start, shardingtest.End, shardingtest.Step)
					require.NoError(t, err)
					return q
				},
			}

			for name, generator := range generators {
				t.Run(name, func(t *testing.T) {
					baseCtx := user.InjectOrgID(context.Background(), "test-user")

					// Run the query without sharding to establish the expected result.
					unshardedEngine, _ := createEngine(t, 0)
					unshardedQuery := generator(t, baseCtx, unshardedEngine)
					unshardedResult := unshardedQuery.Exec(baseCtx)
					require.NoError(t, unshardedResult.Err)
					require.NotEmpty(t, unshardedResult.Value)
					requireValidSamples(t, unshardedResult.Value)

					// The subset header only reaches the sharding optimization pass through the request
					// options, so inject it the same way the query-frontend does after allow-listing it.
					subsetCtx := requestoptions.ContextWithOptions(baseCtx, requestoptions.Options{
						PropagatedHeaders: http.Header{
							experimentalSubsetShardingHeader: {"true"},
						},
					})

					for _, numShards := range []int{2, 4, 8, 16} {
						t.Run(fmt.Sprintf("shards=%d", numShards), func(t *testing.T) {
							// Guard against a silent fallback to classic sharding: confirm the subset header
							// actually rewrites the query into i_of_N_by_<labels> shards. Without this, the
							// correctness comparison below would still pass if the header were ignored.
							requireSubsetRewrite(t, scenario.query, scenario.byLabels, numShards)

							// Classic sharding (no subset header) is the second reference: the experiment
							// compared unsharded, classic, and subset and found all three identical.
							classicEngine, classicReg := createEngine(t, numShards)
							classicQuery := generator(t, baseCtx, classicEngine)
							classicResult := classicQuery.Exec(baseCtx)
							require.NoError(t, classicResult.Err)
							testutils.RequireEqualResults(t, scenario.query, unshardedResult, classicResult, false)
							shardingtest.AssertShardingMetrics(t, classicReg, 1, numShards)

							// Subset-label sharding: the header makes the pass rewrite into per-shard
							// histogram_quantile(...){__query_shard__="i_of_N_by_<labels>"} concatenated together.
							subsetEngine, subsetReg := createEngine(t, numShards)
							subsetQuery := generator(t, subsetCtx, subsetEngine)
							subsetResult := subsetQuery.Exec(subsetCtx)
							require.NoError(t, subsetResult.Err)
							testutils.RequireEqualResults(t, scenario.query, unshardedResult, subsetResult, false)
							shardingtest.AssertShardingMetrics(t, subsetReg, 1, numShards)
						})
					}
				})
			}
		})
	}
}

// requireSubsetRewrite asserts that, under the subset-sharding header, the optimization pass rewrites
// query into per-shard i_of_numShards_by_<byLabels> subset shards rather than classic i_of_numShards shards.
func requireSubsetRewrite(t *testing.T, query string, byLabels []string, numShards int) {
	t.Helper()

	limits := &mockLimits{totalShards: numShards, splitAndMergeShards: 1}
	pass := NewOptimizationPass(limits, 0, nil, prometheus.NewPedanticRegistry(), log.NewNopLogger())

	parsed, err := promqlext.NewPromQLParser().ParseExpr(query)
	require.NoError(t, err)

	ctx := requestoptions.ContextWithOptions(user.InjectOrgID(context.Background(), "test-user"), requestoptions.Options{
		PropagatedHeaders: http.Header{experimentalSubsetShardingHeader: {"true"}},
	})
	output, err := pass.Apply(ctx, parsed, &planning.QueryParameters{TimeRange: types.NewInstantQueryTimeRange(shardingtest.End)})
	require.NoError(t, err)

	// Expect the production shard-label format for shard 1, e.g. 1_of_4_by_span_name (labels sorted,
	// comma-joined). Reuse ShardSelector.LabelValue so the test tracks the real formatting.
	expected := storagesharding.ShardSelector{ShardIndex: 0, ShardCount: uint64(numShards), ByLabels: byLabels}.LabelValue()
	require.Contains(t, output.String(), expected)
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
