// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/caching"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

// TestRangeVectorSplitting_RangeVectorBufferingIncreasesQueryMemory shows that enabling range vector splitting makes
// the common-subexpression Duplicate buffer the raw range vector (see cse_dedup_boundary_test.go), which is charged to
// the MemoryConsumptionTracker. For the same instant query over the same data:
//
//   - the split falls back to unsplit at materialize time, so split_range_vectors stays 0 - the plan reorganization is
//     pure cost with zero splitting benefit,
//   - peak estimated memory is nonetheless several times higher with splitting on, and
//   - with a fixed memory limit set between the two peaks, the query succeeds with splitting off but trips
//     err-mimir-max-estimated-memory-consumption-per-query with splitting on.
//
// This can also change which limit a query fails on, not just how much memory it uses. The estimated-memory limit
// (exercised here) and the actual chunks limit (err-mimir-max-chunks-per-query) are competing ceilings that both climb
// during the same streaming execution: the chunks count is incremented as batches are fetched, while memory is tracked
// as the query runs. With splitting off the query holds little (instant vectors) and can stream far enough to reach
// the chunks ceiling; with splitting on the raw-range-vector buffer makes memory climb faster and reach its ceiling
// first. The chunks limit is enforced in the querier's store-gateway fetch path and cannot be exercised from a
// streamingpromql test, so only the memory side is asserted here.
func TestRangeVectorSplitting_RangeVectorBufferingIncreasesQueryMemory(t *testing.T) {
	store := loadHighCardinalityRangeStorage(t)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	// Subset-selector elimination merges the two overlapping selectors, so enabling splitting moves the buffered
	// Duplicate down onto the raw MatrixSelector; the 3h range exceeds the 2h split interval so splitting applies.
	const expr = `rate(foo{a="1"}[3h]) / rate(foo[3h])`
	ts := timestamp.Time(0).Add(3*time.Hour + 30*time.Minute)

	// Baseline peaks with the memory limit disabled.
	off, err := runInstantQueryWithSplitting(t, store, expr, ts, false, 0)
	require.NoError(t, err)
	on, err := runInstantQueryWithSplitting(t, store, expr, ts, true, 0)
	require.NoError(t, err)

	// split_range_vectors stays 0 even with splitting on. The split node is inserted (see cse_dedup_boundary_test.go)
	// and reorganizes the plan, but at materialize time it falls back to unsplit execution (no cacheable blocks in the
	// test TSDB head), so the split operator's Prepare - the only place SplitRangeVectors is incremented - never runs.
	// The unsplit-fallback counter confirms the fallback happened.
	require.Zero(t, off.splitRangeVectors, "no SplitFunctionCall exists when splitting is disabled")
	require.Zero(t, on.splitRangeVectors, "split_range_vectors stays 0 with splitting on: the split falls back to unsplit, so there is zero splitting benefit")
	require.Zero(t, off.unsplitNodes, "no SplitFunctionCall to fall back when splitting is disabled")
	require.NotZero(t, on.unsplitNodes, "the SplitFunctionCall falls back to unsplit execution at materialize time")

	require.Greater(t, on.peakMemory, 4*off.peakMemory,
		"buffering the raw range vector should multiply peak memory versus buffering the rate(...) instant vector (off=%d on=%d)", off.peakMemory, on.peakMemory)

	// Same query, same limit, set between the two peaks: off stays under it, on trips the estimated-memory limit.
	limit := (off.peakMemory + on.peakMemory) / 2

	_, err = runInstantQueryWithSplitting(t, store, expr, ts, false, limit)
	require.NoError(t, err, "without splitting the query stays under the %d byte limit (peak %d)", limit, off.peakMemory)

	_, err = runInstantQueryWithSplitting(t, store, expr, ts, true, limit)
	require.ErrorContains(t, err, globalerror.MaxEstimatedMemoryConsumptionPerQuery.Error(),
		"with splitting the query exceeds the same %d byte limit (peak %d)", limit, on.peakMemory)
}

// loadHighCardinalityRangeStorage loads many series each with a full range of raw samples, so the raw-range-vector
// duplication buffer (splitting on) dwarfs the one-value-per-series instant-vector buffer (splitting off).
func loadHighCardinalityRangeStorage(t *testing.T) *teststorage.TestStorage {
	var b strings.Builder
	b.WriteString("load 1m\n")
	for i := 0; i < 20; i++ {
		// a="1" series match both foo{a="1"} and foo; a="2" series match only foo.
		fmt.Fprintf(&b, "\tfoo{a=\"1\", idx=\"%d\"} 0+1x220\n", i)
		fmt.Fprintf(&b, "\tfoo{a=\"2\", idx=\"%d\"} 0+2x220\n", i)
	}
	return promqltest.LoadedStorage(t, b.String())
}

type splittingRunResult struct {
	peakMemory        uint64 // Peak estimated memory consumption, from cortex_mimir_query_engine_estimated_query_peak_memory_consumption.
	splitRangeVectors uint64 // The split_range_vectors query stat; only incremented when the split operator actually runs.
	unsplitNodes      uint64 // Split nodes that fell back to unsplit execution at materialize time.
}

// runInstantQueryWithSplitting runs expr as an instant query with range vector splitting enabled or disabled and the
// given estimated-memory limit (0 disables it).
func runInstantQueryWithSplitting(t *testing.T, store *teststorage.TestStorage, expr string, ts time.Time, splittingEnabled bool, memoryLimit uint64) (splittingRunResult, error) {
	t.Helper()

	reg := prometheus.NewPedanticRegistry()
	opts := streamingpromql.NewTestEngineOpts()
	opts.CommonOpts.Reg = reg
	opts.RangeVectorSplitting.Enabled = splittingEnabled
	opts.RangeVectorSplitting.SplitInterval = 2 * time.Hour

	limits := streamingpromql.NewStaticQueryLimitsProvider()
	limits.MaxEstimatedMemoryConsumptionPerQuery = memoryLimit
	opts.Limits = limits

	backend := caching.NewInMemoryCache()
	intermediateCache := cache.NewCacheFactoryWithBackend(backend, streamingpromql.NewStaticQueryLimitsProvider(), createEmptyPrefixCacheKeyGenerator(), prometheus.NewRegistry(), log.NewNopLogger())

	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := streamingpromql.NewEngineWithCache(opts, stats.NewQueryMetrics(reg), planner, intermediateCache)
	require.NoError(t, err)

	ctx := user.InjectOrgID(context.Background(), "test-user")
	q, err := engine.NewInstantQuery(ctx, store, nil, expr, ts)
	require.NoError(t, err)
	defer q.Close()

	queryStats, ctx := stats.ContextWithEmptyStats(ctx)
	res := q.Exec(ctx)

	return splittingRunResult{
		peakMemory:        histogramSum(t, reg, "cortex_mimir_query_engine_estimated_query_peak_memory_consumption"),
		splitRangeVectors: uint64(queryStats.LoadSplitRangeVectors()),
		unsplitNodes:      counterVecTotal(t, reg, "cortex_mimir_query_engine_range_vector_splitting_nodes_materialized_unsplit_total"),
	}, res.Err
}

func histogramSum(t *testing.T, reg *prometheus.Registry, name string) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == name {
			require.Len(t, mf.Metric, 1)
			return uint64(mf.Metric[0].GetHistogram().GetSampleSum())
		}
	}
	t.Fatalf("histogram %q not found", name)
	return 0
}

func counterVecTotal(t *testing.T, reg *prometheus.Registry, name string) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	var total uint64
	for _, mf := range mfs {
		if mf.GetName() == name {
			for _, m := range mf.Metric {
				total += uint64(m.GetCounter().GetValue())
			}
		}
	}
	return total
}
