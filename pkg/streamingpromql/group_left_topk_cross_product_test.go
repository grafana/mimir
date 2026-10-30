// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
)

// A range query that joins every left series to the single top right series in its group.
// The result has groups*leftPerGroup series, but MQE builds groups*leftPerGroup*rightPerGroup output series
// in the join, because topk in a range query returns the metadata of all of its input series.
const groupLeftTopKQuery = `left_metric * on (group) group_left (right_id) topk by (group) (1, right_metric)`

func TestGroupLeftTopKCrossProduct(t *testing.T) {
	const groups, leftPerGroup = 10, 1000
	start := timestamp.Time(0)
	end := start.Add(10 * time.Minute)
	step := time.Minute

	for _, rightPerGroup := range []int{1, 10, 100, 300} {
		t.Run(fmt.Sprintf("rightPerGroup=%d", rightPerGroup), func(t *testing.T) {
			storage := promqltest.LoadedStorage(t, "")
			app := storage.Appender(context.Background())
			for g := 0; g < groups; g++ {
				group := fmt.Sprintf("g%d", g)
				for i := 0; i < leftPerGroup; i++ {
					lbls := labels.FromStrings("__name__", "left_metric", "group", group, "left_id", fmt.Sprintf("l%d", i))
					for ts := start; !ts.After(end); ts = ts.Add(step) {
						_, err := app.Append(0, lbls, timestamp.FromTime(ts), 1)
						require.NoError(t, err)
					}
				}
				for i := 0; i < rightPerGroup; i++ {
					lbls := labels.FromStrings("__name__", "right_metric", "group", group, "right_id", fmt.Sprintf("r%d", i))
					for ts := start; !ts.After(end); ts = ts.Add(step) {
						_, err := app.Append(0, lbls, timestamp.FromTime(ts), float64(i))
						require.NoError(t, err)
					}
				}
			}
			require.NoError(t, app.Commit())

			reg := prometheus.NewPedanticRegistry()
			opts := streamingpromql.NewTestEngineOpts()
			opts.CommonOpts.Reg = reg
			planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)
			engine, err := streamingpromql.NewEngine(opts, stats.NewQueryMetrics(reg), planner)
			require.NoError(t, err)

			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)

			started := time.Now()
			q, err := engine.NewRangeQuery(context.Background(), storage, nil, groupLeftTopKQuery, start, end, step)
			require.NoError(t, err)
			defer q.Close()
			res := q.Exec(context.Background())
			require.NoError(t, res.Err)
			took := time.Since(started)

			var after runtime.MemStats
			runtime.ReadMemStats(&after)

			m, err := res.Matrix()
			require.NoError(t, err)
			require.Len(t, m, groups*leftPerGroup)

			t.Logf("result series=%d, expected join output series (groups*left*right)=%d, estimated peak=%.1fMB, Go heap allocated during query=%.1fMB, took=%s",
				len(m), groups*leftPerGroup*rightPerGroup, peakEstimateMB(t, reg), float64(after.TotalAlloc-before.TotalAlloc)/1e6, took.Round(time.Millisecond))
		})
	}
}

// Without topk, the "one" side must have at most one series per group at each step, so the right series must not
// overlap in time. This happens naturally with churn, for example when the series for a group is replaced every step.
func TestGroupLeftCrossProductWithoutTopK(t *testing.T) {
	const groups, leftPerGroup = 10, 1000
	const query = `left_metric * on (group) group_left (right_id) right_metric`
	start := timestamp.Time(0)
	step := time.Minute

	for _, tc := range []struct {
		name          string
		rightPerGroup int
		overlapping   bool
	}{
		{"churning right side, 1 series per group", 1, false},
		{"churning right side, 10 series per group", 10, false},
		{"churning right side, 100 series per group", 100, false},
		{"churning right side, 300 series per group", 300, false},
		{"overlapping right side, 300 series per group", 300, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			end := start.Add(time.Duration(tc.rightPerGroup-1) * step)
			storage := promqltest.LoadedStorage(t, "")
			app := storage.Appender(context.Background())
			for g := 0; g < groups; g++ {
				group := fmt.Sprintf("g%d", g)
				for i := 0; i < leftPerGroup; i++ {
					lbls := labels.FromStrings("__name__", "left_metric", "group", group, "left_id", fmt.Sprintf("l%d", i))
					for ts := start; !ts.After(end); ts = ts.Add(step) {
						_, err := app.Append(0, lbls, timestamp.FromTime(ts), 1)
						require.NoError(t, err)
					}
				}
				for i := 0; i < tc.rightPerGroup; i++ {
					lbls := labels.FromStrings("__name__", "right_metric", "group", group, "right_id", fmt.Sprintf("r%d", i))
					if tc.overlapping {
						for ts := start; !ts.After(end); ts = ts.Add(step) {
							_, err := app.Append(0, lbls, timestamp.FromTime(ts), float64(i))
							require.NoError(t, err)
						}
						continue
					}
					// One sample per right series, then a stale marker so it does not carry over to the next step.
					ts := start.Add(time.Duration(i) * step)
					_, err := app.Append(0, lbls, timestamp.FromTime(ts), float64(i))
					require.NoError(t, err)
					_, err = app.Append(0, lbls, timestamp.FromTime(ts.Add(time.Second)), math.Float64frombits(value.StaleNaN))
					require.NoError(t, err)
				}
			}
			require.NoError(t, app.Commit())

			reg := prometheus.NewPedanticRegistry()
			opts := streamingpromql.NewTestEngineOpts()
			opts.CommonOpts.Reg = reg
			planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)
			engine, err := streamingpromql.NewEngine(opts, stats.NewQueryMetrics(reg), planner)
			require.NoError(t, err)

			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)

			started := time.Now()
			q, err := engine.NewRangeQuery(context.Background(), storage, nil, query, start, end, step)
			require.NoError(t, err)
			defer q.Close()
			res := q.Exec(context.Background())
			took := time.Since(started)

			var after runtime.MemStats
			runtime.ReadMemStats(&after)

			resultSeries := -1
			if res.Err == nil {
				m, err := res.Matrix()
				require.NoError(t, err)
				resultSeries = len(m)
			}

			t.Logf("err=%v result series=%d, expected join output series (groups*left*right)=%d, estimated peak=%.1fMB, Go heap allocated during query=%.1fMB, took=%s",
				res.Err, resultSeries, groups*leftPerGroup*tc.rightPerGroup, peakEstimateMB(t, reg), float64(after.TotalAlloc-before.TotalAlloc)/1e6, took.Round(time.Millisecond))
		})
	}
}

func TestGroupLeftTopKCrossProduct_CompareWithPrometheus(t *testing.T) {
	const groups, leftPerGroup = 10, 1000
	start := timestamp.Time(0)
	end := start.Add(10 * time.Minute)
	step := time.Minute

	for _, rightPerGroup := range []int{1, 100, 300} {
		storage := promqltest.LoadedStorage(t, "")
		app := storage.Appender(context.Background())
		for g := 0; g < groups; g++ {
			group := fmt.Sprintf("g%d", g)
			for i := 0; i < leftPerGroup; i++ {
				lbls := labels.FromStrings("__name__", "left_metric", "group", group, "left_id", fmt.Sprintf("l%d", i))
				for ts := start; !ts.After(end); ts = ts.Add(step) {
					_, err := app.Append(0, lbls, timestamp.FromTime(ts), 1)
					require.NoError(t, err)
				}
			}
			for i := 0; i < rightPerGroup; i++ {
				lbls := labels.FromStrings("__name__", "right_metric", "group", group, "right_id", fmt.Sprintf("r%d", i))
				for ts := start; !ts.After(end); ts = ts.Add(step) {
					_, err := app.Append(0, lbls, timestamp.FromTime(ts), float64(i))
					require.NoError(t, err)
				}
			}
		}
		require.NoError(t, app.Commit())

		opts := streamingpromql.NewTestEngineOpts()
		planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
		require.NoError(t, err)
		mqe, err := streamingpromql.NewEngine(opts, stats.NewQueryMetrics(nil), planner)
		require.NoError(t, err)
		prom := promql.NewEngine(opts.CommonOpts)

		for _, e := range []struct {
			name   string
			engine promql.QueryEngine
		}{{"MQE", mqe}, {"Prometheus", prom}} {
			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)
			var heapPeak uint64
			stop, done := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(done)
				for {
					var s runtime.MemStats
					runtime.ReadMemStats(&s)
					heapPeak = max(heapPeak, s.HeapInuse)
					select {
					case <-stop:
						return
					case <-time.After(20 * time.Millisecond):
					}
				}
			}()

			started := time.Now()
			q, err := e.engine.NewRangeQuery(context.Background(), storage, nil, groupLeftTopKQuery, start, end, step)
			require.NoError(t, err)
			res := q.Exec(context.Background())
			require.NoError(t, res.Err)
			took := time.Since(started)
			m, err := res.Matrix()
			require.NoError(t, err)
			series := len(m)
			q.Close()
			close(stop)
			<-done

			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			t.Logf("rightPerGroup=%d %-10s result series=%d allocated=%.1fMB heap-in-use peak above baseline=%.1fMB took=%s",
				rightPerGroup, e.name, series, float64(after.TotalAlloc-before.TotalAlloc)/1e6, (float64(heapPeak)-float64(before.HeapInuse))/1e6, took.Round(time.Millisecond))
		}
	}
}

// Two chained group_left joins onto topk. With the data in TestGroupLeftTopKCrossProduct_LargeQueryShape, per group:
//   - left_metric has 600 series (10 left_kind values x 60 left_id values).
//   - The first join builds 600 x 10 = 6,000 output series: topk passes all 10 sum by (group, left_kind) series to the
//     join, and group_left (left_kind) makes each left series and left_kind pair a different output series.
//   - The second join builds 6,000 x 300 = 1,800,000 output series: topk passes all 300 right_metric series, and
//     group_left (right_id) makes each pair a different output series.
//
// Across 20 groups that is 36,000,000 output series from the second join, for a result of only 12,000 series
// (one per left_metric series), because only the topk winners ever get samples.
const chainedGroupLeftTopKQuery = `left_metric
	* on (group) group_left (left_kind) topk by (group) (1, sum by (group, left_kind) (left_metric))
	* on (group) group_left (right_id) topk by (group) (1, right_metric)`

// TestGroupLeftTopKCrossProduct_LargeQueryShape runs chainedGroupLeftTopKQuery over 7 days at a 1h step, with data where the
// right side only has samples in the last hour. One hour before the end, the right side is empty and the joins are cheap.
// At the end, the joins start to build 36M output series, and the query is refused once the estimate reaches the 1 GiB
// per-query limit. By then the Go heap is several times larger than the estimate, which shows how much memory the
// estimate misses.
//
// It needs about 5 GB of memory and 10 seconds to run, so it only runs when MIMIR_RUN_LARGE_CROSS_PRODUCT_REPRO is set.
func TestGroupLeftTopKCrossProduct_LargeQueryShape(t *testing.T) {
	if os.Getenv("MIMIR_RUN_LARGE_CROSS_PRODUCT_REPRO") == "" {
		t.Skip("set MIMIR_RUN_LARGE_CROSS_PRODUCT_REPRO to run this test")
	}

	// 12,000 left_metric series (20 x 10 x 60) with a sample every hour, and 6,000 right_metric series (20 x 300) with a
	// sample only in the last hour.
	const groups, leftKindsPerGroup, leftPerKind, rightPerGroup, points = 20, 10, 60, 300, 200

	// A lower limit than production (4 GiB) is enough to show the gap, and keeps the test fast and small.
	const memoryLimit = 1 << 30

	// The query covers 7 days at a 1h step, ending at end. lastHour is the index of the last hourly sample in that range.
	start := timestamp.Time(0).Add(24 * time.Hour)
	end := start.Add(168 * time.Hour)
	lastHour := int(end.Sub(timestamp.Time(0)) / time.Hour)

	// Load the data. Each left_metric series has the value of its left_kind index at every hour, so the topk winner of
	// sum by (group, left_kind) never changes. Each right_metric series has one sample, at lastHour, so a query that ends
	// before lastHour sees no right_metric series at all.
	storage := promqltest.LoadedStorage(t, "")
	app := storage.Appender(t.Context())
	for g := range groups {
		group := fmt.Sprintf("g%d", g)
		for k := range leftKindsPerGroup {
			for i := range leftPerKind {
				lbls := labels.FromStrings("__name__", "left_metric", "group", group, "left_kind", fmt.Sprintf("k%d", k), "left_id", fmt.Sprintf("l%d-%d", k, i))
				for p := range points {
					_, err := app.Append(0, lbls, int64(p)*time.Hour.Milliseconds(), float64(k))
					require.NoError(t, err)
				}
			}
		}
		for i := range rightPerGroup {
			lbls := labels.FromStrings("__name__", "right_metric", "group", group, "right_id", fmt.Sprintf("r%d", i))
			_, err := app.Append(0, lbls, int64(lastHour)*time.Hour.Milliseconds(), float64(i))
			require.NoError(t, err)
		}
	}
	require.NoError(t, app.Commit())

	// Create a plain MQE engine with the per-query memory limit. The registry records each query's estimated peak.
	reg := prometheus.NewPedanticRegistry()
	opts := streamingpromql.NewTestEngineOpts()
	opts.CommonOpts.Reg = reg
	limits := streamingpromql.NewStaticQueryLimitsProvider()
	limits.MaxEstimatedMemoryConsumptionPerQuery = memoryLimit
	opts.Limits = limits
	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := streamingpromql.NewEngine(opts, stats.NewQueryMetrics(reg), planner)
	require.NoError(t, err)

	t.Logf("left series=%d, right series=%d, join output series=%d, per-query memory limit=%d bytes",
		groups*leftKindsPerGroup*leftPerKind, groups*rightPerGroup, groups*leftKindsPerGroup*leftPerKind*leftKindsPerGroup*rightPerGroup, memoryLimit)

	// Run the same query twice. Ending one hour early, the right side is empty, so the second join has no match groups
	// and builds no output series. Ending at end, the right side has samples, and the second join builds the cross product.
	for _, tc := range []struct {
		name string
		end  time.Time
	}{
		{"one hour before the end: right side empty", end.Add(-time.Hour)},
		{"at the end: right side has samples", end},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Record the heap before the query, then sample it while the query runs to find its peak.
			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)
			var heapPeak uint64
			stop, done := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(done)
				for {
					var s runtime.MemStats
					runtime.ReadMemStats(&s)
					heapPeak = max(heapPeak, s.HeapInuse)
					select {
					case <-stop:
						return
					case <-time.After(200 * time.Millisecond):
					}
				}
			}()

			// Run the query. The engine's estimated peak metric is cumulative, so take the difference.
			estimateBefore := peakEstimateMB(t, reg)
			started := time.Now()
			q, err := engine.NewRangeQuery(t.Context(), storage, nil, chainedGroupLeftTopKQuery, tc.end.Add(-168*time.Hour), tc.end, time.Hour)
			require.NoError(t, err)
			res := q.Exec(t.Context())
			took := time.Since(started)
			close(stop)
			<-done

			resultSeries := -1
			if res.Err == nil {
				m, err := res.Matrix()
				require.NoError(t, err)
				resultSeries = len(m)
			}
			q.Close()

			// Compare the estimated peak with the real heap peak. In the cross product case the query fails with the memory
			// limit error, and the heap peak is several times the estimate.
			t.Logf("err=%v result series=%d estimated peak=%.1fMB Go heap in use peak above baseline=%.2fGB took=%s",
				res.Err, resultSeries, peakEstimateMB(t, reg)-estimateBefore, (float64(heapPeak)-float64(before.HeapInuse))/1e9, took.Round(time.Millisecond))
		})
	}
}

func peakEstimateMB(t *testing.T, reg *prometheus.Registry) float64 {
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "cortex_mimir_query_engine_estimated_query_peak_memory_consumption" {
			return mf.GetMetric()[0].GetHistogram().GetSampleSum() / 1e6
		}
	}
	return 0
}
