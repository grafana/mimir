// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/bench_test.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/util/teststorage/storage.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streaming

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

type benchCase struct {
	expr  string
	steps int
}

func (c benchCase) Name() string {
	name := fmt.Sprintf("%s", c.expr)

	if c.steps == 0 {
		name += ", instant query"
	} else if c.steps == 1 {
		name += fmt.Sprintf(", range query with %d step", c.steps)
	} else {
		name += fmt.Sprintf(", range query with %d steps", c.steps)
	}

	return name
}

func (c benchCase) Run(t testing.TB, ctx context.Context, start, end time.Time, interval time.Duration, engine promql.QueryEngine, db *tsdb.DB) (*promql.Result, func()) {
	var qry promql.Query
	var err error

	if c.steps == 0 {
		qry, err = engine.NewInstantQuery(ctx, db, nil, c.expr, start)
	} else {
		qry, err = engine.NewRangeQuery(ctx, db, nil, c.expr, start, end, interval)
	}

	if err != nil {
		require.NoError(t, err)
		return nil, nil
	}

	res := qry.Exec(ctx)

	if res.Err != nil {
		require.NoError(t, res.Err)
		return nil, nil
	}

	return res, qry.Close
}

// These test cases are taken from https://github.com/prometheus/prometheus/blob/main/promql/bench_test.go.
func testCases(metricSizes []int) []benchCase {
	cases := []benchCase{
		// Plain retrieval.
		{
			expr: "a_X",
		},
		// Simple rate.
		{
			expr: "rate(a_X[1m])",
		},
		{
			expr:  "rate(a_X[1m])",
			steps: 10000,
		},
		//// Holt-Winters and long ranges.
		//{
		//	expr: "holt_winters(a_X[1d], 0.3, 0.3)",
		//},
		//{
		//	expr: "changes(a_X[1d])",
		//},
		{
			expr: "rate(a_X[1d])",
		},
		//{
		//	expr: "absent_over_time(a_X[1d])",
		//},
		//// Unary operators.
		//{
		//	expr: "-a_X",
		//},
		//// Binary operators.
		//{
		//	expr: "a_X - b_X",
		//},
		//{
		//	expr:  "a_X - b_X",
		//	steps: 10000,
		//},
		//{
		//	expr: "a_X and b_X{l=~'.*[0-4]$'}",
		//},
		//{
		//	expr: "a_X or b_X{l=~'.*[0-4]$'}",
		//},
		//{
		//	expr: "a_X unless b_X{l=~'.*[0-4]$'}",
		//},
		//{
		//	expr: "a_X and b_X{l='notfound'}",
		//},
		//// Simple functions.
		//{
		//	expr: "abs(a_X)",
		//},
		//{
		//	expr: "label_replace(a_X, 'l2', '$1', 'l', '(.*)')",
		//},
		//{
		//	expr: "label_join(a_X, 'l2', '-', 'l', 'l')",
		//},
		// Simple aggregations.
		{
			expr: "sum(a_X)",
		},
		//{
		//	expr: "sum without (l)(h_X)",
		//},
		//{
		//	expr: "sum without (le)(h_X)",
		//},
		{
			expr: "sum by (l)(h_X)",
		},
		{
			expr: "sum by (le)(h_X)",
		},
		//{
		//	expr: "count_values('value', h_X)",
		//  steps: 100,
		//},
		//{
		//	expr: "topk(1, a_X)",
		//},
		//{
		//	expr: "topk(5, a_X)",
		//},
		//// Combinations.
		//{
		//	expr: "rate(a_X[1m]) + rate(b_X[1m])",
		//},
		{
			expr: "sum by (le)(rate(h_X[1m]))",
		},
		//{
		//	expr: "sum without (l)(rate(a_X[1m]))",
		//},
		//{
		//	expr: "sum without (l)(rate(a_X[1m])) / sum without (l)(rate(b_X[1m]))",
		//},
		//{
		//	expr: "histogram_quantile(0.9, rate(h_X[5m]))",
		//},
		//// Many-to-one join.
		//{
		//	expr: "a_X + on(l) group_right a_one",
		//},
		//// Label compared to blank string.
		//{
		//	expr:  "count({__name__!=\"\"})",
		//	steps: 1,
		//},
		//{
		//	expr:  "count({__name__!=\"\",l=\"\"})",
		//	steps: 1,
		//},
		//// Functions which have special handling inside eval()
		//{
		//	expr: "timestamp(a_X)",
		//},
	}

	// X in an expr will be replaced by different metric sizes.
	tmp := []benchCase{}
	for _, c := range cases {
		if !strings.Contains(c.expr, "X") {
			tmp = append(tmp, c)
		} else {
			for _, count := range metricSizes {
				tmp = append(tmp, benchCase{expr: strings.ReplaceAll(c.expr, "X", strconv.Itoa(count)), steps: c.steps})
			}
		}
	}
	cases = tmp

	// No step will be replaced by cases with the standard step.
	tmp = []benchCase{}
	for _, c := range cases {
		if c.steps != 0 {
			tmp = append(tmp, c)
		} else {
			tmp = append(tmp, benchCase{expr: c.expr, steps: 0})
			tmp = append(tmp, benchCase{expr: c.expr, steps: 1})
			tmp = append(tmp, benchCase{expr: c.expr, steps: 100})
			tmp = append(tmp, benchCase{expr: c.expr, steps: 1000})
		}
	}
	return tmp
}

// This is based on the benchmarks from https://github.com/prometheus/prometheus/blob/main/promql/bench_test.go.
func BenchmarkQuery(b *testing.B) {
	db := newTestDB(b)
	db.DisableCompactions() // Don't want auto-compaction disrupting timings.
	opts := promql.EngineOpts{
		Logger:               nil,
		Reg:                  nil,
		MaxSamples:           50000000,
		Timeout:              100 * time.Second,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	}

	standardEngine := promql.NewEngine(opts)
	streamingEngine, err := NewEngine(opts)
	require.NoError(b, err)

	engines := map[string]promql.QueryEngine{
		"standard":  standardEngine,
		"streaming": streamingEngine,
	}

	const interval = 10000 // 10s interval.
	// A day of data plus 10k steps.
	numIntervals := 8640 + 10000

	metricSizes := []int{1, 10, 100, 2000}
	err = setupTestData(db, metricSizes, interval, numIntervals)
	require.NoError(b, err)
	cases := testCases(metricSizes)
	ctx := context.Background()

	for _, c := range cases {
		start := time.Unix(int64((numIntervals-c.steps)*10), 0)
		end := time.Unix(int64(numIntervals*10), 0)
		interval := time.Second * 10

		b.Run(c.Name(), func(b *testing.B) {
			// Check both engines produce the same result before running the benchmark.
			standardResult, standardClose := c.Run(b, ctx, start, end, interval, standardEngine, db)
			streamingResult, streamingClose := c.Run(b, ctx, start, end, interval, streamingEngine, db)

			requireEqualResults(b, standardResult, streamingResult)

			standardClose()
			streamingClose()

			for name, engine := range engines {
				b.Run(name, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						res, cleanup := c.Run(b, ctx, start, end, interval, engine, db)

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
	db := newTestDB(t)
	opts := promql.EngineOpts{
		Logger:               nil,
		Reg:                  nil,
		MaxSamples:           50000000,
		Timeout:              100 * time.Second,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	}

	standardEngine := promql.NewEngine(opts)
	streamingEngine, err := NewEngine(opts)
	require.NoError(t, err)

	const interval = 10000 // 10s interval.
	// A day of data plus 10k steps.
	numIntervals := 8640 + 10000

	metricSizes := []int{1, 10, 100} // Don't bother with 2000 series test here: these test cases take a while and they're most interesting as benchmarks, not correctness tests.
	err = setupTestData(db, metricSizes, interval, numIntervals)
	require.NoError(t, err)
	cases := testCases(metricSizes)

	for _, c := range cases {
		t.Run(c.Name(), func(t *testing.T) {
			start := time.Unix(int64((numIntervals-c.steps)*10), 0)
			end := time.Unix(int64(numIntervals*10), 0)
			interval := time.Second * 10
			ctx := context.Background()

			standardResult, standardClose := c.Run(t, ctx, start, end, interval, standardEngine, db)
			streamingResult, streamingClose := c.Run(t, ctx, start, end, interval, streamingEngine, db)

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

		slices.SortFunc(expectedVector, func(a, b promql.Sample) int {
			return labels.Compare(a.Metric, b.Metric)
		})

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

		slices.SortFunc(expectedMatrix, func(a, b promql.Series) int {
			return labels.Compare(a.Metric, b.Metric)
		})

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

func setupTestData(db *tsdb.DB, metricSizes []int, interval, numIntervals int) error {
	totalMetrics := 0

	for _, size := range metricSizes {
		totalMetrics += 13 * size // 2 non-histogram metrics + 11 metrics for histogram buckets
	}

	metrics := make([]labels.Labels, 0, totalMetrics)

	for _, size := range metricSizes {
		aName := "a_" + strconv.Itoa(size)
		bName := "b_" + strconv.Itoa(size)
		histogramName := "h_" + strconv.Itoa(size)

		if size == 1 {
			// We don't want a "l" label on metrics with one series (some test cases rely on this label not being present).
			metrics = append(metrics, labels.FromStrings("__name__", aName))
			metrics = append(metrics, labels.FromStrings("__name__", bName))
			for le := 0; le < 10; le++ {
				metrics = append(metrics, labels.FromStrings("__name__", histogramName, "le", strconv.Itoa(le)))
			}
			metrics = append(metrics, labels.FromStrings("__name__", histogramName, "le", "+Inf"))
		} else {
			for i := 0; i < size; i++ {
				metrics = append(metrics, labels.FromStrings("__name__", aName, "l", strconv.Itoa(i)))
				metrics = append(metrics, labels.FromStrings("__name__", bName, "l", strconv.Itoa(i)))
				for le := 0; le < 10; le++ {
					metrics = append(metrics, labels.FromStrings("__name__", histogramName, "l", strconv.Itoa(i), "le", strconv.Itoa(le)))
				}
				metrics = append(metrics, labels.FromStrings("__name__", histogramName, "l", strconv.Itoa(i), "le", "+Inf"))
			}
		}
	}

	refs := make([]storage.SeriesRef, len(metrics))

	for s := 0; s < numIntervals; s++ {
		a := db.Appender(context.Background())
		ts := int64(s * interval)
		for i, metric := range metrics {
			ref, _ := a.Append(refs[i], metric, ts, float64(s)+float64(i)/float64(len(metrics)))
			refs[i] = ref
		}
		if err := a.Commit(); err != nil {
			return err
		}
	}

	db.ForceHeadMMap() // Ensure we have at most one head chunk for every series.
	return db.Compact(context.Background())
}

// This is based on https://github.com/prometheus/prometheus/blob/main/util/teststorage/storage.go, but with isolation disabled
// to improve test setup performance and mirror Mimir's default configuration.
func newTestDB(t testing.TB) *tsdb.DB {
	dir := t.TempDir()

	// Tests just load data for a series sequentially. Thus we need a long appendable window.
	opts := tsdb.DefaultOptions()
	opts.MinBlockDuration = int64(24 * time.Hour / time.Millisecond)
	opts.MaxBlockDuration = int64(24 * time.Hour / time.Millisecond)
	opts.RetentionDuration = 0
	opts.EnableNativeHistograms = true
	opts.IsolationDisabled = true
	db, err := tsdb.Open(dir, nil, nil, opts, tsdb.NewDBStats())
	require.NoError(t, err, "unexpected error while opening test storage")

	t.Cleanup(func() {
		require.NoError(t, db.Close(), "unexpected error while closing test storage")
	})

	return db
}
