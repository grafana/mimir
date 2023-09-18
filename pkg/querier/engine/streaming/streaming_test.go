// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/bench_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streaming

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/teststorage"
)

func setupTestData(stor *teststorage.TestStorage, interval, numIntervals int) error {
	metrics := make([]labels.Labels, 0, (3+10)+10*(3+10)+100*(3+10)+2000*(3+10))
	metrics = append(metrics, labels.FromStrings("__name__", "a_one"))
	metrics = append(metrics, labels.FromStrings("__name__", "b_one"))
	for j := 0; j < 10; j++ {
		metrics = append(metrics, labels.FromStrings("__name__", "h_one", "le", strconv.Itoa(j)))
	}
	metrics = append(metrics, labels.FromStrings("__name__", "h_one", "le", "+Inf"))

	for i := 0; i < 10; i++ {
		metrics = append(metrics, labels.FromStrings("__name__", "a_ten", "l", strconv.Itoa(i)))
		metrics = append(metrics, labels.FromStrings("__name__", "b_ten", "l", strconv.Itoa(i)))
		for j := 0; j < 10; j++ {
			metrics = append(metrics, labels.FromStrings("__name__", "h_ten", "l", strconv.Itoa(i), "le", strconv.Itoa(j)))
		}
		metrics = append(metrics, labels.FromStrings("__name__", "h_ten", "l", strconv.Itoa(i), "le", "+Inf"))
	}

	for i := 0; i < 100; i++ {
		metrics = append(metrics, labels.FromStrings("__name__", "a_hundred", "l", strconv.Itoa(i)))
		metrics = append(metrics, labels.FromStrings("__name__", "b_hundred", "l", strconv.Itoa(i)))
		for j := 0; j < 10; j++ {
			metrics = append(metrics, labels.FromStrings("__name__", "h_hundred", "l", strconv.Itoa(i), "le", strconv.Itoa(j)))
		}
		metrics = append(metrics, labels.FromStrings("__name__", "h_hundred", "l", strconv.Itoa(i), "le", "+Inf"))
	}

	for i := 0; i < 2000; i++ {
		metrics = append(metrics, labels.FromStrings("__name__", "a_two_thousand", "l", strconv.Itoa(i)))
		metrics = append(metrics, labels.FromStrings("__name__", "b_two_thousand", "l", strconv.Itoa(i)))
		for j := 0; j < 10; j++ {
			metrics = append(metrics, labels.FromStrings("__name__", "h_two_thousand", "l", strconv.Itoa(i), "le", strconv.Itoa(j)))
		}
		metrics = append(metrics, labels.FromStrings("__name__", "h_two_thousand", "l", strconv.Itoa(i), "le", "+Inf"))
	}
	refs := make([]storage.SeriesRef, len(metrics))

	for s := 0; s < numIntervals; s++ {
		a := stor.Appender(context.Background())
		ts := int64(s * interval)
		for i, metric := range metrics {
			ref, _ := a.Append(refs[i], metric, ts, float64(s)+float64(i)/float64(len(metrics)))
			refs[i] = ref
		}
		if err := a.Commit(); err != nil {
			return err
		}
	}
	return nil
}

type benchCase struct {
	expr  string
	steps int
}

func testCases() []benchCase {
	cases := []benchCase{
		// Plain retrieval.
		{
			expr: "a_X",
		},
		// Simple rate.
		{
			expr: "rate(a_X[1m])",
		},
		//{
		//	expr:  "rate(a_X[1m])",
		//	steps: 10000,
		//},
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
	}

	// X in an expr will be replaced by different metric sizes.
	tmp := []benchCase{}
	for _, c := range cases {
		if !strings.Contains(c.expr, "X") {
			tmp = append(tmp, c)
		} else {
			tmp = append(tmp, benchCase{expr: strings.ReplaceAll(c.expr, "X", "one"), steps: c.steps})
			tmp = append(tmp, benchCase{expr: strings.ReplaceAll(c.expr, "X", "ten"), steps: c.steps})
			tmp = append(tmp, benchCase{expr: strings.ReplaceAll(c.expr, "X", "hundred"), steps: c.steps})
			tmp = append(tmp, benchCase{expr: strings.ReplaceAll(c.expr, "X", "two_thousand"), steps: c.steps})
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
			tmp = append(tmp, benchCase{expr: c.expr, steps: 10})
			tmp = append(tmp, benchCase{expr: c.expr, steps: 100})
			tmp = append(tmp, benchCase{expr: c.expr, steps: 1000})
		}
	}
	return tmp
}

func BenchmarkQuery(b *testing.B) {
	stor := teststorage.New(b)
	defer stor.Close()
	opts := promql.EngineOpts{
		Logger:     nil,
		Reg:        nil,
		MaxSamples: 50000000,
		Timeout:    100 * time.Second,
	}

	engines := map[string]promql.QueryEngine{
		"standard":  promql.NewEngine(opts),
		"streaming": NewEngine(opts),
	}

	const interval = 10000 // 10s interval.
	// A day of data plus 10k steps.
	numIntervals := 8640 + 10000

	err := setupTestData(stor, interval, numIntervals)
	if err != nil {
		b.Fatal(err)
	}
	cases := testCases()

	for _, c := range cases {
		b.Run(fmt.Sprintf("expr=%s,steps=%d", c.expr, c.steps), func(b *testing.B) {
			for name, engine := range engines {
				b.Run(name, func(b *testing.B) {
					ctx := context.Background()
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						var qry promql.Query
						var err error

						start := time.Unix(int64((numIntervals-c.steps)*10), 0)
						end := time.Unix(int64(numIntervals*10), 0)
						interval := time.Second * 10

						if c.steps == 0 {
							qry, err = engine.NewInstantQuery(ctx, stor, nil, c.expr, start)
						} else {
							qry, err = engine.NewRangeQuery(ctx, stor, nil, c.expr, start, end, interval)
						}

						if err != nil {
							b.Fatal(err)
						}
						res := qry.Exec(ctx)
						if res.Err != nil {
							b.Fatal(res.Err)
						}
						qry.Close()
					}
				})
			}
		})
	}
}

func TestQuery(t *testing.T) {
	stor := teststorage.New(t)
	defer stor.Close()
	opts := promql.EngineOpts{
		Logger:     nil,
		Reg:        nil,
		MaxSamples: 50000000,
		Timeout:    100 * time.Second,
	}

	standardEngine := promql.NewEngine(opts)
	streamingEngine := NewEngine(opts)

	const interval = 10000 // 10s interval.
	// A day of data plus 10k steps.
	numIntervals := 8640 + 10000

	err := setupTestData(stor, interval, numIntervals)
	require.NoError(t, err)
	cases := testCases()

	runQuery := func(t *testing.T, engine promql.QueryEngine, c benchCase) (*promql.Result, func()) {
		ctx := context.Background()
		var qry promql.Query
		var err error

		start := time.Unix(int64((numIntervals-c.steps)*10), 0)
		end := time.Unix(int64(numIntervals*10), 0)
		interval := time.Second * 10

		if c.steps == 0 {
			qry, err = engine.NewInstantQuery(ctx, stor, nil, c.expr, start)
		} else {
			qry, err = engine.NewRangeQuery(ctx, stor, nil, c.expr, start, end, interval)
		}

		if errors.Is(err, ErrNotSupported) {
			t.Skipf("skipping, query is not supported by streaming engine: %v", err)
		}

		require.NoError(t, err)
		res := qry.Exec(ctx)
		require.NoError(t, res.Err)

		return res, qry.Close
	}

	for _, c := range cases {
		name := fmt.Sprintf("expr=%s,steps=%d", c.expr, c.steps)
		t.Run(name, func(t *testing.T) {
			standardResult, standardClose := runQuery(t, standardEngine, c)
			streamingResult, streamingClose := runQuery(t, streamingEngine, c)

			// Why do we do this rather than require.Equal(t, standardResult, streamingResult)?
			// It's possible that floating point values are slightly different due to imprecision, but require.Equal doesn't allow us to set an allowable difference.
			require.Equal(t, standardResult.Err, streamingResult.Err)
			require.Equal(t, standardResult.Warnings, streamingResult.Warnings)

			if c.steps == 0 {
				standardVector, err := standardResult.Vector()
				require.NoError(t, err)
				streamingVector, err := streamingResult.Vector()
				require.NoError(t, err)

				// There's no guarantee that series from the standard engine are sorted.
				slices.SortFunc(standardVector, func(a, b promql.Sample) bool {
					return labels.Compare(a.Metric, b.Metric) < 0
				})

				require.Len(t, streamingVector, len(standardVector))

				for i, standardSample := range standardVector {
					streamingSample := streamingVector[i]

					require.Equal(t, standardSample.Metric, streamingSample.Metric)
					require.Equal(t, standardSample.T, streamingSample.T)
					require.Equal(t, standardSample.H, streamingSample.H)
					require.InEpsilon(t, standardSample.F, streamingSample.F, 1e-10)
				}
			} else {
				standardMatrix, err := standardResult.Matrix()
				require.NoError(t, err)
				streamingMatrix, err := streamingResult.Matrix()
				require.NoError(t, err)

				// There's no guarantee that series from the standard engine are sorted.
				slices.SortFunc(standardMatrix, func(a, b promql.Series) bool {
					return labels.Compare(a.Metric, b.Metric) < 0
				})

				require.Len(t, streamingMatrix, len(standardMatrix))

				for i, standardSeries := range standardMatrix {
					streamingSeries := streamingMatrix[i]

					require.Equal(t, standardSeries.Metric, streamingSeries.Metric)
					require.Equal(t, standardSeries.Histograms, streamingSeries.Histograms)

					for j, standardPoint := range standardSeries.Floats {
						streamingPoint := streamingSeries.Floats[j]

						require.Equal(t, standardPoint.T, streamingPoint.T)
						require.InEpsilonf(t, standardPoint.F, streamingPoint.F, 1e-10, "expected series %v to have points %v, but streaming result is %v", standardSeries.Metric.String(), standardSeries.Floats, streamingSeries.Floats)
					}
				}
			}

			standardClose()
			streamingClose()
		})
	}
}
