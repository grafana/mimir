// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/bench_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package benchmarks

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

// This file contains the details of the benchmarks so that tools/benchmark-query-engine can use the same information.
// tools/benchmark-query-engine needs to know the exact name of each benchmark case so that it can run each in a separate process.
// (go test -list only lists top-level tests, not sub-tests.)

var MetricSizes = []int{1, 100, 2000}

type BenchCase struct {
	Expr             string
	Steps            int
	InstantQueryOnly bool
}

func (c BenchCase) Name() string {
	name := c.Expr

	if c.Steps == 0 {
		name += ", instant query"
	} else if c.Steps == 1 {
		name += fmt.Sprintf(", range query with %d step", c.Steps)
	} else {
		name += fmt.Sprintf(", range query with %d steps", c.Steps)
	}

	return name
}

func (c BenchCase) Run(ctx context.Context, t testing.TB, start, end time.Time, interval time.Duration, engine promql.QueryEngine, q storage.Queryable) (*promql.Result, func()) {
	var qry promql.Query
	var err error

	if c.Steps == 0 {
		qry, err = engine.NewInstantQuery(ctx, q, nil, c.Expr, start)
	} else {
		qry, err = engine.NewRangeQuery(ctx, q, nil, c.Expr, start, end, interval)
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

// These test cases are taken from https://github.com/prometheus/prometheus/blob/main/promql/bench_test.go
// and enhanced/added to.
func TestCases(metricSizes []int) []BenchCase {
	cases := []BenchCase{
		// Plain retrieval.
		{
			Expr: "a_X",
		},
		// Histogram retrieval
		{
			Expr: "nh_X",
		},
		// Range vector selector.
		{
			Expr:             "a_X[1m]",
			InstantQueryOnly: true,
		},
		// Simple rate.
		{
			Expr: "rate(a_X[1m])",
		},
		{
			Expr:  "rate(a_X[1m])",
			Steps: 10000,
		},
		{
			Expr: "rate(nh_X[1m])",
		},
		{
			Expr:  "rate(nh_X[1m])",
			Steps: 10000,
		},
		//// Holt-Winters and long ranges.
		//{
		//	Expr: "holt_winters(a_X[1d], 0.3, 0.3)",
		//},
		{
			Expr: "changes(a_X[1d])",
		},
		{
			Expr: "rate(a_X[1d])",
		},
		// Test a long range histogram. 1d slows tests down too much, so use 1h.
		{
			Expr: "rate(nh_X[1h])",
		},
		{
			Expr: "avg_over_time(a_X[1m])",
		},
		{
			Expr: "avg_over_time(nh_X[1m])",
		},
		{
			Expr: "sum_over_time(a_X[1m])",
		},
		{
			Expr: "sum_over_time(nh_X[1m])",
		},
		//{
		//	Expr: "absent_over_time(a_X[1d])",
		//},
		// Subqueries.
		{
			Expr: "sum_over_time(a_X[10m:3m])",
		},
		{
			Expr: "sum_over_time(nh_X[10m:3m])",
		},
		{
			Expr: "sum(sum_over_time(a_X[10m:3m]))",
		},
		// Unary operators.
		{
			Expr: "-a_X",
		},
		// Binary operators.
		{
			Expr: "a_X - b_X",
		},
		{
			Expr:  "a_X - b_X",
			Steps: 10000,
		},
		{
			Expr: "nh_X + nh_X",
		},
		{
			Expr: "nh_X / a_X",
		},
		{
			Expr: "a_X == b_X",
		},
		{
			Expr: "2 * a_X",
		},
		{
			Expr: "nh_X / 2",
		},
		{
			Expr: "h_X * on(l) group_left() a_X",
		},
		// Test the case where one side of a binary operation has many more series than the other.
		{
			Expr: `a_100{l=~"[13579]."} - b_100`,
		},
		{
			Expr: `a_2000{l=~"1..."} - b_2000`,
		},
		{
			Expr: `a_2000{l="1234"} - b_2000`,
		},
		{
			Expr: `a_2000 - b_2000{l="1234"}`,
		},
		{
			Expr: "a_X and b_X{l=~'.*[0-4]$'}",
		},
		{
			Expr: "a_X or b_X{l=~'.*[0-4]$'}",
		},
		{
			Expr: "a_X unless b_X{l=~'.*[0-4]$'}",
		},
		{
			Expr: "a_X and b_X{l='notfound'}",
		},
		// Simple functions.
		{
			Expr: "abs(a_X)",
		},
		{
			Expr: "clamp(a_X, 100, 1000)",
		},
		{
			Expr: "label_replace(a_X, 'l2', '$1', 'l', '(.*)')",
		},
		//{
		//	Expr: "label_join(a_X, 'l2', '-', 'l', 'l')",
		//},
		// Simple aggregations.
		{
			Expr: "sum(a_X)",
		},
		{
			Expr: "sum without (l)(h_X)",
		},
		{
			Expr: "sum without (le)(h_X)",
		},
		{
			Expr: "sum(nh_X)",
		},
		{
			Expr: "sum by (l)(h_X)",
		},
		{
			Expr: "sum by (le)(h_X)",
		},
		{
			Expr: "sum by (l)(nh_X)",
		},
		{
			Expr: "avg(a_X)",
		},
		{
			Expr: "avg without (l)(h_X)",
		},
		{
			Expr: "avg without (le)(h_X)",
		},
		{
			Expr: "avg(nh_X)",
		},
		{
			Expr: "avg by (l)(h_X)",
		},
		{
			Expr: "avg by (le)(h_X)",
		},
		{
			Expr: "avg by (l)(nh_X)",
		},
		//{
		//	Expr: "count_values('value', h_X)",
		//  Steps: 100,
		//},
		{
			Expr: "topk(1, a_X)",
		},
		{
			Expr: "topk(5, a_X)",
		},
		// Combinations.
		{
			Expr: "rate(a_X[1m]) + rate(b_X[1m])",
		},
		{
			Expr: "sum(a_X + b_X)",
		},
		{
			Expr: "sum by (le)(rate(h_X[1m]))",
		},
		{
			Expr: "sum without (l)(rate(a_X[1m]))",
		},
		{
			Expr: "sum without (l)(rate(a_X[1m])) / sum without (l)(rate(b_X[1m]))",
		},
		{
			Expr: "histogram_quantile(0.9, h_X)",
		},
		{
			Expr: "histogram_quantile(0.9, nh_X)",
		},
		// Label compared to blank string.
		{
			Expr:  "count({__name__!=\"\"})",
			Steps: 1,
		},
		{
			Expr:  "count({__name__!=\"\",l=\"\"})",
			Steps: 1,
		},
		// Functions which have special handling inside eval()
		{
			Expr: "timestamp(a_X)",
		},
	}

	// X in an expr will be replaced by different metric sizes.
	tmp := []BenchCase{}
	for _, c := range cases {
		if !strings.Contains(c.Expr, "X") {
			tmp = append(tmp, c)
		} else {
			for _, count := range metricSizes {
				tmp = append(tmp, BenchCase{Expr: strings.ReplaceAll(c.Expr, "X", strconv.Itoa(count)), Steps: c.Steps, InstantQueryOnly: c.InstantQueryOnly})
			}
		}
	}
	cases = tmp

	// No step will be replaced by cases with the standard step.
	tmp = []BenchCase{}
	for _, c := range cases {
		if c.Steps != 0 || c.InstantQueryOnly {
			if c.InstantQueryOnly && c.Steps != 0 {
				panic(fmt.Sprintf("invalid test case '%v': configured as instant query with non-zero number of steps %v", c.Expr, c.Steps))
			}

			if c.Steps >= NumIntervals {
				// Note that this doesn't check we have enough data to cover any range selectors.
				panic(fmt.Sprintf("invalid test case '%v' with %v steps: test setup only creates %v steps", c.Expr, c.Steps, NumIntervals))
			}

			tmp = append(tmp, c)
		} else {
			tmp = append(tmp, BenchCase{Expr: c.Expr, Steps: 0})
			tmp = append(tmp, BenchCase{Expr: c.Expr, Steps: 100})
			tmp = append(tmp, BenchCase{Expr: c.Expr, Steps: 1000})
			// Important: if adding test cases with larger numbers of steps, make sure to adjust NumIntervals as well.
		}
	}

	return tmp
}
