// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"fmt"
	rand "math/rand/v2"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/querier/stats"
)

func TestConcurrentQueries(t *testing.T) {
	// This test exists to catch issues with memory pooling, particularly issues like returning the same instance to the pool multiple times,
	// or using the same instance in two queries simultaneously.
	//
	// It is currently fairly basic and focuses on the riskiest area, native histograms, where both HPoint slices and the FloatHistogram instances
	// themselves are reused in different ways.
	//
	// Future improvements would include adding other kinds of queries, testing instant queries, and varying the time range queried to catch other
	// issues.

	const data = `
		load 1m
			native_histogram{group="a", instance="1"} {{schema:3 sum:4 count:4 buckets:[1 2 1]}}+{{schema:3 sum:2 count:6 buckets:[1 2 3]}}x9
			native_histogram{group="a", instance="2"} {{schema:5 sum:8 count:7 buckets:[1 5 1]}}+{{schema:5 sum:10 count:7 buckets:[1 2 3 1]}}x9
			native_histogram{group="b", instance="1"} {{schema:5 sum:10 count:7 buckets:[1 2 3 1]}}+{{schema:5 sum:8 count:7 buckets:[1 5 1]}}x9
			native_histogram{group="b", instance="2"} {{schema:3 sum:4 count:4 buckets:[4]}}
			float{group="a", instance="1"} 1+2x9
			float{group="a", instance="2"} 3+2.5x9
			float{group="b", instance="1"} 1 2 4 2 7 1 2 3 4
			float{group="b", instance="2"} 1 9 2 8 11 12 13 14 15
	`

	startT := timestamp.Time(0)

	testCases := []concurrentQueryTestCase{
		{
			expr:  `rate(native_histogram[5m])`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			// Sum of native histograms into single group
			expr:  `sum(native_histogram)`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			// Sum of native histograms into many groups
			expr:  `sum by (group) (native_histogram)`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			expr:  `sum(rate(native_histogram[5m]))`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			expr:  `count_over_time(native_histogram[5m])`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			expr:  `sum_over_time(native_histogram[5m])`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			expr:  `avg_over_time(native_histogram[5m])`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			expr:  `rate(float[5m])`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			// Sum of floats into single group
			expr:  `sum(float)`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			// Sum of floats into many groups
			expr:  `sum by (group) (float)`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			expr:  `sum(rate(float[5m]))`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			expr:  `count_over_time(float[5m])`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			expr:  `sum_over_time(float[5m])`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			expr:  `avg_over_time(float[5m])`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			expr:  `float{group="a"} and on (instance) float{group="b"}`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			expr:  `native_histogram{group="a"} and on (instance) float{group="b"}`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			expr:  `native_histogram{group="a"} and on (instance) native_histogram{group="b"}`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
		{
			expr:  `{group="a"} and on (instance) float{group="b"}`,
			start: startT,
			end:   startT.Add(10 * time.Minute),
			step:  time.Minute,
		},
	}

	storage := promqltest.LoadedStorage(t, data)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	engine, err := NewEngine(NewTestEngineOpts(), NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)

	// Populate the expected result for each query.
	for i, testCase := range testCases {
		q, err := testCase.NewQuery(context.Background(), engine, storage)
		require.NoErrorf(t, err, "could not create query for test case at index %v (%v)", i, testCase.expr)

		res := q.Exec(context.Background())
		require.NoErrorf(t, err, "could not execute query for test case at index %v (%v)", i, testCase.expr)

		// Turn the result into a string, so we don't have to worry about making a copy of the data returned.
		testCases[i].expectedResult = res.String()

		q.Close()
	}

	workerCount := len(testCases) * 3 // Run enough workers that we're likely to be running each query simultaneously.
	const queriesPerWorker = 100

	eg, ctx := errgroup.WithContext(context.Background())

	runOneQuery := func() error {
		if ctx.Err() != nil {
			// If the context has been cancelled, stop now.
			return ctx.Err()
		}

		testCase := testCases[rand.IntN(len(testCases))]
		q, err := testCase.NewQuery(ctx, engine, storage)
		if err != nil {
			return fmt.Errorf("test case '%v': creating query failed: %w", testCase.expr, err)
		}

		defer q.Close()
		res := q.Exec(ctx)

		if res.Err != nil {
			return res.Err
		}

		resString := res.String()
		if resString != testCase.expectedResult {
			return fmt.Errorf("test case '%v':\nexpected:\n%v\n\ngot:\n%v", testCase.expr, testCase.expectedResult, resString)
		}

		return nil
	}

	for i := 0; i < workerCount; i++ {
		eg.Go(func() error {
			for i := 0; i < queriesPerWorker; i++ {
				if err := runOneQuery(); err != nil {
					return err
				}
			}

			return nil
		})
	}

	require.NoError(t, eg.Wait())
}

type concurrentQueryTestCase struct {
	expr           string
	start          time.Time
	end            time.Time
	step           time.Duration
	expectedResult string
}

func (c concurrentQueryTestCase) NewQuery(ctx context.Context, engine promql.QueryEngine, q storage.Queryable) (promql.Query, error) {
	return engine.NewRangeQuery(ctx, q, nil, c.expr, c.start, c.end, c.step)
}
