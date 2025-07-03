// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"
)

func TestQueryPruning(t *testing.T) {
	numSamples := 100
	seriesName := `test_float`
	seriesName2 := `test_float2`
	replacer := strings.NewReplacer("<series name>", seriesName, "<series name 2>", seriesName2, "<num samples>", fmt.Sprintf("%d", numSamples))
	data := replacer.Replace(`
		load 1m
			<series name>{series="1"} 0+1x<num samples>
			<series name>{series="2"} 0+2x<num samples>
			<series name>{series="3"} 0+3x<num samples>
			<series name>{series="4"} 0+4x<num samples>
			<series name>{series="5"} 0+5x<num samples>
			<series name 2>{series="1"} 0+1x<num samples>
			<series name 2>{series="2"} 0+2x<num samples>
			<series name 2>{series="3"} 0+3x<num samples>
			<series name 2>{series="4"} 0+4x<num samples>
			<series name 2>{series="5"} 0+5x<num samples>
	`)

	const step = 20 * time.Second

	queryable := promqltest.LoadedStorage(t, data)

	type testCase struct {
		query   string
		IsEmpty bool
	}

	testCases := []testCase{
		{`(avg(rate(test_float[1m1s])))`, false},
		{`(avg(rate(test_float[1m1s]))) and on() (vector(0) == 1)`, true},
		{`(avg(rate(test_float[1m1s]))) and on() (vector(1) == 1)`, false},
		{`avg(rate(test_float[1m1s])) or (avg(rate(test_float[1m1s]))) and on() (vector(0) == 1)`, false},
		{`test_float{series="3"} * test_float2`, false},
		{`test_float + test_float2{series="2"}`, false},
		{`test_float{series="1"} / test_float2{series="5"}`, true},
	}
	for _, tc := range testCases {
		t.Run(tc.query, func(t *testing.T) {
			runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
				pruningware := newPruneMiddleware(
					log.NewNopLogger(),
					Config{
						PruneQueriesToggle:           true,
						PruneQueriesMatcherPropagate: true,
					},
				)
				downstream := &downstreamHandler{
					engine:    eng,
					queryable: queryable,
				}

				req := &PrometheusRangeQueryRequest{
					path:      "/query_range",
					start:     0,
					end:       int64(numSamples) * time.Minute.Milliseconds(),
					step:      step.Milliseconds(),
					queryExpr: parseQuery(t, tc.query),
				}

				injectedContext := user.InjectOrgID(context.Background(), "test")

				// Run the query without pruning.
				expectedRes, err := downstream.Do(injectedContext, req)
				require.Nil(t, err)
				expectedPrometheusResponse, ok := expectedRes.GetPrometheusResponse()
				require.True(t, ok)

				if !tc.IsEmpty {
					// Ensure the query produces some results.
					require.NotEmpty(t, expectedPrometheusResponse.Data.Result)
					requireValidSamples(t, expectedPrometheusResponse.Data.Result)
				}

				// Run the query with pruning.
				prunedRes, err := pruningware.Wrap(downstream).Do(injectedContext, req)
				require.Nil(t, err)
				prunedPromethusResponse, ok := prunedRes.GetPrometheusResponse()
				require.True(t, ok)

				if !tc.IsEmpty {
					// Ensure the query produces some results.
					require.NotEmpty(t, prunedPromethusResponse.Data.Result)
					requireValidSamples(t, prunedPromethusResponse.Data.Result)
				}

				// Ensure the results are approximately equal.
				approximatelyEqualsSamples(t, expectedPrometheusResponse, prunedPromethusResponse)
			})
		})
	}
}
