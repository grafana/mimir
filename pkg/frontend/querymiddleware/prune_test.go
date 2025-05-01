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
	replacer := strings.NewReplacer("<series name>", seriesName, "<num samples>", fmt.Sprintf("%d", numSamples))
	data := replacer.Replace(`
		load 1m
			<series name>{series="1"} 0+1x<num samples>
			<series name>{series="2"} 0+2x<num samples>
			<series name>{series="3"} 0+3x<num samples>
			<series name>{series="4"} 0+4x<num samples>
			<series name>{series="5"} 0+5x<num samples>
	`)

	const step = 20 * time.Second

	queryable := promqltest.LoadedStorage(t, data)

	type template struct {
		query   string
		IsEmpty bool
	}

	templates := []template{
		{`(avg(rate(%s[1m1s])))`, false},
		{`(avg(rate(%s[1m1s]))) and on() (vector(0) == 1)`, true},
		{`(avg(rate(%s[1m1s]))) and on() (vector(1) == 1)`, false},
		{`avg(rate(%s[1m1s])) or (avg(rate(test_float[1m1s]))) and on() (vector(0) == 1)`, false},
	}
	for _, template := range templates {
		t.Run(template.query, func(t *testing.T) {
			runForEngines(t, func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine) {
				pruningware := newPruneMiddleware(log.NewNopLogger())
				downstream := &downstreamHandler{
					engine:    eng,
					queryable: queryable,
				}

				query := fmt.Sprintf(template.query, seriesName)
				req := &PrometheusRangeQueryRequest{
					path:      "/query_range",
					start:     0,
					end:       int64(numSamples) * time.Minute.Milliseconds(),
					step:      step.Milliseconds(),
					queryExpr: parseQuery(t, query),
				}

				injectedContext := user.InjectOrgID(context.Background(), "test")

				// Run the query without pruning.
				expectedRes, err := downstream.Do(injectedContext, req)
				require.Nil(t, err)
				expectedPrometheusResponse, ok := expectedRes.GetPrometheusResponse()
				require.True(t, ok)

				if !template.IsEmpty {
					// Ensure the query produces some results.
					require.NotEmpty(t, expectedPrometheusResponse.Data.Result)
					requireValidSamples(t, expectedPrometheusResponse.Data.Result)
				}

				// Run the query with pruning.
				prunedRes, err := pruningware.Wrap(downstream).Do(injectedContext, req)
				require.Nil(t, err)
				prunedPromethusResponse, ok := prunedRes.GetPrometheusResponse()
				require.True(t, ok)

				if !template.IsEmpty {
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
