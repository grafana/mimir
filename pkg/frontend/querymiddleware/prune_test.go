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
	queryable := promqltest.LoadedStorage(t, data)

	const step = 20 * time.Second

	engine := newEngine()
	pruningware := newPruneMiddleware(
		log.NewNopLogger(),
	)
	downstream := &downstreamHandler{
		engine:    engine,
		queryable: queryable,
	}

	type template struct {
		query   string
		IsEmpty bool
	}

	templates := []template{
		{`avg(rate(%s[1m])) < (-1 * +Inf)`, true},
		{`avg(rate(%s[1m])) < (+1 * +Inf)`, false},
		{`avg(rate(%s[1m])) < (-1 * -Inf)`, false},
		{`avg(rate(%s[1m])) < (+1 * -Inf)`, true},
		{`(-1 * -Inf) < avg(rate(%s[1m]))`, true},
		{`((-1 * -Inf) < avg(rate(foo[2m]))) or avg(rate(%s[1m]))`, false},
		{`((-1 * -Inf) < avg(rate(foo[2m]))) and avg(rate(%s[1m]))`, true},
		{`((-1 * -Inf) < avg(rate(foo[2m]))) unless avg(rate(%s[1m]))`, true},
		{`avg(rate(%s[1m])) unless ((-1 * -Inf) < avg(rate(foo[2m])))`, false},
		{`(((-1 * -Inf) < avg(rate(foo[3m]))) unless avg(rate(foo[2m]))) or avg(rate(%s[1m]))`, true},
		{`((((-1 * -Inf) < avg(rate(foo[4m]))) unless avg(rate(foo[3m]))) and avg(rate(foo[2m]))) or avg(rate(%s[1m]))`, true},
	}
	for _, template := range templates {
		t.Run(template.query, func(t *testing.T) {
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

			if !template.IsEmpty {
				// Ensure the query produces some results.
				require.NotEmpty(t, expectedRes.(*PrometheusResponse).Data.Result)
				requireValidSamples(t, expectedRes.(*PrometheusResponse).Data.Result)
			}

			// Run the query with pruning.
			prunedRes, err := pruningware.Wrap(downstream).Do(injectedContext, req)
			require.Nil(t, err)

			if !template.IsEmpty {
				// Ensure the query produces some results.
				require.NotEmpty(t, prunedRes.(*PrometheusResponse).Data.Result)
				requireValidSamples(t, prunedRes.(*PrometheusResponse).Data.Result)
			}

			// Ensure the results are approximately equal.
			approximatelyEqualsSamples(t, expectedRes.(*PrometheusResponse), prunedRes.(*PrometheusResponse))
		})
	}
}
