// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestQueryPruning(t *testing.T) {
	numSeries := 10
	endTime := 100
	storageSeries := make([]*promql.StorageSeries, 0, numSeries)
	floats := make([]promql.FPoint, 0, endTime)
	for i := 0; i < endTime; i++ {
		floats = append(floats, promql.FPoint{
			T: int64(i * 1000),
			F: float64(i),
		})
	}
	histograms := make([]promql.HPoint, 0)
	seriesName := `test_float`
	for i := 0; i < numSeries; i++ {
		nss := promql.NewStorageSeries(promql.Series{
			Metric:     labels.FromStrings("__name__", seriesName, "series", fmt.Sprint(i)),
			Floats:     floats,
			Histograms: histograms,
		})
		storageSeries = append(storageSeries, nss)
	}
	queryable := storageSeriesQueryable(storageSeries)

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
				end:       int64(endTime * 1000),
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
