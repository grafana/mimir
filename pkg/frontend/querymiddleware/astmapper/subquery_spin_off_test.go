// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubquerySpinOffMapper(t *testing.T) {
	for _, tt := range []struct {
		name                      string
		in                        string
		out                       string
		expectedSubqueries        int
		expectedDownstreamQueries int
	}{
		{
			name:                      "subquery too simple",
			in:                        `avg_over_time(foo[3d:1m])`,
			out:                       `__downstream_query__{__query__="avg_over_time(foo[3d:1m])"}`,
			expectedSubqueries:        0,
			expectedDownstreamQueries: 1,
		},
		{
			name:                      "spin off subquery",
			in:                        `avg_over_time((foo * bar)[3d:1m])`,
			out:                       `avg_over_time(__subquery_spinoff__{__query__="(foo * bar)",__range__="72h0m0s",__step__="1m0s"}[3d])`,
			expectedSubqueries:        1,
			expectedDownstreamQueries: 0,
		},
		{
			name:                      "range too short",
			in:                        `avg_over_time((foo * bar)[30m:1m])`,
			out:                       `__downstream_query__{__query__="avg_over_time((foo * bar)[30m:1m])"}`,
			expectedSubqueries:        0,
			expectedDownstreamQueries: 1,
		},
		{
			name:                      "too few steps",
			in:                        `avg_over_time((foo * bar)[3d:1d])`,
			out:                       `__downstream_query__{__query__="avg_over_time((foo * bar)[3d:1d])"}`,
			expectedSubqueries:        0,
			expectedDownstreamQueries: 1,
		},
		{
			name: "spin off multiple subqueries",
			in:   `avg_over_time((foo * bar)[3d:1m]) * max_over_time((foo * bar)[2d:2m])`,
			out: `avg_over_time(__subquery_spinoff__{__query__="(foo * bar)",__range__="72h0m0s",__step__="1m0s"}[3d])
									* max_over_time(__subquery_spinoff__{__query__="(foo * bar)",__range__="48h0m0s",__step__="2m0s"}[2d])`,
			expectedSubqueries:        2,
			expectedDownstreamQueries: 0,
		},
		{
			name:                      "downstream query",
			in:                        `avg_over_time((foo * bar)[3d:1m]) * avg_over_time(foo[3d])`,
			out:                       `avg_over_time(__subquery_spinoff__{__query__="(foo * bar)",__range__="72h0m0s",__step__="1m0s"}[3d]) * __downstream_query__{__query__="avg_over_time(foo[3d])"}`,
			expectedSubqueries:        1,
			expectedDownstreamQueries: 1,
		},
		{
			name:                      "scalars",
			in:                        `avg_over_time((foo * bar)[3d:1m]) * 2`,
			out:                       `avg_over_time(__subquery_spinoff__{__query__="(foo * bar)",__range__="72h0m0s",__step__="1m0s"}[3d]) * 2`,
			expectedSubqueries:        1,
			expectedDownstreamQueries: 0,
		},
		{
			name:                      "offsets aren't supported",
			in:                        `avg_over_time((foo * bar)[3d:1m] offset 3d) * 2`,
			out:                       `__downstream_query__{__query__="avg_over_time((foo * bar)[3d:1m] offset 3d)"} * 2`,
			expectedSubqueries:        0,
			expectedDownstreamQueries: 1,
		},
		{
			name:                      "aggregated query",
			in:                        `sum(avg_over_time((foo * bar)[3d:1m]) * avg_over_time(foo[3d]))`,
			out:                       `sum(avg_over_time(__subquery_spinoff__{__query__="(foo * bar)",__range__="72h0m0s",__step__="1m0s"}[3d]) * __downstream_query__{__query__="avg_over_time(foo[3d])"})`,
			expectedSubqueries:        1,
			expectedDownstreamQueries: 1,
		},
		{
			name:                      "ignore single selector subquery",
			in:                        `sum(avg_over_time((foo > 1)[3d:1m]) * avg_over_time(foo[3d]))`,
			out:                       `sum(__downstream_query__{__query__="avg_over_time((foo > 1)[3d:1m])"} * __downstream_query__{__query__="avg_over_time(foo[3d])"})`,
			expectedSubqueries:        0,
			expectedDownstreamQueries: 2,
		},
		{
			name: "complex query 1",
			in: `
				  (
				    (
				        (
				          sum(
				            count_over_time(
				              (
				                  (
				                      (
				                          (
				                            sum(
				                              increase(
				                                kafka_event_processed_failure{aws_region="eu-central-1",pods=~".*prd.*",service="my-service"}[1m:]
				                              )
				                            )
				                          )
				                        or
				                          vector(0)
				                      )
				                    /
				                      (
				                          (
				                            sum(
				                              increase(
				kafka_event_handled{aws_region="eu-central-1",pods=~".*prd.*",service="my-service"}[1m:]
				                              )
				                            )
				                          )
				                        >
				                          0
				                      )
				                  )
				                >
				                  0.01
				              )[3d:]
				            )
				          )
				        )
				      or
				        vector(0)
				    )
				  )
				/
				  (count_over_time(vector(1)[3d:]))`,
			out: `  (
				    (
				        (
				          sum(
				            count_over_time(
				              __subquery_spinoff__{__query__="((((sum(increase(kafka_event_processed_failure{aws_region=\"eu-central-1\",pods=~\".*prd.*\",service=\"my-service\"}[1m:]))) or vector(0)) / ((sum(increase(kafka_event_handled{aws_region=\"eu-central-1\",pods=~\".*prd.*\",service=\"my-service\"}[1m:]))) > 0)) > 0.01)",__range__="72h0m0s",__step__="1m0s"}[3d]
				            )
				          )
				        )
				      or
				        vector(0)
				    )
				  )
				/
				  (count_over_time(vector(1)[3d:]))`,
			expectedSubqueries:        1,
			expectedDownstreamQueries: 0,
		},
		{
			name: "complex query 2",
			in: `
		    1 - grafana_slo_sli_6h{grafana_slo_uuid="ktr6jo1nptzickyko7k98"} > 1 * 0.0050000000000000044
		  and
		    1 - grafana_slo_sli_3d{grafana_slo_uuid="ktr6jo1nptzickyko7k98"} > 1 * 0.0050000000000000044
		and
		      300
		    *
		      (
		          sum_over_time((increase(grafana_slo_total_rate_5m{grafana_slo_uuid="ktr6jo1nptzickyko7k98"}[5m]) < 1e+308)[3d:5m])
		        -
		          sum_over_time(
		(increase(grafana_slo_success_rate_5m{grafana_slo_uuid="ktr6jo1nptzickyko7k98"}[5m]) < 1e+308)[3d:5m]
		          )
		      )
		  >
		    25`,
			out: `
		  __downstream_query__{__query__="1 - grafana_slo_sli_6h{grafana_slo_uuid=\"ktr6jo1nptzickyko7k98\"} > 1 * 0.0050000000000000044 and 1 - grafana_slo_sli_3d{grafana_slo_uuid=\"ktr6jo1nptzickyko7k98\"} > 1 * 0.0050000000000000044"}
		and
		      300
		    *
		      (
		          sum_over_time(
		            __subquery_spinoff__{__query__="(increase(grafana_slo_total_rate_5m{grafana_slo_uuid=\"ktr6jo1nptzickyko7k98\"}[5m]) < 1e+308)",__range__="72h0m0s",__step__="5m0s"}[3d]
		          )
		        -
		          sum_over_time(
		            __subquery_spinoff__{__query__="(increase(grafana_slo_success_rate_5m{grafana_slo_uuid=\"ktr6jo1nptzickyko7k98\"}[5m]) < 1e+308)",__range__="72h0m0s",__step__="5m0s"}[3d]
		          )
		      )
		  >
		    25`,
			expectedSubqueries:        2,
			expectedDownstreamQueries: 1,
		},
		{
			name:                      "complex query 3",
			in:                        `max_over_time( deriv( rate(metric_counter[10m])[5m:1m] )[3d:] )`,
			out:                       `max_over_time(__subquery_spinoff__{__query__="deriv(rate(metric_counter[10m])[5m:1m])",__range__="72h0m0s",__step__="1m0s"}[3d])`,
			expectedSubqueries:        1,
			expectedDownstreamQueries: 0,
		},
	} {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			stats := NewSubquerySpinOffMapperStats()
			mapper := NewSubquerySpinOffMapper(context.Background(), defaultStepFunc, log.NewNopLogger(), stats)
			expr, err := parser.ParseExpr(tt.in)
			require.NoError(t, err)
			out, err := parser.ParseExpr(tt.out)
			require.NoError(t, err)

			mapped, err := mapper.Map(expr)
			require.NoError(t, err)
			require.Equal(t, out.String(), mapped.String())
			assert.Equal(t, tt.expectedSubqueries, stats.SpunOffSubqueries())
			assert.Equal(t, tt.expectedDownstreamQueries, stats.DownstreamQueries())
		})
	}
}

var defaultStepFunc = func(int64) int64 {
	return (1 * time.Minute).Milliseconds()
}
