package astmapper

import (
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpinoffSubqueries(t *testing.T) {
	for _, tt := range []struct {
		in                        string
		out                       string
		expectedSpunOffSubqueries int
	}{

		{
			in:                        `max_over_time((max(hello) > max(hello2))[5m:1m])`,
			out:                       `max_over_time(__aggregated_subquery__{__aggregated_subquery__="(max(hello) > max(hello2))[5m:1m]"}[5m])`,
			expectedSpunOffSubqueries: 1,
		},
		{
			in:                        `max_over_time((max by (job) (kube_state_metrics_total_shards) * max by (job) (kube_state_metrics_watch_total))[2w:1m])`,
			out:                       `max_over_time(__aggregated_subquery__{__aggregated_subquery__="(max by (job) (kube_state_metrics_total_shards) * max by (job) (kube_state_metrics_watch_total))[2w:1m]"}[2w])`,
			expectedSpunOffSubqueries: 1,
		},
		{
			in: "max_over_time(hello[5m:1m]) * " +
				"max_over_time(hello[5m:1m]) / " +
				"(1 - max_over_time(hello[5m:1m]))",
			out: `max_over_time(__aggregated_subquery__{__aggregated_subquery__="hello[5m:1m]"}[5m]) * ` +
				`max_over_time(__aggregated_subquery__{__aggregated_subquery__="hello[5m:1m]"}[5m]) / ` +
				`(1 - max_over_time(__aggregated_subquery__{__aggregated_subquery__="hello[5m:1m]"}[5m]))`,
			expectedSpunOffSubqueries: 3,
		},
		{
			in:                        `min by (asserts_env, asserts_site, namespace) (1 - avg_over_time(((asserts:latency:p99{asserts_request_context!~"/v1/stack/(sanity|vendor-integration)",asserts_request_type=~"inbound|outbound",job="api-server"}) > bool 12)[1d:]))`,
			out:                       `min by (asserts_env,asserts_site,namespace) (1 - avg_over_time(__aggregated_subquery__{__aggregated_subquery__="((asserts:latency:p99{asserts_request_context!~\"/v1/stack/(sanity|vendor-integration)\",asserts_request_type=~\"inbound|outbound\",job=\"api-server\"}) > bool 12)[1d:]"}[1d]))`,
			expectedSpunOffSubqueries: 1,
		},
	} {
		tt := tt

		t.Run(tt.in, func(t *testing.T) {
			stats := NewSubqueryMapperStats()
			mapper := NewSubqueryMapper(stats)

			expr, err := parser.ParseExpr(tt.in)
			require.NoError(t, err)
			out, err := parser.ParseExpr(tt.out)
			require.NoError(t, err, tt.out)

			mapped, err := mapper.Map(expr)
			require.NoError(t, err)
			require.Equal(t, out.String(), mapped.String())
			assert.Equal(t, tt.expectedSpunOffSubqueries, stats.GetSpunOffSubqueries())
		})
	}
}
