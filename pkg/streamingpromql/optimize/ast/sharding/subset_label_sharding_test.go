// SPDX-License-Identifier: AGPL-3.0-only

package sharding

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

func TestSubsetLabelSharding_CanceledRewrite(t *testing.T) {
	expr, err := promqlext.NewPromQLParser().ParseExpr(`histogram_quantile(0.9, sum by (le, span_name) (rate(metric[5m])))`)
	require.NoError(t, err)
	plan, ok := planSubsetLabelSharding(expr, ConcatSquasher)
	require.True(t, ok)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	rewritten, _, err := plan.Rewrite(ctx, 4)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, rewritten)
}

func TestOptimizationPass_SubsetLabelSharding(t *testing.T) {
	const histogram = `histogram_quantile(0.9, sum by (le, span_name) (rate(metric[5m])))`

	testCases := map[string]struct {
		input           string
		header          string
		totalShards     int32
		totalQueries    int32
		maxQueries      int
		maxRegexpBytes  int
		seriesCount     *querymiddleware.EstimatedSeriesCount
		expectedMatcher string
		expectedQueries uint32
	}{
		"enabled": {
			input: histogram, header: "true", expectedMatcher: "1_of_4_by_span_name", expectedQueries: 4,
		},
		"header is case insensitive": {
			input: histogram, header: "TRUE", expectedMatcher: "1_of_4_by_span_name", expectedQueries: 4,
		},
		"grouping labels are sorted and deduplicated": {
			input:  `histogram_quantile(0.9, sum by (z, le, a, z) (rate(metric[5m])))`,
			header: "true", expectedMatcher: "1_of_4_by_a,z", expectedQueries: 4,
		},
		"requested shard count": {
			input: histogram, header: "true", totalShards: 2, expectedMatcher: "1_of_2_by_span_name", expectedQueries: 2,
		},
		"query budget includes time splits": {
			input: histogram, header: "true", totalQueries: 2, maxQueries: 4, expectedMatcher: "1_of_2_by_span_name", expectedQueries: 2,
		},
		"cardinality estimate": {
			input: histogram, header: "true", seriesCount: &querymiddleware.EstimatedSeriesCount{EstimatedSeriesCount: 1500},
			expectedMatcher: "1_of_2_by_span_name", expectedQueries: 2,
		},
		"single shard does not rewrite": {
			input: histogram, header: "true", totalShards: 1,
		},
		"regexp limit prevents rewriting": {
			input:  `histogram_quantile(0.9, sum by (le, span_name) (rate(metric{job=~"long.*"}[5m])))`,
			header: "true", maxRegexpBytes: 2,
		},
		"header absent": {input: histogram},
		"header false":  {input: histogram, header: "false"},
		"unsupported shape uses ordinary sharding": {input: `sum(foo)`, header: "true"},
		"no non-bucket grouping labels": {
			input: `histogram_quantile(0.9, sum by (le) (rate(metric[5m])))`, header: "true",
		},
		"missing bucket grouping label": {
			input: `histogram_quantile(0.9, sum by (span_name) (rate(metric[5m])))`, header: "true",
		},
		"without aggregation": {
			input: `histogram_quantile(0.9, sum without (job) (rate(metric[5m])))`, header: "true",
		},
		"nonliteral quantile": {
			input: `histogram_quantile(scalar(foo), sum by (le, span_name) (rate(metric[5m])))`, header: "true",
		},
		"unsupported rate function": {
			input: `histogram_quantile(0.9, sum by (le, span_name) (irate(metric[5m])))`, header: "true",
		},
		"already sharded selector": {
			input: `histogram_quantile(0.9, sum by (le, span_name) (rate(metric{__query_shard__="1_of_2"}[5m])))`, header: "true",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			limits := &mockLimits{totalShards: 4, maxShardedQueries: tc.maxQueries, regexpSizeBytes: tc.maxRegexpBytes}
			apply := func(header string) (string, uint32) {
				pass := NewOptimizationPass(limits, 1000, NewRequestHintsCardinalityEstimator(), prometheus.NewPedanticRegistry(), log.NewNopLogger())
				expr, err := promqlext.NewPromQLParser().ParseExpr(tc.input)
				require.NoError(t, err)
				queryStats, ctx := stats.ContextWithEmptyStats(user.InjectOrgID(t.Context(), "tenant-1"))
				ctx = requestoptions.ContextWithOptions(ctx, requestoptions.Options{
					TotalShards:       tc.totalShards,
					PropagatedHeaders: map[string][]string{experimentalSubsetShardingHeader: {header}},
				})
				ctx = querymiddleware.ContextWithRequestHints(ctx, &querymiddleware.Hints{TotalQueries: tc.totalQueries, CardinalityEstimate: tc.seriesCount})
				output, err := pass.Apply(ctx, expr, &planning.QueryParameters{})
				require.NoError(t, err)
				if tc.expectedMatcher != "" && header != "" {
					unchanged, err := promqlext.NewPromQLParser().ParseExpr(tc.input)
					require.NoError(t, err)
					require.Equal(t, unchanged.String(), expr.String(), "subset rewriting must not mutate the input")
				}
				return output.String(), queryStats.LoadShardedQueries()
			}

			output, queries := apply(tc.header)
			if tc.expectedMatcher != "" {
				require.Contains(t, output, `__query_shard__="`+tc.expectedMatcher+`"`)
				require.Equal(t, tc.expectedQueries, queries)
			} else {
				ordinaryOutput, ordinaryQueries := apply("")
				require.Equal(t, ordinaryOutput, output)
				require.Equal(t, ordinaryQueries, queries)
			}
		})
	}
}
