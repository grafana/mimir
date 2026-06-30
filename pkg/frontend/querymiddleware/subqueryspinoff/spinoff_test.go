// SPDX-License-Identifier: AGPL-3.0-only

package subqueryspinoff

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/promqlext"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var testDefaultStepFunc = func(int64) int64 { return (1 * time.Minute).Milliseconds() }

func TestMap(t *testing.T) {
	const mapperTimeout = 10 * time.Second

	for _, tt := range []struct {
		name string
		in   string
		// cancelContext cancels the context before Map runs, to exercise the mapping-failure path.
		cancelContext bool

		expectedOK         bool
		expectedOut        string // only checked when expectedOK is true
		expectedSpunOff    int    // number of subqueries spun off, recorded on success
		expectedSkipReason string // skip reason recorded when expectedOK is false
	}{
		{
			name:            "spin-off worthwhile",
			in:              `avg_over_time((foo * bar)[3d:1m])`,
			expectedOK:      true,
			expectedOut:     `avg_over_time(__subquery_spinoff__{__query__="(foo * bar)",__range__="72h0m0s",__step__="1m0s"}[3d])`,
			expectedSpunOff: 1,
		},
		{
			name:            "both downstream queries and spun-off subqueries",
			in:              `avg_over_time((foo * bar)[3d:1m]) * avg_over_time(foo[3d])`,
			expectedOK:      true,
			expectedOut:     `avg_over_time(__subquery_spinoff__{__query__="(foo * bar)",__range__="72h0m0s",__step__="1m0s"}[3d]) * __downstream_query__{__query__="avg_over_time(foo[3d])"}`,
			expectedSpunOff: 1,
		},
		{
			name:            "many spun-off subqueries",
			in:              `avg_over_time((foo * bar)[3d:1m]) * max_over_time((foo * bar)[2d:5m])`,
			expectedOK:      true,
			expectedOut:     `avg_over_time(__subquery_spinoff__{__query__="(foo * bar)",__range__="72h0m0s",__step__="1m0s"}[3d]) * max_over_time(__subquery_spinoff__{__query__="(foo * bar)",__range__="48h0m0s",__step__="5m0s"}[2d])`,
			expectedSpunOff: 2,
		},
		// The following cases mirror the queries exercised by TestSubquerySpinOff_Correctness in the
		// querymiddleware package, to make sure they are mapped as expected.
		{
			name:            "spin-off subquery with offset",
			in:              `max_over_time(rate(metric_counter[1m])[2h:1m] offset 1h)`,
			expectedOK:      true,
			expectedOut:     `max_over_time(__subquery_spinoff__{__offset__="1h0m0s",__query__="rate(metric_counter[1m])",__range__="2h0m0s",__step__="1m0s"}[2h] offset 1h)`,
			expectedSpunOff: 1,
		},
		{
			name:            "spin-off subquery with inner aggregation",
			in:              `min_over_time(sum by(group_1) (rate(metric_counter[5m]))[2h:])`,
			expectedOK:      true,
			expectedOut:     `min_over_time(__subquery_spinoff__{__query__="sum by (group_1) (rate(metric_counter[5m]))",__range__="2h0m0s",__step__="1m0s"}[2h])`,
			expectedSpunOff: 1,
		},
		{
			name:            "spin-off nested subqueries (double)",
			in:              `max_over_time(deriv(rate(metric_counter[10m])[5m:1m])[2h:])`,
			expectedOK:      true,
			expectedOut:     `max_over_time(__subquery_spinoff__{__query__="deriv(rate(metric_counter[10m])[5m:1m])",__range__="2h0m0s",__step__="1m0s"}[2h])`,
			expectedSpunOff: 1,
		},
		{
			name:            "spin-off nested subqueries (triple)",
			in:              `max_over_time(stddev_over_time(deriv(rate(metric_counter[10m])[5m:1m])[2m:])[2h:])`,
			expectedOK:      true,
			expectedOut:     `max_over_time(__subquery_spinoff__{__query__="stddev_over_time(deriv(rate(metric_counter[10m])[5m:1m])[2m:])",__range__="2h0m0s",__step__="1m0s"}[2h])`,
			expectedSpunOff: 1,
		},
		{
			name:            "spin-off subquery alongside a downstream join",
			in:              `max_over_time(rate(metric_counter[1m])[2h:1m]) * on (group_1) group_left() max by (group_1)(rate(metric_counter[1m]))`,
			expectedOK:      true,
			expectedOut:     `max_over_time(__subquery_spinoff__{__query__="rate(metric_counter[1m])",__range__="2h0m0s",__step__="1m0s"}[2h]) * on (group_1) group_left () __downstream_query__{__query__="max by (group_1) (rate(metric_counter[1m]))"}`,
			expectedSpunOff: 1,
		},
		{
			name:            "spin-off subquery with offset shorter than step alongside a downstream query",
			in:              `sum by (group_1) (sum_over_time(avg by (group_1) (metric_counter{group_2="1"})[1h:5m] offset 1m) * avg by (group_1) (avg_over_time(metric_counter{group_2="2"}[1h:5m] offset 1m)) * 0.083333)`,
			expectedOK:      true,
			expectedOut:     `sum by (group_1) (sum_over_time(__subquery_spinoff__{__offset__="1m0s",__query__="avg by (group_1) (metric_counter{group_2=\"1\"})",__range__="1h0m0s",__step__="5m0s"}[1h] offset 1m) * __downstream_query__{__query__="avg by (group_1) (avg_over_time(metric_counter{group_2=\"2\"}[1h:5m] offset 1m))"} * 0.083333)`,
			expectedSpunOff: 1,
		},
		{
			name:               "no subqueries to spin off",
			in:                 `avg_over_time(foo[3d:1m])`,
			expectedOK:         false,
			expectedSkipReason: SkippedReasonNoSubqueries,
		},
		{
			name:               "no subquery present at all",
			in:                 `sum(count(count(metric_counter) by (group_1, group_2)) by (group_1))`,
			expectedOK:         false,
			expectedSkipReason: SkippedReasonNoSubqueries,
		},
		{
			name:               "subquery range too short to spin off",
			in:                 `max_over_time(rate(metric_counter[1m])[30m:1m])`,
			expectedOK:         false,
			expectedSkipReason: SkippedReasonNoSubqueries,
		},
		{
			name:               "subquery has too few steps to spin off",
			in:                 `max_over_time(rate(metric_counter[1m])[2h:15m])`,
			expectedOK:         false,
			expectedSkipReason: SkippedReasonNoSubqueries,
		},
		{
			name:               "more downstream queries than spun-off subqueries",
			in:                 `avg_over_time((foo * bar)[3d:1m]) * avg_over_time(foo[3d]) * avg_over_time(baz[3d])`,
			expectedOK:         false,
			expectedSkipReason: SkippedReasonDownstreamQueries,
		},
		{
			name:               "mapping cancelled",
			in:                 `avg_over_time((foo * bar)[3d:1m])`,
			cancelContext:      true,
			expectedOK:         false,
			expectedSkipReason: SkippedReasonMappingFailed,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			metrics := NewMetrics(reg)
			wrapper := astmapper.NewSelectorSubquerySpinOffWrapper()

			queryStats, ctx := stats.ContextWithEmptyStats(context.Background())
			if tt.cancelContext {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			logger := spanlogger.FromContext(ctx, log.NewNopLogger())
			expr, err := promqlext.NewPromQLParser().ParseExpr(tt.in)
			require.NoError(t, err)

			// Capture the original expression so we can assert it is not modified when Map returns false.
			originalExpr := expr.String()

			out, ok := Map(ctx, expr, wrapper, metrics, testDefaultStepFunc, mapperTimeout, logger)

			require.Equal(t, tt.expectedOK, ok)

			if tt.expectedOK {
				require.NotNil(t, out)
				expectedOut, err := promqlext.NewPromQLParser().ParseExpr(tt.expectedOut)
				require.NoError(t, err)
				require.Equal(t, expectedOut.String(), out.String())

				require.Equal(t, uint32(tt.expectedSpunOff), queryStats.LoadSpunOffSubqueries())
				require.Equal(t, float64(1), testutil.ToFloat64(metrics.SpinOffSuccesses))
				require.Equal(t, float64(tt.expectedSpunOff), testutil.ToFloat64(metrics.SpunOffSubqueries))
				require.Equal(t, uint64(1), histogramSampleCount(t, metrics.SpunOffSubqueriesPerQuery))
				assertSkippedCounts(t, metrics, nil)
			} else {
				require.Nil(t, out)
				require.Equal(t, originalExpr, expr.String(), "the original expression must not be modified when Map returns false")
				require.Equal(t, uint32(0), queryStats.LoadSpunOffSubqueries())
				require.Equal(t, float64(0), testutil.ToFloat64(metrics.SpinOffSuccesses))
				require.Equal(t, float64(0), testutil.ToFloat64(metrics.SpunOffSubqueries))
				require.Equal(t, uint64(0), histogramSampleCount(t, metrics.SpunOffSubqueriesPerQuery))
				assertSkippedCounts(t, metrics, map[string]float64{tt.expectedSkipReason: 1})
			}
		})
	}
}

// assertSkippedCounts asserts the value of every reason of the spin-off-skipped metric, defaulting to 0 for
// any reason not present in expected.
func assertSkippedCounts(t *testing.T, metrics Metrics, expected map[string]float64) {
	t.Helper()
	for _, reason := range []string{
		SkippedReasonParsingFailed,
		SkippedReasonMappingFailed,
		SkippedReasonNoSubqueries,
		SkippedReasonDownstreamQueries,
	} {
		require.Equalf(t, expected[reason], testutil.ToFloat64(metrics.SpinOffSkipped.WithLabelValues(reason)), "skipped reason %q", reason)
	}
}

func histogramSampleCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, h.Write(m))
	return m.GetHistogram().GetSampleCount()
}
