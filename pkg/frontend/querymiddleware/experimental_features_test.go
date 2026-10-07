// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
	"github.com/grafana/mimir/pkg/util/promqlext"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestContainedExperimentalFunctions(t *testing.T) {
	testCases := map[string]struct {
		query  string
		expect []string
		err    string
	}{
		"sum by": {
			query: `sum(up) by (namespace)`,
			err:   `function "sum by" is not enabled for tenant`,
		},
		"mad_over_time": {
			query:  `mad_over_time(up[5m])`,
			expect: []string{"mad_over_time"},
			err:    `function "mad_over_time" is not enabled for tenant`,
		},
		"mad_over_time with sum and by": {
			query:  `sum(mad_over_time(up[5m])) by (namespace)`,
			expect: []string{"mad_over_time"},
			err:    `function "mad_over_time" is not enabled for tenant`,
		},
		"sort_by_label": {
			query:  `sort_by_label({__name__=~".+"}, "__name__")`,
			expect: []string{"sort_by_label"},
			err:    `function "sort_by_label" is not enabled for tenant`,
		},
		"sort_by_label_desc": {
			query:  `sort_by_label_desc({__name__=~".+"}, "__name__")`,
			expect: []string{"sort_by_label_desc"},
			err:    `function "sort_by_label_desc" is not enabled for tenant`,
		},
		"limitk": {
			query:  `limitk by (group) (0, up)`,
			expect: []string{"limitk"},
			err:    `aggregation "limitk" is not enabled for tenant`,
		},
		"limit_ratio": {
			query:  `limit_ratio(0.5, up)`,
			expect: []string{"limit_ratio"},
			err:    `aggregation "limit_ratio" is not enabled for tenant`,
		},
		"limit_ratio with mad_over_time": {
			query:  `limit_ratio(0.5, mad_over_time(up[5m]))`,
			expect: []string{"limit_ratio", "mad_over_time"},
		},
		"metric smoothed": {
			query: `metric smoothed`,
		},
		"metric[1m] smoothed": {
			query: `metric[1m] smoothed`,
		},
		"metric[1m] anchored": {
			query: `metric[1m] anchored`,
		},
		"rate(metric[1m] smoothed)": {
			query: `rate(metric[1m] smoothed)`,
		},
		"increase(metric[1m] anchored)": {
			query: `increase(metric[1m] anchored)`,
		},
		"experimental function with anchored selector": {
			query:  `mad_over_time(metric[1m] anchored)`,
			expect: []string{"mad_over_time"},
			err:    `function "mad_over_time" is not enabled for tenant`,
		},
		"experimental function with smoothed selector": {
			query:  `sort_by_label(metric smoothed, "job")`,
			expect: []string{"sort_by_label"},
			err:    `function "sort_by_label" is not enabled for tenant`,
		},
		"fill both sides": {
			query:  `a + fill(1) b`,
			expect: []string{"fill"},
			err:    `binary operation fill modifier "fill" is not enabled for tenant`,
		},
		"fill_left only": {
			query:  `a + fill_left(1) b`,
			expect: []string{"fill_left"},
			err:    `binary operation fill modifier "fill_left" is not enabled for tenant`,
		},
		"fill_right only": {
			query:  `a + fill_right(1) b`,
			expect: []string{"fill_right"},
			err:    `binary operation fill modifier "fill_right" is not enabled for tenant`,
		},
		"fill_left and fill_right different values": {
			query:  `a + fill_left(1) fill_right(2) b`,
			expect: []string{"fill_left", "fill_right"},
		},
		"fill_left and fill_right same values": {
			// fill_left(v) fill_right(v) with equal values is canonically equivalent to fill(v),
			// so it is gated on the "fill" feature, consistent with the PromQL printer.
			query:  `a + fill_left(1) fill_right(1) b`,
			expect: []string{"fill"},
			err:    `binary operation fill modifier "fill" is not enabled for tenant`,
		},
		"fill NaN": {
			query:  `a + fill(NaN) b`,
			expect: []string{"fill"},
			err:    `binary operation fill modifier "fill" is not enabled for tenant`,
		},
		"fill_left and fill_right NaN same value": {
			// fill_left(NaN) fill_right(NaN) with equal NaN values is canonically equivalent to fill(NaN),
			// so it is gated on the "fill" feature. NaN != NaN by IEEE 754, so equality must use math.IsNaN.
			query:  `a + fill_left(NaN) fill_right(NaN) b`,
			expect: []string{"fill"},
			err:    `binary operation fill modifier "fill" is not enabled for tenant`,
		},
		"fill_left NaN and fill_right finite": {
			// fill_left(NaN) fill_right(1) must NOT be classified as fill(NaN).
			// The two sides are different, so this is fill_left + fill_right.
			query:  `a + fill_left(NaN) fill_right(1) b`,
			expect: []string{"fill_left", "fill_right"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			expr, err := promqlext.NewPromQLParser().ParseExpr(tc.query)
			require.NoError(t, err)
			var enabled []string
			for op, opType := range containedExperimentalFeatures(expr) {
				enabled = append(enabled, op)
				if len(tc.err) > 0 {
					// test that if an error was raised for this function/aggregate/modifier that the expected error string is formed
					// an empty tc.err allows for the case to be skipped - such as where we have multiple errors which are validated elsewhere
					err := createExperimentalFeatureError(opType, op)
					require.ErrorContains(t, err, tc.err)
				}
			}
			require.ElementsMatch(t, tc.expect, enabled)
		})
	}
}

func TestExperimentalFeaturesMiddleware_ExtendedRangeSelectorSettingIsNoOp(t *testing.T) {
	for _, enabled := range []string{"", "anchored", "smoothed", "anchored,smoothed", "all"} {
		t.Run(fmt.Sprintf("enabled=%q", enabled), func(t *testing.T) {
			tenantLimits := validation.Limits{IngestStorageReadConsistency: "eventual"}
			err := yaml.Unmarshal([]byte(fmt.Sprintf("enabled_promql_extended_range_selectors: %q\n", enabled)), &tenantLimits)
			require.NoError(t, err)
			limits := validation.NewOverrides(validation.Limits{}, validation.NewMockTenantLimits(map[string]*validation.Limits{
				"test": &tenantLimits,
			}))

			for _, query := range []string{
				"rate(metric[2m + 3m] anchored)",
				"rate(metric[2m + 3m] smoothed)",
				"metric smoothed",
				"rate(metric[2m + 3m] offset (2 * 1m))",
			} {
				for _, instant := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/instant=%v", query, instant), func(t *testing.T) {
						expr, err := promqlext.NewPromQLParser().ParseExpr(query)
						require.NoError(t, err)
						var req MetricsQueryRequest
						if instant {
							req = NewPrometheusInstantQueryRequest("/", nil, 1000, 5*time.Minute, expr, requestoptions.Options{}, nil, "")
						} else {
							req = NewPrometheusRangeQueryRequest("/", nil, 1000, 2000, 1000, 5*time.Minute, expr, requestoptions.Options{}, nil, "")
						}

						called := false
						innerResponse := NewEmptyPrometheusResponse()
						inner := HandlerFunc(func(_ context.Context, _ MetricsQueryRequest) (Response, error) {
							called = true
							return innerResponse, nil
						})
						handler := newExperimentalFeaturesMiddleware(limits, log.NewNopLogger()).Wrap(inner)
						resp, err := handler.Do(user.InjectOrgID(t.Context(), "test"), req)
						require.NoError(t, err)
						require.Same(t, innerResponse, resp)
						require.True(t, called)
					})
				}
			}
		})
	}
}

func TestExperimentalFunctionsMiddleware_ShouldNotPanicOnNilQueryExpression(t *testing.T) {
	inner := mockHandlerWith(nil, nil)
	middleware := newExperimentalFeaturesMiddleware(mockLimits{}, log.NewNopLogger())
	handler := middleware.Wrap(inner)

	// Create a request with a nil queryExpr to simulate a failed parse.
	req := NewPrometheusInstantQueryRequest("/", nil, timestamp.FromTime(time.Now()), 5*time.Minute, nil, requestoptions.Options{}, nil, "")

	ctx := user.InjectOrgID(context.Background(), "test")

	require.NotPanics(t, func() {
		resp, err := handler.Do(ctx, req)
		require.ErrorContains(t, err, errRequestNoQuery.Error())
		require.Nil(t, resp)
	})
}
