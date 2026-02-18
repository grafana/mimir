// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/promqlext"
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
			query:  `metric smoothed`,
			expect: []string{"smoothed"},
			err:    `extended range selector modifier "smoothed" is not enabled for tenant`,
		},
		"metric[1m] smoothed": {
			query:  `metric[1m] smoothed`,
			expect: []string{"smoothed"},
			err:    `extended range selector modifier "smoothed" is not enabled for tenant`,
		},
		"metric[1m] anchored": {
			query:  `metric[1m] anchored`,
			expect: []string{"anchored"},
			err:    `extended range selector modifier "anchored" is not enabled for tenant`,
		},
		"rate(metric[1m] smoothed)": {
			query:  `rate(metric[1m] smoothed)`,
			expect: []string{"smoothed"},
			err:    `extended range selector modifier "smoothed" is not enabled for tenant`,
		},
		"increase(metric[1m] anchored)": {
			query:  `increase(metric[1m] anchored)`,
			expect: []string{"anchored"},
			err:    `extended range selector modifier "anchored" is not enabled for tenant`,
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

func TestExperimentalFunctionsMiddleware_ShouldNotPanicOnNilQueryExpression(t *testing.T) {
	inner := mockHandlerWith(nil, nil)
	middleware := newExperimentalFeaturesMiddleware(mockLimits{}, log.NewNopLogger())
	handler := middleware.Wrap(inner)

	// Create a request with a nil queryExpr to simulate a failed parse.
	req := NewPrometheusInstantQueryRequest("/", nil, timestamp.FromTime(time.Now()), 5*time.Minute, nil, Options{}, nil, "")

	ctx := user.InjectOrgID(context.Background(), "test")

	require.NotPanics(t, func() {
		resp, err := handler.Do(ctx, req)
		require.ErrorContains(t, err, errRequestNoQuery.Error())
		require.Nil(t, resp)
	})
}
