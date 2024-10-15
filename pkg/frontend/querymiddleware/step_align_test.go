// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/step_align_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/test"
)

func TestStepAlignMiddleware_SingleUser(t *testing.T) {
	for _, tc := range []struct {
		name            string
		input, expected *PrometheusRangeQueryRequest
	}{
		{
			name: "no adjustment needed",
			input: &PrometheusRangeQueryRequest{
				start: 0,
				end:   100,
				step:  10,
			},
			expected: &PrometheusRangeQueryRequest{
				start: 0,
				end:   100,
				step:  10,
				minT:  0,
				maxT:  0,
			},
		},

		{
			name: "adjust start and end",
			input: &PrometheusRangeQueryRequest{
				start: 2,
				end:   102,
				step:  10,
			},
			expected: &PrometheusRangeQueryRequest{
				start: 0,
				end:   100,
				step:  10,
				minT:  0,
				maxT:  100,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var result *PrometheusRangeQueryRequest

			next := HandlerFunc(func(_ context.Context, req MetricsQueryRequest) (Response, error) {
				result = req.(*PrometheusRangeQueryRequest)
				return nil, nil
			})

			limits := mockLimits{alignQueriesWithStep: true}
			log := test.NewTestingLogger(t)
			ctx := user.InjectOrgID(context.Background(), "123")

			s := newStepAlignMiddleware(limits, log, prometheus.NewPedanticRegistry()).Wrap(next)
			_, err := s.Do(ctx, tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestStepAlignMiddleware_MultipleUsers(t *testing.T) {
	for _, tc := range []struct {
		name            string
		limits          *multiTenantMockLimits
		input, expected *PrometheusRangeQueryRequest
	}{
		{
			name: "no adjustment needed",
			limits: &multiTenantMockLimits{
				byTenant: map[string]mockLimits{
					"123": {alignQueriesWithStep: true},
					"456": {alignQueriesWithStep: true},
				},
			},
			input: &PrometheusRangeQueryRequest{
				start: 0,
				end:   100,
				step:  10,
			},
			expected: &PrometheusRangeQueryRequest{
				start: 0,
				end:   100,
				step:  10,
				minT:  0,
				maxT:  0,
			},
		},
		{
			name: "adjust start and end",
			limits: &multiTenantMockLimits{
				byTenant: map[string]mockLimits{
					"123": {alignQueriesWithStep: true},
					"456": {alignQueriesWithStep: true},
				},
			},
			input: &PrometheusRangeQueryRequest{
				start: 2,
				end:   102,
				step:  10,
			},
			expected: &PrometheusRangeQueryRequest{
				start: 0,
				end:   100,
				step:  10,
				minT:  0,
				maxT:  100,
			},
		},
		{
			name: "not enabled for all users",
			limits: &multiTenantMockLimits{
				byTenant: map[string]mockLimits{
					"123": {alignQueriesWithStep: false},
					"456": {alignQueriesWithStep: true},
				},
			},
			input: &PrometheusRangeQueryRequest{
				start: 2,
				end:   102,
				step:  10,
			},
			expected: &PrometheusRangeQueryRequest{
				start: 2,
				end:   102,
				step:  10,
				minT:  0,
				maxT:  0,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var result *PrometheusRangeQueryRequest

			next := HandlerFunc(func(_ context.Context, req MetricsQueryRequest) (Response, error) {
				result = req.(*PrometheusRangeQueryRequest)
				return nil, nil
			})

			log := test.NewTestingLogger(t)
			ctx := user.InjectOrgID(context.Background(), "123|456")

			s := newStepAlignMiddleware(tc.limits, log, prometheus.NewPedanticRegistry()).Wrap(next)
			_, err := s.Do(ctx, tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestIsRequestStepAligned(t *testing.T) {
	tests := map[string]struct {
		req      MetricsQueryRequest
		expected bool
	}{
		"should return true if start and end are aligned to step": {
			req:      &PrometheusRangeQueryRequest{start: 10, end: 20, step: 10},
			expected: true,
		},
		"should return false if start is not aligned to step": {
			req:      &PrometheusRangeQueryRequest{start: 11, end: 20, step: 10},
			expected: false,
		},
		"should return false if end is not aligned to step": {
			req:      &PrometheusRangeQueryRequest{start: 10, end: 19, step: 10},
			expected: false,
		},
		"should return true if step is 0": {
			req:      &PrometheusRangeQueryRequest{start: 10, end: 11, step: 0},
			expected: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			require.Equal(t, testData.expected, isRequestStepAligned(testData.req))
		})
	}
}
