// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/step_align_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"strings"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/test"
)

func TestStepAlignMiddleware_SingleUser(t *testing.T) {
	for _, tc := range []struct {
		name            string
		input, expected *PrometheusRangeQueryRequest
		expectedMetrics *strings.Reader
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
			expectedMetrics: strings.NewReader(`
			# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
			# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
			cortex_query_frontend_non_step_aligned_queries_total 0
			`),
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
			expectedMetrics: strings.NewReader(`
			# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
			# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
			cortex_query_frontend_non_step_aligned_queries_total 1
			# HELP cortex_query_frontend_non_step_aligned_queries_adjusted_total Number of queries whose start or end times have been adjusted to be step-aligned.
			# TYPE cortex_query_frontend_non_step_aligned_queries_adjusted_total counter
			cortex_query_frontend_non_step_aligned_queries_adjusted_total{user="123"} 1
			`),
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
			reg := prometheus.NewPedanticRegistry()

			s := newStepAlignMiddleware(limits, log, reg).Wrap(next)
			_, err := s.Do(ctx, tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
			require.NoError(t, testutil.GatherAndCompare(reg, tc.expectedMetrics))
		})
	}
}

func TestStepAlignMiddleware_MultipleUsers(t *testing.T) {
	for _, tc := range []struct {
		name            string
		limits          *multiTenantMockLimits
		input, expected *PrometheusRangeQueryRequest
		expectedMetrics *strings.Reader
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
			expectedMetrics: strings.NewReader(`
			# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
			# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
			cortex_query_frontend_non_step_aligned_queries_total 0
			`),
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
			expectedMetrics: strings.NewReader(`
			# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
			# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
			cortex_query_frontend_non_step_aligned_queries_total 1
			# HELP cortex_query_frontend_non_step_aligned_queries_adjusted_total Number of queries whose start or end times have been adjusted to be step-aligned.
			# TYPE cortex_query_frontend_non_step_aligned_queries_adjusted_total counter
			cortex_query_frontend_non_step_aligned_queries_adjusted_total{user="123"} 1
			cortex_query_frontend_non_step_aligned_queries_adjusted_total{user="456"} 1
			`),
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
			expectedMetrics: strings.NewReader(`
			# HELP cortex_query_frontend_non_step_aligned_queries_total Total queries sent that are not step aligned.
			# TYPE cortex_query_frontend_non_step_aligned_queries_total counter
			cortex_query_frontend_non_step_aligned_queries_total 1
			`),
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
			reg := prometheus.NewPedanticRegistry()

			s := newStepAlignMiddleware(tc.limits, log, reg).Wrap(next)
			_, err := s.Do(ctx, tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
			require.NoError(t, testutil.GatherAndCompare(reg, tc.expectedMetrics))
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
