// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/step_align_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStepAlignMiddleware(t *testing.T) {
	for i, tc := range []struct {
		input, expected *PrometheusRangeQueryRequest
	}{
		{
			input: &PrometheusRangeQueryRequest{
				Start: 0,
				End:   100,
				Step:  10,
			},
			expected: &PrometheusRangeQueryRequest{
				Start: 0,
				End:   100,
				Step:  10,
			},
		},

		{
			input: &PrometheusRangeQueryRequest{
				Start: 2,
				End:   102,
				Step:  10,
			},
			expected: &PrometheusRangeQueryRequest{
				Start: 0,
				End:   100,
				Step:  10,
			},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var result *PrometheusRangeQueryRequest

			next := HandlerFunc(func(_ context.Context, req Request) (Response, error) {
				result = req.(*PrometheusRangeQueryRequest)
				return nil, nil
			})
			s := newStepAlignMiddleware().Wrap(next)
			_, err := s.Do(context.Background(), tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestIsRequestStepAligned(t *testing.T) {
	tests := map[string]struct {
		req      Request
		expected bool
	}{
		"should return true if start and end are aligned to step": {
			req:      &PrometheusRangeQueryRequest{Start: 10, End: 20, Step: 10},
			expected: true,
		},
		"should return false if start is not aligned to step": {
			req:      &PrometheusRangeQueryRequest{Start: 11, End: 20, Step: 10},
			expected: false,
		},
		"should return false if end is not aligned to step": {
			req:      &PrometheusRangeQueryRequest{Start: 10, End: 19, Step: 10},
			expected: false,
		},
		"should return true if step is 0": {
			req:      &PrometheusRangeQueryRequest{Start: 10, End: 11, Step: 0},
			expected: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			require.Equal(t, testData.expected, isRequestStepAligned(testData.req))
		})
	}
}
