// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/step_align_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

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
