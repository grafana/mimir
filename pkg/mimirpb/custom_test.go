// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/compat_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/histogram"
)

func TestIsFloatHistogram(t *testing.T) {
	tests := map[string]struct {
		histogram Histogram
		expected  bool
	}{
		"empty int histogram": {
			histogram: FromHistogramToHistogramProto(0, &histogram.Histogram{
				Sum:           0,
				Count:         0,
				ZeroThreshold: 0.001,
				Schema:        2,
				PositiveSpans: []histogram.Span{
					{Offset: 0, Length: 1},
					{Offset: 3, Length: 1},
					{Offset: 2, Length: 2},
				},
				PositiveBuckets: []int64{0, 0, 0, 0},
			}),
			expected: false,
		},
		"empty float histogram": {
			histogram: FromFloatHistogramToHistogramProto(0, &histogram.FloatHistogram{
				Sum:           0,
				Count:         0,
				ZeroThreshold: 0.001,
				Schema:        2,
				PositiveSpans: []histogram.Span{
					{Offset: 0, Length: 1},
					{Offset: 3, Length: 1},
					{Offset: 2, Length: 2},
				},
				PositiveBuckets: []float64{0, 0, 0, 0},
			}),
			expected: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			res := testData.histogram.IsFloatHistogram()
			require.Equal(t, testData.expected, res)
		})
	}
}
