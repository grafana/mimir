// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/compat_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteRequest_MinTimestamp(t *testing.T) {
	req := &WriteRequest{
		Timeseries: []PreallocTimeseries{{TimeSeries: &TimeSeries{
			Samples:    []Sample{{TimestampMs: 10}},
			Exemplars:  []Exemplar{{TimestampMs: 20}},
			Histograms: []Histogram{{Timestamp: 30}},
		}}},
	}
	assert.Equal(t, int64(10), req.MinTimestamp())

	req = &WriteRequest{
		Timeseries: []PreallocTimeseries{{TimeSeries: &TimeSeries{
			Samples:    []Sample{{TimestampMs: 20}},
			Exemplars:  []Exemplar{{TimestampMs: 10}},
			Histograms: []Histogram{{Timestamp: 30}},
		}}},
	}
	assert.Equal(t, int64(10), req.MinTimestamp())

	req = &WriteRequest{
		Timeseries: []PreallocTimeseries{{TimeSeries: &TimeSeries{
			Samples:    []Sample{{TimestampMs: 20}},
			Exemplars:  []Exemplar{{TimestampMs: 30}},
			Histograms: []Histogram{{Timestamp: 10}},
		}}},
	}
	assert.Equal(t, int64(10), req.MinTimestamp())
}

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
