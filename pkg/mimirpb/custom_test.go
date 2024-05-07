// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/compat_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
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

func TestWriteRequest_IsEmpty(t *testing.T) {
	t.Run("should return false if a WriteRequest has both Timeseries and Metadata", func(t *testing.T) {
		req := &WriteRequest{
			Timeseries: []PreallocTimeseries{{TimeSeries: &TimeSeries{
				Samples: []Sample{{TimestampMs: 20}},
			}}},
			Metadata: []*MetricMetadata{
				{Type: COUNTER, MetricFamilyName: "test_metric", Help: "This is a test metric."},
			},
		}

		assert.False(t, req.IsEmpty())
	})

	t.Run("should return false if a WriteRequest has only Timeseries", func(t *testing.T) {
		req := &WriteRequest{
			Timeseries: []PreallocTimeseries{{TimeSeries: &TimeSeries{
				Samples: []Sample{{TimestampMs: 20}},
			}}},
		}

		assert.False(t, req.IsEmpty())
	})

	t.Run("should return false if a WriteRequest has only Metadata", func(t *testing.T) {
		req := &WriteRequest{
			Metadata: []*MetricMetadata{
				{Type: COUNTER, MetricFamilyName: "test_metric", Help: "This is a test metric."},
			},
		}

		assert.False(t, req.IsEmpty())
	})

	t.Run("should return true if a WriteRequest has no Timeseries and Metadata", func(t *testing.T) {
		req := &WriteRequest{
			Source:                  API,
			SkipLabelNameValidation: true,
		}

		assert.True(t, req.IsEmpty())
	})
}

func TestWriteRequest_SizeWithoutMetadata_SizeWithoutTimeseries(t *testing.T) {
	req := &WriteRequest{
		Timeseries: []PreallocTimeseries{{TimeSeries: &TimeSeries{
			Samples:    []Sample{{TimestampMs: 20}},
			Exemplars:  []Exemplar{{TimestampMs: 30}},
			Histograms: []Histogram{{Timestamp: 10}},
		}}},
		Metadata: []*MetricMetadata{
			{Type: COUNTER, MetricFamilyName: "test_metric", Help: "This is a test metric."},
		},
	}

	origSize := req.Size()
	assert.Less(t, req.SizeWithoutTimeseries(), origSize)
	assert.Less(t, req.SizeWithoutMetadata(), origSize)
	assert.Equal(t, origSize, req.Size())

	reqWithoutTimeseries := *req
	reqWithoutTimeseries.Timeseries = nil
	assert.Equal(t, reqWithoutTimeseries.Size(), reqWithoutTimeseries.SizeWithoutTimeseries())

	reqWithoutMetadata := *req
	reqWithoutMetadata.Metadata = nil
	assert.Equal(t, reqWithoutMetadata.Size(), reqWithoutMetadata.SizeWithoutMetadata())
}

func TestWriteRequest_SplitByMaxMarshalSize(t *testing.T) {
	req := &WriteRequest{
		Source:                  RULE,
		SkipLabelNameValidation: true,
		Timeseries: []PreallocTimeseries{
			{TimeSeries: &TimeSeries{
				Labels:     FromLabelsToLabelAdapters(labels.FromStrings(labels.MetricName, "series_1", "pod", "test-application-123456")),
				Samples:    []Sample{{TimestampMs: 20}},
				Exemplars:  []Exemplar{{TimestampMs: 30}},
				Histograms: []Histogram{{Timestamp: 10}},
			}},
			{TimeSeries: &TimeSeries{
				Labels:  FromLabelsToLabelAdapters(labels.FromStrings(labels.MetricName, "series_2", "pod", "test-application-123456")),
				Samples: []Sample{{TimestampMs: 30}},
			}},
		},
		Metadata: []*MetricMetadata{
			{Type: COUNTER, MetricFamilyName: "series_1", Help: "This is the first test metric."},
			{Type: COUNTER, MetricFamilyName: "series_2", Help: "This is the second test metric."},
			{Type: COUNTER, MetricFamilyName: "series_3", Help: "This is the third test metric."},
		},
	}

	// Pre-requisite check: WriteRequest fields are set to non-zero values.
	require.NotZero(t, req.Source)
	require.NotZero(t, req.SkipLabelNameValidation)
	require.NotZero(t, req.Timeseries)
	require.NotZero(t, req.Metadata)

	t.Run("should return the input WriteRequest is size is less than the size limit", func(t *testing.T) {
		partials := req.SplitByMaxMarshalSize(100000)
		require.Len(t, partials, 1)
		assert.Equal(t, req, partials[0])
	})

	t.Run("should split the input WriteRequest into multiple requests, honoring the size limit", func(t *testing.T) {
		const limit = 100

		partials := req.SplitByMaxMarshalSize(limit)
		assert.Equal(t, []*WriteRequest{
			{
				Source:                  RULE,
				SkipLabelNameValidation: true,
				Timeseries:              []PreallocTimeseries{req.Timeseries[0]},
			}, {
				Source:                  RULE,
				SkipLabelNameValidation: true,
				Timeseries:              []PreallocTimeseries{req.Timeseries[1]},
			}, {
				Source:                  RULE,
				SkipLabelNameValidation: true,
				Metadata:                []*MetricMetadata{req.Metadata[0], req.Metadata[1]},
			}, {
				Source:                  RULE,
				SkipLabelNameValidation: true,
				Metadata:                []*MetricMetadata{req.Metadata[2]},
			},
		}, partials)

		for _, partial := range partials {
			assert.Less(t, partial.Size(), limit)
		}
	})

	t.Run("should return partial WriteRequests with size bigger than limit if a single entity (Timeseries or Metadata) is larger than the limit", func(t *testing.T) {
		const limit = 10

		partials := req.SplitByMaxMarshalSize(limit)
		assert.Equal(t, []*WriteRequest{
			{
				Source:                  RULE,
				SkipLabelNameValidation: true,
				Timeseries:              []PreallocTimeseries{req.Timeseries[0]},
			}, {
				Source:                  RULE,
				SkipLabelNameValidation: true,
				Timeseries:              []PreallocTimeseries{req.Timeseries[1]},
			}, {
				Source:                  RULE,
				SkipLabelNameValidation: true,
				Metadata:                []*MetricMetadata{req.Metadata[0]},
			}, {
				Source:                  RULE,
				SkipLabelNameValidation: true,
				Metadata:                []*MetricMetadata{req.Metadata[1]},
			}, {
				Source:                  RULE,
				SkipLabelNameValidation: true,
				Metadata:                []*MetricMetadata{req.Metadata[2]},
			},
		}, partials)

		for _, partial := range partials {
			assert.Greater(t, partial.Size(), limit)
		}
	})
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
