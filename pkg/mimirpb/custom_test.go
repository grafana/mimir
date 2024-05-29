// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/compat_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	"fmt"
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

// TODO split the benchmark is end-to-end and not
func BenchmarkWriteRequest_SplitByMaxMarshalSize(b *testing.B) {
	tests := map[string]struct {
		numSeries           int
		numLabelsPerSeries  int
		numSamplesPerSeries int
		numMetadata         int
	}{
		"write request with few series, few labels each, and no metadata": {
			numSeries:           50,
			numLabelsPerSeries:  10,
			numSamplesPerSeries: 1,
			numMetadata:         0,
		},
		"write request with few series, many labels each, and no metadata": {
			numSeries:           50,
			numLabelsPerSeries:  100,
			numSamplesPerSeries: 1,
			numMetadata:         0,
		},
		"write request with many series, few labels each, and no metadata": {
			numSeries:           1000,
			numLabelsPerSeries:  10,
			numSamplesPerSeries: 1,
			numMetadata:         0,
		},
		"write request with many series, many labels each, and no metadata": {
			numSeries:           1000,
			numLabelsPerSeries:  100,
			numSamplesPerSeries: 1,
			numMetadata:         0,
		},
		"write request with few metadata, and no series": {
			numSeries:           0,
			numLabelsPerSeries:  0,
			numSamplesPerSeries: 0,
			numMetadata:         50,
		},
		"write request with many metadata, and no series": {
			numSeries:           0,
			numLabelsPerSeries:  0,
			numSamplesPerSeries: 0,
			numMetadata:         1000,
		},
		"write request with both series and metadata": {
			numSeries:           500,
			numLabelsPerSeries:  25,
			numSamplesPerSeries: 1,
			numMetadata:         500,
		},
	}

	for testName, testData := range tests {
		b.Run(testName, func(b *testing.B) {
			req := generateWriteRequest(testData.numSeries, testData.numLabelsPerSeries, testData.numSamplesPerSeries, testData.numMetadata)
			reqSize := req.Size()

			// Test with different split size.
			splitScenarios := map[string]struct {
				maxSize              int
				expectedApproxSplits int
			}{
				"no splitting": {
					maxSize:              reqSize * 2,
					expectedApproxSplits: 1,
				},
				"split in few requests": {
					maxSize:              int(float64(reqSize) * 0.8),
					expectedApproxSplits: 2,
				},
				"split in many requests": {
					maxSize:              int(float64(reqSize) * 0.11),
					expectedApproxSplits: 10,
				},
			}

			for splitName, splitScenario := range splitScenarios {
				b.Run(splitName, func(b *testing.B) {
					// The actual number of splits may be slightly different then the expected, due to implementation
					// details (e.g. if a request both contain series and metadata, they're never mixed in the same split request).
					minExpectedSplits := splitScenario.expectedApproxSplits - 1
					maxExpectedSplits := splitScenario.expectedApproxSplits + 1
					if splitScenario.expectedApproxSplits == 0 {
						minExpectedSplits = 0
						maxExpectedSplits = 0
					}

					for n := 0; n < b.N; n++ {
						actualSplits := req.SplitByMaxMarshalSize(splitScenario.maxSize)

						// Ensure the number of splits match the expected ones.
						if numActualSplits := len(actualSplits); (numActualSplits < minExpectedSplits) || (numActualSplits > maxExpectedSplits) {
							b.Fatalf("expected between %d and %d splits but got %d", minExpectedSplits, maxExpectedSplits, numActualSplits)
						}

						// Marshal each split request. The assumption is that the split request will be then marshalled.
						// This is also offer a fair comparison with an alternative implementation (we're considering)
						// which does the splitting starting from the marshalled request.
						//for _, split := range actualSplits {
						//	if data, err := split.Marshal(); err != nil {
						//		b.Fatal(err)
						//	} else if len(data) >= splitScenario.maxSize {
						//		b.Fatalf("the marshalled split request (%d bytes) is larger than max size (%d bytes)", len(data), splitScenario.maxSize)
						//	}
						//}
					}
				})
			}
		})
	}
}

func generateWriteRequest(numSeries, numLabelsPerSeries, numSamplesPerSeries, numMetadata int) *WriteRequest {
	builder := labels.NewScratchBuilder(numLabelsPerSeries)

	// Generate timeseries.
	timeseries := make([]PreallocTimeseries, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		curr := PreallocTimeseries{TimeSeries: &TimeSeries{}}

		// Generate series labels.
		builder.Reset()
		builder.Add(labels.MetricName, fmt.Sprintf("series_%d", i))
		for l := 1; l < numLabelsPerSeries; l++ {
			builder.Add(fmt.Sprintf("label_%d", l), fmt.Sprintf("this-is-the-value-of-label-%d", l))
		}
		curr.Labels = FromLabelsToLabelAdapters(builder.Labels())

		// Generate samples.
		curr.Samples = make([]Sample, 0, numSamplesPerSeries)
		for s := 0; s < numSamplesPerSeries; s++ {
			curr.Samples = append(curr.Samples, Sample{
				TimestampMs: int64(s),
				Value:       float64(s),
			})
		}

		// Add an exemplar.
		builder.Reset()
		builder.Add("trace_id", fmt.Sprintf("the-trace-id-for-%d", i))
		curr.Exemplars = []Exemplar{{
			Labels:      FromLabelsToLabelAdapters(builder.Labels()),
			TimestampMs: int64(i),
			Value:       float64(i),
		}}

		timeseries = append(timeseries, curr)
	}

	// Generate metadata.
	metadata := make([]*MetricMetadata, 0, numMetadata)
	for i := 0; i < numMetadata; i++ {
		metadata = append(metadata, &MetricMetadata{
			Type:             COUNTER,
			MetricFamilyName: fmt.Sprintf("series_%d", i),
			Help:             fmt.Sprintf("this is the help description for series %d", i),
			Unit:             "seconds",
		})
	}

	return &WriteRequest{
		Source:                  RULE,
		SkipLabelNameValidation: true,
		Timeseries:              timeseries,
		Metadata:                metadata,
	}
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
