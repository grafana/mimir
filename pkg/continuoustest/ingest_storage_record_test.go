// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestDropExactDuplicates(t *testing.T) {
	tests := []struct {
		name     string
		input    []*mimirpb.MetricMetadata
		expected []*mimirpb.MetricMetadata
	}{
		{
			name:     "empty input",
			input:    []*mimirpb.MetricMetadata{},
			expected: []*mimirpb.MetricMetadata{},
		},
		{
			name:     "nil input",
			input:    nil,
			expected: []*mimirpb.MetricMetadata{},
		},
		{
			name: "single item",
			input: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "test_metric",
					Help:             "Test help text",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
			},
			expected: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "test_metric",
					Help:             "Test help text",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
			},
		},
		{
			name: "no duplicates",
			input: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
			},
			expected: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
			},
		},
		{
			name: "exact duplicates",
			input: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
			},
			expected: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
			},
		},
		{
			name: "partial duplicates - different help",
			input: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1 different",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
			},
			expected: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1 different",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
			},
		},
		{
			name: "partial duplicates - different type",
			input: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.GAUGE,
					Unit:             "seconds",
				},
			},
			expected: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.GAUGE,
					Unit:             "seconds",
				},
			},
		},
		{
			name: "partial duplicates - different unit",
			input: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "bytes",
				},
			},
			expected: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "bytes",
				},
			},
		},
		{
			name: "multiple exact duplicates",
			input: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
			},
			expected: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
			},
		},
		{
			name: "complex case with mixed duplicates",
			input: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric3",
					Help:             "Help 3",
					Type:             mimirpb.HISTOGRAM,
					Unit:             "count",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
			},
			expected: []*mimirpb.MetricMetadata{
				{
					MetricFamilyName: "metric1",
					Help:             "Help 1",
					Type:             mimirpb.COUNTER,
					Unit:             "seconds",
				},
				{
					MetricFamilyName: "metric2",
					Help:             "Help 2",
					Type:             mimirpb.GAUGE,
					Unit:             "bytes",
				},
				{
					MetricFamilyName: "metric3",
					Help:             "Help 3",
					Type:             mimirpb.HISTOGRAM,
					Unit:             "count",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dropExactDuplicates(tt.input)

			// Check that the result has the expected length
			assert.Equal(t, len(tt.expected), len(result), "Result length mismatch")

			// Check that all expected items are present in the result
			for i, expected := range tt.expected {
				require.Less(t, i, len(result), "Result has fewer items than expected")
				assert.Equal(t, expected.MetricFamilyName, result[i].MetricFamilyName, "MetricFamilyName mismatch at index %d", i)
				assert.Equal(t, expected.Help, result[i].Help, "Help mismatch at index %d", i)
				assert.Equal(t, expected.Type, result[i].Type, "Type mismatch at index %d", i)
				assert.Equal(t, expected.Unit, result[i].Unit, "Unit mismatch at index %d", i)
			}

			// Check that the result doesn't contain any exact duplicates
			seen := make(map[string]bool)
			for _, item := range result {
				key := item.MetricFamilyName + "|" + item.Help + "|" + item.Type.String() + "|" + item.Unit
				assert.False(t, seen[key], "Duplicate found: %s", key)
				seen[key] = true
			}
		})
	}
}

func TestDropExactDuplicatesPreservesOrder(t *testing.T) {
	input := []*mimirpb.MetricMetadata{
		{
			MetricFamilyName: "metric1",
			Help:             "Help 1",
			Type:             mimirpb.COUNTER,
			Unit:             "seconds",
		},
		{
			MetricFamilyName: "metric2",
			Help:             "Help 2",
			Type:             mimirpb.GAUGE,
			Unit:             "bytes",
		},
		{
			MetricFamilyName: "metric1", // Duplicate of first item
			Help:             "Help 1",
			Type:             mimirpb.COUNTER,
			Unit:             "seconds",
		},
		{
			MetricFamilyName: "metric3",
			Help:             "Help 3",
			Type:             mimirpb.HISTOGRAM,
			Unit:             "count",
		},
	}

	result := dropExactDuplicates(input)

	// The first occurrence of metric1 should be preserved
	assert.Equal(t, "metric1", result[0].MetricFamilyName)
	assert.Equal(t, "metric2", result[1].MetricFamilyName)
	assert.Equal(t, "metric3", result[2].MetricFamilyName)
	assert.Equal(t, 3, len(result))
}

func TestDropMeaninglessMetadata(t *testing.T) {
	t.Run("valid metadata, no change", func(t *testing.T) {
		input := []*mimirpb.MetricMetadata{
			{
				MetricFamilyName: "metric1",
				Help:             "Help 1",
				Type:             mimirpb.COUNTER,
				Unit:             "seconds",
			},
			{
				MetricFamilyName: "metric2",
				Help:             "Help 2",
				Type:             mimirpb.GAUGE,
				Unit:             "bytes",
			},
		}

		result := dropMeaninglessMetadata(input)

		assert.Len(t, result, 2)
		assert.Equal(t, "metric1", result[0].MetricFamilyName)
		assert.Equal(t, "metric2", result[1].MetricFamilyName)
	})

	t.Run("meaningless metadata are dropped", func(t *testing.T) {
		input := []*mimirpb.MetricMetadata{
			{
				MetricFamilyName: "metric1",
				Help:             "Help 1",
				Type:             mimirpb.COUNTER,
				Unit:             "seconds",
			},
			{
				MetricFamilyName: "metric3",
				Help:             "",
				Type:             mimirpb.UNKNOWN,
				Unit:             "",
			},
			{
				MetricFamilyName: "metric2",
				Help:             "Help 2",
				Type:             mimirpb.GAUGE,
				Unit:             "bytes",
			},
		}

		result := dropMeaninglessMetadata(input)

		assert.Len(t, result, 2)
		assert.Equal(t, "metric1", result[0].MetricFamilyName)
		assert.Equal(t, "metric2", result[1].MetricFamilyName)
	})

	t.Run("all meaningless returns empty slice", func(t *testing.T) {
		input := []*mimirpb.MetricMetadata{
			{
				MetricFamilyName: "metric1",
				Help:             "",
				Type:             mimirpb.UNKNOWN,
				Unit:             "",
			},
			{
				MetricFamilyName: "metric2",
				Help:             "",
				Type:             mimirpb.UNKNOWN,
				Unit:             "",
			},
		}

		result := dropMeaninglessMetadata(input)

		assert.NotNil(t, result)
		assert.Empty(t, result)
	})
}

func TestExemplarEqual(t *testing.T) {
	t.Run("equal", func(t *testing.T) {
		exemplar1 := &mimirpb.Exemplar{
			Labels: []mimirpb.UnsafeMutableLabel{
				{Name: "trace_id", Value: "abc123"},
				{Name: "span_id", Value: "def456"},
			},
			Value:       42.5,
			TimestampMs: 1234567890,
		}
		exemplar2 := &mimirpb.Exemplar{
			Labels: []mimirpb.UnsafeMutableLabel{
				{Name: "trace_id", Value: "abc123"},
				{Name: "span_id", Value: "def456"},
			},
			Value:       42.5,
			TimestampMs: 1234567890,
		}

		require.True(t, ExemplarEqual(exemplar1, exemplar2))
	})

	t.Run("not equal", func(t *testing.T) {
		exemplar1 := &mimirpb.Exemplar{
			Labels: []mimirpb.UnsafeMutableLabel{
				{Name: "trace_id", Value: "abc123"},
			},
			Value:       42.5,
			TimestampMs: 1234567890,
		}
		exemplar2 := &mimirpb.Exemplar{
			Labels: []mimirpb.UnsafeMutableLabel{
				{Name: "trace_id", Value: "xyz789"},
			},
			Value:       42.5,
			TimestampMs: 1234567890,
		}

		require.False(t, ExemplarEqual(exemplar1, exemplar2))
	})

	t.Run("equal with NaN values", func(t *testing.T) {
		exemplar1 := &mimirpb.Exemplar{
			Labels: []mimirpb.UnsafeMutableLabel{
				{Name: "trace_id", Value: "abc123"},
			},
			Value:       math.NaN(),
			TimestampMs: 1234567890,
		}
		exemplar2 := &mimirpb.Exemplar{
			Labels: []mimirpb.UnsafeMutableLabel{
				{Name: "trace_id", Value: "abc123"},
			},
			Value:       math.NaN(),
			TimestampMs: 1234567890,
		}

		require.True(t, ExemplarEqual(exemplar1, exemplar2))
	})
}

func TestHistogramEqual(t *testing.T) {
	t.Run("equal", func(t *testing.T) {
		histogram1 := &mimirpb.Histogram{
			Count:         &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:           42.5,
			Schema:        0,
			ZeroThreshold: 0.001,
			ZeroCount:     &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 5},
			NegativeSpans: []mimirpb.BucketSpan{
				{Offset: 0, Length: 2},
			},
			NegativeDeltas: []int64{1, 2},
			PositiveSpans: []mimirpb.BucketSpan{
				{Offset: 0, Length: 3},
			},
			PositiveDeltas: []int64{3, 4, 5},
			ResetHint:      mimirpb.Histogram_UNKNOWN,
			Timestamp:      1234567890,
			CustomValues:   []float64{1.1, 2.2},
		}
		histogram2 := &mimirpb.Histogram{
			Count:         &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:           42.5,
			Schema:        0,
			ZeroThreshold: 0.001,
			ZeroCount:     &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 5},
			NegativeSpans: []mimirpb.BucketSpan{
				{Offset: 0, Length: 2},
			},
			NegativeDeltas: []int64{1, 2},
			PositiveSpans: []mimirpb.BucketSpan{
				{Offset: 0, Length: 3},
			},
			PositiveDeltas: []int64{3, 4, 5},
			ResetHint:      mimirpb.Histogram_UNKNOWN,
			Timestamp:      1234567890,
			CustomValues:   []float64{1.1, 2.2},
		}

		require.True(t, HistogramEqual(histogram1, histogram2))
	})

	t.Run("not equal", func(t *testing.T) {
		histogram1 := &mimirpb.Histogram{
			Count:         &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:           42.5,
			Schema:        0,
			ZeroThreshold: 0.001,
			ZeroCount:     &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 5},
			NegativeSpans: []mimirpb.BucketSpan{
				{Offset: 0, Length: 2},
			},
			NegativeDeltas: []int64{1, 2},
			PositiveSpans: []mimirpb.BucketSpan{
				{Offset: 0, Length: 3},
			},
			PositiveDeltas: []int64{3, 4, 5},
			ResetHint:      mimirpb.Histogram_UNKNOWN,
			Timestamp:      1234567890,
			CustomValues:   []float64{1.1, 2.2},
		}
		histogram2 := &mimirpb.Histogram{
			Count:         &mimirpb.Histogram_CountInt{CountInt: 200}, // Different count
			Sum:           42.5,
			Schema:        0,
			ZeroThreshold: 0.001,
			ZeroCount:     &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 5},
			NegativeSpans: []mimirpb.BucketSpan{
				{Offset: 0, Length: 2},
			},
			NegativeDeltas: []int64{1, 2},
			PositiveSpans: []mimirpb.BucketSpan{
				{Offset: 0, Length: 3},
			},
			PositiveDeltas: []int64{3, 4, 5},
			ResetHint:      mimirpb.Histogram_UNKNOWN,
			Timestamp:      1234567890,
			CustomValues:   []float64{1.1, 2.2},
		}

		require.False(t, HistogramEqual(histogram1, histogram2))
	})

	t.Run("equal with NaN Sum", func(t *testing.T) {
		histogram1 := &mimirpb.Histogram{
			Count: &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:   math.NaN(),
		}
		histogram2 := &mimirpb.Histogram{
			Count: &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:   math.NaN(),
		}

		require.True(t, HistogramEqual(histogram1, histogram2))
	})

	t.Run("equal with NaN ZeroThreshold", func(t *testing.T) {
		histogram1 := &mimirpb.Histogram{
			Count:         &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:           42.5,
			ZeroThreshold: math.NaN(),
		}
		histogram2 := &mimirpb.Histogram{
			Count:         &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:           42.5,
			ZeroThreshold: math.NaN(),
		}

		require.True(t, HistogramEqual(histogram1, histogram2))
	})

	t.Run("equal with NaN in NegativeCounts", func(t *testing.T) {
		histogram1 := &mimirpb.Histogram{
			Count:          &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:            42.5,
			NegativeSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 2}},
			NegativeCounts: []float64{1.0, math.NaN()},
		}
		histogram2 := &mimirpb.Histogram{
			Count:          &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:            42.5,
			NegativeSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 2}},
			NegativeCounts: []float64{1.0, math.NaN()},
		}

		require.True(t, HistogramEqual(histogram1, histogram2))
	})

	t.Run("equal with NaN in PositiveCounts", func(t *testing.T) {
		histogram1 := &mimirpb.Histogram{
			Count:          &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:            42.5,
			PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 2}},
			PositiveCounts: []float64{math.NaN(), 2.0},
		}
		histogram2 := &mimirpb.Histogram{
			Count:          &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:            42.5,
			PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 2}},
			PositiveCounts: []float64{math.NaN(), 2.0},
		}

		require.True(t, HistogramEqual(histogram1, histogram2))
	})

	t.Run("equal with NaN in CustomValues", func(t *testing.T) {
		histogram1 := &mimirpb.Histogram{
			Count:        &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:          42.5,
			CustomValues: []float64{1.1, math.NaN(), 3.3},
		}
		histogram2 := &mimirpb.Histogram{
			Count:        &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:          42.5,
			CustomValues: []float64{1.1, math.NaN(), 3.3},
		}

		require.True(t, HistogramEqual(histogram1, histogram2))
	})

	t.Run("not equal when one Sum is NaN and other is not", func(t *testing.T) {
		histogram1 := &mimirpb.Histogram{
			Count: &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:   math.NaN(),
		}
		histogram2 := &mimirpb.Histogram{
			Count: &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:   42.5,
		}

		require.False(t, HistogramEqual(histogram1, histogram2))
	})

	t.Run("not equal when one ZeroThreshold is NaN and other is not", func(t *testing.T) {
		histogram1 := &mimirpb.Histogram{
			Count:         &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:           42.5,
			ZeroThreshold: math.NaN(),
		}
		histogram2 := &mimirpb.Histogram{
			Count:         &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:           42.5,
			ZeroThreshold: 0.001,
		}

		require.False(t, HistogramEqual(histogram1, histogram2))
	})

	t.Run("not equal when one NegativeCounts has NaN and other does not", func(t *testing.T) {
		histogram1 := &mimirpb.Histogram{
			Count:          &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:            42.5,
			NegativeSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 2}},
			NegativeCounts: []float64{1.0, math.NaN()},
		}
		histogram2 := &mimirpb.Histogram{
			Count:          &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:            42.5,
			NegativeSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 2}},
			NegativeCounts: []float64{1.0, 2.0},
		}

		require.False(t, HistogramEqual(histogram1, histogram2))
	})

	t.Run("not equal when one PositiveCounts has NaN and other does not", func(t *testing.T) {
		histogram1 := &mimirpb.Histogram{
			Count:          &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:            42.5,
			PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 2}},
			PositiveCounts: []float64{math.NaN(), 2.0},
		}
		histogram2 := &mimirpb.Histogram{
			Count:          &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:            42.5,
			PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 2}},
			PositiveCounts: []float64{1.0, 2.0},
		}

		require.False(t, HistogramEqual(histogram1, histogram2))
	})

	t.Run("not equal when one CustomValues has NaN and other does not", func(t *testing.T) {
		histogram1 := &mimirpb.Histogram{
			Count:        &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:          42.5,
			CustomValues: []float64{1.1, math.NaN(), 3.3},
		}
		histogram2 := &mimirpb.Histogram{
			Count:        &mimirpb.Histogram_CountInt{CountInt: 100},
			Sum:          42.5,
			CustomValues: []float64{1.1, 2.2, 3.3},
		}

		require.False(t, HistogramEqual(histogram1, histogram2))
	})
}
