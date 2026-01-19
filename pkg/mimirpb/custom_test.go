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

	"github.com/grafana/mimir/pkg/util/test"
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
			Source:              API,
			SkipLabelValidation: true,
		}

		assert.True(t, req.IsEmpty())
	})
}

func TestWriteRequest_FieldSize(t *testing.T) {
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

	t.Run("MetadataSize and TimeseriesSize", func(t *testing.T) {
		origSize := req.Size()
		metadataSize := req.MetadataSize()
		timeseriesSize := req.TimeseriesSize()

		assert.Less(t, metadataSize, origSize)
		assert.Less(t, timeseriesSize, origSize)
		assert.Equal(t, metadataSize+timeseriesSize, req.Size())

		reqWithoutTimeseries := *req
		reqWithoutTimeseries.Timeseries = nil
		assert.Equal(t, origSize-timeseriesSize, reqWithoutTimeseries.Size())

		reqWithoutMetadata := *req
		reqWithoutMetadata.Metadata = nil
		assert.Equal(t, origSize-metadataSize, reqWithoutMetadata.Size())
	})

	reqv2 := &WriteRequest{
		SymbolsRW2: []string{"", "1", "2", "3", "4"},
		TimeseriesRW2: []TimeSeriesRW2{{
			LabelsRefs: []uint32{1, 2, 3, 4},
			Samples:    []Sample{{TimestampMs: 20}},
			Exemplars:  []ExemplarRW2{{Timestamp: 30}},
			Histograms: []Histogram{{Timestamp: 10}},
		}},
	}

	t.Run("TimeseriesRW2Size and SymbolsRW2Size", func(t *testing.T) {
		origSize := reqv2.Size()
		timeseriesSize := reqv2.TimeseriesRW2Size()
		symbolsSize := reqv2.SymbolsRW2Size()
		assert.Less(t, timeseriesSize, origSize)
		assert.Less(t, symbolsSize, origSize)
		assert.Equal(t, timeseriesSize+symbolsSize, reqv2.Size())

		reqWithoutTimeseries := *reqv2
		reqWithoutTimeseries.TimeseriesRW2 = nil
		assert.Equal(t, origSize-timeseriesSize, reqWithoutTimeseries.Size())

		reqWithoutSymbols := *reqv2
		reqWithoutSymbols.SymbolsRW2 = nil
		assert.Equal(t, origSize-symbolsSize, reqWithoutSymbols.Size())
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

func TestCodecV2_Unmarshal(t *testing.T) {
	c := codecV2{}

	var origReq WriteRequest
	data, err := c.Marshal(&origReq)
	require.NoError(t, err)

	var req WriteRequest
	require.NoError(t, c.Unmarshal(data, &req))

	require.True(t, origReq.Equal(req))

	require.NotNil(t, req.Buffer())
	req.FreeBuffer()
}

func TestHistogram_BucketsCount(t *testing.T) {
	tests := []struct {
		name      string
		histogram Histogram
		expected  int
	}{
		{
			name:      "empty histogram",
			histogram: Histogram{},
			expected:  0,
		},
		{
			name:      "int histogram",
			histogram: FromHistogramToHistogramProto(0, test.GenerateTestHistogram(0)),
			expected:  8,
		},
		{
			name:      "float histogram",
			histogram: FromFloatHistogramToHistogramProto(0, test.GenerateTestFloatHistogram(0)),
			expected:  8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.histogram.BucketCount())
		})
	}
}
