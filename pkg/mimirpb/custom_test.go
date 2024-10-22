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
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
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

func TestWriteRequest_MetadataSize_TimeseriesSize(t *testing.T) {
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
	metadataSize := req.MetadataSize()
	timeseriesSize := req.TimeseriesSize()

	assert.Less(t, metadataSize, origSize)
	assert.Less(t, timeseriesSize, origSize)
	assert.Equal(t, origSize, req.Size())

	reqWithoutTimeseries := *req
	reqWithoutTimeseries.Timeseries = nil
	assert.Equal(t, origSize-timeseriesSize, reqWithoutTimeseries.Size())

	reqWithoutMetadata := *req
	reqWithoutMetadata.Metadata = nil
	assert.Equal(t, origSize-metadataSize, reqWithoutMetadata.Size())
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
	c := codecV2{codec: fakeCodecV2{}}

	var origReq WriteRequest
	data, err := c.Marshal(&origReq)
	require.NoError(t, err)

	var req WriteRequest
	require.NoError(t, c.Unmarshal(data, &req))

	require.NotNil(t, req.buffer)
	req.FreeBuffer()
}

type fakeCodecV2 struct {
	encoding.CodecV2
}

func (c fakeCodecV2) Marshal(v any) (mem.BufferSlice, error) {
	return encoding.GetCodecV2(proto.Name).Marshal(v)
}
