// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/compat_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	stdlibjson "encoding/json"
	"math"
	"strconv"
	"testing"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/test"
)

// This test verifies that jsoninter uses our custom method for marshalling.
// We do that by using "test sample" recognized by marshal function when in testing mode.
func TestJsoniterMarshalForSample(t *testing.T) {
	testMarshalling(t, jsoniter.Marshal, "test sample")
}

func TestStdlibJsonMarshalForSample(t *testing.T) {
	testMarshalling(t, stdlibjson.Marshal, "json: error calling MarshalJSON for type mimirpb.Sample: test sample")
}

func testMarshalling(t *testing.T, marshalFn func(v interface{}) ([]byte, error), expectedError string) {
	isTesting = true
	defer func() { isTesting = false }()

	out, err := marshalFn(Sample{Value: 12345, TimestampMs: 98765})
	require.NoError(t, err)
	require.Equal(t, `[98.765,"12345"]`, string(out))

	_, err = marshalFn(Sample{Value: math.NaN(), TimestampMs: 0})
	require.EqualError(t, err, expectedError)

	// If not testing, we get normal output.
	isTesting = false
	out, err = marshalFn(Sample{Value: math.NaN(), TimestampMs: 0})
	require.NoError(t, err)
	require.Equal(t, `[0,"NaN"]`, string(out))
}

// This test verifies that jsoninter uses our custom method for unmarshalling Sample.
// As with Marshal, we rely on testing mode and special value that reports error.
func TestJsoniterUnmarshalForSample(t *testing.T) {
	testUnmarshalling(t, jsoniter.Unmarshal, "test sample")
}

func TestStdlibJsonUnmarshalForSample(t *testing.T) {
	testUnmarshalling(t, stdlibjson.Unmarshal, "test sample")
}

func testUnmarshalling(t *testing.T, unmarshalFn func(data []byte, v interface{}) error, expectedError string) {
	isTesting = true
	defer func() { isTesting = false }()

	sample := Sample{}

	err := unmarshalFn([]byte(`[98.765,"12345"]`), &sample)
	require.NoError(t, err)
	require.Equal(t, Sample{Value: 12345, TimestampMs: 98765}, sample)

	err = unmarshalFn([]byte(`[0.0,"NaN"]`), &sample)
	require.EqualError(t, err, expectedError)

	isTesting = false
	err = unmarshalFn([]byte(`[0.0,"NaN"]`), &sample)
	require.NoError(t, err)
	require.Equal(t, int64(0), sample.TimestampMs)
	require.True(t, math.IsNaN(sample.Value))
}

func TestMarshalSampleHistogramPair(t *testing.T) {
	tests := map[string]struct {
		marshaller func(any) ([]byte, error)
	}{
		"standard": {
			marshaller: stdlibjson.Marshal,
		},
		"jsoniter": {
			marshaller: jsoniter.Marshal,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			shp := SampleHistogramPair{
				Timestamp: 1234,
				Histogram: &SampleHistogram{
					Count: 10,
					Sum:   18.4,
					Buckets: []*HistogramBucket{
						{1, -4, -2.82842712474619, 1},
						{1, -2.82842712474619, -2, 1},
					},
				},
			}
			expected := `[1.234,{"count":"10","sum":"18.4","buckets":[[1,"-4","-2.82842712474619","1"],[1,"-2.82842712474619","-2","1"]]}]`
			data, err := test.marshaller(shp)
			require.NoError(t, err)
			require.Equal(t, expected, string(data))
		})
	}
}

func TestUnmarshalSampleHistogramPair(t *testing.T) {
	tests := map[string]struct {
		marshaller func([]byte, any) error
	}{
		"standard": {
			marshaller: stdlibjson.Unmarshal,
		},
		"jsoniter": {
			marshaller: jsoniter.Unmarshal,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			plain := `[1.234,{"count":"10","sum":"18.4","buckets":[[1,"-4","-2.82842712474619","1"],[1,"-2.82842712474619","-2","1"]]}]`
			expected := SampleHistogramPair{
				Timestamp: 1234,
				Histogram: &SampleHistogram{
					Count: 10,
					Sum:   18.4,
					Buckets: []*HistogramBucket{
						{1, -4, -2.82842712474619, 1},
						{1, -2.82842712474619, -2, 1},
					},
				},
			}
			shp := SampleHistogramPair{}
			err := test.marshaller([]byte(plain), &shp)
			require.NoError(t, err)
			require.Equal(t, expected, shp)
		})
	}
}

func TestMetricMetadataToMetricTypeToMetricType(t *testing.T) {
	tc := []struct {
		desc     string
		input    MetricMetadata_MetricType
		expected textparse.MetricType
	}{
		{
			desc:     "with a single-word metric",
			input:    COUNTER,
			expected: textparse.MetricTypeCounter,
		},
		{
			desc:     "with a two-word metric",
			input:    STATESET,
			expected: textparse.MetricTypeStateset,
		},
		{
			desc:     "with an unknown metric",
			input:    MetricMetadata_MetricType(100),
			expected: textparse.MetricTypeUnknown,
		},
	}

	for _, tt := range tc {
		t.Run(tt.desc, func(t *testing.T) {
			m := MetricMetadataMetricTypeToMetricType(tt.input)
			assert.Equal(t, tt.expected, m)
		})
	}
}

func TestFromLabelAdaptersToLabels(t *testing.T) {
	input := []LabelAdapter{{Name: "hello", Value: "world"}}
	expected := labels.FromStrings("hello", "world")
	actual := FromLabelAdaptersToLabels(input)

	assert.Equal(t, expected, actual)

	// All strings must NOT be copied.
	assert.Equal(t, uintptr(unsafe.Pointer(&input[0].Name)), uintptr(unsafe.Pointer(&actual[0].Name)))
	assert.Equal(t, uintptr(unsafe.Pointer(&input[0].Value)), uintptr(unsafe.Pointer(&actual[0].Value)))
}

func TestFromLabelAdaptersToLabelsWithCopy(t *testing.T) {
	input := []LabelAdapter{{Name: "hello", Value: "world"}}
	expected := labels.FromStrings("hello", "world")
	actual := FromLabelAdaptersToLabelsWithCopy(input)

	assert.Equal(t, expected, actual)

	// All strings must be copied.
	assert.NotEqual(t, uintptr(unsafe.Pointer(&input[0].Name)), uintptr(unsafe.Pointer(&actual[0].Name)))
	assert.NotEqual(t, uintptr(unsafe.Pointer(&input[0].Value)), uintptr(unsafe.Pointer(&actual[0].Value)))
}

func BenchmarkFromLabelAdaptersToLabelsWithCopy(b *testing.B) {
	input := []LabelAdapter{
		{Name: "hello", Value: "world"},
		{Name: "some label", Value: "and its value"},
		{Name: "long long long long long label name", Value: "perhaps even longer label value, but who's counting anyway?"}}

	for i := 0; i < b.N; i++ {
		FromLabelAdaptersToLabelsWithCopy(input)
	}
}

func TestFromPointsToSamples(t *testing.T) {
	input := []promql.Point{{T: 1, V: 2}, {T: 3, V: 4}, {T: 5, H: test.GenerateTestFloatHistogram(0)}}
	expected := []Sample{{TimestampMs: 1, Value: 2}, {TimestampMs: 3, Value: 4}}

	assert.Equal(t, expected, FromPointsToSamples(input))
}

func TestFromPointsToHistograms(t *testing.T) {
	input := []promql.Point{{T: 1, V: 2}, {T: 3, H: test.GenerateTestFloatHistogram(0)}, {T: 5, H: test.GenerateTestFloatHistogram(1)}}
	expected := []FloatHistogramPair{
		{TimestampMs: 3, Histogram: *FloatHistogramFromPrometheusModel(test.GenerateTestFloatHistogram(0))},
		{TimestampMs: 5, Histogram: *FloatHistogramFromPrometheusModel(test.GenerateTestFloatHistogram(1))},
	}

	assert.Equal(t, expected, FromPointsToHistograms(input))
}

func TestPreallocatingMetric(t *testing.T) {
	t.Run("should be unmarshallable from the bytes of a default Metric", func(t *testing.T) {
		metric := Metric{
			Labels: []LabelAdapter{
				{Name: "l1", Value: "v1"},
				{Name: "l2", Value: "v2"},
				{Name: "l3", Value: "v3"},
				{Name: "l4", Value: "v4"},
			},
		}

		metricBytes, err := metric.Marshal()
		require.NoError(t, err)

		preallocMetric := &PreallocatingMetric{}
		require.NoError(t, preallocMetric.Unmarshal(metricBytes))

		assert.Equal(t, metric, preallocMetric.Metric)
	})

	t.Run("should not break with invalid protobuf bytes (no panic)", func(t *testing.T) {
		preallocMetric := &PreallocatingMetric{}
		assert.Error(t, preallocMetric.Unmarshal([]byte{1, 3, 5, 6, 238, 55, 135}))
	})

	t.Run("should correctly preallocate Labels slice", func(t *testing.T) {
		metric := Metric{
			Labels: []LabelAdapter{
				{Name: "l1", Value: "v1"},
				{Name: "l2", Value: "v2"},
				{Name: "l3", Value: "v3"},
				{Name: "l4", Value: "v4"},
				{Name: "l5", Value: "v5"},
			},
		}

		metricBytes, err := metric.Marshal()
		require.NoError(t, err)

		preallocMetric := &PreallocatingMetric{}
		require.NoError(t, preallocMetric.Unmarshal(metricBytes))

		assert.Equal(t, metric, preallocMetric.Metric)
		assert.Equal(t, len(metric.Labels), cap(preallocMetric.Labels))
	})

	t.Run("should not allocate a slice when there are 0 Labels (same as Metric's behaviour)", func(t *testing.T) {
		metric := Metric{
			Labels: []LabelAdapter{},
		}

		metricBytes, err := metric.Marshal()
		require.NoError(t, err)

		preallocMetric := &PreallocatingMetric{}
		require.NoError(t, preallocMetric.Unmarshal(metricBytes))

		assert.Nil(t, preallocMetric.Labels)
	})

	t.Run("should marshal to the same bytes as Metric", func(t *testing.T) {
		preallocMetric := &PreallocatingMetric{Metric{
			Labels: []LabelAdapter{
				{Name: "l1", Value: "v1"},
				{Name: "l2", Value: "v2"},
				{Name: "l3", Value: "v3"},
				{Name: "l4", Value: "v4"},
				{Name: "l5", Value: "v5"},
			},
		}}

		metric := Metric{
			Labels: []LabelAdapter{
				{Name: "l1", Value: "v1"},
				{Name: "l2", Value: "v2"},
				{Name: "l3", Value: "v3"},
				{Name: "l4", Value: "v4"},
				{Name: "l5", Value: "v5"},
			},
		}

		preallocBytes, err := preallocMetric.Marshal()
		require.NoError(t, err)
		metricBytes, err := metric.Marshal()
		require.NoError(t, err)

		assert.Equal(t, metricBytes, preallocBytes)
	})
}

func TestRemoteWriteContainsHistogram(t *testing.T) {
	// Prometheus
	remoteWrite := prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Histograms: []prompb.Histogram{
					remote.HistogramToHistogramProto(1337, test.GenerateTestHistogram(0)),
				},
			},
		},
	}
	data, err := remoteWrite.Marshal()
	assert.NoError(t, err, "marshal to protbuf")

	// Mimir
	receivedRemoteWrite := &WriteRequest{}
	err = receivedRemoteWrite.Unmarshal(data)
	assert.NoError(t, err, "marshal to protbuf")

	assert.NotEmpty(t, receivedRemoteWrite.Timeseries)
	assert.NotEmpty(t, receivedRemoteWrite.Timeseries[0].Histograms)
}

func TestFromPromRemoteWriteHistogramToMimir(t *testing.T) {
	tests := map[string]struct {
		tsdbHistogram *histogram.Histogram
		expectGauge   bool
	}{
		"counter": {
			tsdbHistogram: test.GenerateTestHistogram(0),
			expectGauge:   false,
		},
		"gauge": {
			tsdbHistogram: test.GenerateTestGaugeHistogram(0),
			expectGauge:   true,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Prometheus
			remoteWriteHistogram := remote.HistogramToHistogramProto(1337, test.tsdbHistogram)
			data, err := remoteWriteHistogram.Marshal()
			assert.NoError(t, err, "marshal to protbuf")

			// Mimir
			receivedHistogram := &Histogram{}
			err = receivedHistogram.Unmarshal(data)
			assert.NoError(t, err, "unmarshal from protobuf")
			assert.False(t, receivedHistogram.IsFloatHistogram())
			assert.Equal(t, test.expectGauge, receivedHistogram.IsGauge())
			mimirHistogram := FromHistogramProtoToHistogram(receivedHistogram)

			// Is equal
			assert.Equal(t, test.tsdbHistogram, mimirHistogram, "mimir unmarshal results the same")
		})
	}
}

func TestFromPromRemoteWriteFloatHistogramToMimir(t *testing.T) {
	tests := map[string]struct {
		tsdbHistogram *histogram.FloatHistogram
		expectGauge   bool
	}{
		"counter": {
			tsdbHistogram: test.GenerateTestFloatHistogram(0),
			expectGauge:   false,
		},
		"gauge": {
			tsdbHistogram: test.GenerateTestGaugeFloatHistogram(0),
			expectGauge:   true,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Prometheus
			remoteWriteHistogram := remote.FloatHistogramToHistogramProto(1337, test.tsdbHistogram)
			data, err := remoteWriteHistogram.Marshal()
			assert.NoError(t, err, "marshal to protbuf")

			// Mimir
			receivedHistogram := &Histogram{}
			err = receivedHistogram.Unmarshal(data)
			assert.NoError(t, err, "unmarshal from protobuf")
			assert.True(t, receivedHistogram.IsFloatHistogram())
			assert.Equal(t, test.expectGauge, receivedHistogram.IsGauge())
			mimirHistogram := FromHistogramProtoToFloatHistogram(receivedHistogram)

			// Is equal
			assert.Equal(t, test.tsdbHistogram, mimirHistogram, "mimir unmarshal results the same")
		})
	}
}

func TestCounterResetHint(t *testing.T) {
	// Use protobuf generated code to check equivalence
	assert.Equal(t, prompb.Histogram_ResetHint_value, Histogram_ResetHint_value)
}

func TestFromHistogramToHistogramProto(t *testing.T) {
	var ts int64 = 1
	h := test.GenerateTestHistogram(int(ts))
	h.CounterResetHint = histogram.NotCounterReset

	p := FromHistogramToHistogramProto(ts, h)

	expected := Histogram{
		Count:          &Histogram_CountInt{18},
		Sum:            36.8,
		Schema:         1,
		ZeroThreshold:  0.001,
		ZeroCount:      &Histogram_ZeroCountInt{3},
		NegativeSpans:  []BucketSpan{{Offset: 0, Length: 2}, {Offset: 1, Length: 2}},
		NegativeDeltas: []int64{2, 1, -1, 0},
		PositiveSpans:  []BucketSpan{{Offset: 0, Length: 2}, {Offset: 1, Length: 2}},
		PositiveDeltas: []int64{2, 1, -1, 0},
		ResetHint:      Histogram_NO,
		Timestamp:      ts,
	}
	assert.Equal(t, expected, p)

	// Also check via JSON encode/decode
	promP := remote.HistogramToHistogramProto(ts, h)
	d, err := promP.Marshal()
	assert.NoError(t, err)
	p2 := Histogram{}
	assert.NoError(t, p2.Unmarshal(d))
	assert.Equal(t, expected, p2)
}

func TestFromFloatHistogramToHistogramProto(t *testing.T) {
	var ts int64 = 1
	h := test.GenerateTestFloatHistogram(int(ts))
	h.CounterResetHint = histogram.NotCounterReset

	p := FromFloatHistogramToHistogramProto(ts, h)

	expected := Histogram{
		Count:          &Histogram_CountFloat{18},
		Sum:            36.8,
		Schema:         1,
		ZeroThreshold:  0.001,
		ZeroCount:      &Histogram_ZeroCountFloat{3},
		NegativeSpans:  []BucketSpan{{Offset: 0, Length: 2}, {Offset: 1, Length: 2}},
		NegativeCounts: []float64{2, 3, 2, 2},
		PositiveSpans:  []BucketSpan{{Offset: 0, Length: 2}, {Offset: 1, Length: 2}},
		PositiveCounts: []float64{2, 3, 2, 2},
		ResetHint:      Histogram_NO,
		Timestamp:      ts,
	}
	assert.Equal(t, expected, p)

	// Also check via JSON encode/decode
	promP := remote.FloatHistogramToHistogramProto(ts, h)
	d, err := promP.Marshal()
	assert.NoError(t, err)
	p2 := Histogram{}
	assert.NoError(t, p2.Unmarshal(d))
	assert.Equal(t, expected, p2)
}

func TestFromFloatHistogramToPromHistogram(t *testing.T) {
	cases := []struct {
		h   histogram.FloatHistogram
		exp model.SampleHistogram
	}{
		{
			h: histogram.FloatHistogram{
				Count:         18,
				ZeroCount:     2,
				ZeroThreshold: 0.001,
				Sum:           18.4,
				Schema:        0,
				PositiveSpans: []histogram.Span{
					{Offset: 0, Length: 2},
					{Offset: 1, Length: 1},
				},
				PositiveBuckets: []float64{1, 2, 1},
				NegativeSpans: []histogram.Span{
					{Offset: 0, Length: 2},
					{Offset: 1, Length: 1},
				},
				NegativeBuckets: []float64{1, 2, 1},
			},
			exp: model.SampleHistogram{
				Count: 18,
				Sum:   18.4,
				Buckets: model.HistogramBuckets{
					{Boundaries: 1, Lower: -8, Upper: -4, Count: 1},
					{Boundaries: 1, Lower: -2, Upper: -1, Count: 2},
					{Boundaries: 1, Lower: -1, Upper: -0.5, Count: 1},
					{Boundaries: 3, Lower: -0.001, Upper: 0.001, Count: 2},
					{Boundaries: 0, Lower: 0.5, Upper: 1, Count: 1},
					{Boundaries: 0, Lower: 1, Upper: 2, Count: 2},
					{Boundaries: 0, Lower: 4, Upper: 8, Count: 1},
				},
			},
		},
		{ // Empty buckets don't show up.
			h: histogram.FloatHistogram{
				Count:         18,
				ZeroCount:     0,
				ZeroThreshold: 0.001,
				Sum:           18.4,
				Schema:        0,
				PositiveSpans: []histogram.Span{
					{Offset: 0, Length: 2},
					{Offset: 1, Length: 1},
				},
				PositiveBuckets: []float64{1, 0, 1},
				NegativeSpans: []histogram.Span{
					{Offset: 0, Length: 2},
					{Offset: 1, Length: 1},
				},
				NegativeBuckets: []float64{1, 2, 0},
			},
			exp: model.SampleHistogram{
				Count: 18,
				Sum:   18.4,
				Buckets: model.HistogramBuckets{
					{Boundaries: 1, Lower: -2, Upper: -1, Count: 2},
					{Boundaries: 1, Lower: -1, Upper: -0.5, Count: 1},
					{Boundaries: 0, Lower: 0.5, Upper: 1, Count: 1},
					{Boundaries: 0, Lower: 4, Upper: 8, Count: 1},
				},
			},
		},
	}

	for i, c := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			require.Equal(t, c.exp, *FromFloatHistogramToPromHistogram(&c.h))
		})
	}
}
