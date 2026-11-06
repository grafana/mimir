// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/compat_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	stdlibjson "encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/test"
)

// This test verifies that jsoniter uses our custom method for marshalling.
// We do that by using "test sample" recognized by marshal function when in testing mode.
func TestJsoniterMarshalForFloatSample(t *testing.T) {
	testMarshalling(t, jsoniter.Marshal, "test sample")
}

func TestStdlibJsonMarshalForFloatSample(t *testing.T) {
	testMarshalling(t, stdlibjson.Marshal, "json: error calling MarshalJSON for type *mimirpb.FloatSample: test sample")
}

func testMarshalling(t *testing.T, marshalFn func(v interface{}) ([]byte, error), expectedError string) {
	isTesting = true
	defer func() { isTesting = false }()

	out, err := marshalFn(FloatSample{Value: 12345, TimestampMs: 98765})
	require.NoError(t, err)
	require.Equal(t, `[98.765,"12345"]`, string(out))

	_, err = marshalFn(FloatSample{Value: math.NaN(), TimestampMs: 0})
	require.EqualError(t, err, expectedError)

	// If not testing, we get normal output.
	isTesting = false
	out, err = marshalFn(FloatSample{Value: math.NaN(), TimestampMs: 0})
	require.NoError(t, err)
	require.Equal(t, `[0,"NaN"]`, string(out))
}

// This test verifies that jsoninter uses our custom method for unmarshalling FloatSample.
// As with Marshal, we rely on testing mode and special value that reports error.
func TestJsoniterUnmarshalForFloatSample(t *testing.T) {
	testUnmarshalling(t, jsoniter.Unmarshal, "test sample")
}

func TestStdlibJsonUnmarshalForFloatSample(t *testing.T) {
	testUnmarshalling(t, stdlibjson.Unmarshal, "test sample")
}

func testUnmarshalling(t *testing.T, unmarshalFn func(data []byte, v interface{}) error, expectedError string) {
	isTesting = true
	defer func() { isTesting = false }()

	sample := FloatSample{}

	err := unmarshalFn([]byte(`[98.765,"12345"]`), &sample)
	require.NoError(t, err)
	require.Equal(t, FloatSample{Value: 12345, TimestampMs: 98765}, sample)

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
		expected model.MetricType
	}{
		{
			desc:     "with a single-word metric",
			input:    COUNTER,
			expected: model.MetricTypeCounter,
		},
		{
			desc:     "with a two-word metric",
			input:    STATESET,
			expected: model.MetricTypeStateset,
		},
		{
			desc:     "with an unknown metric",
			input:    MetricMetadata_MetricType(100),
			expected: model.MetricTypeUnknown,
		},
	}

	for _, tt := range tc {
		t.Run(tt.desc, func(t *testing.T) {
			m := MetricMetadataMetricTypeToMetricType(tt.input)
			assert.Equal(t, tt.expected, m)
		})
	}
}

func TestUnsafeFromLabelAdaptersToLabels(t *testing.T) {
	input := []LabelAdapter{{Name: BorrowString("hello"), Value: BorrowString("world")}}
	expected := labels.FromStrings("hello", "world")
	actual := UnsafeFromLabelAdaptersToLabels(input)

	assert.Equal(t, expected, actual)
}

func TestFromLabelAdaptersToLabelsWithCopy(t *testing.T) {
	input := []LabelAdapter{{Name: BorrowString("hello"), Value: BorrowString("world")}}
	expected := labels.FromStrings("hello", "world")
	actual := FromLabelAdaptersToLabelsWithCopy(input)

	assert.Equal(t, expected, actual)

	// All strings must be copied.
	actualValue := actual.Get("hello")
	assert.NotSame(t, unsafe.StringData(input[0].Value.value), unsafe.StringData(actualValue))
}

func BenchmarkFromLabelAdaptersToLabelsWithCopy(b *testing.B) {
	input := []LabelAdapter{
		{Name: BorrowString("hello"), Value: BorrowString("world")},
		{Name: BorrowString("some label"), Value: BorrowString("and its value")},
		{Name: BorrowString("long long long long long label name"), Value: BorrowString("perhaps even longer label value, but who's counting anyway?")}}

	for i := 0; i < b.N; i++ {
		FromLabelAdaptersToLabelsWithCopy(input)
	}
}

func BenchmarkUnsafeFromLabelAdaptersToLabels(b *testing.B) {
	testCases := [][]LabelAdapter{
		{}, // Empty labels.
		{
			{Name: BorrowString("hello"), Value: BorrowString("world")},
		},
		{
			{Name: BorrowString("hello"), Value: BorrowString("world")},
			{Name: BorrowString("some label"), Value: BorrowString("and its value")},
		},
		{
			{Name: BorrowString("hello"), Value: BorrowString("world")},
			{Name: BorrowString("some label"), Value: BorrowString("and its value")},
			{Name: BorrowString("another label"), Value: BorrowString("another value")},
		},
		{
			{Name: BorrowString("hello"), Value: BorrowString("world")},
			{Name: BorrowString("some label"), Value: BorrowString("and its value")},
			{Name: BorrowString("another label"), Value: BorrowString("another value")},
			{Name: BorrowString("label 4"), Value: BorrowString("value 4")},
			{Name: BorrowString("label 5"), Value: BorrowString("value 5")},
			{Name: BorrowString("label 6"), Value: BorrowString("value 6")},
			{Name: BorrowString("label 7"), Value: BorrowString("value 7")},
			{Name: BorrowString("label 8"), Value: BorrowString("value 8")},
		},
	}

	for _, testCase := range testCases {
		b.Run(fmt.Sprintf("%v labels", len(testCase)), func(b *testing.B) {
			for b.Loop() {
				UnsafeFromLabelAdaptersToLabels(testCase)
			}
		})
	}
}

func TestFromFPointsToFloatSamples(t *testing.T) {
	input := []promql.FPoint{{T: 1, F: 2}, {T: 3, F: 4}}
	expected := []FloatSample{{TimestampMs: 1, Value: 2}, {TimestampMs: 3, Value: 4}}

	assert.Equal(t, expected, FromFPointsToFloatSamples(input))
}

func TestFromFloatSamplesToFPoints(t *testing.T) {
	input := []FloatSample{{TimestampMs: 1, Value: 2}, {TimestampMs: 3, Value: 4}}
	expected := []promql.FPoint{{T: 1, F: 2}, {T: 3, F: 4}}

	assert.Equal(t, expected, FromFloatSamplesToFPoints(input))
}

// Check that Prometheus FPoint and Mimir FloatSample types converted
// into each other with unsafe.Pointer are compatible
func TestPrometheusFPointInSyncWithMimirPbFloatSample(t *testing.T) {
	test.RequireSameShape(t, promql.FPoint{}, FloatSample{}, true, false)
}

func BenchmarkFromFPointsToFloatSamples(b *testing.B) {
	n := 100
	input := make([]promql.FPoint, n)
	for i := 0; i < n; i++ {
		input[i] = promql.FPoint{T: int64(i), F: float64(i)}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromFPointsToFloatSamples(input)
	}
}

func TestFromHPointsToHistograms(t *testing.T) {
	input := []promql.HPoint{{T: 3, H: test.GenerateTestFloatHistogram(0)}, {T: 5, H: test.GenerateTestFloatHistogram(1)}}
	expected := []FloatHistogramPair{
		{TimestampMs: 3, Histogram: FloatHistogramFromPrometheusModel(test.GenerateTestFloatHistogram(0))},
		{TimestampMs: 5, Histogram: FloatHistogramFromPrometheusModel(test.GenerateTestFloatHistogram(1))},
	}

	assert.Equal(t, expected, FromHPointsToHistograms(input))
}

func TestFromHistogramsToHPoints(t *testing.T) {
	input := []FloatHistogramPair{
		{TimestampMs: 3, Histogram: FloatHistogramFromPrometheusModel(test.GenerateTestFloatHistogram(0))},
		{TimestampMs: 5, Histogram: FloatHistogramFromPrometheusModel(test.GenerateTestFloatHistogram(1))},
	}
	expected := []promql.HPoint{{T: 3, H: test.GenerateTestFloatHistogram(0)}, {T: 5, H: test.GenerateTestFloatHistogram(1)}}

	assert.Equal(t, expected, FromHistogramsToHPoints(input))
}

// Check that Prometheus HPoint and Mimir FloatHistogramPair types converted
// into each other with unsafe.Pointer are compatible
func TestPrometheusHPointInSyncWithMimirPbFloatHistogramPair(t *testing.T) {
	test.RequireSameShape(t, promql.HPoint{}, FloatHistogramPair{}, true, false)
}

func BenchmarkFromHPointsToHistograms(b *testing.B) {
	n := 100
	input := make([]promql.HPoint, n)
	for i := 0; i < n; i++ {
		input[i] = promql.HPoint{T: int64(i), H: test.GenerateTestFloatHistogram(i)}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FromHPointsToHistograms(input)
	}
}

func TestPreallocatingMetric(t *testing.T) {
	t.Run("should be unmarshallable from the bytes of a default Metric", func(t *testing.T) {
		metric := Metric{
			Labels: []LabelAdapter{
				{Name: BorrowString("l1"), Value: BorrowString("v1")},
				{Name: BorrowString("l2"), Value: BorrowString("v2")},
				{Name: BorrowString("l3"), Value: BorrowString("v3")},
				{Name: BorrowString("l4"), Value: BorrowString("v4")},
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
				{Name: BorrowString("l1"), Value: BorrowString("v1")},
				{Name: BorrowString("l2"), Value: BorrowString("v2")},
				{Name: BorrowString("l3"), Value: BorrowString("v3")},
				{Name: BorrowString("l4"), Value: BorrowString("v4")},
				{Name: BorrowString("l5"), Value: BorrowString("v5")},
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
				{Name: BorrowString("l1"), Value: BorrowString("v1")},
				{Name: BorrowString("l2"), Value: BorrowString("v2")},
				{Name: BorrowString("l3"), Value: BorrowString("v3")},
				{Name: BorrowString("l4"), Value: BorrowString("v4")},
				{Name: BorrowString("l5"), Value: BorrowString("v5")},
			},
		}}

		metric := Metric{
			Labels: []LabelAdapter{
				{Name: BorrowString("l1"), Value: BorrowString("v1")},
				{Name: BorrowString("l2"), Value: BorrowString("v2")},
				{Name: BorrowString("l3"), Value: BorrowString("v3")},
				{Name: BorrowString("l4"), Value: BorrowString("v4")},
				{Name: BorrowString("l5"), Value: BorrowString("v5")},
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
					prompb.FromIntHistogram(1337, test.GenerateTestHistogram(0)),
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
			remoteWriteHistogram := prompb.FromIntHistogram(1337, test.tsdbHistogram)
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
			remoteWriteHistogram := prompb.FromFloatHistogram(1337, test.tsdbHistogram)
			data, err := remoteWriteHistogram.Marshal()
			assert.NoError(t, err, "marshal to protbuf")

			// Mimir
			receivedHistogram := &Histogram{}
			err = receivedHistogram.Unmarshal(data)
			assert.NoError(t, err, "unmarshal from protobuf")
			assert.True(t, receivedHistogram.IsFloatHistogram())
			assert.Equal(t, test.expectGauge, receivedHistogram.IsGauge())
			mimirHistogram := FromFloatHistogramProtoToFloatHistogram(receivedHistogram)

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
		Count:          &Histogram_CountInt{21},
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
	h2 := FromHistogramProtoToHistogram(&p)
	assert.Equal(t, h, h2)

	// Also check via JSON encode/decode
	promP := prompb.FromIntHistogram(ts, h)
	d, err := promP.Marshal()
	assert.NoError(t, err)
	p2 := Histogram{}
	assert.NoError(t, p2.Unmarshal(d))
	assert.Equal(t, expected, p2)
}

func BenchmarkFromHistogramToHistogramProto(b *testing.B) {
	n := 100
	input := make([]*histogram.Histogram, n)
	for i := 0; i < n; i++ {
		input[i] = test.GenerateTestHistogram(int(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, h := range input {
			p := FromHistogramToHistogramProto(int64(i), h)
			FromHistogramProtoToHistogram(&p)
		}
	}
}

func TestFromFloatHistogramToHistogramProto(t *testing.T) {
	var ts int64 = 1
	h := test.GenerateTestFloatHistogram(int(ts))
	h.CounterResetHint = histogram.NotCounterReset

	p := FromFloatHistogramToHistogramProto(ts, h)

	expected := Histogram{
		Count:          &Histogram_CountFloat{21},
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
	h2 := FromFloatHistogramProtoToFloatHistogram(&p)
	assert.Equal(t, h, h2)

	// Also check via JSON encode/decode
	promP := prompb.FromFloatHistogram(ts, h)
	d, err := promP.Marshal()
	assert.NoError(t, err)
	p2 := Histogram{}
	assert.NoError(t, p2.Unmarshal(d))
	assert.Equal(t, expected, p2)
}

func BenchmarkFromFloatHistogramToHistogramProto(b *testing.B) {
	n := 100
	input := make([]*histogram.FloatHistogram, n)
	for i := 0; i < n; i++ {
		input[i] = test.GenerateTestFloatHistogram(int(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, h := range input {
			p := FromFloatHistogramToHistogramProto(int64(i), h)
			FromFloatHistogramProtoToFloatHistogram(&p)
		}
	}
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

// Check that Prometheus and Mimir SampleHistogram types converted
// into each other with unsafe.Pointer are compatible
func TestPrometheusSampleHistogramInSyncWithMimirPbSampleHistogram(t *testing.T) {
	test.RequireSameShape(t, model.SampleHistogram{}, SampleHistogram{}, false, false)
}

// Check the memory layout required by the narrowly scoped unsafe label conversions.
func TestPrometheusLabelsInSyncWithMimirPbLabelAdapter(t *testing.T) {
	var adapter LabelAdapter
	var label labels.Label
	require.Equal(t, unsafe.Sizeof(label), unsafe.Sizeof(adapter))
	require.Equal(t, unsafe.Alignof(label), unsafe.Alignof(adapter))
	require.Equal(t, unsafe.Offsetof(label.Name), unsafe.Offsetof(adapter.Name))
	require.Equal(t, unsafe.Offsetof(label.Value), unsafe.Offsetof(adapter.Value))
	require.Equal(t, unsafe.Sizeof(label.Name), unsafe.Sizeof(adapter.Name))
	require.Equal(t, unsafe.Alignof(label.Name), unsafe.Alignof(adapter.Name))
}

// Check that Prometheus histogram.Span and MimirPb BucketSpan types
// are compatible, same as above.
func TestPrometheusHistogramSpanInSyncWithMimirPbBucketSpan(_ *testing.T) {
	_ = histogram.Span(BucketSpan{})
}

func TestCompareLabelAdapters(t *testing.T) {
	labels := []LabelAdapter{
		{Name: BorrowString("aaa"), Value: BorrowString("111")},
		{Name: BorrowString("bbb"), Value: BorrowString("222")},
	}

	tests := []struct {
		compared []LabelAdapter
		expected int
	}{
		{
			compared: []LabelAdapter{
				{Name: BorrowString("aaa"), Value: BorrowString("110")},
				{Name: BorrowString("bbb"), Value: BorrowString("222")},
			},
			expected: 1,
		},
		{
			compared: []LabelAdapter{
				{Name: BorrowString("aaa"), Value: BorrowString("111")},
				{Name: BorrowString("bbb"), Value: BorrowString("233")},
			},
			expected: -1,
		},
		{
			compared: []LabelAdapter{
				{Name: BorrowString("aaa"), Value: BorrowString("111")},
				{Name: BorrowString("bar"), Value: BorrowString("222")},
			},
			expected: 1,
		},
		{
			compared: []LabelAdapter{
				{Name: BorrowString("aaa"), Value: BorrowString("111")},
				{Name: BorrowString("bbc"), Value: BorrowString("222")},
			},
			expected: -1,
		},
		{
			compared: []LabelAdapter{
				{Name: BorrowString("aaa"), Value: BorrowString("111")},
				{Name: BorrowString("bb"), Value: BorrowString("222")},
			},
			expected: 1,
		},
		{
			compared: []LabelAdapter{
				{Name: BorrowString("aaa"), Value: BorrowString("111")},
				{Name: BorrowString("bbbb"), Value: BorrowString("222")},
			},
			expected: -1,
		},
		{
			compared: []LabelAdapter{
				{Name: BorrowString("aaa"), Value: BorrowString("111")},
			},
			expected: 1,
		},
		{
			compared: []LabelAdapter{
				{Name: BorrowString("aaa"), Value: BorrowString("111")},
				{Name: BorrowString("bbb"), Value: BorrowString("222")},
				{Name: BorrowString("ccc"), Value: BorrowString("333")},
				{Name: BorrowString("ddd"), Value: BorrowString("444")},
			},
			expected: -2,
		},
		{
			compared: []LabelAdapter{
				{Name: BorrowString("aaa"), Value: BorrowString("111")},
				{Name: BorrowString("bbb"), Value: BorrowString("222")},
			},
			expected: 0,
		},
		{
			compared: []LabelAdapter{},
			expected: 1,
		},
	}

	sign := func(a int) int {
		switch {
		case a < 0:
			return -1
		case a > 0:
			return 1
		}
		return 0
	}

	for i, test := range tests {
		got := CompareLabelAdapters(labels, test.compared)
		require.Equal(t, sign(test.expected), sign(got), "unexpected comparison result for test case %d", i)
		got = CompareLabelAdapters(test.compared, labels)
		require.Equal(t, -sign(test.expected), sign(got), "unexpected comparison result for reverse test case %d", i)
	}
}

// The main usecase for `LabelsToKeyString` is to generate hashKeys
// for maps. We are benchmarking that here.
func BenchmarkSeriesMap(b *testing.B) {
	benchmarkSeriesMap(100000, b)
}

func benchmarkSeriesMap(numSeries int, b *testing.B) {
	series := makeSeries(numSeries)
	sm := make(map[string]int, numSeries)

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i, s := range series {
			sm[FromLabelAdaptersToKeyString(s)] = i
		}

		for _, s := range series {
			_, ok := sm[FromLabelAdaptersToKeyString(s)]
			if !ok {
				b.Fatal("element missing")
			}
		}

		if len(sm) != numSeries {
			b.Fatal("the number of series expected:", numSeries, "got:", len(sm))
		}
	}
}

func makeSeries(n int) [][]LabelAdapter {
	series := make([][]LabelAdapter, 0, n)
	for i := 0; i < n; i++ {
		series = append(series, []LabelAdapter{
			{Name: BorrowString("label0"), Value: BorrowString("value0")},
			{Name: BorrowString("label1"), Value: BorrowString("value1")},
			{Name: BorrowString("label2"), Value: BorrowString("value2")},
			{Name: BorrowString("label3"), Value: BorrowString("value3")},
			{Name: BorrowString("label4"), Value: BorrowString("value4")},
			{Name: BorrowString("label5"), Value: BorrowString("value5")},
			{Name: BorrowString("label6"), Value: BorrowString("value6")},
			{Name: BorrowString("label7"), Value: BorrowString("value7")},
			{Name: BorrowString("label8"), Value: BorrowString("value8")},
			{Name: BorrowString("label9"), Value: BorrowString(strconv.Itoa(i))},
		})
	}

	return series
}

func TestNonStableHash(t *testing.T) {
	// serializedHash replicates the name\xffvalue\xff serialization NonStableHash
	// hashes over, so we can assert both the fast path and the >1KB streaming
	// fallback produce xxhash.Sum64 of exactly those bytes.
	serializedHash := func(ls []LabelAdapter) uint64 {
		var b []byte
		for _, l := range ls {
			b = append(b, l.Name.value...)
			b = append(b, '\xff')
			b = append(b, l.Value.value...)
			b = append(b, '\xff')
		}
		return xxhash.Sum64(b)
	}

	small := []LabelAdapter{{Name: BorrowString("__name__"), Value: BorrowString("metric")}, {Name: BorrowString("lbl"), Value: BorrowString("value")}}

	// big is sized relative to the fast-path cap so its serialization always
	// overflows into the streaming fallback branch — even if the cap changes —
	// rather than pinning a duplicated magic number.
	big := make([]LabelAdapter, 0)
	bigLen := 0
	for bigLen <= nonStableHashFastPathCap*2 {
		l := LabelAdapter{Name: BorrowString(fmt.Sprintf("label_%03d", len(big))), Value: BorrowString(strings.Repeat("v", 40))}
		big = append(big, l)
		bigLen += l.Name.Len() + l.Value.Len() + 2
	}
	require.Greater(t, bigLen, nonStableHashFastPathCap, "big must exceed the fast-path cap to cover the streaming fallback branch")

	t.Run("is deterministic", func(t *testing.T) {
		assert.Equal(t, NonStableHash(small), NonStableHash(small))
		assert.Equal(t, NonStableHash(big), NonStableHash(big))
	})

	t.Run("fast path equals xxhash of serialized labels", func(t *testing.T) {
		assert.Equal(t, serializedHash(small), NonStableHash(small))
	})

	t.Run("streaming fallback (>1KB) equals xxhash of serialized labels", func(t *testing.T) {
		// This pins that the two branches implement the identical hashing scheme.
		assert.Equal(t, serializedHash(big), NonStableHash(big))
	})

	t.Run("nil and empty hash equally", func(t *testing.T) {
		assert.Equal(t, NonStableHash(nil), NonStableHash([]LabelAdapter{}))
	})

	t.Run("distinct label sets hash differently", func(t *testing.T) {
		a := []LabelAdapter{{Name: BorrowString("__name__"), Value: BorrowString("metric")}, {Name: BorrowString("lbl"), Value: BorrowString("a")}}
		b := []LabelAdapter{{Name: BorrowString("__name__"), Value: BorrowString("metric")}, {Name: BorrowString("lbl"), Value: BorrowString("b")}}
		assert.NotEqual(t, NonStableHash(a), NonStableHash(b))
	})

	t.Run("is order sensitive", func(t *testing.T) {
		a := []LabelAdapter{{Name: BorrowString("a"), Value: BorrowString("1")}, {Name: BorrowString("b"), Value: BorrowString("2")}}
		b := []LabelAdapter{{Name: BorrowString("b"), Value: BorrowString("2")}, {Name: BorrowString("a"), Value: BorrowString("1")}}
		assert.NotEqual(t, NonStableHash(a), NonStableHash(b))
	})

	t.Run("separator prevents name/value boundary collisions", func(t *testing.T) {
		a := []LabelAdapter{{Name: BorrowString("ab"), Value: BorrowString("c")}}
		b := []LabelAdapter{{Name: BorrowString("a"), Value: BorrowString("bc")}}
		assert.NotEqual(t, NonStableHash(a), NonStableHash(b))
	})
}

// benchHashSink prevents the compiler from eliminating the hashed value.
var benchHashSink uint64

// BenchmarkNonStableHash compares hashing LabelAdapters directly against the
// previous UnsafeFromLabelAdaptersToLabels(...).Hash(), which allocates a labels.Labels
// on every call in the default (stringlabels) build.
func BenchmarkNonStableHash(b *testing.B) {
	ls := []LabelAdapter{
		{Name: BorrowString("__name__"), Value: BorrowString("http_requests_total")},
		{Name: BorrowString("handler"), Value: BorrowString("/api/v1/query_range")},
		{Name: BorrowString("instance"), Value: BorrowString("10.0.0.1:9090")},
		{Name: BorrowString("job"), Value: BorrowString("api-server")},
		{Name: BorrowString("method"), Value: BorrowString("GET")},
		{Name: BorrowString("status"), Value: BorrowString("200")},
	}

	b.Run("NonStableHash", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			benchHashSink = NonStableHash(ls)
		}
	})

	b.Run("FromLabelAdaptersToLabels.Hash", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			benchHashSink = UnsafeFromLabelAdaptersToLabels(ls).Hash()
		}
	})
}
