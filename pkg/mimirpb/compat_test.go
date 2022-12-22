// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/compat_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	"encoding/json"
	stdlibjson "encoding/json"
	"math"
	"testing"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	testUnmarshalling(t, json.Unmarshal, "test sample")
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
