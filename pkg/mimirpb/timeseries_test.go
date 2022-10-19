// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/timeseries_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLabelAdapter_Marshal(t *testing.T) {
	tests := []struct {
		bs *LabelAdapter
	}{
		{&LabelAdapter{Name: "foo", Value: "bar"}},
		{&LabelAdapter{Name: "very long label name", Value: "very long label value"}},
		{&LabelAdapter{Name: "", Value: "foo"}},
		{&LabelAdapter{}},
	}
	for _, tt := range tests {
		t.Run(tt.bs.Name, func(t *testing.T) {
			bytes, err := tt.bs.Marshal()
			require.NoError(t, err)
			lbs := &LabelAdapter{}
			require.NoError(t, lbs.Unmarshal(bytes))
			require.EqualValues(t, tt.bs, lbs)
		})
	}
}

func TestPreallocTimeseriesSliceFromPool(t *testing.T) {
	t.Run("new instance is provided when not available to reuse", func(t *testing.T) {
		first := PreallocTimeseriesSliceFromPool()
		second := PreallocTimeseriesSliceFromPool()

		assert.NotSame(t, first, second)
	})

	t.Run("instance is cleaned before reusing", func(t *testing.T) {
		slice := PreallocTimeseriesSliceFromPool()
		slice = append(slice, PreallocTimeseries{TimeSeries: &TimeSeries{}})
		ReuseSlice(slice)

		reused := PreallocTimeseriesSliceFromPool()
		assert.Len(t, reused, 0)
	})
}

func TestTimeseriesFromPool(t *testing.T) {
	t.Run("new instance is provided when not available to reuse", func(t *testing.T) {
		first := TimeseriesFromPool()
		second := TimeseriesFromPool()

		assert.NotSame(t, first, second)
	})

	t.Run("instance is cleaned before reusing", func(t *testing.T) {
		ts := TimeseriesFromPool()
		ts.Labels = []LabelAdapter{{Name: "foo", Value: "bar"}}
		ts.Samples = []Sample{{Value: 1, TimestampMs: 2}}
		ReuseTimeseries(ts)

		reused := TimeseriesFromPool()
		assert.Len(t, reused.Labels, 0)
		assert.Len(t, reused.Samples, 0)
	})
}

func TestCopyToYoloString(t *testing.T) {
	stringByteArray := func(val string) uintptr {
		return (*reflect.SliceHeader)(unsafe.Pointer(&val)).Data
	}

	testString := yoloString([]byte("testString"))
	testStringByteArray := stringByteArray(testString)

	// Verify that the unsafe copy is unsafe.
	unsafeCopy := testString
	unsafeCopyByteArray := stringByteArray(unsafeCopy)
	assert.Equal(t, testStringByteArray, unsafeCopyByteArray)

	// Create a safe copy by using the newBuf byte slice.
	newBuf := make([]byte, 0, len(testString))
	safeCopy, remainingBuf := copyToYoloString(newBuf, unsafeCopy)

	// Verify that the safe copy is safe by checking that the underlying byte arrays are different.
	safeCopyByteArray := stringByteArray(safeCopy)
	assert.NotEqual(t, testStringByteArray, safeCopyByteArray)

	// Verify that the remainingBuf has been used up completely.
	assert.Len(t, remainingBuf, 0)

	// Verify that the remainingBuf is using the same underlying byte array as safeCopy but advanced by the length.
	remainingBufArray := (*reflect.SliceHeader)(unsafe.Pointer(&remainingBuf)).Data
	assert.Equal(t, int(safeCopyByteArray)+len(newBuf), int(remainingBufArray))
}

func TestDeepCopyTimeseries(t *testing.T) {
	src := PreallocTimeseries{
		TimeSeries: &TimeSeries{
			Labels: []LabelAdapter{
				{Name: "sampleLabel1", Value: "sampleValue1"},
				{Name: "sampleLabel2", Value: "sampleValue2"},
			},
			Samples: []Sample{
				{Value: 1, TimestampMs: 2},
				{Value: 3, TimestampMs: 4},
			},
			Exemplars: []Exemplar{{
				Value:       1,
				TimestampMs: 2,
				Labels: []LabelAdapter{
					{Name: "exemplarLabel1", Value: "exemplarValue1"},
					{Name: "exemplarLabel2", Value: "exemplarValue2"},
				},
			}},
		},
	}
	dst := PreallocTimeseries{}
	dst = DeepCopyTimeseries(dst, src, true)

	// Check that the values in src and dst are the same.
	assert.Equal(t, src.TimeSeries, dst.TimeSeries)

	// Check that the TimeSeries in dst refers to a different address than the one in src.
	assert.NotSame(t, src.TimeSeries, dst.TimeSeries)

	// Check all the slices in the struct to ensure that
	// none of them refer to the same underlying array.
	assert.NotEqual(t,
		(*reflect.SliceHeader)(unsafe.Pointer(&src.Labels)).Data,
		(*reflect.SliceHeader)(unsafe.Pointer(&dst.Labels)).Data,
	)
	assert.NotEqual(t,
		(*reflect.SliceHeader)(unsafe.Pointer(&src.Samples)).Data,
		(*reflect.SliceHeader)(unsafe.Pointer(&dst.Samples)).Data,
	)
	assert.NotEqual(t,
		(*reflect.SliceHeader)(unsafe.Pointer(&src.Exemplars)).Data,
		(*reflect.SliceHeader)(unsafe.Pointer(&dst.Exemplars)).Data,
	)
	for exemplarIdx := range src.Exemplars {
		assert.NotEqual(t,
			(*reflect.SliceHeader)(unsafe.Pointer(&src.Exemplars[exemplarIdx].Labels)).Data,
			(*reflect.SliceHeader)(unsafe.Pointer(&dst.Exemplars[exemplarIdx].Labels)).Data,
		)
	}

	dst = PreallocTimeseries{}
	dst = DeepCopyTimeseries(dst, src, false)
	assert.NotNil(t, dst.Exemplars)
	assert.Len(t, dst.Exemplars, 0)
}

func TestDeepCopyTimeseriesExemplars(t *testing.T) {
	src := PreallocTimeseries{
		TimeSeries: &TimeSeries{
			Labels: []LabelAdapter{
				{Name: "sampleLabel1", Value: "sampleValue1"},
				{Name: "sampleLabel2", Value: "sampleValue2"},
			},
			Samples: []Sample{
				{Value: 1, TimestampMs: 2},
				{Value: 3, TimestampMs: 4},
			},
		},
	}

	for i := 0; i < 100; i++ {
		src.Exemplars = append(src.Exemplars, Exemplar{
			Value:       1,
			TimestampMs: 2,
			Labels: []LabelAdapter{
				{Name: "exemplarLabel1", Value: "exemplarValue1"},
				{Name: "exemplarLabel2", Value: "exemplarValue2"},
			},
		})
	}

	dst1 := PreallocTimeseries{}
	dst1 = DeepCopyTimeseries(dst1, src, false)

	dst2 := PreallocTimeseries{}
	dst2 = DeepCopyTimeseries(dst2, src, true)

	// dst1 should use much smaller buffer than dst2.
	assert.Less(t, cap(*dst1.yoloSlice), cap(*dst2.yoloSlice))
}
