// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/timeseries_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	reflect "reflect"
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

func TestDeepCopyTimeseries(t *testing.T) {
	src := &TimeSeries{
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
	}
	dst := &TimeSeries{}
	DeepCopyTimeseries(dst, src)

	// Check that the values in ts1 and ts2 are the same.
	assert.Equal(t, src, dst)

	// Check that ts1 refers to a different address than t2.
	assert.NotSame(t, src, dst)

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
}
