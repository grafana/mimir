// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/timeseries_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	"crypto/rand"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
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
		// Ignore deprecation warning for now
		//nolint:staticcheck
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
	// Ignore deprecation warning for now
	//nolint:staticcheck
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
			Histograms: []Histogram{
				{
					Timestamp:      4*time.Minute.Milliseconds() - 1,
					Count:          &Histogram_CountInt{CountInt: 35},
					Sum:            108,
					ZeroCount:      &Histogram_ZeroCountInt{ZeroCountInt: 2},
					ZeroThreshold:  0.01,
					NegativeSpans:  []BucketSpan{{Offset: -1, Length: 1}, {Offset: -2, Length: 1}},
					NegativeDeltas: []int64{7, 3},
					PositiveSpans:  []BucketSpan{{Offset: 0, Length: 1}, {Offset: 2, Length: 1}},
					PositiveDeltas: []int64{2, 21},
					ResetHint:      Histogram_UNKNOWN,
				},
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
	dst = DeepCopyTimeseries(dst, src, true, true)

	// Check that the values in src and dst are the same.
	assert.Equal(t, src.TimeSeries, dst.TimeSeries)

	// Check that the TimeSeries in dst refers to a different address than the one in src.
	assert.NotSame(t, src.TimeSeries, dst.TimeSeries)

	// Check all the slices in the struct to ensure that
	// none of them refer to the same underlying array.
	assert.NotEqual(t,
		// Ignore deprecation warning for now
		//nolint:staticcheck
		(*reflect.SliceHeader)(unsafe.Pointer(&src.Labels)).Data,
		// Ignore deprecation warning for now
		//nolint:staticcheck
		(*reflect.SliceHeader)(unsafe.Pointer(&dst.Labels)).Data,
	)
	assert.NotEqual(t,
		// Ignore deprecation warning for now
		//nolint:staticcheck
		(*reflect.SliceHeader)(unsafe.Pointer(&src.Samples)).Data,
		// Ignore deprecation warning for now
		//nolint:staticcheck
		(*reflect.SliceHeader)(unsafe.Pointer(&dst.Samples)).Data,
	)
	assert.NotEqual(t,
		// Ignore deprecation warning for now
		//nolint:staticcheck
		(*reflect.SliceHeader)(unsafe.Pointer(&src.Histograms)).Data,
		// Ignore deprecation warning for now
		//nolint:staticcheck
		(*reflect.SliceHeader)(unsafe.Pointer(&dst.Histograms)).Data,
	)
	assert.NotEqual(t,
		// Ignore deprecation warning for now
		//nolint:staticcheck
		(*reflect.SliceHeader)(unsafe.Pointer(&src.Exemplars)).Data,
		// Ignore deprecation warning for now
		//nolint:staticcheck
		(*reflect.SliceHeader)(unsafe.Pointer(&dst.Exemplars)).Data,
	)
	for histogramIdx := range src.Histograms {
		assert.NotEqual(t,
			// Ignore deprecation warning for now
			//nolint:staticcheck
			(*reflect.SliceHeader)(unsafe.Pointer(&src.Histograms[histogramIdx].NegativeSpans)).Data,
			// Ignore deprecation warning for now
			//nolint:staticcheck
			(*reflect.SliceHeader)(unsafe.Pointer(&dst.Histograms[histogramIdx].NegativeSpans)).Data,
		)
		assert.NotEqual(t,
			// Ignore deprecation warning for now
			//nolint:staticcheck
			(*reflect.SliceHeader)(unsafe.Pointer(&src.Histograms[histogramIdx].NegativeDeltas)).Data,
			// Ignore deprecation warning for now
			//nolint:staticcheck
			(*reflect.SliceHeader)(unsafe.Pointer(&dst.Histograms[histogramIdx].NegativeDeltas)).Data,
		)
		assert.NotEqual(t,
			// Ignore deprecation warning for now
			//nolint:staticcheck
			(*reflect.SliceHeader)(unsafe.Pointer(&src.Histograms[histogramIdx].PositiveSpans)).Data,
			// Ignore deprecation warning for now
			//nolint:staticcheck
			(*reflect.SliceHeader)(unsafe.Pointer(&dst.Histograms[histogramIdx].PositiveSpans)).Data,
		)
		assert.NotEqual(t,
			// Ignore deprecation warning for now
			//nolint:staticcheck
			(*reflect.SliceHeader)(unsafe.Pointer(&src.Histograms[histogramIdx].PositiveDeltas)).Data,
			// Ignore deprecation warning for now
			//nolint:staticcheck
			(*reflect.SliceHeader)(unsafe.Pointer(&dst.Histograms[histogramIdx].PositiveDeltas)).Data,
		)
	}

	for exemplarIdx := range src.Exemplars {
		assert.NotEqual(t,
			// Ignore deprecation warning for now
			//nolint:staticcheck
			(*reflect.SliceHeader)(unsafe.Pointer(&src.Exemplars[exemplarIdx].Labels)).Data,
			// Ignore deprecation warning for now
			//nolint:staticcheck
			(*reflect.SliceHeader)(unsafe.Pointer(&dst.Exemplars[exemplarIdx].Labels)).Data,
		)
	}

	dst = PreallocTimeseries{}
	dst = DeepCopyTimeseries(dst, src, false, false)
	assert.NotNil(t, dst.Exemplars)
	assert.Len(t, dst.Exemplars, 0)
	assert.Len(t, dst.Histograms, 0)
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
	dst1 = DeepCopyTimeseries(dst1, src, false, false)

	dst2 := PreallocTimeseries{}
	dst2 = DeepCopyTimeseries(dst2, src, false, true)

	// dst1 should use much smaller buffer than dst2.
	assert.Less(t, cap(*dst1.yoloSlice), cap(*dst2.yoloSlice))
}

func TestPreallocTimeseries_Unmarshal(t *testing.T) {
	defer func() {
		TimeseriesUnmarshalCachingEnabled = true
	}()

	// Prepare message
	msg := PreallocTimeseries{}
	{
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

		data, err := src.Marshal()
		require.NoError(t, err)

		TimeseriesUnmarshalCachingEnabled = false

		require.NoError(t, msg.Unmarshal(data))
		require.True(t, src.Equal(msg.TimeSeries))
		require.Nil(t, msg.marshalledData)

		TimeseriesUnmarshalCachingEnabled = true

		require.NoError(t, msg.Unmarshal(data))
		require.True(t, src.Equal(msg.TimeSeries))
		require.NotNil(t, msg.marshalledData)
	}

	correctMarshaledData := make([]byte, len(msg.marshalledData))
	copy(correctMarshaledData, msg.marshalledData)

	randomData := make([]byte, 100)
	_, err := rand.Read(randomData)
	require.NoError(t, err)

	// Set cached version to random bytes. We make a new slice, because labels in TimeSeries use the original byte slice.
	msg.marshalledData = make([]byte, len(randomData))
	copy(msg.marshalledData, randomData)

	t.Run("message with cached marshalled version: Size returns length of cached data", func(t *testing.T) {
		require.Equal(t, len(randomData), msg.Size())
	})

	t.Run("message with cached marshalled version: Marshal returns cached data", func(t *testing.T) {
		out, err := msg.Marshal()
		require.NoError(t, err)
		require.Equal(t, randomData, out)
	})

	t.Run("message with cached marshalled version: MarshalTo returns cached data", func(t *testing.T) {
		out := make([]byte, 2*msg.Size())
		n, err := msg.MarshalTo(out)
		require.NoError(t, err)
		require.Equal(t, n, msg.Size())
		require.Equal(t, randomData, out[:msg.Size()])
	})

	t.Run("message with cached marshalled version: MarshalToSizedBuffer returns cached data", func(t *testing.T) {
		out := make([]byte, msg.Size())
		n, err := msg.MarshalToSizedBuffer(out)
		require.NoError(t, err)
		require.Equal(t, n, len(out))
		require.Equal(t, randomData, out)
	})

	msg.clearUnmarshalData()
	require.Nil(t, msg.marshalledData)

	t.Run("message without cached marshalled version: Marshal returns correct data", func(t *testing.T) {
		out, err := msg.Marshal()
		require.NoError(t, err)
		require.Equal(t, correctMarshaledData, out)
	})

	t.Run("message without cached marshalled version: MarshalTo returns correct data", func(t *testing.T) {
		out := make([]byte, 2*msg.Size())
		n, err := msg.MarshalTo(out)
		require.NoError(t, err)
		require.Equal(t, n, msg.Size())
		require.Equal(t, correctMarshaledData, out[:msg.Size()])
	})

	t.Run("message with cached marshalled version: MarshalToSizedBuffer returns correct data", func(t *testing.T) {
		out := make([]byte, msg.Size())
		n, err := msg.MarshalToSizedBuffer(out)
		require.NoError(t, err)
		require.Equal(t, n, len(out))
		require.Equal(t, correctMarshaledData, out[:msg.Size()])
	})
}

func TestPreallocTimeseries_SortLabelsIfNeeded(t *testing.T) {
	t.Run("sorted", func(t *testing.T) {
		sorted := PreallocTimeseries{
			TimeSeries: &TimeSeries{
				Labels: []LabelAdapter{
					{Name: "__name__", Value: "foo"},
					{Name: "bar", Value: "baz"},
					{Name: "cluster", Value: "cluster"},
					{Name: "sample", Value: "1"},
				},
			},
			marshalledData: []byte{1, 2, 3},
		}
		// no allocations if input is already sorted
		require.Equal(t, 0.0, testing.AllocsPerRun(100, func() {
			sorted.SortLabelsIfNeeded()
		}))
		require.NotNil(t, sorted.marshalledData)
	})

	t.Run("unsorted", func(t *testing.T) {
		unsorted := PreallocTimeseries{
			TimeSeries: &TimeSeries{
				Labels: []LabelAdapter{
					{Name: "__name__", Value: "foo"},
					{Name: "sample", Value: "1"},
					{Name: "cluster", Value: "cluster"},
					{Name: "bar", Value: "baz"},
				},
			},
			marshalledData: []byte{1, 2, 3},
		}

		unsorted.SortLabelsIfNeeded()

		require.True(t, sort.SliceIsSorted(unsorted.Labels, func(i, j int) bool {
			return unsorted.Labels[i].Name < unsorted.Labels[j].Name
		}))
		require.Nil(t, unsorted.marshalledData)
	})
}

func TestPreallocTimeseries_RemoveLabel(t *testing.T) {
	t.Run("with label", func(t *testing.T) {
		p := PreallocTimeseries{
			TimeSeries: &TimeSeries{
				Labels: []LabelAdapter{
					{Name: "__name__", Value: "foo"},
					{Name: "bar", Value: "baz"},
				},
			},
			marshalledData: []byte{1, 2, 3},
		}
		p.RemoveLabel("bar")

		require.Equal(t, []LabelAdapter{{Name: "__name__", Value: "foo"}}, p.Labels)
		require.Nil(t, p.marshalledData)
	})

	t.Run("with no matching label", func(t *testing.T) {
		p := PreallocTimeseries{
			TimeSeries: &TimeSeries{
				Labels: []LabelAdapter{
					{Name: "__name__", Value: "foo"},
					{Name: "bar", Value: "baz"},
				},
			},
			marshalledData: []byte{1, 2, 3},
		}
		p.RemoveLabel("foo")

		require.Equal(t, []LabelAdapter{{Name: "__name__", Value: "foo"}, {Name: "bar", Value: "baz"}}, p.Labels)
		require.NotNil(t, p.marshalledData)
	})
}

func TestPreallocTimeseries_RemoveEmptyLabelValues(t *testing.T) {
	t.Run("with empty labels", func(t *testing.T) {
		p := PreallocTimeseries{
			TimeSeries: &TimeSeries{
				Labels: []LabelAdapter{
					{Name: "__name__", Value: "foo"},
					{Name: "empty1", Value: ""},
					{Name: "bar", Value: "baz"},
					{Name: "empty2", Value: ""},
				},
			},
			marshalledData: []byte{1, 2, 3},
		}
		p.RemoveEmptyLabelValues()

		require.Equal(t, []LabelAdapter{{Name: "__name__", Value: "foo"}, {Name: "bar", Value: "baz"}}, p.Labels)
		require.Nil(t, p.marshalledData)
	})

	t.Run("without empty labels", func(t *testing.T) {
		p := PreallocTimeseries{
			TimeSeries: &TimeSeries{
				Labels: []LabelAdapter{
					{Name: "__name__", Value: "foo"},
					{Name: "bar", Value: "baz"},
				},
			},
			marshalledData: []byte{1, 2, 3},
		}
		p.RemoveLabel("foo")

		require.Equal(t, []LabelAdapter{{Name: "__name__", Value: "foo"}, {Name: "bar", Value: "baz"}}, p.Labels)
		require.NotNil(t, p.marshalledData)
	})
}

func TestPreallocTimeseries_SetLabels(t *testing.T) {
	p := PreallocTimeseries{
		TimeSeries: &TimeSeries{
			Labels: []LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
			},
		},
		marshalledData: []byte{1, 2, 3},
	}
	expected := []LabelAdapter{{Name: "__name__", Value: "hello"}, {Name: "lbl", Value: "world"}}
	p.SetLabels(expected)

	require.Equal(t, expected, p.Labels)
	require.Nil(t, p.marshalledData)
}

func TestPreallocTimeseries_ResizeExemplars(t *testing.T) {
	t.Run("should resize Exemplars when size is bigger than target size", func(t *testing.T) {
		p := PreallocTimeseries{
			TimeSeries: &TimeSeries{
				Exemplars: make([]Exemplar, 10),
			},
			marshalledData: []byte{1, 2, 3},
		}

		for i := range p.Exemplars {
			p.Exemplars[i] = Exemplar{Labels: []LabelAdapter{{Name: "trace", Value: "1"}, {Name: "service", Value: "A"}}, Value: 1, TimestampMs: int64(i)}
		}
		p.ResizeExemplars(5)
		require.Len(t, p.Exemplars, 5)
		require.Nil(t, p.marshalledData)
	})
}

func BenchmarkPreallocTimeseries_SortLabelsIfNeeded(b *testing.B) {
	bcs := []int{10, 40, 100}

	for _, lbCount := range bcs {
		b.Run(fmt.Sprintf("num_labels=%d", lbCount), func(b *testing.B) {
			// Generate labels set in reverse order for worst case.
			unorderedLabels := make([]LabelAdapter, 0, lbCount)
			for i := 0; i < lbCount; i++ {
				lbName := fmt.Sprintf("lbl_%d", lbCount-i)
				lbValue := fmt.Sprintf("val_%d", lbCount-i)
				unorderedLabels = append(unorderedLabels, LabelAdapter{Name: lbName, Value: lbValue})
			}

			b.Run("unordered", benchmarkSortLabelsIfNeeded(unorderedLabels))

			slices.SortFunc(unorderedLabels, func(a, b LabelAdapter) int {
				switch {
				case a.Name < b.Name:
					return -1
				case a.Name > b.Name:
					return 1
				default:
					return 0
				}
			})
			b.Run("ordered", benchmarkSortLabelsIfNeeded(unorderedLabels))
		})
	}
}

func benchmarkSortLabelsIfNeeded(inputLabels []LabelAdapter) func(b *testing.B) {
	return func(b *testing.B) {
		// Copy unordered labels set for each benchmark iteration.
		benchmarkUnorderedLabels := make([][]LabelAdapter, b.N)
		for i := 0; i < b.N; i++ {
			benchmarkLabels := make([]LabelAdapter, len(inputLabels))
			copy(benchmarkLabels, inputLabels)
			benchmarkUnorderedLabels[i] = benchmarkLabels
		}

		p := PreallocTimeseries{
			TimeSeries: &TimeSeries{},
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			p.SetLabels(benchmarkUnorderedLabels[i])
			p.SortLabelsIfNeeded()
		}
	}
}

func TestClearExemplars(t *testing.T) {
	t.Run("should reset TimeSeries.Exemplars keeping the slices if there are <= 10 entries", func(t *testing.T) {
		ts := &TimeSeries{Exemplars: []Exemplar{
			{Labels: []LabelAdapter{{Name: "trace", Value: "1"}, {Name: "service", Value: "A"}}, Value: 1, TimestampMs: 2},
			{Labels: []LabelAdapter{{Name: "trace", Value: "2"}, {Name: "service", Value: "B"}}, Value: 2, TimestampMs: 3},
		}}

		ClearExemplars(ts)

		assert.Equal(t, &TimeSeries{Exemplars: []Exemplar{}}, ts)
		assert.Equal(t, 2, cap(ts.Exemplars))

		ts.Exemplars = ts.Exemplars[:2]
		require.Len(t, ts.Exemplars, 2)
		assert.Equal(t, 2, cap(ts.Exemplars[0].Labels))
		assert.Equal(t, 2, cap(ts.Exemplars[1].Labels))
	})

	t.Run("should reset TimeSeries.Exemplars releasing the slices if there are > 10 entries", func(t *testing.T) {
		ts := &TimeSeries{Exemplars: make([]Exemplar, 11)}
		for i := range ts.Exemplars {
			ts.Exemplars[i] = Exemplar{Labels: []LabelAdapter{{Name: "trace", Value: "1"}, {Name: "service", Value: "A"}}, Value: 1, TimestampMs: 2}
		}

		ClearExemplars(ts)

		assert.Equal(t, &TimeSeries{Exemplars: nil}, ts)
		assert.Equal(t, 0, cap(ts.Exemplars))
	})
}

func TestSortExemplars(t *testing.T) {
	t.Run("should sort TimeSeries.Exemplars in order", func(t *testing.T) {
		p := PreallocTimeseries{
			TimeSeries: &TimeSeries{
				Exemplars: []Exemplar{
					{Labels: []LabelAdapter{{Name: "trace", Value: "1"}, {Name: "service", Value: "A"}}, Value: 1, TimestampMs: 3},
					{Labels: []LabelAdapter{{Name: "trace", Value: "2"}, {Name: "service", Value: "B"}}, Value: 2, TimestampMs: 2},
				},
			},
			marshalledData: []byte{1, 2, 3},
		}

		p.SortExemplars()
		require.Len(t, p.Exemplars, 2)
		assert.Equal(t, int64(2), p.Exemplars[0].TimestampMs)
		assert.Equal(t, int64(3), p.Exemplars[1].TimestampMs)
		assert.Nil(t, p.marshalledData)
	})
}
