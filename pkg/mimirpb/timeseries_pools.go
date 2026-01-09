// SPDX-License-Identifier: AGPL-3.0-only

//go:build !goexperiment.arenas

package mimirpb

import (
	"sync"

	"github.com/grafana/mimir/pkg/util/arena"
	"github.com/prometheus/prometheus/util/zeropool"
)

var (
	preallocTimeseriesSlicePool = zeropool.New(func() []PreallocTimeseries {
		return make([]PreallocTimeseries, 0, minPreallocatedTimeseries)
	})

	timeSeriesPool = sync.Pool{
		New: func() interface{} {
			return &TimeSeries{
				Labels:     make([]LabelAdapter, 0, minPreallocatedLabels),
				Samples:    make([]Sample, 0, minPreallocatedSamplesPerSeries),
				Exemplars:  make([]Exemplar, 0, minPreallocatedExemplarsPerSeries),
				Histograms: nil,
			}
		},
	}

	// yoloSlicePool is a pool of byte slices which are used to back the yoloStrings of this package.
	yoloSlicePool = sync.Pool{
		New: func() interface{} {
			// The initial cap of 200 is an arbitrary number which has been chosen because the default
			// of 0 is guaranteed to be insufficient, so any number greater than 0 would be better.
			// 200 should be enough to back all the strings of one TimeSeries in many cases.
			val := make([]byte, 0, 200)
			return &val
		},
	}
)

// AllocPreallocTimeseriesSlice retrieves a slice of PreallocTimeseries from a sync.Pool.
// The arena parameter is ignored in non-arena builds.
// ReuseSlice should be called once done.
func AllocPreallocTimeseriesSlice(a *arena.Arena) []PreallocTimeseries {
	return preallocTimeseriesSlicePool.Get()
}

// AllocTimeSeries retrieves a pointer to a TimeSeries from a sync.Pool.
// The arena parameter is ignored in non-arena builds.
// ReuseTimeseries should be called once done, unless ReuseSlice was called on the slice that contains this TimeSeries.
func AllocTimeSeries(a *arena.Arena) *TimeSeries {
	ts := timeSeriesPool.Get().(*TimeSeries)

	// Panic if the pool returns a TimeSeries that wasn't properly cleaned,
	// which is indicative of a hard bug that we want to catch as soon as possible.
	if len(ts.Labels) > 0 || len(ts.Samples) > 0 || len(ts.Histograms) > 0 || len(ts.Exemplars) > 0 || ts.CreatedTimestamp != 0 || ts.SkipUnmarshalingExemplars {
		panic("pool returned dirty TimeSeries: this indicates a bug where ReuseTimeseries was called on a TimeSeries still in use")
	}

	return ts
}

// allocYoloSlice allocates byte slices which are used to back the yoloStrings of this package.
// The arena parameter is ignored in non-arena builds.
func allocYoloSlice(a *arena.Arena) *[]byte {
	return yoloSlicePool.Get().(*[]byte)
}

// ReuseSlice puts the slice back into a sync.Pool for reuse.
func ReuseSlice(ts []PreallocTimeseries) {
	if cap(ts) == 0 {
		return
	}

	for i := range ts {
		ReusePreallocTimeseries(&ts[i])
	}

	ReuseSliceOnly(ts)
}

// ReuseSliceOnly reuses the slice of timeseries, but not its contents.
// Only use this if you have another means of reusing the individual timeseries contained within.
// Most times, you want to use ReuseSlice instead.
func ReuseSliceOnly(ts []PreallocTimeseries) {
	preallocTimeseriesSlicePool.Put(ts[:0])
}

// ReuseTimeseries puts the timeseries back into a sync.Pool for reuse.
func ReuseTimeseries(ts *TimeSeries) {
	// Name and Value may point into a large gRPC buffer, so clear the reference to allow GC
	for i := 0; i < len(ts.Labels); i++ {
		ts.Labels[i].Name = ""
		ts.Labels[i].Value = ""
	}

	// Retain the slices only if their capacity is not bigger than the desired max pre-allocated size.
	// This allows us to ensure we don't put very large slices back to the pool (e.g. a few requests with
	// a huge number of samples may cause in-use heap memory to significantly increase, because the slices
	// allocated by such poison requests would be reused by other requests with a normal number of samples).
	if cap(ts.Labels) > maxPreallocatedLabels {
		ts.Labels = nil
	} else {
		ts.Labels = ts.Labels[:0]
	}

	if cap(ts.Samples) > maxPreallocatedSamplesPerSeries {
		ts.Samples = nil
	} else {
		ts.Samples = ts.Samples[:0]
	}

	if cap(ts.Histograms) > maxPreallocatedHistogramsPerSeries {
		ts.Histograms = nil
	} else {
		ts.Histograms = ts.Histograms[:0]
	}

	ts.CreatedTimestamp = 0

	ClearExemplars(ts)
	timeSeriesPool.Put(ts)
}

// ReusePreallocTimeseries puts the timeseries and the yoloSlice back into their respective pools for re-use.
func ReusePreallocTimeseries(ts *PreallocTimeseries) {
	if ts.TimeSeries != nil {
		ReuseTimeseries(ts.TimeSeries)
	}

	if ts.yoloSlice != nil {
		reuseYoloSlice(ts.yoloSlice)
		ts.yoloSlice = nil
	}

	ts.marshalledData = nil
}

func reuseYoloSlice(val *[]byte) {
	*val = (*val)[:0]
	yoloSlicePool.Put(val)
}

// PreallocTimeseriesSliceFromPool is an alias for AllocPreallocTimeseriesSlice for backward compatibility.
func PreallocTimeseriesSliceFromPool() []PreallocTimeseries {
	return AllocPreallocTimeseriesSlice(nil)
}

// TimeseriesFromPool is an alias for AllocTimeSeries for backward compatibility.
func TimeseriesFromPool() *TimeSeries {
	return AllocTimeSeries(nil)
}

// DeepCopyTimeseries copies the timeseries of one PreallocTimeseries into another one.
// It copies all the properties, sub-properties and strings by value to ensure that the two timeseries are not sharing
// anything after the deep copying.
// The returned PreallocTimeseries has a yoloSlice property which should be returned to the yoloSlicePool on cleanup.
func DeepCopyTimeseries(a *arena.Arena, dst, src PreallocTimeseries, keepHistograms, keepExemplars bool) PreallocTimeseries {
	if dst.TimeSeries == nil {
		dst.TimeSeries = AllocTimeSeries(a)
	}

	srcTs := src.TimeSeries
	dstTs := dst.TimeSeries

	// Prepare a buffer which is large enough to hold all the label names and values of src.
	requiredYoloSliceCap := countTotalLabelLen(srcTs, keepExemplars)
	dst.yoloSlice = allocYoloSlice(a)
	buf := ensureCap(dst.yoloSlice, requiredYoloSliceCap)

	// Copy the time series labels by using the prepared buffer.
	dstTs.Labels, buf = copyToYoloLabels(buf, dstTs.Labels, srcTs.Labels)

	// Copy the samples.
	if cap(dstTs.Samples) < len(srcTs.Samples) {
		dstTs.Samples = make([]Sample, len(srcTs.Samples))
	} else {
		dstTs.Samples = dstTs.Samples[:len(srcTs.Samples)]
	}
	copy(dstTs.Samples, srcTs.Samples)

	// Copy the histograms.
	if keepHistograms {
		if cap(dstTs.Histograms) < len(srcTs.Histograms) {
			dstTs.Histograms = make([]Histogram, len(srcTs.Histograms))
		} else {
			dstTs.Histograms = dstTs.Histograms[:len(srcTs.Histograms)]
		}
		for i := range srcTs.Histograms {
			dstTs.Histograms[i] = copyHistogram(srcTs.Histograms[i])
		}
	} else {
		dstTs.Histograms = nil
	}

	// Prepare the slice of exemplars.
	if keepExemplars {
		if cap(dstTs.Exemplars) < len(srcTs.Exemplars) {
			dstTs.Exemplars = make([]Exemplar, len(srcTs.Exemplars))
		} else {
			dstTs.Exemplars = dstTs.Exemplars[:len(srcTs.Exemplars)]
		}

		for exemplarIdx := range srcTs.Exemplars {
			// Copy the exemplar labels by using the prepared buffer.
			dstTs.Exemplars[exemplarIdx].Labels, buf = copyToYoloLabels(buf, dstTs.Exemplars[exemplarIdx].Labels, srcTs.Exemplars[exemplarIdx].Labels)

			// Copy the other exemplar properties.
			dstTs.Exemplars[exemplarIdx].Value = srcTs.Exemplars[exemplarIdx].Value
			dstTs.Exemplars[exemplarIdx].TimestampMs = srcTs.Exemplars[exemplarIdx].TimestampMs
		}
	} else {
		dstTs.Exemplars = dstTs.Exemplars[:0]
	}

	return dst
}
