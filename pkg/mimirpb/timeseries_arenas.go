// SPDX-License-Identifier: AGPL-3.0-only

//go:build goexperiment.arenas

package mimirpb

import (
	"github.com/grafana/mimir/pkg/util/arena"
)

// AllocPreallocTimeseriesSlice allocates a slice of PreallocTimeseries using the provided arena.
func AllocPreallocTimeseriesSlice(a *arena.Arena) []PreallocTimeseries {
	return arena.MakeSlice[PreallocTimeseries](a, 0, minPreallocatedTimeseries)
}

// AllocTimeSeries allocates a pointer to a TimeSeries using the provided arena.
func AllocTimeSeries(a *arena.Arena) *TimeSeries {
	t := arena.New[TimeSeries](a)
	t.Labels = arena.MakeSlice[LabelAdapter](a, 0, minPreallocatedLabels)
	t.Samples = arena.MakeSlice[Sample](a, 0, minPreallocatedSamplesPerSeries)
	t.Exemplars = arena.MakeSlice[Exemplar](a, 0, minPreallocatedExemplarsPerSeries)
	return t
}

// allocYoloSlice allocates byte slices which are used to back the yoloStrings of this package.
func allocYoloSlice(a *arena.Arena) *[]byte {
	// The initial cap of 200 is an arbitrary number which has been chosen because the default
	// of 0 is guaranteed to be insufficient, so any number greater than 0 would be better.
	// 200 should be enough to back all the strings of one TimeSeries in many cases.
	val := arena.MakeSlice[byte](a, 0, 200)
	return &val
}

// ReuseSlice is a no-op in arena builds. Arena-allocated slices don't need explicit reuse.
func ReuseSlice(ts []PreallocTimeseries) {
	// No-op: arena-based allocation doesn't use pools
}

// ReuseSliceOnly is a no-op in arena builds. Arena-allocated slices don't need explicit reuse.
func ReuseSliceOnly(ts []PreallocTimeseries) {
	// No-op: arena-based allocation doesn't use pools
}

// ReuseTimeseries is a no-op in arena builds. Arena-allocated timeseries don't need explicit reuse.
func ReuseTimeseries(ts *TimeSeries) {
	// No-op: arena-based allocation doesn't use pools
}

// ReusePreallocTimeseries is a no-op in arena builds. Arena-allocated timeseries don't need explicit reuse.
func ReusePreallocTimeseries(ts *PreallocTimeseries) {
	// No-op: arena-based allocation doesn't use pools
}

// reuseYoloSlice is a no-op in arena builds. Arena-allocated slices don't need explicit reuse.
func reuseYoloSlice(val *[]byte) {
	// No-op: arena-based allocation doesn't use pools
}

// PreallocTimeseriesSliceFromPool is an alias for AllocPreallocTimeseriesSlice for backward compatibility.
// In arena builds, this allocates using regular heap allocation instead of an arena.
func PreallocTimeseriesSliceFromPool() []PreallocTimeseries {
	return make([]PreallocTimeseries, 0, minPreallocatedTimeseries)
}

// TimeseriesFromPool is an alias for AllocTimeSeries for backward compatibility.
// In arena builds, this allocates using regular heap allocation instead of an arena.
func TimeseriesFromPool() *TimeSeries {
	return &TimeSeries{
		Labels:     make([]LabelAdapter, 0, minPreallocatedLabels),
		Samples:    make([]Sample, 0, minPreallocatedSamplesPerSeries),
		Exemplars:  make([]Exemplar, 0, minPreallocatedExemplarsPerSeries),
		Histograms: nil,
	}
}

// DeepCopyTimeseries copies the timeseries of one PreallocTimeseries into another one.
// It copies all the properties, sub-properties and strings by value to ensure that the two timeseries are not sharing
// anything after the deep copying.
func DeepCopyTimeseries(a *arena.Arena, dst, src PreallocTimeseries, keepHistograms, keepExemplars bool) PreallocTimeseries {
	srcTs := src.TimeSeries
	dstTs := dst.TimeSeries

	// Prepare a buffer which is large enough to hold all the label names and values of src.
	requiredYoloSliceCap := countTotalLabelLen(srcTs, keepExemplars)
	buf := arena.MakeSlice[byte](a, 0, requiredYoloSliceCap)
	dst.yoloSlice = arena.New[[]byte](a)
	*dst.yoloSlice = buf

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
