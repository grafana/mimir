// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/timeseries.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

//go:build !nopools

package mimirpb

import (
	"sync"

	"github.com/prometheus/prometheus/util/zeropool"
)

var (
	preallocTimeseriesSlicePool = zeropool.New(newPreallocTimeseriesSlice)

	timeSeriesPool = sync.Pool{
		New: func() any { return newTimeSeries() },
	}

	// yoloSlicePool is a pool of byte slices which are used to back the yoloStrings of this package.
	yoloSlicePool = sync.Pool{
		New: func() any {
			val := newYoloSlice()
			return &val
		},
	}
)

// PreallocTimeseriesSliceFromPool retrieves a slice of PreallocTimeseries from a sync.Pool.
// ReuseSlice should be called once done.
func PreallocTimeseriesSliceFromPool() []PreallocTimeseries {
	return preallocTimeseriesSlicePool.Get()
}

// ReuseSliceOnly reuses the slice of timeseries, but not its contents.
// Only use this if you have another means of reusing the individual timeseries contained within.
// Most times, you want to use ReuseSlice instead.
func ReuseSliceOnly(ts []PreallocTimeseries) {
	preallocTimeseriesSlicePool.Put(ts[:0])
}

// TimeseriesFromPool retrieves a pointer to a TimeSeries from a sync.Pool.
// ReuseTimeseries should be called once done, unless ReuseSlice was called on the slice that contains this TimeSeries.
func TimeseriesFromPool() *TimeSeries {
	ts := timeSeriesPool.Get().(*TimeSeries)

	// Panic if the pool returns a TimeSeries that wasn't properly cleaned,
	// which is indicative of a hard bug that we want to catch as soon as possible.
	if len(ts.Labels) > 0 || len(ts.Samples) > 0 || len(ts.Histograms) > 0 || len(ts.Exemplars) > 0 || ts.CreatedTimestamp != 0 || ts.SkipUnmarshalingExemplars {
		panic("pool returned dirty TimeSeries: this indicates a bug where ReuseTimeseries was called on a TimeSeries still in use")
	}

	return ts
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
	ts.SkipUnmarshalingExemplars = false

	ClearExemplars(ts)
	timeSeriesPool.Put(ts)
}

func yoloSliceFromPool() *[]byte {
	return yoloSlicePool.Get().(*[]byte)
}

func reuseYoloSlice(val *[]byte) {
	*val = (*val)[:0]
	yoloSlicePool.Put(val)
}
