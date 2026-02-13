// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/timeseries.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

//go:build nopools

package mimirpb

// PreallocTimeseriesSliceFromPool returns a new [PreallocTimeseries] slice under the nopools build tag.
func PreallocTimeseriesSliceFromPool() []PreallocTimeseries {
	return newPreallocTimeseriesSlice()
}

// ReuseSliceOnly is a no-op under the nopools build tag.
func ReuseSliceOnly(ts []PreallocTimeseries) {}

// TimeseriesFromPool returns a new [TimeSeries] under the nopools build tag.
func TimeseriesFromPool() *TimeSeries {
	return newTimeSeries()
}

// ReuseTimeseries is a no-op under the nopools build tag.
func ReuseTimeseries(ts *TimeSeries) {}

func yoloSliceFromPool() *[]byte {
	val := newYoloSlice()
	return &val
}

func reuseYoloSlice(val *[]byte) {
	*val = (*val)[:0]
}
