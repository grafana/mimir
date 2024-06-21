//go:build !validatingpools

package mimirpb

import (
	"sync"

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

// PreallocTimeseriesSliceFromPool retrieves a slice of PreallocTimeseries from a sync.Pool.
// ReuseSlice should be called once done.
func PreallocTimeseriesSliceFromPool() []PreallocTimeseries {
	return preallocTimeseriesSlicePool.Get()
}
