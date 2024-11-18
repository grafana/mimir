// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"unsafe"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/util/pool"
)

const (
	maxExpectedPointsPerSeries  = 100_000 // There's not too much science behind this number: 100000 points allows for a point per minute for just under 70 days.
	pointsPerSeriesBucketFactor = 2

	// Treat a native histogram sample as equivalent to this many float samples when considering max in-memory bytes limit.
	// Keep in mind that float sample = timestamp + float value, so 5x this is equivalent to five timestamps and five floats.
	nativeHistogramSampleSizeFactor = 5

	FPointSize           = uint64(unsafe.Sizeof(promql.FPoint{}))
	HPointSize           = uint64(FPointSize * nativeHistogramSampleSizeFactor)
	VectorSampleSize     = uint64(unsafe.Sizeof(promql.Sample{})) // This assumes each sample is a float sample, not a histogram.
	Float64Size          = uint64(unsafe.Sizeof(float64(0)))
	BoolSize             = uint64(unsafe.Sizeof(false))
	HistogramPointerSize = uint64(unsafe.Sizeof((*histogram.FloatHistogram)(nil)))
)

var (
	FPointSlicePool = NewLimitingBucketedPool(
		pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) []promql.FPoint {
			return make([]promql.FPoint, 0, size)
		}),
		FPointSize,
		false,
	)

	HPointSlicePool = NewLimitingBucketedPool(
		pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) []promql.HPoint {
			return make([]promql.HPoint, 0, size)
		}),
		HPointSize,
		false,
	)

	VectorPool = NewLimitingBucketedPool(
		pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) promql.Vector {
			return make(promql.Vector, 0, size)
		}),
		VectorSampleSize,
		false,
	)

	Float64SlicePool = NewLimitingBucketedPool(
		pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) []float64 {
			return make([]float64, 0, size)
		}),
		Float64Size,
		true,
	)

	BoolSlicePool = NewLimitingBucketedPool(
		pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) []bool {
			return make([]bool, 0, size)
		}),
		BoolSize,
		true,
	)

	HistogramSlicePool = NewLimitingBucketedPool(
		pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) []*histogram.FloatHistogram {
			return make([]*histogram.FloatHistogram, 0, size)
		}),
		HistogramPointerSize,
		true,
	)
)

// LimitingBucketedPool pools slices across multiple query evaluations, and applies any max in-memory bytes limit.
//
// LimitingBucketedPool only estimates the in-memory size of the slices it returns. For example, it ignores the overhead of slice headers,
// assumes all native histograms are the same size, and assumes all elements of a promql.Vector are float samples.
type LimitingBucketedPool[S ~[]E, E any] struct {
	inner       *pool.BucketedPool[S, E]
	elementSize uint64
	clearOnGet  bool
}

func NewLimitingBucketedPool[S ~[]E, E any](inner *pool.BucketedPool[S, E], elementSize uint64, clearOnGet bool) *LimitingBucketedPool[S, E] {
	return &LimitingBucketedPool[S, E]{
		inner:       inner,
		elementSize: elementSize,
		clearOnGet:  clearOnGet,
	}
}

// Get returns a slice of E of length 0 and capacity greater than or equal to size.
//
// If the capacity of the returned slice would cause the max memory consumption limit to be exceeded, then an error is returned.
//
// Note that the capacity of the returned slice may be significantly larger than size, depending on the configuration of the underlying bucketed pool.
func (p *LimitingBucketedPool[S, E]) Get(size int, tracker *limiting.MemoryConsumptionTracker) (S, error) {
	// We don't bother checking the limit before we get the slice for a couple of reasons:
	// - we prefer to enforce the limit based on the capacity of the returned slices, not the requested size, to more accurately capture the true memory utilisation
	// - we expect that the vast majority of the time, the limit won't be hit, so the extra caution just slows things down
	// - we assume that allocating a single slice won't consume an enormous amount of memory and therefore risk this process OOMing.
	s := p.inner.Get(size)

	// We use the capacity of the slice, not 'size', for two reasons:
	// - it more accurately reflects the true memory utilisation, as BucketedPool will always round up to the next nearest bucket, to make reuse of slices easier
	// - there's no guarantee the slice will have size 'size' when it's returned to us in putWithElementSize, so using 'size' would make the accounting below impossible
	estimatedBytes := uint64(cap(s)) * p.elementSize

	if err := tracker.IncreaseMemoryConsumption(estimatedBytes); err != nil {
		p.inner.Put(s)
		return nil, err
	}

	if p.clearOnGet {
		clear(s[:size])
	}

	return s, nil
}

// Put returns a slice of E to the pool and updates the current memory consumption.
func (p *LimitingBucketedPool[S, E]) Put(s S, tracker *limiting.MemoryConsumptionTracker) {
	if s == nil {
		return
	}

	tracker.DecreaseMemoryConsumption(uint64(cap(s)) * p.elementSize)
	p.inner.Put(s)
}

// PutInstantVectorSeriesData is equivalent to calling FPointSlicePool.Put(d.Floats) and HPointSlicePool.Put(d.Histograms).
func PutInstantVectorSeriesData(d InstantVectorSeriesData, tracker *limiting.MemoryConsumptionTracker) {
	FPointSlicePool.Put(d.Floats, tracker)
	HPointSlicePool.Put(d.Histograms, tracker)
}
