// SPDX-License-Identifier: AGPL-3.0-only

package pooling

import (
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/pool"
)

const (
	maxExpectedPointsPerSeries  = 100_000 // There's not too much science behind this number: 100000 points allows for a point per minute for just under 70 days.
	pointsPerSeriesBucketFactor = 2.0

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
	fPointSlicePool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) []promql.FPoint {
		return make([]promql.FPoint, 0, size)
	})

	hPointSlicePool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) []promql.HPoint {
		return make([]promql.HPoint, 0, size)
	})

	vectorPool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) promql.Vector {
		return make(promql.Vector, 0, size)
	})

	float64SlicePool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) []float64 {
		return make([]float64, 0, size)
	})

	boolSlicePool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) []bool {
		return make([]bool, 0, size)
	})

	histogramSlicePool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) []*histogram.FloatHistogram {
		return make([]*histogram.FloatHistogram, 0, size)
	})
)

// LimitingPool manages sample slices for a single query evaluation, and applies any max in-memory bytes limit.
//
// It also tracks the peak number of in-memory bytes for use in query statistics.
//
// It is not safe to use this type from multiple goroutines simultaneously.
//
// LimitingPool only estimates the in-memory size of the slices it returns. For example, it ignores the overhead of slice headers,
// assumes all native histograms are the same size, and assumes all elements of a promql.Vector are float samples.
type LimitingPool struct {
	MaxEstimatedMemoryConsumptionBytes     uint64
	CurrentEstimatedMemoryConsumptionBytes uint64
	PeakEstimatedMemoryConsumptionBytes    uint64

	rejectionCount        prometheus.Counter
	haveRecordedRejection bool
}

func NewLimitingPool(maxEstimatedMemoryConsumptionBytes uint64, rejectionCount prometheus.Counter) *LimitingPool {
	return &LimitingPool{
		MaxEstimatedMemoryConsumptionBytes: maxEstimatedMemoryConsumptionBytes,

		rejectionCount: rejectionCount,
	}
}

func getWithElementSize[E any, S ~[]E](p *LimitingPool, pool *pool.BucketedPool[S, E], size int, elementSize uint64) (S, error) {
	// We don't bother checking the limit before we get the slice for a couple of reasons:
	// - we prefer to enforce the limit based on the capacity of the returned slices, not the requested size, to more accurately capture the true memory utilisation
	// - we expect that the vast majority of the time, the limit won't be hit, so the extra caution just slows things down
	// - we assume that allocating a single slice won't consume an enormous amount of memory and therefore risk this process OOMing.

	s := pool.Get(size)

	// We use the capacity of the slice, not 'size', for two reasons:
	// - it more accurately reflects the true memory utilisation, as BucketedPool will always round up to the next nearest bucket, to make reuse of slices easier
	// - there's no guarantee the slice will have size 'size' when it's returned to us in putWithElementSize, so using 'size' would make the accounting below impossible
	estimatedBytes := uint64(cap(s)) * elementSize

	if p.MaxEstimatedMemoryConsumptionBytes > 0 && p.CurrentEstimatedMemoryConsumptionBytes+estimatedBytes > p.MaxEstimatedMemoryConsumptionBytes {
		pool.Put(s)

		if !p.haveRecordedRejection {
			p.haveRecordedRejection = true
			p.rejectionCount.Inc()
		}

		return nil, limiter.NewMaxEstimatedMemoryConsumptionPerQueryLimitError(p.MaxEstimatedMemoryConsumptionBytes)
	}

	p.CurrentEstimatedMemoryConsumptionBytes += estimatedBytes
	p.PeakEstimatedMemoryConsumptionBytes = max(p.PeakEstimatedMemoryConsumptionBytes, p.CurrentEstimatedMemoryConsumptionBytes)

	return s, nil
}

func putWithElementSize[E any, S ~[]E](p *LimitingPool, pool *pool.BucketedPool[S, E], elementSize uint64, s S) {
	if s == nil {
		return
	}

	p.CurrentEstimatedMemoryConsumptionBytes -= uint64(cap(s)) * elementSize
	pool.Put(s)
}

// GetFPointSlice returns a slice of promql.FPoint of length 0 and capacity greater than or equal to size.
//
// If the capacity of the returned slice would cause the max memory consumption limit to be exceeded, then an error is returned.
//
// Note that the capacity of the returned slice may be significantly larger than size, depending on the configuration of the underlying bucketed pool.
func (p *LimitingPool) GetFPointSlice(size int) ([]promql.FPoint, error) {
	return getWithElementSize(p, fPointSlicePool, size, FPointSize)
}

// PutFPointSlice returns a slice of promql.FPoint to the pool and updates the current number of in-memory samples.
func (p *LimitingPool) PutFPointSlice(s []promql.FPoint) {
	putWithElementSize(p, fPointSlicePool, FPointSize, s)
}

// GetHPointSlice returns a slice of promql.HPoint of length 0 and capacity greater than or equal to size.
//
// If the capacity of the returned slice would cause the max memory consumption limit to be exceeded, then an error is returned.
//
// Note that the capacity of the returned slice may be significantly larger than size, depending on the configuration of the underlying bucketed pool.
func (p *LimitingPool) GetHPointSlice(size int) ([]promql.HPoint, error) {
	return getWithElementSize(p, hPointSlicePool, size, HPointSize)
}

// PutHPointSlice returns a slice of promql.HPoint to the pool and updates the current number of in-memory samples.
func (p *LimitingPool) PutHPointSlice(s []promql.HPoint) {
	putWithElementSize(p, hPointSlicePool, HPointSize, s)
}

// GetVector returns a promql.Vector of length 0 and capacity greater than or equal to size.
//
// If the capacity of the returned slice would cause the max memory consumption limit to be exceeded, then an error is returned.
//
// Note that the capacity of the returned vector may be significantly larger than size, depending on the configuration of the underlying bucketed pool.
func (p *LimitingPool) GetVector(size int) (promql.Vector, error) {
	return getWithElementSize(p, vectorPool, size, VectorSampleSize)
}

// PutVector returns a promql.Vector to the pool and updates the current number of in-memory samples.
func (p *LimitingPool) PutVector(v promql.Vector) {
	putWithElementSize(p, vectorPool, VectorSampleSize, v)
}

// GetFloatSlice returns a slice of float64 of length 0 and capacity greater than or equal to size.
//
// If the capacity of the returned slice would cause the max memory consumption limit to be exceeded, then an error is returned.
//
// Every element of the returned slice up to the requested size will have value 0.
//
// Note that the capacity of the returned slice may be significantly larger than size, depending on the configuration of the underlying bucketed pool.
func (p *LimitingPool) GetFloatSlice(size int) ([]float64, error) {
	s, err := getWithElementSize(p, float64SlicePool, size, Float64Size)
	if err != nil {
		return nil, err
	}

	// This is not necessary if we've just created a new slice, it'll already have all elements reset.
	// But we do it unconditionally for simplicity.
	clear(s[:size])

	return s, nil
}

// PutFloatSlice returns a slice of float64 to the pool and updates the current number of in-memory samples.
func (p *LimitingPool) PutFloatSlice(s []float64) {
	putWithElementSize(p, float64SlicePool, Float64Size, s)
}

// GetBoolSlice returns a slice of bool of length 0 and capacity greater than or equal to size.
//
// If the capacity of the returned slice would cause the max memory consumption limit to be exceeded, then an error is returned.
//
// Every element of the returned slice up to the requested size will have value false.
//
// Note that the capacity of the returned slice may be significantly larger than size, depending on the configuration of the underlying bucketed pool.
func (p *LimitingPool) GetBoolSlice(size int) ([]bool, error) {
	s, err := getWithElementSize(p, boolSlicePool, size, BoolSize)
	if err != nil {
		return nil, err
	}

	// This is not necessary if we've just created a new slice, it'll already have all elements reset.
	// But we do it unconditionally for simplicity.
	clear(s[:size])

	return s, nil
}

// PutBoolSlice returns a slice of bool to the pool and updates the current number of in-memory samples.
func (p *LimitingPool) PutBoolSlice(s []bool) {
	putWithElementSize(p, boolSlicePool, BoolSize, s)
}

// GetHistogramPointerSlice returns a slice of FloatHistogram of length 0 and capacity greater than or equal to size.
//
// If the capacity of the returned slice would cause the max memory consumption limit to be exceeded, then an error is returned.
//
// Every element of the returned slice up to the requested size will have an empty histogram.
//
// Note that the capacity of the returned slice may be significantly larger than size, depending on the configuration of the underlying bucketed pool.
func (p *LimitingPool) GetHistogramPointerSlice(size int) ([]*histogram.FloatHistogram, error) {
	s, err := getWithElementSize(p, histogramSlicePool, size, HistogramPointerSize)
	if err != nil {
		return nil, err
	}

	// This is not necessary if we've just created a new slice, it'll already have all elements reset.
	// But we do it unconditionally for simplicity.
	clear(s[:size])

	return s, nil
}

// PutHistogramPointerSlice returns a slice of FloatHistogram to the pool and updates the current number of in-memory samples.
func (p *LimitingPool) PutHistogramPointerSlice(s []*histogram.FloatHistogram) {
	putWithElementSize(p, histogramSlicePool, HistogramPointerSize, s)
}

// PutInstantVectorSeriesData is equivalent to calling PutFPointSlice(d.Floats) and PutHPointSlice(d.Histograms).
func (p *LimitingPool) PutInstantVectorSeriesData(d types.InstantVectorSeriesData) {
	p.PutFPointSlice(d.Floats)
	p.PutHPointSlice(d.Histograms)
}
