// SPDX-License-Identifier: AGPL-3.0-only

package pooling

import (
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/pool"
)

const (
	maxExpectedPointsPerSeries  = 100_000 // There's not too much science behind this number: 100000 points allows for a point per minute for just under 70 days.
	pointsPerSeriesBucketFactor = 2.0

	// Treat a native histogram sample as equivalent to this many float samples when considering max in-memory samples limit.
	nativeHistogramSampleCountFactor = 10
)

type sizedPool[S any] interface {
	Get(size int) S
	Put(s S)
}

var (
	fPointSlicePool sizedPool[[]promql.FPoint] = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) []promql.FPoint {
		return make([]promql.FPoint, 0, size)
	})

	hPointSlicePool sizedPool[[]promql.HPoint] = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) []promql.HPoint {
		return make([]promql.HPoint, 0, size)
	})

	vectorPool sizedPool[promql.Vector] = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) promql.Vector {
		return make(promql.Vector, 0, size)
	})

	floatSlicePool sizedPool[[]float64] = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(_ int) []float64 {
		// Don't allocate a new slice now - we'll allocate one in GetFloatSlice if we need it, so we can differentiate between reused and new slices.
		return nil
	})

	boolSlicePool sizedPool[[]bool] = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(_ int) []bool {
		// Don't allocate a new slice now - we'll allocate one in GetBoolSlice if we need it, so we can differentiate between reused and new slices.
		return nil
	})
)

// LimitingPool manages sample slices for a single query evaluation, and applies any max in-memory samples limit.
//
// It also tracks the peak number of in-memory samples for use in query statistics.
//
// It is not safe to use this type from multiple goroutines simultaneously.
type LimitingPool struct {
	MaxInMemorySamples     int
	CurrentInMemorySamples int
	PeakInMemorySamples    int
}

func NewLimitingPool(maxInMemorySamples int) *LimitingPool {
	return &LimitingPool{
		MaxInMemorySamples: maxInMemorySamples,
	}
}

func getWithMultiplier[E any, S ~[]E](p *LimitingPool, pool sizedPool[S], size int, multiplier int) (S, error) {
	// Check that the requested size fits under the limit.
	// If not, we can stop right now without taking a slice from the pool.
	if p.MaxInMemorySamples > 0 && p.CurrentInMemorySamples+(size*multiplier) > p.MaxInMemorySamples {
		return nil, limiter.NewMaxInMemorySamplesPerQueryLimitError(uint64(p.MaxInMemorySamples))
	}

	s := pool.Get(size)

	// We must use the capacity of the slice, not 'size', as there's no guarantee the slice will have size 'size' when it's returned to us in putWithMultiplier.
	size = cap(s) * multiplier

	// Check that the capacity of the slice fits under the limit.
	// (There's no guarantee that the slice has capacity equal to the size we requested.)
	if p.MaxInMemorySamples > 0 && p.CurrentInMemorySamples+size > p.MaxInMemorySamples {
		pool.Put(s)
		return nil, limiter.NewMaxInMemorySamplesPerQueryLimitError(uint64(p.MaxInMemorySamples))
	}

	p.CurrentInMemorySamples += size
	p.PeakInMemorySamples = max(p.PeakInMemorySamples, p.CurrentInMemorySamples)

	return s, nil
}

func putWithMultiplier[E any, S ~[]E](p *LimitingPool, pool sizedPool[S], multiplier int, s S) {
	if s == nil {
		return
	}

	p.CurrentInMemorySamples -= cap(s) * multiplier
	pool.Put(s)
}

func getForIntrinsicTypeSlice[T any](p *LimitingPool, pool sizedPool[[]T], size int) ([]T, error) {
	// For slices of intrinsic types, we treat them as if they are equivalent to half a sample per element,
	// rounded down to the nearest whole number of samples (but always at least 1 sample, to avoid not accounting
	// for slices with just one element).
	//
	// Check that the requested size fits under the limit.
	// If not, we can stop right now without taking a slice from the pool.
	if p.MaxInMemorySamples > 0 && p.CurrentInMemorySamples+max(size/2, 1) > p.MaxInMemorySamples {
		return nil, limiter.NewMaxInMemorySamplesPerQueryLimitError(uint64(p.MaxInMemorySamples))
	}

	s := pool.Get(size)

	// We must use the capacity of the slice, not 'size', as there's no guarantee the slice will have size 'size' when it's returned to us in putWithMultiplier.
	size = max(cap(s)/2, 1)

	// Check that the capacity of the slice fits under the limit.
	// (There's no guarantee that the slice has capacity equal to the size we requested.)
	if p.MaxInMemorySamples > 0 && p.CurrentInMemorySamples+size > p.MaxInMemorySamples {
		pool.Put(s)
		return nil, limiter.NewMaxInMemorySamplesPerQueryLimitError(uint64(p.MaxInMemorySamples))
	}

	p.CurrentInMemorySamples += size
	p.PeakInMemorySamples = max(p.PeakInMemorySamples, p.CurrentInMemorySamples)

	return s, nil
}

func putForIntrinsicTypeSlice[T any](p *LimitingPool, pool sizedPool[[]T], s []T) {
	if s == nil {
		return
	}

	p.CurrentInMemorySamples -= max(cap(s)/2, 1)
	pool.Put(s)
}

// GetFPointSlice returns a slice of promql.FPoint of length 0 and capacity greater than or equal to size.
//
// If the capacity of the returned slice would cause the max in-memory samples limit to be exceeded, then an error is returned.
//
// Note that the capacity of the returned slice may be significantly larger than size, depending on the configuration of the underlying bucketed pool.
func (p *LimitingPool) GetFPointSlice(size int) ([]promql.FPoint, error) {
	return getWithMultiplier(p, fPointSlicePool, size, 1)
}

// PutFPointSlice returns a slice of promql.FPoint to the pool and updates the current number of in-memory samples.
func (p *LimitingPool) PutFPointSlice(s []promql.FPoint) {
	putWithMultiplier(p, fPointSlicePool, 1, s)
}

// GetHPointSlice returns a slice of promql.HPoint of length 0 and capacity greater than or equal to size.
//
// If the capacity of the returned slice would cause the max in-memory samples limit to be exceeded, then an error is returned.
// The capacity of the slice is converted to an estimated equivalent number of floating point samples.
//
// Note that the capacity of the returned slice may be significantly larger than size, depending on the configuration of the underlying bucketed pool.
func (p *LimitingPool) GetHPointSlice(size int) ([]promql.HPoint, error) {
	return getWithMultiplier(p, hPointSlicePool, size, nativeHistogramSampleCountFactor)
}

// PutHPointSlice returns a slice of promql.HPoint to the pool and updates the current number of in-memory samples.
func (p *LimitingPool) PutHPointSlice(s []promql.HPoint) {
	putWithMultiplier(p, hPointSlicePool, nativeHistogramSampleCountFactor, s)
}

// GetVector returns a promql.Vector of length 0 and capacity greater than or equal to size.
//
// If the capacity of the returned vector would cause the max in-memory samples limit to be exceeded, then an error is returned.
//
// Note that the capacity of the returned vector may be significantly larger than size, depending on the configuration of the underlying bucketed pool.
func (p *LimitingPool) GetVector(size int) (promql.Vector, error) {
	return getWithMultiplier(p, vectorPool, size, 1)
}

// PutVector returns a promql.Vector to the pool and updates the current number of in-memory samples.
func (p *LimitingPool) PutVector(v promql.Vector) {
	putWithMultiplier(p, vectorPool, 1, v)
}

// GetFloatSlice returns a slice of float64 of length 0 and capacity greater than or equal to size.
//
// If the capacity of the returned slice would cause the max in-memory samples limit to be exceeded, then an error is returned.
// The capacity of the slice is converted to an estimated equivalent number of floating point samples.
//
// Every element of the returned slice up to the requested size will have value 0.
//
// Note that the capacity of the returned slice may be significantly larger than size, depending on the configuration of the underlying bucketed pool.
func (p *LimitingPool) GetFloatSlice(size int) ([]float64, error) {
	return getForIntrinsicTypeSlice(p, floatSlicePool, size)
}

// PutFloatSlice returns a slice of float64 to the pool and updates the current number of in-memory samples.
func (p *LimitingPool) PutFloatSlice(s []float64) {
	putForIntrinsicTypeSlice(p, floatSlicePool, s)
}

// GetBoolSlice returns a slice of bool of length 0 and capacity greater than or equal to size.
//
// If the capacity of the returned slice would cause the max in-memory samples limit to be exceeded, then an error is returned.
// The capacity of the slice is converted to an estimated equivalent number of floating point samples.
//
// Every element of the returned slice up to the requested size will have value 0.
//
// Note that the capacity of the returned slice may be significantly larger than size, depending on the configuration of the underlying bucketed pool.
func (p *LimitingPool) GetBoolSlice(size int) ([]bool, error) {
	return getForIntrinsicTypeSlice(p, boolSlicePool, size)
}

// PutBoolSlice returns a slice of bool to the pool and updates the current number of in-memory samples.
func (p *LimitingPool) PutBoolSlice(s []bool) {
	putForIntrinsicTypeSlice(p, boolSlicePool, s)
}

// PutInstantVectorSeriesData is equivalent to calling PutFPointSlice(d.Floats) and PutHPointSlice(d.Histograms).
func (p *LimitingPool) PutInstantVectorSeriesData(d types.InstantVectorSeriesData) {
	p.PutFPointSlice(d.Floats)
	p.PutHPointSlice(d.Histograms)
}
