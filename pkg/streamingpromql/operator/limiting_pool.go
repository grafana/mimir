// SPDX-License-Identifier: AGPL-3.0-only

package operator

import (
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/pool"
)

const (
	maxExpectedPointsPerSeries  = 100_000 // There's not too much science behind this number: 100000 points allows for a point per minute for just under 70 days.
	pointsPerSeriesBucketFactor = 2.0

	// Treat a native histogram sample as equivalent to this many float samples when considering max in-memory samples limit.
	// TODO: should this be configurable?
	// TODO: what is a reasonable default value for this?
	nativeHistogramSampleCountFactor = 10
)

var (
	fPointSlicePool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) []promql.FPoint {
		return make([]promql.FPoint, 0, size)
	})

	hPointSlicePool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, seriesPerResultBucketFactor, func(size int) []promql.HPoint {
		return make([]promql.HPoint, 0, size)
	})

	// Overrides used only during tests.
	getFPointSliceForLimitingPool = fPointSlicePool.Get
	putFPointSliceForLimitingPool = fPointSlicePool.Put
	getHPointSliceForLimitingPool = hPointSlicePool.Get
	putHPointSliceForLimitingPool = hPointSlicePool.Put
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

// GetFPointSlice returns a slice of promql.FPoint of length 0 and capacity greater than or equal to size.
// If the capacity of the returned slice would cause the max in-memory samples limit to be exceeded, then an error is returned.
//
// Note that the capacity of the returned slice may be significantly larger than size, depending on the configuration of the underlying bucketed pool.
func (p *LimitingPool) GetFPointSlice(size int) ([]promql.FPoint, error) {
	// Check that the requested size fits under the limit.
	// If not, we can stop right now without taking a slice from the pool.
	if p.MaxInMemorySamples > 0 && p.CurrentInMemorySamples+size > p.MaxInMemorySamples {
		return nil, limiter.NewMaxInMemorySamplesPerQueryLimitError(uint64(p.MaxInMemorySamples))
	}

	s := getFPointSliceForLimitingPool(size)

	// We must use the capacity of the slice, not 'size', as there's no guarantee the slice will have size 'size' when it's returned to us in PutFPointSlice.
	size = cap(s)

	// Check that the capacity of the slice fits under the limit.
	// (There's no guarantee that the slice has capacity equal to the size we requested.)
	if p.MaxInMemorySamples > 0 && p.CurrentInMemorySamples+size > p.MaxInMemorySamples {
		putFPointSliceForLimitingPool(s)
		return nil, limiter.NewMaxInMemorySamplesPerQueryLimitError(uint64(p.MaxInMemorySamples))
	}

	p.CurrentInMemorySamples += size
	p.PeakInMemorySamples = max(p.PeakInMemorySamples, p.CurrentInMemorySamples)

	return s, nil
}

// PutFPointSlice returns a slice of promql.FPoint to the pool and updates the current number of in-memory samples.
func (p *LimitingPool) PutFPointSlice(s []promql.FPoint) {
	if s == nil {
		return
	}

	p.CurrentInMemorySamples -= cap(s)
	putFPointSliceForLimitingPool(s)
}

// GetHPointSlice returns a slice of promql.HPoint of length 0 and capacity greater than or equal to size.
//
// If the capacity of the returned slice would cause the max in-memory samples limit to be exceeded, then an error is returned.
// The capacity of the slice is converted to an estimated equivalent number of floating point samples.
//
// Note that the capacity of the returned slice may be significantly larger than size, depending on the configuration of the underlying bucketed pool.
func (p *LimitingPool) GetHPointSlice(size int) ([]promql.HPoint, error) {
	// Check that the requested size fits under the limit.
	// If not, we can stop right now without taking a slice from the pool.
	if p.MaxInMemorySamples > 0 && p.CurrentInMemorySamples+(size*nativeHistogramSampleCountFactor) > p.MaxInMemorySamples {
		return nil, limiter.NewMaxInMemorySamplesPerQueryLimitError(uint64(p.MaxInMemorySamples))
	}

	s := getHPointSliceForLimitingPool(size)

	// We must use the capacity of the slice, not 'size', as there's no guarantee the slice will have size 'size' when it's returned to us in PutFPointSlice.
	size = cap(s) * nativeHistogramSampleCountFactor

	// Check that the capacity of the slice fits under the limit.
	// (There's no guarantee that the slice has capacity equal to the size we requested.)
	if p.MaxInMemorySamples > 0 && p.CurrentInMemorySamples+size > p.MaxInMemorySamples {
		putHPointSliceForLimitingPool(s)
		return nil, limiter.NewMaxInMemorySamplesPerQueryLimitError(uint64(p.MaxInMemorySamples))
	}

	p.CurrentInMemorySamples += size
	p.PeakInMemorySamples = max(p.PeakInMemorySamples, p.CurrentInMemorySamples)

	return s, nil
}

// PutHPointSlice returns a slice of promql.HPoint to the pool and updates the current number of in-memory samples.
func (p *LimitingPool) PutHPointSlice(s []promql.HPoint) {
	if s == nil {
		return
	}

	p.CurrentInMemorySamples -= cap(s) * nativeHistogramSampleCountFactor
	putHPointSliceForLimitingPool(s)
}

// PutInstantVectorSeriesData is equivalent to calling PutFPointSlice(d.Floats) and PutHPointSlice(d.Histograms).
func (p *LimitingPool) PutInstantVectorSeriesData(d InstantVectorSeriesData) {
	p.PutFPointSlice(d.Floats)
	p.PutHPointSlice(d.Histograms)
}
