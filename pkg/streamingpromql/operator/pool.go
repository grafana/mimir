// SPDX-License-Identifier: AGPL-3.0-only

package operator

import (
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/util/pool"
)

const (
	maxExpectedPointsPerSeries  = 100_000 // There's not too much science behind this number: 100000 points allows for a point per minute for just under 70 days.
	pointsPerSeriesBucketFactor = 2.0

	maxExpectedSeriesPerResult  = 10_000_000 // Likewise, there's not too much science behind this number: this is the based on examining the largest queries seen at Grafana Labs.
	seriesPerResultBucketFactor = 2.0
)

var (
	fPointSlicePool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) []promql.FPoint {
		return make([]promql.FPoint, 0, size)
	})

	hPointSlicePool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, seriesPerResultBucketFactor, func(size int) []promql.HPoint {
		return make([]promql.HPoint, 0, size)
	})

	matrixPool = pool.NewBucketedPool(1, maxExpectedSeriesPerResult, seriesPerResultBucketFactor, func(size int) promql.Matrix {
		return make(promql.Matrix, 0, size)
	})

	vectorPool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) promql.Vector {
		return make(promql.Vector, 0, size)
	})

	seriesMetadataSlicePool = pool.NewBucketedPool(1, maxExpectedSeriesPerResult, seriesPerResultBucketFactor, func(size int) []SeriesMetadata {
		return make([]SeriesMetadata, 0, size)
	})

	floatSlicePool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(_ int) []float64 {
		// Don't allocate a new slice now - we'll allocate one in GetFloatSlice if we need it, so we can differentiate between reused and new slices.
		return nil
	})
	boolSlicePool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(_ int) []bool {
		// Don't allocate a new slice now - we'll allocate one in GetBoolSlice if we need it, so we can differentiate between reused and new slices.
		return nil
	})
)

func GetFPointSlice(size int) []promql.FPoint {
	return fPointSlicePool.Get(size)
}

func PutFPointSlice(s []promql.FPoint) {
	fPointSlicePool.Put(s)
}

func GetHPointSlice(size int) []promql.HPoint {
	return hPointSlicePool.Get(size)
}

func PutHPointSlice(s []promql.HPoint) {
	hPointSlicePool.Put(s)
}

func GetMatrix(size int) promql.Matrix {
	return matrixPool.Get(size)
}

func PutMatrix(m promql.Matrix) {
	matrixPool.Put(m)
}

func GetVector(size int) promql.Vector {
	return vectorPool.Get(size)
}

func PutVector(v promql.Vector) {
	vectorPool.Put(v)
}

func GetSeriesMetadataSlice(size int) []SeriesMetadata {
	return seriesMetadataSlicePool.Get(size)
}

func PutSeriesMetadataSlice(s []SeriesMetadata) {
	seriesMetadataSlicePool.Put(s)
}

func GetFloatSlice(size int) []float64 {
	s := floatSlicePool.Get(size)
	if s != nil {
		clear(s[:size])
		return s
	}

	return make([]float64, 0, size)
}

func PutFloatSlice(s []float64) {
	floatSlicePool.Put(s)
}

func GetBoolSlice(size int) []bool {
	s := boolSlicePool.Get(size)

	if s != nil {
		clear(s[:size])
		return s
	}

	return make([]bool, 0, size)
}

func PutBoolSlice(s []bool) {
	boolSlicePool.Put(s)
}
