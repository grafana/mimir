// SPDX-License-Identifier: AGPL-3.0-only

package operator

import (
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/util/pool"
)

const (
	maxExpectedPointsPerSeries = 100_000 // There's not too much science behind this number: 100000 points allows for a point per minute for just under 70 days.

	maxExpectedSeriesPerResult = 10_000_000 // Likewise, there's not too much science behind this number: this is the based on examining the largest queries seen at Grafana Labs.
)

var (
	fPointSlicePool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, 10, func(size int) []promql.FPoint {
		return make([]promql.FPoint, 0, size)
	})

	matrixPool = pool.NewBucketedPool(1, maxExpectedSeriesPerResult, 10, func(size int) promql.Matrix {
		return make(promql.Matrix, 0, size)
	})

	vectorPool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, 10, func(size int) promql.Vector {
		return make(promql.Vector, 0, size)
	})

	seriesMetadataSlicePool = pool.NewBucketedPool(1, maxExpectedSeriesPerResult, 10, func(size int) []SeriesMetadata {
		return make([]SeriesMetadata, 0, size)
	})

	floatSlicePool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, 10, func(_ int) []float64 {
		// Don't allocate a new slice now - we'll allocate one in GetFloatSlice if we need it, so we can differentiate between reused and new slices.
		return nil
	})
	boolSlicePool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, 10, func(_ int) []bool {
		// Don't allocate a new slice now - we'll allocate one in GetBoolSlice if we need it, so we can differentiate between reused and new slices.
		return nil
	})
)

func GetFPointSlice(size int) []promql.FPoint {
	return fPointSlicePool.Get(size)
}

func PutFPointSlice(s []promql.FPoint) {
	if s != nil {
		fPointSlicePool.Put(s)
	}
}

func GetMatrix(size int) promql.Matrix {
	return matrixPool.Get(size)
}

func PutMatrix(m promql.Matrix) {
	if m != nil {
		matrixPool.Put(m)
	}
}

func GetVector(size int) promql.Vector {
	return vectorPool.Get(size)
}

func PutVector(v promql.Vector) {
	if v != nil {
		vectorPool.Put(v)
	}
}

func GetSeriesMetadataSlice(size int) []SeriesMetadata {
	return seriesMetadataSlicePool.Get(size)
}

func PutSeriesMetadataSlice(s []SeriesMetadata) {
	if s != nil {
		seriesMetadataSlicePool.Put(s)
	}
}

func GetFloatSlice(size int) []float64 {
	s := floatSlicePool.Get(size)
	if s != nil {
		return zeroFloatSlice(s, size)
	}

	return make([]float64, 0, size)
}

func PutFloatSlice(s []float64) {
	if s != nil {
		floatSlicePool.Put(s)
	}
}

func GetBoolSlice(size int) []bool {
	s := boolSlicePool.Get(size)

	if s != nil {
		return zeroBoolSlice(s, size)
	}

	return make([]bool, 0, size)
}

func PutBoolSlice(s []bool) {
	if s != nil {
		boolSlicePool.Put(s)
	}
}

func zeroFloatSlice(s []float64, size int) []float64 {
	s = s[:size]

	for i := range s {
		s[i] = 0
	}

	return s[:0]
}

func zeroBoolSlice(s []bool, size int) []bool {
	s = s[:size]

	for i := range s {
		s[i] = false
	}

	return s[:0]
}
