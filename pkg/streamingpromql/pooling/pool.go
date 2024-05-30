// SPDX-License-Identifier: AGPL-3.0-only

package pooling

import (
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/pool"
)

const (
	maxExpectedSeriesPerResult  = 10_000_000 // Likewise, there's not too much science behind this number: this is the based on examining the largest queries seen at Grafana Labs.
	seriesPerResultBucketFactor = 2.0
)

var (
	matrixPool = pool.NewBucketedPool(1, maxExpectedSeriesPerResult, seriesPerResultBucketFactor, func(size int) promql.Matrix {
		return make(promql.Matrix, 0, size)
	})

	vectorPool = pool.NewBucketedPool(1, maxExpectedPointsPerSeries, pointsPerSeriesBucketFactor, func(size int) promql.Vector {
		return make(promql.Vector, 0, size)
	})

	seriesMetadataSlicePool = pool.NewBucketedPool(1, maxExpectedSeriesPerResult, seriesPerResultBucketFactor, func(size int) []types.SeriesMetadata {
		return make([]types.SeriesMetadata, 0, size)
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

func GetSeriesMetadataSlice(size int) []types.SeriesMetadata {
	return seriesMetadataSlicePool.Get(size)
}

func PutSeriesMetadataSlice(s []types.SeriesMetadata) {
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
