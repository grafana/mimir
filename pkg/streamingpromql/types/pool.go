// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/util/pool"
)

const (
	// There's not too much science behind this number: this is the based on examining the largest queries seen at Grafana Labs.
	// The number must also align with a power of two for our pools.
	maxExpectedSeriesPerResult = 8_388_608
)

var (
	matrixPool = pool.NewBucketedPool(maxExpectedSeriesPerResult, func(size int) promql.Matrix {
		return make(promql.Matrix, 0, size)
	})

	seriesMetadataSlicePool = pool.NewBucketedPool(maxExpectedSeriesPerResult, func(size int) []SeriesMetadata {
		return make([]SeriesMetadata, 0, size)
	})
)

func GetMatrix(size int) promql.Matrix {
	return matrixPool.Get(size)
}

func PutMatrix(m promql.Matrix) {
	matrixPool.Put(m)
}

func GetSeriesMetadataSlice(size int) []SeriesMetadata {
	return seriesMetadataSlicePool.Get(size)
}

func PutSeriesMetadataSlice(s []SeriesMetadata) {
	seriesMetadataSlicePool.Put(s)
}
