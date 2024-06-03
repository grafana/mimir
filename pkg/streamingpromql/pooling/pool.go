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

	seriesMetadataSlicePool = pool.NewBucketedPool(1, maxExpectedSeriesPerResult, seriesPerResultBucketFactor, func(size int) []types.SeriesMetadata {
		return make([]types.SeriesMetadata, 0, size)
	})
)

func GetMatrix(size int) promql.Matrix {
	return matrixPool.Get(size)
}

func PutMatrix(m promql.Matrix) {
	matrixPool.Put(m)
}

func GetSeriesMetadataSlice(size int) []types.SeriesMetadata {
	return seriesMetadataSlicePool.Get(size)
}

func PutSeriesMetadataSlice(s []types.SeriesMetadata) {
	seriesMetadataSlicePool.Put(s)
}
