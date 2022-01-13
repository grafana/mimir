// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/matrix.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/chunk"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/util"
)

func mergeChunks(chunks []chunk.Chunk, from, through model.Time) chunkenc.Iterator {
	samples := make([][]model.SamplePair, 0, len(chunks))
	for _, c := range chunks {
		ss, err := c.Samples(from, through)
		if err != nil {
			return series.NewErrIterator(err)
		}

		samples = append(samples, ss)
	}

	merged := util.MergeNSampleSets(samples...)
	return series.NewConcreteSeriesIterator(series.NewConcreteSeries(nil, merged))
}
