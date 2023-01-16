// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/matrix.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/util/modelutil"
)

func mergeChunks(chunks []chunk.Chunk, from, through model.Time) chunkenc.Iterator {
	samples := make([][]model.SamplePair, 0, len(chunks))
	histograms := make([][]mimirpb.Histogram, 0, len(chunks))
	for _, c := range chunks {
		sf, sh, err := c.Samples(from, through)
		if err != nil {
			return series.NewErrIterator(err)
		}
		if len(sf) > 0 {
			samples = append(samples, sf)
		}
		if len(sh) > 0 {
			histograms = append(histograms, sh)
		}
	}
	mergedSamples := modelutil.MergeNSampleSets(samples...)
	mergedHistograms := modelutil.MergeNHistogramSets(histograms...)

	return series.NewConcreteSeriesIterator(series.NewConcreteSeries(nil, mergedSamples, mergedHistograms))
}
