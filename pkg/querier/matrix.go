// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/matrix.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/util/modelutil"
)

func mergeChunks(_ chunkenc.Iterator, chunks []chunk.Chunk, from, through model.Time) chunkenc.Iterator {
	var (
		samples          = make([][]model.SamplePair, 0, len(chunks))
		histograms       [][]mimirpb.Histogram
		mergedSamples    []model.SamplePair
		mergedHistograms []mimirpb.Histogram
	)
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
	if len(histograms) > 0 {
		mergedHistograms = modelutil.MergeNHistogramSets(histograms...)
	}
	if len(samples) > 0 {
		mergedSamples = modelutil.MergeNSampleSets(samples...)
	}

	return series.NewConcreteSeriesIterator(series.NewConcreteSeries(labels.EmptyLabels(), mergedSamples, mergedHistograms))
}
