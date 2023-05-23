// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/series"
)

type streamingChunkSeries struct {
	labels            labels.Labels
	chunkIteratorFunc chunkIteratorFunc
	mint, maxt        int64
	sources           []client.StreamingSeriesSource
	queryChunkMetrics *QueryChunkMetrics
}

func (s *streamingChunkSeries) Labels() labels.Labels {
	return s.labels
}

func (s *streamingChunkSeries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	var rawChunks []client.Chunk
	totalChunks := 0

	for _, source := range s.sources {
		c, err := source.StreamReader.GetChunks(source.SeriesIndex)

		if err != nil {
			return series.NewErrIterator(err)
		}

		totalChunks += len(c)
		rawChunks = client.AccumulateChunks(rawChunks, c)
	}

	s.queryChunkMetrics.IngesterChunksTotal.Add(float64(totalChunks))
	s.queryChunkMetrics.IngesterChunksDeduplicated.Add(float64(totalChunks - len(rawChunks)))

	chunks, err := client.FromChunks(s.labels, rawChunks)
	if err != nil {
		return series.NewErrIterator(err)
	}

	return s.chunkIteratorFunc(chunks, model.Time(s.mint), model.Time(s.maxt))
}
