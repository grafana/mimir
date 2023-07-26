// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/series"
)

type streamingChunkSeriesContext struct {
	chunkIteratorFunc chunkIteratorFunc
	mint, maxt        int64
	queryMetrics      *stats.QueryMetrics
	queryStats        *stats.Stats
}

// streamingChunkSeries is a storage.Series that reads chunks from sources in a streaming way. The chunks are read from
// each source's client.SeriesChunksStreamReader when the series' iterator is created. The stream reader only reads
// further chunks from its underlying gRPC stream when the current buffer is exhausted, limiting the total number of
// chunks in memory at a time.
type streamingChunkSeries struct {
	labels  labels.Labels
	sources []client.StreamingSeriesSource
	context *streamingChunkSeriesContext

	alreadyCreated bool
}

func (s *streamingChunkSeries) Labels() labels.Labels {
	return s.labels
}

func (s *streamingChunkSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	if s.alreadyCreated {
		return series.NewErrIterator(fmt.Errorf("can't create iterator multiple times for the one streaming series (%v)", s.labels.String()))
	}

	s.alreadyCreated = true

	var uniqueChunks []client.Chunk
	totalChunks := 0

	for _, source := range s.sources {
		c, err := source.StreamReader.GetChunks(source.SeriesIndex)

		if err != nil {
			return series.NewErrIterator(err)
		}

		totalChunks += len(c)
		uniqueChunks = client.AccumulateChunks(uniqueChunks, c)
	}

	s.context.queryMetrics.IngesterChunksTotal.Add(float64(totalChunks))
	s.context.queryMetrics.IngesterChunksDeduplicated.Add(float64(totalChunks - len(uniqueChunks)))

	s.context.queryStats.AddFetchedChunks(uint64(len(uniqueChunks)))

	chunkBytes := 0

	for _, c := range uniqueChunks {
		chunkBytes += c.Size()
	}

	s.context.queryStats.AddFetchedChunkBytes(uint64(chunkBytes))

	chunks, err := client.FromChunks(s.labels, uniqueChunks)
	if err != nil {
		return series.NewErrIterator(err)
	}

	return s.context.chunkIteratorFunc(it, chunks, model.Time(s.context.mint), model.Time(s.context.maxt))
}
