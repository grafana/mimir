// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/chunk_store_queryable.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/querier/batch"
	"github.com/grafana/mimir/pkg/storage/chunk"
	seriesset "github.com/grafana/mimir/pkg/storage/series"
)

// Series in the returned set are sorted alphabetically by labels.
func partitionChunks(chunks []chunk.Chunk) storage.SeriesSet {
	chunksBySeries := map[string][]chunk.Chunk{}
	var buf [1024]byte
	for _, c := range chunks {
		key := c.Metric.Bytes(buf[:0])
		chunksBySeries[string(key)] = append(chunksBySeries[string(key)], c)
	}

	series := make([]storage.Series, 0, len(chunksBySeries))
	for i := range chunksBySeries {
		series = append(series, &chunkSeries{
			labels: chunksBySeries[i][0].Metric,
			chunks: chunksBySeries[i],
		})
	}

	return seriesset.NewConcreteSeriesSetFromUnsortedSeries(series)
}

// Implements SeriesWithChunks
type chunkSeries struct {
	labels labels.Labels
	chunks []chunk.Chunk
}

func (s *chunkSeries) Labels() labels.Labels {
	return s.labels
}

// Iterator returns a new iterator of the data of the series.
func (s *chunkSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	return batch.NewChunkMergeIterator(it, s.labels, s.chunks)
}

// Chunks implements SeriesWithChunks interface.
func (s *chunkSeries) Chunks() []chunk.Chunk {
	return s.chunks
}
