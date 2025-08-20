package parquet

import (
	"github.com/efficientgo/core/errors"
	"github.com/prometheus-community/parquet-common/search"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
)

func newConcreteLabelsChunkSeriesSet(labels []labels.Labels, chunksIter search.ChunksIteratorIterator) prom_storage.ChunkSeriesSet {
	return &concreteLabelsChunkSeriesSet{
		labels:     labels,
		chunksIter: chunksIter,
	}
}

type concreteLabelsChunkSeriesSet struct {
	labels     []labels.Labels
	chunksIter search.ChunksIteratorIterator

	labelIdx      int
	currentSeries prom_storage.ChunkSeries
	err           error
}

func (s *concreteLabelsChunkSeriesSet) Next() bool {
	if !s.chunksIter.Next() {
		s.err = s.chunksIter.Err()
		return false
	}

	if s.labelIdx >= len(s.labels) {
		s.err = errors.Newf("chunk/label mismatch: got chunk but no corresponding label (labelIdx=%d, labels=%d)", s.labelIdx, len(s.labels))
		return false
	}

	lbls := s.labels[s.labelIdx]
	chunkIter := s.chunksIter.At()

	s.currentSeries = &concreteLabelsChunkSeries{
		labels:    lbls,
		chunkIter: chunkIter,
	}
	s.labelIdx++
	return true
}

func (s *concreteLabelsChunkSeriesSet) At() prom_storage.ChunkSeries {
	return s.currentSeries
}

func (s *concreteLabelsChunkSeriesSet) Err() error {
	return s.err
}

func (s *concreteLabelsChunkSeriesSet) Warnings() annotations.Annotations {
	return nil
}

func (s *concreteLabelsChunkSeriesSet) Close() error {
	return s.chunksIter.Close()
}

// concreteLabelsChunkSeries is a simple ChunkSeries that pairs labels with a chunk iterator
type concreteLabelsChunkSeries struct {
	labels    labels.Labels
	chunkIter chunks.Iterator
}

func (s *concreteLabelsChunkSeries) Labels() labels.Labels {
	return s.labels
}

func (s *concreteLabelsChunkSeries) Iterator(_ chunks.Iterator) chunks.Iterator {
	return s.chunkIter
}

func (s *concreteLabelsChunkSeries) ChunkCount() (int, error) {
	// We don't have a way to count chunks without consuming the iterator.
	// This is just to get things building anyway, we don't and should not use this.
	panic("ChunkCount not implemented for Parquet")
}
