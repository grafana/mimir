// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package search

import (
	"context"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
)

var _ prom_storage.ChunkSeries = &concreteChunksSeries{}

type concreteChunksSeries struct {
	lbls labels.Labels
	chks []chunks.Meta
}

func (c concreteChunksSeries) Labels() labels.Labels {
	return c.lbls
}

func (c concreteChunksSeries) Iterator(_ chunks.Iterator) chunks.Iterator {
	return prom_storage.NewListChunkSeriesIterator(c.chks...)
}

type iteratorChunksSeries struct {
	lbls labels.Labels
	chks chunks.Iterator
}

func (i *iteratorChunksSeries) Labels() labels.Labels {
	return i.lbls
}

func (i *iteratorChunksSeries) Iterator(_ chunks.Iterator) chunks.Iterator {
	return i.chks
}

func (i *iteratorChunksSeries) ChunkCount() (int, error) {
	return 0, nil
}


type ChunkSeriesSetCloser interface {
	prom_storage.ChunkSeriesSet

	// Close releases any memory buffers held by the ChunkSeriesSet or the
	// underlying ChunkSeries. It is not safe to use the ChunkSeriesSet
	// or any of its ChunkSeries after calling Close.
	Close()
}

type noChunksConcreteLabelsSeriesSet struct {
	seriesSet        []*concreteChunksSeries
	currentSeriesIdx int
}

func newNoChunksConcreteLabelsSeriesSet(sLbls []labels.Labels) *noChunksConcreteLabelsSeriesSet {
	seriesSet := make([]*concreteChunksSeries, len(sLbls))
	for i, lbls := range sLbls {
		seriesSet[i] = &concreteChunksSeries{lbls: lbls}
	}
	return &noChunksConcreteLabelsSeriesSet{
		seriesSet:        seriesSet,
		currentSeriesIdx: -1,
	}
}

func (s *noChunksConcreteLabelsSeriesSet) At() prom_storage.ChunkSeries {
	return s.seriesSet[s.currentSeriesIdx]
}

func (s *noChunksConcreteLabelsSeriesSet) Next() bool {
	if s.currentSeriesIdx+1 == len(s.seriesSet) {
		return false
	}
	s.currentSeriesIdx++
	return true
}

func (s *noChunksConcreteLabelsSeriesSet) Err() error {
	return nil
}

func (s *noChunksConcreteLabelsSeriesSet) Warnings() annotations.Annotations {
	return nil
}

func (s *noChunksConcreteLabelsSeriesSet) Close() {
}

// filterEmptyChunkSeriesSet is a ChunkSeriesSet that lazily filters out series with no chunks.
// It takes a set of materialized labels and a lazy iterator of chunks.Iterators;
// the labels and iterators are iterated in tandem to yield series with chunks.
// The materialized series callback is applied to each series when the iterator advances during Next().
type filterEmptyChunkSeriesSet struct {
	ctx     context.Context
	lblsSet []labels.Labels
	chnkSet ChunksIteratorIterator

	currentSeries              *iteratorChunksSeries
	materializedSeriesCallback MaterializedSeriesFunc
	err                        error
}

func newFilterEmptyChunkSeriesSet(
	ctx context.Context,
	lblsSet []labels.Labels,
	chnkSet ChunksIteratorIterator,
	materializeSeriesCallback MaterializedSeriesFunc,
) *filterEmptyChunkSeriesSet {
	return &filterEmptyChunkSeriesSet{
		ctx:                        ctx,
		lblsSet:                    lblsSet,
		chnkSet:                    chnkSet,
		materializedSeriesCallback: materializeSeriesCallback,
	}
}

func (s *filterEmptyChunkSeriesSet) At() prom_storage.ChunkSeries {
	return s.currentSeries
}

func (s *filterEmptyChunkSeriesSet) Next() bool {
	for s.chnkSet.Next() {
		if len(s.lblsSet) == 0 {
			s.err = errors.New("less labels than chunks, this should not happen")
			return false
		}
		lbls := s.lblsSet[0]
		s.lblsSet = s.lblsSet[1:]
		iter := s.chnkSet.At()
		if iter.Next() {
			// The series has chunks, keep it
			meta := iter.At()
			s.currentSeries = &iteratorChunksSeries{
				lbls: lbls,
				chks: &peekedChunksIterator{
					inner:       iter,
					peekedValue: &meta,
				},
			}
			s.err = s.materializedSeriesCallback(s.ctx, s.currentSeries)
			return s.err == nil
		}

		if iter.Err() != nil {
			s.err = iter.Err()
			return false
		}
		// This series has no chunks, skip it and continue to the next
	}
	if s.chnkSet.Err() != nil {
		s.err = s.chnkSet.Err()
		return false
	}

	if len(s.lblsSet) > 0 {
		s.err = errors.New("more labels than chunks, this should not happen")
	}
	return false
}

func (s *filterEmptyChunkSeriesSet) Err() error {
	if s.err != nil {
		return s.err
	}
	return s.chnkSet.Err()
}

func (s *filterEmptyChunkSeriesSet) Warnings() annotations.Annotations {
	return nil
}

func (s *filterEmptyChunkSeriesSet) Close() {
	s.chnkSet.Close()
}

// peekedChunksIterator is used to yield the first chunk of chunks.Iterator
// which has already had its Next() method called to check if it has any chunks.
// The already-consumed chunks.Meta is stored in peekedValue and returned on the first call to At().
// Subsequent calls to Next then continue with the inner chunks.Iterator.
type peekedChunksIterator struct {
	inner       chunks.Iterator
	peekedValue *chunks.Meta
	nextCalled  bool
}

func (p *peekedChunksIterator) Next() bool {
	if !p.nextCalled {
		p.nextCalled = true
		return true
	}
	if p.peekedValue != nil {
		// This is the second call to Next, discard the peeked value
		p.peekedValue = nil
	}
	return p.inner.Next()
}

func (p *peekedChunksIterator) At() chunks.Meta {
	if p.peekedValue != nil {
		return *p.peekedValue
	}
	return p.inner.At()
}

func (p *peekedChunksIterator) Err() error {
	return p.inner.Err()
}

type ChunksIteratorIterator interface {
	Next() bool
	At() chunks.Iterator
	Err() error
	Close()
}

// multiColumnChunksDecodingIterator yields a prometheus chunks.Iterator from multiple parquet Columns.
// The column iterators are called in order for each column and zipped together,
// yielding a single iterator that in turn can yield all chunks for the same row.
//
// The "column iterators" are pagesRowValueIterator which are expected to be:
// 1. in order of the columns in the parquet file
// 2. initialized with the same set of pages and row ranges
// An error is returned if the iterators have different lengths.
type multiColumnChunksDecodingIterator struct {
	mint int64
	maxt int64

	columnValueIterators []*columnValueIterator
	d                    *schema.PrometheusParquetChunksDecoder

	// current is a chunk-decoding iterator for the materialized parquet Values
	// combined by calling and zipping all column iterators in order
	current *valueDecodingChunkIterator
	err     error
}

func (c *multiColumnChunksDecodingIterator) At() chunks.Iterator {
	return c.current
}

func (c *multiColumnChunksDecodingIterator) Next() bool {
	if c.err != nil || len(c.columnValueIterators) == 0 {
		return false
	}

	multiColumnValues := make([]parquet.Value, 0, len(c.columnValueIterators))
	for _, columnValueIter := range c.columnValueIterators {
		if !columnValueIter.Next() {
			c.err = columnValueIter.Err()
			if c.err != nil {
				return false
			}
			continue
		}
		at := columnValueIter.At()
		multiColumnValues = append(multiColumnValues, at)
	}
	if len(multiColumnValues) == 0 {
		return false
	}

	c.current = &valueDecodingChunkIterator{
		mint:   c.mint,
		maxt:   c.maxt,
		values: multiColumnValues,
		d:      c.d,
	}
	return true
}

func (c *multiColumnChunksDecodingIterator) Err() error {
	return c.err
}

func (c *multiColumnChunksDecodingIterator) Close() {
}

// valueDecodingChunkIterator decodes and yields chunks from a parquet Values slice.
type valueDecodingChunkIterator struct {
	// TODO: why do we need these for decoding? can't we discard out of range chunks in advance?
	mint   int64
	maxt   int64
	values []parquet.Value
	d      *schema.PrometheusParquetChunksDecoder

	decoded []chunks.Meta
	current chunks.Meta
	err     error
}

func (c *valueDecodingChunkIterator) At() chunks.Meta {
	return c.current
}

func (c *valueDecodingChunkIterator) Next() bool {
	if c.err != nil {
		return false
	}
	if len(c.values) == 0 && len(c.decoded) == 0 {
		return false
	}
	if len(c.decoded) > 0 {
		c.current = c.decoded[0]
		c.decoded = c.decoded[1:]
		return true
	}
	value := c.values[0]
	c.values = c.values[1:]

	// TODO: we can do better at pooling here.
	c.decoded, c.err = c.d.Decode(value.ByteArray(), c.mint, c.maxt)
	return c.Next()
}

func (c *valueDecodingChunkIterator) Err() error {
	return c.err
}

func (c concreteChunksSeries) ChunkCount() (int, error) {
	return len(c.chks), nil
}

type columnValueIterator struct {
	currentIteratorIndex int
	rowRangesIterators   []*rowRangesValueIterator

	current parquet.Value
	err     error
}

func (ci *columnValueIterator) At() parquet.Value {
	return ci.current
}

func (ci *columnValueIterator) Next() bool {
	if ci.err != nil {
		return false
	}

	found := false
	for !found {
		if ci.currentIteratorIndex >= len(ci.rowRangesIterators) {
			return false
		}

		currentIterator := ci.rowRangesIterators[ci.currentIteratorIndex]
		hasNext := currentIterator.Next()

		if !hasNext {
			if err := currentIterator.Err(); err != nil {
				ci.err = err
				_ = currentIterator.Close()
				return false
			}
			// Iterator exhausted without error; close and move on to the next one
			_ = currentIterator.Close()
			ci.currentIteratorIndex++
			continue
		}
		ci.current = currentIterator.At()
		found = true
	}
	return found
}

func (ci *columnValueIterator) Err() error {
	return ci.err
}

// rowRangesValueIterator yields individual parquet Values from specified row ranges in its FilePages
type rowRangesValueIterator struct {
	pgs          *parquet.FilePages
	pageIterator *pageValueIterator

	remainingRr []RowRange
	currentRr   RowRange
	next        int64
	remaining   int64
	currentRow  int64

	buffer             []parquet.Value
	currentBufferIndex int
	err                error
}

func newRowRangesValueIterator(
	ctx context.Context,
	file *storage.ParquetFile,
	cc parquet.ColumnChunk,
	pageRange pageToReadWithRow,
	dictOff uint64,
	dictSz uint64,
) (*rowRangesValueIterator, error) {
	minOffset := uint64(pageRange.off)
	maxOffset := uint64(pageRange.off + pageRange.csz)

	// if dictOff == 0, it means that the collum is not dictionary encoded
	if dictOff > 0 && int(minOffset-(dictOff+dictSz)) < file.Cfg.PagePartitioningMaxGapSize {
		minOffset = dictOff
	}

	pgs, err := file.GetPages(ctx, cc, int64(minOffset), int64(maxOffset))
	if err != nil {
		if pgs != nil {
			_ = pgs.Close()
		}
		return nil, errors.Wrap(err, "failed to get pages")
	}

	err = pgs.SeekToRow(pageRange.rows[0].From)
	if err != nil {
		_ = pgs.Close()
		return nil, errors.Wrap(err, "failed to seek to row")
	}

	remainingRr := pageRange.rows

	currentRr := remainingRr[0]
	next := currentRr.From
	remaining := currentRr.Count
	currentRow := currentRr.From

	remainingRr = remainingRr[1:]
	return &rowRangesValueIterator{
		pgs:          pgs,
		pageIterator: new(pageValueIterator),

		remainingRr: remainingRr,
		currentRr:   currentRr,
		next:        next,
		remaining:   remaining,
		currentRow:  currentRow,
	}, nil
}

func (ri *rowRangesValueIterator) At() parquet.Value {
	return ri.buffer[ri.currentBufferIndex]
}

func (ri *rowRangesValueIterator) Next() bool {
	if ri.err != nil {
		return false
	}

	if len(ri.buffer) > 0 && ri.currentBufferIndex < len(ri.buffer)-1 {
		// Still have buffered values from previous page reads to yield
		ri.currentBufferIndex++
		return true
	}

	if len(ri.remainingRr) == 0 && ri.remaining == 0 {
		// Done; all rows of all pages have been read
		// and all buffered values from the row ranges have been yielded
		return false
	}

	// Read pages until we find values for the next row range.
	found := false
	for !found {
		// Prepare inner iterator
		page, err := ri.pgs.ReadPage()
		if err != nil {
			ri.err = errors.Wrap(err, "failed to read page")
			return false
		}
		ri.pageIterator.Reset(page)
		// Reset page values buffer
		ri.currentBufferIndex = 0
		ri.buffer = ri.buffer[:0]

		for ri.pageIterator.Next() {
			if ri.currentRow == ri.next {
				found = true
				ri.buffer = append(ri.buffer, ri.pageIterator.At())

				ri.remaining--
				if ri.remaining > 0 {
					ri.next = ri.next + 1
				} else if len(ri.remainingRr) > 0 {
					ri.currentRr = ri.remainingRr[0]
					ri.next = ri.currentRr.From
					ri.remaining = ri.currentRr.Count
					ri.remainingRr = ri.remainingRr[1:]
				}
			}
			ri.currentRow++
		}
		parquet.Release(page)
		if ri.pageIterator.Err() != nil {
			ri.err = errors.Wrap(ri.pageIterator.Err(), "failed to read page values")
			return false
		}
	}
	return found
}

func (ri *rowRangesValueIterator) Err() error {
	return ri.err
}

func (ri *rowRangesValueIterator) Close() error {
	return ri.pgs.Close()
}

// pageValueIterator yields individual parquet Values from its Page.
type pageValueIterator struct {
	p parquet.Page

	// TODO: consider using unique.Handle
	cachedSymbols map[int32]parquet.Value
	st            symbolTable

	vr parquet.ValueReader

	current            int
	buffer             []parquet.Value
	currentBufferIndex int
	err                error
}

func (pi *pageValueIterator) At() parquet.Value {
	if pi.vr == nil {
		dicIndex := pi.st.GetIndex(pi.current)
		// Cache a clone of the current symbol table entry.
		// This allows us to release the original page while avoiding unnecessary future clones.
		if _, ok := pi.cachedSymbols[dicIndex]; !ok {
			pi.cachedSymbols[dicIndex] = pi.st.Get(pi.current).Clone()
		}
		return pi.cachedSymbols[dicIndex]
	}
	// TODO: can we reduce the number of allocations caused by the clone here?
	return pi.buffer[pi.currentBufferIndex].Clone()
}

func (pi *pageValueIterator) Next() bool {
	if pi.err != nil {
		return false
	}

	pi.current++
	if pi.current >= int(pi.p.NumRows()) {
		return false
	}

	pi.currentBufferIndex++

	if pi.currentBufferIndex == len(pi.buffer) {
		n, err := pi.vr.ReadValues(pi.buffer[:cap(pi.buffer)])
		if err != nil && err != io.EOF {
			pi.err = err
		}
		pi.buffer = pi.buffer[:n]
		pi.currentBufferIndex = 0
	}

	return true
}

func (pi *pageValueIterator) Reset(p parquet.Page) {
	pi.p = p
	pi.vr = nil
	if p.Dictionary() != nil {
		pi.st.Reset(p)
		pi.cachedSymbols = make(map[int32]parquet.Value, p.Dictionary().Len())
	} else {
		pi.vr = p.Values()
		if pi.buffer != nil {
			pi.buffer = pi.buffer[:0]
		} else {
			pi.buffer = make([]parquet.Value, 0, 128)
		}
		pi.currentBufferIndex = -1
	}
	pi.current = -1
}

func (pi *pageValueIterator) Err() error {
	return pi.err
}
