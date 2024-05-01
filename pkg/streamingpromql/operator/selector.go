// SPDX-License-Identifier: AGPL-3.0-only

package operator

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type Selector struct {
	Queryable storage.Queryable
	Start     int64 // Milliseconds since Unix epoch
	End       int64 // Milliseconds since Unix epoch
	Timestamp *int64
	Interval  int64 // In milliseconds
	Matchers  []*labels.Matcher

	// Set for instant vector selectors, otherwise 0.
	LookbackDelta time.Duration

	// Set for range vector selectors, otherwise 0.
	Range time.Duration

	querier storage.Querier
	series  *seriesList
}

func (s *Selector) SeriesMetadata(ctx context.Context) ([]SeriesMetadata, error) {
	if s.series != nil {
		return nil, errors.New("should not call Selector.SeriesMetadata() multiple times")
	}

	if s.LookbackDelta != 0 && s.Range != 0 {
		return nil, errors.New("invalid Selector configuration: both LookbackDelta and Range are non-zero")
	}

	startTimestamp := s.Start
	endTimestamp := s.End

	if s.Timestamp != nil {
		startTimestamp = *s.Timestamp
		endTimestamp = *s.Timestamp
	}

	rangeMilliseconds := s.Range.Milliseconds()
	start := startTimestamp - s.LookbackDelta.Milliseconds() - rangeMilliseconds

	hints := &storage.SelectHints{
		Start: start,
		End:   endTimestamp,
		Step:  s.Interval,
		Range: rangeMilliseconds,

		// Mimir doesn't use Grouping or By, so there's no need to include them here.
		//
		// Mimir does use Func to determine if it's a /series request, but this doesn't go
		// through the PromQL engine, so we don't need to include it here either.
		//
		// Mimir does use ShardCount, ShardIndex and DisableTrimming, but not at this level:
		// ShardCount and ShardIndex are set by ingesters and store-gateways when a sharding
		// label matcher is present, and ingesters set DisableTrimming to true.
	}

	var err error
	s.querier, err = s.Queryable.Querier(start, endTimestamp)
	if err != nil {
		return nil, err
	}

	ss := s.querier.Select(ctx, true, hints, s.Matchers...)
	s.series = newSeriesList()

	for ss.Next() {
		s.series.Add(ss.At())
	}

	return s.series.ToSeriesMetadata(), ss.Err()
}

func (s *Selector) Next(existing chunkenc.Iterator) (chunkenc.Iterator, error) {
	if s.series.Len() == 0 {
		return nil, EOS
	}

	return s.series.Pop().Iterator(existing), nil
}

func (s *Selector) Close() {
	if s.series != nil {
		s.series.Close()
	}

	if s.querier != nil {
		_ = s.querier.Close()
		s.querier = nil
	}
}

// seriesList is a FIFO queue of storage.Series.
//
// It is implemented as a linked list of slices of storage.Series, to allow O(1) insertion
// without the memory overhead of a linked list node per storage.Series.
type seriesList struct {
	currentSeriesBatch        *seriesBatch
	seriesIndexInCurrentBatch int

	lastSeriesBatch *seriesBatch

	length int
}

func newSeriesList() *seriesList {
	firstBatch := getSeriesBatch()

	return &seriesList{
		currentSeriesBatch: firstBatch,
		lastSeriesBatch:    firstBatch,
	}
}

// Add adds s to the end of this seriesList.
func (l *seriesList) Add(s storage.Series) {
	if len(l.lastSeriesBatch.series) == cap(l.lastSeriesBatch.series) {
		nextBatch := getSeriesBatch()
		l.lastSeriesBatch.next = nextBatch
		l.lastSeriesBatch = nextBatch
	}

	l.lastSeriesBatch.series = append(l.lastSeriesBatch.series, s)
	l.length++
}

// Len returns the total number of series ever added to this seriesList.
func (l *seriesList) Len() int {
	return l.length
}

// ToSeriesMetadata returns a SeriesMetadata value for each series added to this seriesList.
//
// Calling ToSeriesMetadata after calling Pop may return an incomplete list.
func (l *seriesList) ToSeriesMetadata() []SeriesMetadata {
	metadata := GetSeriesMetadataSlice(l.length)
	batch := l.currentSeriesBatch

	for batch != nil {
		for _, s := range batch.series {
			metadata = append(metadata, SeriesMetadata{Labels: s.Labels()})
		}

		batch = batch.next
	}

	return metadata
}

// Pop returns the next series from the head of this seriesList, and advances
// to the next item in this seriesList.
func (l *seriesList) Pop() storage.Series {
	if l.currentSeriesBatch == nil || len(l.currentSeriesBatch.series) == 0 {
		panic("no more series to pop")
	}

	s := l.currentSeriesBatch.series[l.seriesIndexInCurrentBatch]
	l.seriesIndexInCurrentBatch++

	if l.seriesIndexInCurrentBatch == len(l.currentSeriesBatch.series) {
		b := l.currentSeriesBatch
		l.currentSeriesBatch = l.currentSeriesBatch.next
		putSeriesBatch(b)
		l.seriesIndexInCurrentBatch = 0
	}

	return s
}

// Close releases resources associated with this seriesList.
func (l *seriesList) Close() {
	for l.currentSeriesBatch != nil {
		b := l.currentSeriesBatch
		l.currentSeriesBatch = l.currentSeriesBatch.next
		putSeriesBatch(b)
	}

	l.lastSeriesBatch = nil // Should have been put back in the pool as part of the loop above.
}

type seriesBatch struct {
	series []storage.Series
	next   *seriesBatch
}

// There's not too much science behind this number: this is based on the batch size used for chunks streaming.
const seriesBatchSize = 256

var seriesBatchPool = sync.Pool{New: func() any {
	return &seriesBatch{
		series: make([]storage.Series, 0, seriesBatchSize),
		next:   nil,
	}
}}

func getSeriesBatch() *seriesBatch {
	return seriesBatchPool.Get().(*seriesBatch)
}

func putSeriesBatch(b *seriesBatch) {
	b.series = b.series[:0]
	b.next = nil
	seriesBatchPool.Put(b)
}
