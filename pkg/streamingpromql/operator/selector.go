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

	querier                   storage.Querier
	currentSeriesBatch        *seriesBatch
	seriesIndexInCurrentBatch int
}

// There's not too much science behind this number: this is based on the batch size used for chunks streaming.
const seriesBatchSize = 256

var seriesBatchPool = sync.Pool{New: func() any {
	return &seriesBatch{
		series: make([]storage.Series, 0, seriesBatchSize),
		next:   nil,
	}
}}

func (s *Selector) SeriesMetadata(ctx context.Context) ([]SeriesMetadata, error) {
	if s.currentSeriesBatch != nil {
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
	s.currentSeriesBatch = getSeriesBatch()
	incompleteBatch := s.currentSeriesBatch
	totalSeries := 0

	for ss.Next() {
		if len(incompleteBatch.series) == cap(incompleteBatch.series) {
			nextBatch := getSeriesBatch()
			incompleteBatch.next = nextBatch
			incompleteBatch = nextBatch
		}

		incompleteBatch.series = append(incompleteBatch.series, ss.At())
		totalSeries++
	}

	metadata := GetSeriesMetadataSlice(totalSeries)
	batch := s.currentSeriesBatch
	for batch != nil {
		for _, s := range batch.series {
			metadata = append(metadata, SeriesMetadata{Labels: s.Labels()})
		}

		batch = batch.next
	}

	return metadata, ss.Err()
}

func (s *Selector) Next(existing chunkenc.Iterator) (chunkenc.Iterator, error) {
	if s.currentSeriesBatch == nil || len(s.currentSeriesBatch.series) == 0 {
		return nil, EOS
	}

	it := s.currentSeriesBatch.series[s.seriesIndexInCurrentBatch].Iterator(existing)
	s.seriesIndexInCurrentBatch++

	if s.seriesIndexInCurrentBatch == len(s.currentSeriesBatch.series) {
		b := s.currentSeriesBatch
		s.currentSeriesBatch = s.currentSeriesBatch.next
		putSeriesBatch(b)
		s.seriesIndexInCurrentBatch = 0
	}

	return it, nil
}

func (s *Selector) Close() {
	for s.currentSeriesBatch != nil {
		b := s.currentSeriesBatch
		s.currentSeriesBatch = s.currentSeriesBatch.next
		putSeriesBatch(b)
	}

	if s.querier != nil {
		_ = s.querier.Close()
		s.querier = nil
	}
}

type seriesBatch struct {
	series []storage.Series
	next   *seriesBatch
}

func getSeriesBatch() *seriesBatch {
	return seriesBatchPool.Get().(*seriesBatch)
}

func putSeriesBatch(b *seriesBatch) {
	b.series = b.series[:0]
	b.next = nil
	seriesBatchPool.Put(b)
}
