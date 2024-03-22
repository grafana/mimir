// SPDX-License-Identifier: AGPL-3.0-only

package operator

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type Selector struct {
	Queryable storage.Queryable
	Start     time.Time
	End       time.Time
	Interval  time.Duration
	Matchers  []*labels.Matcher

	// Set for instant vector selectors, otherwise 0.
	LookbackDelta time.Duration

	// Set for range vector selectors, otherwise 0.
	Range time.Duration

	querier storage.Querier

	// TODO: create separate type for linked list of SeriesBatches
	currentSeriesBatch        *SeriesBatch
	seriesIndexInCurrentBatch int
}

// TODO: incorporate changes required for range vector selector
// - Need to incorporate s.Range
// - Range vector selector will need to drop metric name on returned metadata
func (s *Selector) Series(ctx context.Context) ([]SeriesMetadata, error) {
	if s.currentSeriesBatch != nil {
		return nil, errors.New("should not call Selector.Series() multiple times")
	}

	if s.LookbackDelta != 0 && s.Range != 0 {
		return nil, errors.New("invalid Selector configuration: both LookbackDelta and Range are non-zero")
	}

	startTimestamp := timestamp.FromTime(s.Start)
	endTimestamp := timestamp.FromTime(s.End)
	intervalMilliseconds := durationMilliseconds(s.Interval)

	start := startTimestamp - durationMilliseconds(s.LookbackDelta)

	hints := &storage.SelectHints{
		Start: start,
		End:   endTimestamp,
		Step:  intervalMilliseconds,
		// TODO: do we need to include other hints like Func, By, Grouping?
	}

	var err error
	s.querier, err = s.Queryable.Querier(start, endTimestamp)
	if err != nil {
		return nil, err
	}

	ss := s.querier.Select(ctx, true, hints, s.Matchers...)
	s.currentSeriesBatch = GetSeriesBatch()
	incompleteBatch := s.currentSeriesBatch
	totalSeries := 0

	for ss.Next() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if len(incompleteBatch.series) == cap(incompleteBatch.series) {
			nextBatch := GetSeriesBatch()
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
		PutSeriesBatch(b)
		s.seriesIndexInCurrentBatch = 0
	}

	return it, nil
}

func (s *Selector) Close() {
	for s.currentSeriesBatch != nil {
		b := s.currentSeriesBatch
		s.currentSeriesBatch = s.currentSeriesBatch.next
		PutSeriesBatch(b)
	}

	if s.querier != nil {
		_ = s.querier.Close()
		s.querier = nil
	}
}

type SeriesBatch struct {
	series []storage.Series
	next   *SeriesBatch
}
