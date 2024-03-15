// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operator

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type InstantVectorSelector struct {
	Queryable     storage.Queryable
	Start         time.Time
	End           time.Time
	Interval      time.Duration
	LookbackDelta time.Duration
	Matchers      []*labels.Matcher

	querier storage.Querier
	// TODO: create separate type for linked list of SeriesBatches, use here and in RangeVectorSelectorWithTransformation
	currentSeriesBatch        *SeriesBatch
	seriesIndexInCurrentBatch int
	chunkIterator             chunkenc.Iterator
	memoizedIterator          *storage.MemoizedSeriesIterator

	// TODO: is it cheaper to just recompute these every time we need them rather than holding them?
	startTimestamp       int64
	endTimestamp         int64
	intervalMilliseconds int64
}

var _ InstantVectorOperator = &InstantVectorSelector{}

func (v *InstantVectorSelector) Series(ctx context.Context) ([]SeriesMetadata, error) {
	if v.currentSeriesBatch != nil {
		panic("should not call Series() multiple times")
	}

	v.startTimestamp = timestamp.FromTime(v.Start)
	v.endTimestamp = timestamp.FromTime(v.End)
	v.intervalMilliseconds = durationMilliseconds(v.Interval)

	start := v.startTimestamp - durationMilliseconds(v.LookbackDelta)

	hints := &storage.SelectHints{
		Start: start,
		End:   v.endTimestamp,
		Step:  v.intervalMilliseconds,
		// TODO: do we need to include other hints like Func, By, Grouping?
	}

	var err error
	v.querier, err = v.Queryable.Querier(start, v.endTimestamp)
	if err != nil {
		return nil, err
	}

	ss := v.querier.Select(ctx, true, hints, v.Matchers...)
	v.currentSeriesBatch = GetSeriesBatch()
	incompleteBatch := v.currentSeriesBatch
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
	batch := v.currentSeriesBatch
	for batch != nil {
		for _, s := range batch.series {
			metadata = append(metadata, SeriesMetadata{Labels: s.Labels()})
		}

		batch = batch.next
	}

	return metadata, ss.Err()
}

func (v *InstantVectorSelector) Next(ctx context.Context) (InstantVectorSeriesData, error) {
	if v.currentSeriesBatch == nil || len(v.currentSeriesBatch.series) == 0 {
		return InstantVectorSeriesData{}, EOS
	}

	if ctx.Err() != nil {
		return InstantVectorSeriesData{}, ctx.Err()
	}

	if v.memoizedIterator == nil {
		v.memoizedIterator = storage.NewMemoizedEmptyIterator(durationMilliseconds(v.LookbackDelta))
	}

	v.chunkIterator = v.currentSeriesBatch.series[v.seriesIndexInCurrentBatch].Iterator(v.chunkIterator)
	v.memoizedIterator.Reset(v.chunkIterator)
	v.seriesIndexInCurrentBatch++

	if v.seriesIndexInCurrentBatch == len(v.currentSeriesBatch.series) {
		b := v.currentSeriesBatch
		v.currentSeriesBatch = v.currentSeriesBatch.next
		PutSeriesBatch(b)
		v.seriesIndexInCurrentBatch = 0
	}

	numSteps := stepCount(v.startTimestamp, v.endTimestamp, v.intervalMilliseconds)
	data := InstantVectorSeriesData{
		Floats: GetFPointSlice(numSteps),
	}

	for ts := v.startTimestamp; ts <= v.endTimestamp; ts += v.intervalMilliseconds {
		var t int64
		var val float64
		var h *histogram.FloatHistogram

		valueType := v.memoizedIterator.Seek(ts)

		switch valueType {
		case chunkenc.ValNone:
			if v.memoizedIterator.Err() != nil {
				return InstantVectorSeriesData{}, v.memoizedIterator.Err()
			}
		case chunkenc.ValFloat:
			t, val = v.memoizedIterator.At()
		default:
			// TODO: handle native histograms
			return InstantVectorSeriesData{}, fmt.Errorf("unknown value type %s", valueType.String())
		}

		if valueType == chunkenc.ValNone || t > ts {
			var ok bool
			t, val, h, ok = v.memoizedIterator.PeekPrev()
			if h != nil {
				panic("don't support histograms")
			}
			if !ok || t < ts-durationMilliseconds(v.LookbackDelta) {
				continue
			}
		}
		if value.IsStaleNaN(val) || (h != nil && value.IsStaleNaN(h.Sum)) {
			continue
		}

		data.Floats = append(data.Floats, promql.FPoint{T: ts, F: val})
	}

	if v.memoizedIterator.Err() != nil {
		return InstantVectorSeriesData{}, v.memoizedIterator.Err()
	}

	return data, nil
}

func (v *InstantVectorSelector) Close() {
	for v.currentSeriesBatch != nil {
		b := v.currentSeriesBatch
		v.currentSeriesBatch = v.currentSeriesBatch.next
		PutSeriesBatch(b)
	}

	if v.querier != nil {
		_ = v.querier.Close()
		v.querier = nil
	}
}

func stepCount(start, end, interval int64) int {
	return int((end-start)/interval) + 1
}

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}

type SeriesBatch struct {
	series []storage.Series
	next   *SeriesBatch
}
