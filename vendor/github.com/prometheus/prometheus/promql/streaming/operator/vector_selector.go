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

type VectorSelector struct {
	Queryable     storage.Queryable
	Start         time.Time
	End           time.Time
	Interval      time.Duration
	LookbackDelta time.Duration
	Matchers      []*labels.Matcher
	Pool          *Pool

	querier                 storage.Querier
	currentSeriesBatch      *SeriesBatch
	currentSeriesBatchIndex int
	chunkIterator           chunkenc.Iterator
	memoizedIterator        *storage.MemoizedSeriesIterator

	// TODO: is it cheaper to just recompute these every time we need them rather than holding them?
	startTimestamp       int64
	endTimestamp         int64
	intervalMilliseconds int64
}

var _ Operator = &VectorSelector{}

func (v *VectorSelector) Series(ctx context.Context) ([]SeriesMetadata, error) {
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
	v.querier, err = v.Queryable.Querier(ctx, start, v.endTimestamp)
	if err != nil {
		return nil, err
	}

	ss := v.querier.Select(true, hints, v.Matchers...)
	v.currentSeriesBatch = v.Pool.GetSeriesBatch()
	incompleteBatch := v.currentSeriesBatch
	totalSeries := 0

	for ss.Next() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if len(incompleteBatch.series) == cap(incompleteBatch.series) {
			nextBatch := v.Pool.GetSeriesBatch()
			incompleteBatch.next = nextBatch
			incompleteBatch = nextBatch
		}

		incompleteBatch.series = append(incompleteBatch.series, ss.At())
		totalSeries++
	}

	metadata := v.Pool.GetSeriesMetadataSlice(totalSeries)
	batch := v.currentSeriesBatch
	for batch != nil {
		for _, s := range batch.series {
			metadata = append(metadata, SeriesMetadata{Labels: s.Labels()})
		}

		batch = batch.next
	}

	return metadata, ss.Err()
}

func (v *VectorSelector) Next(ctx context.Context) (bool, SeriesData, error) {
	if v.currentSeriesBatch == nil || len(v.currentSeriesBatch.series) == 0 {
		return false, SeriesData{}, nil
	}

	if ctx.Err() != nil {
		return false, SeriesData{}, ctx.Err()
	}

	if v.memoizedIterator == nil {
		v.memoizedIterator = storage.NewMemoizedEmptyIterator(durationMilliseconds(v.LookbackDelta))
	}

	v.chunkIterator = v.currentSeriesBatch.series[v.currentSeriesBatchIndex].Iterator(v.chunkIterator)
	v.memoizedIterator.Reset(v.chunkIterator)
	v.currentSeriesBatchIndex++

	if v.currentSeriesBatchIndex == len(v.currentSeriesBatch.series) {
		b := v.currentSeriesBatch
		v.currentSeriesBatch = v.currentSeriesBatch.next
		v.Pool.PutSeriesBatch(b)
		v.currentSeriesBatchIndex = 0
	}

	numSteps := stepCount(v.startTimestamp, v.endTimestamp, v.intervalMilliseconds)
	data := SeriesData{
		Floats: v.Pool.GetFPointSlice(numSteps),
	}

	for ts := v.startTimestamp; ts <= v.endTimestamp; ts += v.intervalMilliseconds {
		// TODO: adjust time sought for offset

		var t int64
		var val float64
		var h *histogram.FloatHistogram

		valueType := v.memoizedIterator.Seek(ts)

		switch valueType {
		case chunkenc.ValNone:
			if v.memoizedIterator.Err() != nil {
				return false, SeriesData{}, v.memoizedIterator.Err()
			}
		case chunkenc.ValFloat:
			t, val = v.memoizedIterator.At()
		default:
			// TODO: handle native histograms
			return false, SeriesData{}, fmt.Errorf("unknown value type %s", valueType.String())
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
		return false, SeriesData{}, v.memoizedIterator.Err()
	}

	return true, data, nil
}

func (v *VectorSelector) Close() {
	for v.currentSeriesBatch != nil {
		b := v.currentSeriesBatch
		v.currentSeriesBatch = v.currentSeriesBatch.next
		v.Pool.PutSeriesBatch(b)
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
