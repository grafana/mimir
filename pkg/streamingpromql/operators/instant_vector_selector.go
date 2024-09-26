// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operators

import (
	"context"
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type InstantVectorSelector struct {
	Selector                 *Selector
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	chunkIterator    chunkenc.Iterator
	memoizedIterator *storage.MemoizedSeriesIterator
}

var _ types.InstantVectorOperator = &InstantVectorSelector{}

func (v *InstantVectorSelector) ExpressionPosition() posrange.PositionRange {
	return v.Selector.ExpressionPosition
}

func (v *InstantVectorSelector) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	return v.Selector.SeriesMetadata(ctx)
}

func (v *InstantVectorSelector) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if v.memoizedIterator == nil {
		v.memoizedIterator = storage.NewMemoizedEmptyIterator(v.Selector.LookbackDelta.Milliseconds())
	}

	var err error
	v.chunkIterator, err = v.Selector.Next(ctx, v.chunkIterator)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	v.memoizedIterator.Reset(v.chunkIterator)

	data := types.InstantVectorSeriesData{}

	// Keep track of the last histogram we saw.
	// This is important for a few reasons:
	// - it allows us to avoid unnecessarily creating FloatHistograms when the same histogram is used at multiple points
	//   due to lookback
	// - it allows consuming operators that mutate histograms to avoid making copies of FloatHistograms where possible,
	//   as they can check if the same FloatHistogram instance is used for multiple points, and then only make a copy
	//   if the histogram is used for multiple points
	lastHistogramT := int64(math.MinInt64)
	var lastHistogram *histogram.FloatHistogram

	for stepT := v.Selector.TimeRange.StartT; stepT <= v.Selector.TimeRange.EndT; stepT += v.Selector.TimeRange.IntervalMilliseconds {
		var t int64
		var f float64
		var h *histogram.FloatHistogram

		ts := stepT

		if v.Selector.Timestamp != nil {
			// Timestamp from @ modifier takes precedence over query evaluation timestamp.
			ts = *v.Selector.Timestamp
		}

		// Apply offset after adjusting for timestamp from @ modifier.
		ts = ts - v.Selector.Offset
		valueType := v.memoizedIterator.Seek(ts)

		switch valueType {
		case chunkenc.ValNone:
			if v.memoizedIterator.Err() != nil {
				return types.InstantVectorSeriesData{}, v.memoizedIterator.Err()
			}
		case chunkenc.ValFloat:
			t, f = v.memoizedIterator.At()
		case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
			if atT := v.memoizedIterator.AtT(); atT == lastHistogramT && lastHistogram != nil {
				// We're still looking at the last histogram we used, don't bother creating another FloatHistogram.
				// Consuming operators are expected to check for the same FloatHistogram instance used at multiple points and copy it
				// if they are going to mutate it, so this is safe to do.
				t, h = atT, lastHistogram
			} else {
				t, h = v.memoizedIterator.AtFloatHistogram()
			}

		default:
			return types.InstantVectorSeriesData{}, fmt.Errorf("streaming PromQL engine: unknown value type %s", valueType.String())
		}

		if valueType == chunkenc.ValNone || t > ts {
			var ok bool
			t, f, h, ok = v.memoizedIterator.PeekPrev()
			if !ok || t < ts-v.Selector.LookbackDelta.Milliseconds() {
				continue
			}
			if h != nil {
				if t == lastHistogramT && lastHistogram != nil {
					// Reuse exactly the same FloatHistogram as last time.
					// PeekPrev can return a new FloatHistogram instance with the same underlying bucket slices as a previous call
					// to AtFloatHistogram.
					// Consuming operators are expected to check for the same FloatHistogram instance used at multiple points and copy
					// it if they are going to mutate it, but consuming operators don't check the underlying bucket slices, so without
					// this, we can end up with incorrect query results.
					h = lastHistogram
				}
			}
		}
		if value.IsStaleNaN(f) || (h != nil && value.IsStaleNaN(h.Sum)) {
			continue
		}

		// if (f, h) have been set by PeekPrev, we do not know if f is 0 because that's the actual value, or because
		// the previous value had a histogram.
		// PeekPrev will set the histogram to nil, or the value to 0 if the other type exists.
		// So check if histograms is nil first. If we don't have a histogram, then we should have a value and vice-versa.
		if h != nil {
			if len(data.Histograms) == 0 {
				// Only create the slice once we know the series is a histogram or not.
				// (It is possible to over-allocate in the case where we have both floats and histograms, but that won't be common).
				var err error
				if data.Histograms, err = types.HPointSlicePool.Get(v.Selector.TimeRange.StepCount, v.MemoryConsumptionTracker); err != nil {
					return types.InstantVectorSeriesData{}, err
				}
			}
			data.Histograms = append(data.Histograms, promql.HPoint{T: stepT, H: h})
			lastHistogramT = t
			lastHistogram = h
		} else {
			if len(data.Floats) == 0 {
				// Only create the slice once we know the series is a histogram or not
				var err error
				if data.Floats, err = types.FPointSlicePool.Get(v.Selector.TimeRange.StepCount, v.MemoryConsumptionTracker); err != nil {
					return types.InstantVectorSeriesData{}, err
				}
			}
			data.Floats = append(data.Floats, promql.FPoint{T: stepT, F: f})
		}
	}

	if v.memoizedIterator.Err() != nil {
		return types.InstantVectorSeriesData{}, v.memoizedIterator.Err()
	}

	return data, nil
}

func (v *InstantVectorSelector) Close() {
	v.Selector.Close()
}
