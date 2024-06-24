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
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type InstantVectorSelector struct {
	Selector *Selector
	Pool     *pooling.LimitingPool

	numSteps int

	chunkIterator    chunkenc.Iterator
	memoizedIterator *storage.MemoizedSeriesIterator
}

var _ types.InstantVectorOperator = &InstantVectorSelector{}

func (v *InstantVectorSelector) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	// Compute value we need on every call to NextSeries() once, here.
	v.numSteps = stepCount(v.Selector.Start, v.Selector.End, v.Selector.Interval)

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

	lastHistogramT := int64(math.MinInt64)
	var lastHistogram *histogram.FloatHistogram

	for stepT := v.Selector.Start; stepT <= v.Selector.End; stepT += v.Selector.Interval {
		var t int64
		var f float64
		var h *histogram.FloatHistogram

		ts := stepT
		if v.Selector.Timestamp != nil {
			ts = *v.Selector.Timestamp
		}

		valueType := v.memoizedIterator.Seek(ts)

		switch valueType {
		case chunkenc.ValNone:
			if v.memoizedIterator.Err() != nil {
				return types.InstantVectorSeriesData{}, v.memoizedIterator.Err()
			}
		case chunkenc.ValFloat:
			t, f = v.memoizedIterator.At()
		case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
			if atT := v.memoizedIterator.AtT(); atT == lastHistogramT {
				// We're still looking at the last histogram we saw, don't bother creating another FloatHistogram.
				t, h = atT, lastHistogram
			} else {
				t, h = v.memoizedIterator.AtFloatHistogram()
				lastHistogramT = t
				lastHistogram = h
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
				if data.Histograms, err = v.Pool.GetHPointSlice(v.numSteps); err != nil {
					return types.InstantVectorSeriesData{}, err
				}
			}
			data.Histograms = append(data.Histograms, promql.HPoint{T: stepT, H: h})
		} else {
			if len(data.Floats) == 0 {
				// Only create the slice once we know the series is a histogram or not
				var err error
				if data.Floats, err = v.Pool.GetFPointSlice(v.numSteps); err != nil {
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
